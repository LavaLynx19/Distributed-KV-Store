// Command kvnode runs one Node: a Raft core in the real shell (A§4.3), with
// TCP to the other Members and the HTTP client API (A§7).
//
//	kvnode -id 1 \
//	  -peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//	  -clients 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/transport"
)

func main() {
	id := flag.Uint64("id", 0, "this Node's id (must appear in -peers and -clients)")
	peersFlag := flag.String("peers", "", "every Member's address for other Members: id=host:port,…")
	clientsFlag := flag.String("clients", "", "every Member's client API address: id=host:port,…")
	listenPeer := flag.String("listen-peer", "", "address to accept Members on (default: this Node's -peers entry)")
	listenClient := flag.String("listen-client", "", "address to serve clients on (default: this Node's -clients entry)")
	tick := flag.Duration("tick", 10*time.Millisecond, "length of one core tick")
	electionTicks := flag.Int("election-ticks", 10, "ticks of silence before an election (randomized up to 2×)")
	heartbeatTicks := flag.Int("heartbeat-ticks", 1, "ticks between a Leader's heartbeats")
	timeout := flag.Duration("request-timeout", 5*time.Second, "how long a client request waits to commit")
	data := flag.String("data", "", "directory for this Node's durable state; empty keeps nothing across a restart")
	reads := flag.String("reads", "index", "how gets are answered: index (read index, A§6.2) or log (as Log Entries)")
	flag.Parse()

	var mode raft.ReadMode
	switch *reads {
	case "index":
		mode = raft.ReadsByIndex
	case "log":
		mode = raft.ReadsThroughLog
	default:
		log.Fatalf("kvnode: -reads must be index or log, not %q", *reads)
	}
	if err := run(core.NodeID(*id), *peersFlag, *clientsFlag, *listenPeer, *listenClient, *tick, *electionTicks, *heartbeatTicks, *timeout, mode, *data); err != nil {
		log.Fatalf("kvnode: %v", err)
	}
}

func run(id core.NodeID, peersFlag, clientsFlag, listenPeer, listenClient string, tick time.Duration, electionTicks, heartbeatTicks int, timeout time.Duration, reads raft.ReadMode, data string) error {
	peers, err := parseAddrs(peersFlag)
	if err != nil {
		return fmt.Errorf("-peers: %w", err)
	}
	clients, err := parseAddrs(clientsFlag)
	if err != nil {
		return fmt.Errorf("-clients: %w", err)
	}
	if peers[id] == "" || clients[id] == "" {
		return fmt.Errorf("-id %d must appear in both -peers and -clients", id)
	}
	if listenPeer == "" {
		listenPeer = peers[id]
	}
	if listenClient == "" {
		listenClient = clients[id]
	}
	members := make([]core.NodeID, 0, len(peers))
	for m := range peers {
		members = append(members, m)
	}
	slices.Sort(members)

	var store *storage.Store
	var stored core.Stored
	if data != "" {
		if store, stored, err = storage.Open(data, 0); err != nil {
			return err
		}
		defer store.Close()
	}

	transport.Register(raft.MessageBodies()...)
	ln, err := net.Listen("tcp", listenPeer)
	if err != nil {
		return fmt.Errorf("listen for Members: %w", err)
	}

	var node *server.Node
	tr := transport.New(id, ln, peers, func(m core.Message) { node.Deliver(m) })
	defer tr.Close()
	c := raft.New(raft.Config{
		ID: id, Members: members,
		ElectionTicks: electionTicks, HeartbeatTicks: heartbeatTicks,
		Rand:   rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id))),
		Reads:  reads,
		Stored: stored,
	})
	node = server.NewNode(c, fsm.New(), tr.Send, tick)
	if store != nil {
		node.Storage = store
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go node.Run(ctx)

	// The hints in not_leader answers are URLs a client can use as they are.
	hints := map[core.NodeID]string{}
	for m, addr := range clients {
		hints[m] = "http://" + addr
	}
	api := &server.API{Node: node, Clients: hints, Timeout: timeout, ReadsBypassLog: reads != raft.ReadsThroughLog}
	srv := &http.Server{Addr: listenClient, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown) // exiting anyway
	}()

	log.Printf("kvnode %d: Members on %s, clients on %s, %d Members, tick %s, data %q (Term %d, %d Entries stored)",
		id, listenPeer, listenClient, len(members), tick, data, stored.HardState.Term, len(stored.Entries))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("client API: %w", err)
	}
	return nil
}

// parseAddrs reads "1=host:port,2=host:port".
func parseAddrs(s string) (map[core.NodeID]string, error) {
	addrs := map[core.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("%q is not id=host:port", part)
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%q: id must be a positive integer", part)
		}
		if _, dup := addrs[core.NodeID(id)]; dup {
			return nil, fmt.Errorf("id %d appears twice", id)
		}
		addrs[core.NodeID(id)] = addr
	}
	return addrs, nil
}
