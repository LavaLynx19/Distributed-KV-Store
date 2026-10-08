package server_test

import (
	"context"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/transport"
)

// durable is a Group of real Members that keep their state in directories
// under dir, and can be stopped and started again.
type durable struct {
	t       *testing.T
	dir     string
	ids     []core.NodeID
	peers   map[core.NodeID]string
	clients map[core.NodeID]string
	stops   map[core.NodeID]func()
}

func newDurable(t *testing.T, n int) *durable {
	t.Helper()
	transport.Register(raft.MessageBodies()...)
	d := &durable{t: t, dir: t.TempDir(), peers: map[core.NodeID]string{}, clients: map[core.NodeID]string{}, stops: map[core.NodeID]func(){}}
	for i := 1; i <= n; i++ {
		id := core.NodeID(i)
		d.ids = append(d.ids, id)
		// Reserve two addresses per Member that it keeps across restarts.
		for _, addrs := range []map[core.NodeID]string{d.peers, d.clients} {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addrs[id] = ln.Addr().String()
			ln.Close()
		}
	}
	for _, id := range d.ids {
		d.start(id)
	}
	t.Cleanup(func() {
		for _, id := range d.ids {
			d.stop(id)
		}
	})
	return d
}

func (d *durable) start(id core.NodeID) {
	d.t.Helper()
	store, stored, err := storage.Open(filepath.Join(d.dir, "node"+strconv.Itoa(int(id))), 0)
	if err != nil {
		d.t.Fatal(err)
	}
	ln, err := net.Listen("tcp", d.peers[id])
	if err != nil {
		d.t.Fatal(err)
	}
	var node *server.Node
	tr := transport.New(id, ln, d.peers, func(m core.Message) { node.Deliver(m) })
	c := raft.New(raft.Config{ID: id, Members: d.ids, ElectionTicks: 10, HeartbeatTicks: 1,
		Rand: rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano()))), Reads: raft.ReadsByIndex, Stored: stored})
	node = server.NewNode(c, fsm.New(), tr.Send, 5*time.Millisecond)
	node.Storage = store
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { node.Run(ctx); close(done) }()

	hints := map[core.NodeID]string{}
	for m, addr := range d.clients {
		hints[m] = "http://" + addr
	}
	api := &server.API{Node: node, Clients: hints, Timeout: 2 * time.Second, ReadsBypassLog: true}
	cl, err := net.Listen("tcp", d.clients[id])
	if err != nil {
		d.t.Fatal(err)
	}
	srv := &httptest.Server{Listener: cl, Config: &http.Server{Handler: api.Handler()}}
	srv.Start()
	d.stops[id] = func() {
		srv.Close()
		cancel()
		<-done
		tr.Close()
		store.Close()
	}
}

func (d *durable) stop(id core.NodeID) {
	if stop := d.stops[id]; stop != nil {
		stop()
		delete(d.stops, id)
	}
}

// leader waits for a running Member to say it leads.
func (d *durable) leader() string {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id := range d.stops {
			url := "http://" + d.clients[id]
			resp, err := http.Get(url + "/v1/status")
			if err != nil {
				continue
			}
			resp.Body.Close()
			if a := call(d.t, "GET", url+"/v1/status", ""); a.body["role"] == "leader" {
				return url
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	d.t.Fatal("no Leader within 10s")
	return ""
}

// Every Acknowledged write survives all Members stopping and starting again.
func TestFullRestartKeepsAcknowledgedWrites(t *testing.T) {
	d := newDurable(t, 3)
	leader := d.leader()
	for i := range 20 {
		key := "k" + strconv.Itoa(i)
		if a := call(t, "PUT", leader+"/v1/kv/"+key, `{"value":"v`+strconv.Itoa(i)+`"}`); a.code != 200 {
			t.Fatalf("put %s: %+v", key, a)
		}
	}
	before := call(t, "GET", leader+"/v1/kv/k7", "")

	for _, id := range d.ids {
		d.stop(id)
	}
	for _, id := range d.ids {
		d.start(id)
	}
	leader = d.leader()
	for i := range 20 {
		key := "k" + strconv.Itoa(i)
		if a := call(t, "GET", leader+"/v1/kv/"+key, ""); a.code != 200 || a.body["value"] != "v"+strconv.Itoa(i) {
			t.Fatalf("after a full restart, %s: %+v", key, a)
		}
	}
	// Versions are Log positions, and the Log is the same Log.
	if after := call(t, "GET", leader+"/v1/kv/k7", ""); after.body["version"] != before.body["version"] {
		t.Fatalf("k7 was version %v before the restart and %v after", before.body["version"], after.body["version"])
	}
	// And the Group still takes writes.
	if a := call(t, "PUT", leader+"/v1/kv/new", `{"value":"x"}`); a.code != 200 {
		t.Fatalf("put after restart: %+v", a)
	}
}

// One Member stops, misses writes, and catches up from the Log when it
// returns.
func TestRestartedMemberCatchesUp(t *testing.T) {
	d := newDurable(t, 3)
	leader := d.leader()
	var down core.NodeID
	for _, id := range d.ids {
		if "http://"+d.clients[id] != leader {
			down = id
			break
		}
	}
	d.stop(down)
	for i := range 10 {
		if a := call(t, "PUT", leader+"/v1/kv/k"+strconv.Itoa(i), `{"value":"x"}`); a.code != 200 {
			t.Fatalf("put with one Member down: %+v", a)
		}
	}
	d.start(down)

	url := "http://" + d.clients[down] + "/v1/debug/items"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			var items []server.ItemJSON
			decode(t, resp, &items)
			if len(items) == 10 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the restarted Member didn't catch up within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
