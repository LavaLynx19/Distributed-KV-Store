package server

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/transport"
)

// StoreConfig describes one Node of a store with several Groups.
type StoreConfig struct {
	// Node is this Node's number, from 1. Nodes is how many Nodes the store
	// was founded with: with Groups and Replicas it says where every
	// Group's replicas were at first (shard.Hosts). Slots is how many Slots
	// the store has. Every Node must be given the same four numbers. A Node
	// numbered above Nodes hosts no Group to begin with: it is a Spare
	// (A§11.10).
	Node, Nodes, Groups, Replicas, Slots int
	// Peers and Clients are the addresses, for other Nodes and for clients,
	// of this Node and of the Nodes it is told of at start, as host:port. A
	// founder is told of every founder. A Node joining later needs only
	// itself and one other in Clients; gossip brings the rest.
	Peers, Clients map[int]string
	// Detector is how quiet Nodes are noticed, and GossipEvery the length
	// of a gossip round (default 10 ticks).
	Detector    gossip.Detector
	GossipEvery time.Duration
	// Listener accepts other Nodes' connections.
	Listener net.Listener
	// Data is where this Node keeps its replicas' durable state, one
	// directory per Group. Empty keeps nothing across a restart.
	Data string

	Tick                          time.Duration
	ElectionTicks, HeartbeatTicks int
	SnapshotEvery                 int
	SessionTTL, Timeout           time.Duration
	// SharedSync makes this Node's replicas share each flush of the drive
	// instead of each asking for its own (storage.SharedSyncFS). It helps
	// where a flush covers the whole drive and they queue, as on macOS.
	SharedSync bool
	// Auto makes the store replace dead Nodes with Spares and move Slots
	// off busy Groups by itself (A§11.11). DeadWait is how long a Node must
	// have been thought dead before this Node says so (default 5 s).
	Auto     bool
	DeadWait time.Duration
}

// OpenStore starts this Node's replicas and its Store, and runs them until
// ctx ends. The returned function waits for them to stop and closes what
// they held.
func OpenStore(ctx context.Context, cfg StoreConfig) (*Store, func(), error) {
	transport.Register(raft.MessageBodies()...)
	s := &Store{
		Node: cfg.Node, Nodes: cfg.Nodes, Groups: cfg.Groups, Replicas: cfg.Replicas, Slots: cfg.Slots,
		Local: map[shard.GroupID]*Node{}, Clients: map[int]string{}, Peers: map[int]string{},
		Timeout: cfg.Timeout, Tick: 2 * cfg.Tick, TimeEvery: 10 * cfg.Tick,
		GossipEvery: cfg.GossipEvery, Auto: cfg.Auto, DeadWait: cfg.DeadWait,
		handles: map[shard.GroupID]*handle{}, roles: map[shard.GroupID]string{},
	}
	if s.GossipEvery == 0 {
		s.GossipEvery = 10 * cfg.Tick
	}
	if s.DeadWait == 0 {
		s.DeadWait = 5 * time.Second
	}
	seeds := make([]gossip.Member, 0, len(cfg.Clients))
	for n, addr := range cfg.Clients {
		s.Clients[n] = "http://" + addr
		s.Peers[n] = cfg.Peers[n]
		seeds = append(seeds, gossip.Member{ID: n, Peer: cfg.Peers[n], Client: addr})
	}
	s.Gossip = gossip.New(gossip.Config{
		ID: cfg.Node, Peer: cfg.Peers[cfg.Node], Client: cfg.Clients[cfg.Node], Seeds: seeds,
		Rand:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(cfg.Node))),
		Detector: cfg.Detector,
	})

	// One network carries every Group's messages: a replica's id says which
	// Node it is on and which Group it belongs to. A Node the store wasn't
	// founded with is found by the address gossip brought (A§11.10).
	var tr *transport.Transport
	send := func(m core.Message) { tr.Send(m) }
	start := meta.NewPlaced(cfg.Slots, cfg.Groups, cfg.Nodes, cfg.Replicas).Table()
	var fs storage.FS = storage.OSFS{}
	if cfg.Data != "" {
		if err := os.MkdirAll(cfg.Data, 0o755); err != nil {
			return nil, nil, err
		}
	}
	if cfg.SharedSync && cfg.Data != "" {
		shared, err := storage.NewSharedSyncFS(filepath.Join(cfg.Data, "barrier"))
		if err != nil {
			return nil, nil, err
		}
		fs = shared
	}
	dir := func(g shard.GroupID) string { return filepath.Join(cfg.Data, fmt.Sprintf("group%d", g)) }

	// open builds this Node's replica of Group g, with members as the list
	// its core starts from, and starts it.
	s.open = func(g shard.GroupID, members []core.NodeID) error {
		var machine Machine
		if g == shard.Meta {
			machine = meta.NewPlaced(cfg.Slots, cfg.Groups, cfg.Nodes, cfg.Replicas)
		} else {
			var owned []shard.Slot
			for slot, o := range start.Slots {
				if o.Group == g {
					owned = append(owned, shard.Slot(slot))
				}
			}
			machine = shardfsm.New(shardfsm.Config{Group: g, Slots: cfg.Slots, Owned: owned, SessionTTL: cfg.SessionTTL.Milliseconds()})
		}
		h := &handle{done: make(chan struct{})}
		var stored core.Stored
		if cfg.Data != "" {
			var err error
			if h.disk, stored, err = storage.OpenWith(fs, dir(g), storage.Options{}); err != nil {
				return fmt.Errorf("Group %d: %w", g, err)
			}
			if stored.Damaged {
				log.Printf("node %d, Group %d: found damage and removed what it couldn't verify; Recovering", cfg.Node, g)
			}
		}
		id := core.NodeID(shard.ReplicaID(cfg.Node, g))
		c := raft.New(raft.Config{
			ID: id, Members: members,
			ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
			Rand:   rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id))),
			Reads:  raft.ReadsByIndex,
			Stored: stored,
		})
		h.node = NewNode(c, machine, send, cfg.Tick)
		h.node.SnapshotEvery = cfg.SnapshotEvery
		if h.disk != nil {
			h.node.Storage = h.disk
		}
		if snap := stored.Snapshot; snap != nil {
			if err := machine.Restore(snap.Data); err != nil {
				return fmt.Errorf("Group %d: restoring the Snapshot at Entry %d: %w", g, snap.Index, err)
			}
			h.node.Restored = snap.Index
		}
		run, cancel := context.WithCancel(ctx)
		h.cancel = cancel
		s.lmu.Lock()
		s.Local[g], s.handles[g] = h.node, h
		s.lmu.Unlock()
		go func() {
			h.node.Run(run)
			close(h.done)
		}()
		return nil
	}
	// close stops the replica of Group g and lets go of its disk.
	closeReplica := func(g shard.GroupID) {
		s.lmu.Lock()
		h := s.handles[g]
		delete(s.Local, g)
		delete(s.handles, g)
		s.lmu.Unlock()
		if h == nil {
			return
		}
		h.cancel()
		<-h.done
		if h.disk != nil {
			if err := h.disk.Close(); err != nil {
				log.Printf("node %d: closing Group %d's storage: %v", cfg.Node, g, err)
			}
		}
	}
	// A replica's role is kept beside its directory: "joined" for one that
	// didn't found its Group, "dropped" for one whose Node is no longer a
	// Member. With no Data it is kept in memory, like everything else.
	rolePath := func(g shard.GroupID) string { return dir(g) + ".role" }
	s.role = func(g shard.GroupID) string {
		if cfg.Data == "" {
			return s.roles[g]
		}
		raw, _ := os.ReadFile(rolePath(g))
		return string(raw)
	}
	s.setRole = func(g shard.GroupID, role string) error {
		if cfg.Data == "" {
			s.roles[g] = role
			return nil
		}
		return writeFileSync(rolePath(g), []byte(role))
	}
	s.drop = func(g shard.GroupID) error {
		// The mark goes down first: a crash from here on leaves a replica
		// this Node won't start again by itself.
		if err := s.setRole(g, "dropped"); err != nil {
			return err
		}
		closeReplica(g)
		if cfg.Data == "" {
			return nil
		}
		return storage.DropData(fs, dir(g), storage.Options{})
	}

	remote := map[core.NodeID]string{}
	for g := shard.GroupID(0); int(g) <= cfg.Groups; g++ {
		hosts := shard.Hosts(g, cfg.Nodes, cfg.Replicas)
		founder := slices.Contains(hosts, cfg.Node)
		var members []core.NodeID
		for _, n := range hosts {
			id := core.NodeID(shard.ReplicaID(n, g))
			// A replica that joined, or rejoined after being dropped, never
			// starts out believing it is a Member (A§11.11).
			if n != cfg.Node || s.role(g) == "" {
				members = append(members, id)
			}
			// A Node this one wasn't told the address of is found later,
			// by what gossip brings.
			if n != cfg.Node && cfg.Peers[n] != "" {
				remote[id] = cfg.Peers[n]
			}
		}
		if role := s.role(g); role == "dropped" || role == "" && !founder {
			continue
		}
		if err := s.open(g, members); err != nil {
			return nil, nil, err
		}
	}

	tr = transport.New(0, cfg.Listener, remote, func(m core.Message) {
		_, g := shard.SplitReplicaID(uint64(m.To))
		if n, ok := s.replica(g); ok {
			n.Deliver(m)
		}
	})
	tr.Resolve(func(id core.NodeID) string {
		n, _ := shard.SplitReplicaID(uint64(id))
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.Peers[n]
	})
	s.Start(ctx, cfg.Slots)
	stop := func() {
		<-ctx.Done()
		s.lmu.RLock()
		var groups []shard.GroupID
		for g := range s.handles {
			groups = append(groups, g)
		}
		s.lmu.RUnlock()
		for _, g := range groups {
			closeReplica(g)
		}
		tr.Close()
	}
	return s, stop, nil
}

// handle is one replica this Node is running.
type handle struct {
	node   *Node
	cancel context.CancelFunc
	done   chan struct{}
	disk   *storage.Store
}

// writeFileSync writes a small file so that it survives a crash.
func writeFileSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
