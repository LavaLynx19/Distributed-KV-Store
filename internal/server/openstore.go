package server

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/transport"
)

// StoreConfig describes one Node of a store with several Groups.
type StoreConfig struct {
	// Node is this Node's number, from 1. Nodes, Groups and Replicas say
	// where every Group's replicas are (shard.Hosts), and Slots how many
	// Slots the store has. Every Node must be given the same four numbers.
	Node, Nodes, Groups, Replicas, Slots int
	// Peers is every Node's address for other Nodes, and Clients every
	// Node's client API address, both as host:port.
	Peers, Clients map[int]string
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
}

// OpenStore starts this Node's replicas and its Store, and runs them until
// ctx ends. The returned function waits for them to stop and closes what
// they held.
func OpenStore(ctx context.Context, cfg StoreConfig) (*Store, func(), error) {
	transport.Register(raft.MessageBodies()...)
	s := &Store{
		Node: cfg.Node, Nodes: cfg.Nodes, Groups: cfg.Groups, Replicas: cfg.Replicas,
		Local: map[shard.GroupID]*Node{}, Clients: map[int]string{},
		Timeout: cfg.Timeout, Tick: 2 * cfg.Tick, TimeEvery: 10 * cfg.Tick,
	}
	for n, addr := range cfg.Clients {
		s.Clients[n] = "http://" + addr
	}

	// One network carries every Group's messages: a replica's id says which
	// Node it is on and which Group it belongs to.
	remote := map[core.NodeID]string{}
	var tr *transport.Transport
	send := func(m core.Message) { tr.Send(m) }
	var stores []*storage.Store
	start := meta.New(cfg.Slots, cfg.Groups).Table()
	var fs storage.FS = storage.OSFS{}
	if cfg.SharedSync && cfg.Data != "" {
		if err := os.MkdirAll(cfg.Data, 0o755); err != nil {
			return nil, nil, err
		}
		shared, err := storage.NewSharedSyncFS(filepath.Join(cfg.Data, "barrier"))
		if err != nil {
			return nil, nil, err
		}
		fs = shared
	}

	for g := shard.GroupID(0); int(g) <= cfg.Groups; g++ {
		hosts := shard.Hosts(g, cfg.Nodes, cfg.Replicas)
		var members []core.NodeID
		hosted := false
		for _, n := range hosts {
			id := core.NodeID(shard.ReplicaID(n, g))
			members = append(members, id)
			if n == cfg.Node {
				hosted = true
			} else {
				remote[id] = cfg.Peers[n]
			}
		}
		if !hosted {
			continue
		}
		var machine Machine
		if g == shard.Meta {
			machine = meta.New(cfg.Slots, cfg.Groups)
		} else {
			var owned []shard.Slot
			for slot, o := range start.Slots {
				if o.Group == g {
					owned = append(owned, shard.Slot(slot))
				}
			}
			machine = shardfsm.New(shardfsm.Config{Group: g, Slots: cfg.Slots, Owned: owned, SessionTTL: cfg.SessionTTL.Milliseconds()})
		}
		var disk *storage.Store
		var stored core.Stored
		if cfg.Data != "" {
			var err error
			if disk, stored, err = storage.OpenWith(fs, filepath.Join(cfg.Data, fmt.Sprintf("group%d", g)), storage.Options{}); err != nil {
				return nil, nil, fmt.Errorf("Group %d: %w", g, err)
			}
			stores = append(stores, disk)
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
		n := NewNode(c, machine, send, cfg.Tick)
		n.SnapshotEvery = cfg.SnapshotEvery
		if disk != nil {
			n.Storage = disk
		}
		if snap := stored.Snapshot; snap != nil {
			if err := machine.Restore(snap.Data); err != nil {
				return nil, nil, fmt.Errorf("Group %d: restoring the Snapshot at Entry %d: %w", g, snap.Index, err)
			}
			n.Restored = snap.Index
		}
		s.Local[g] = n
	}

	tr = transport.New(0, cfg.Listener, remote, func(m core.Message) {
		_, g := shard.SplitReplicaID(uint64(m.To))
		if n, ok := s.Local[g]; ok {
			n.Deliver(m)
		}
	})
	done := make(chan struct{}, len(s.Local))
	for _, n := range s.Local {
		go func() {
			n.Run(ctx)
			done <- struct{}{}
		}()
	}
	s.Start(ctx, cfg.Slots)
	stop := func() {
		for range s.Local {
			<-done
		}
		tr.Close()
		for _, d := range stores {
			if err := d.Close(); err != nil {
				log.Printf("node %d: closing storage: %v", cfg.Node, err)
			}
		}
	}
	return s, stop, nil
}
