package rungtest_test

import (
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
)

// sharded builds a store of 5 Nodes with three data Groups and the Meta
// Group, each on 3 of the Nodes, and 8 Slots.
func sharded(tune func(*rungtest.ShardedStore)) rungtest.ShardedStore {
	s := rungtest.ShardedStore{
		NewCore: func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
			cfg := raftConfig(id, members, rng, raft.ReadsByIndex)
			cfg.Stored = stored
			return raft.New(cfg)
		},
		Workload: cluster.DefaultWorkload,
		Nodes:    5, Groups: 3, Replicas: 3, Slots: 8,
		DiskDelay:     rung3Store.DiskDelay,
		SnapshotEvery: rung3Store.SnapshotEvery,
		TearWrites:    true,
	}
	tune(&s)
	return s
}

func shardedScenario(t *testing.T, name string) rungtest.ShardedScenario {
	t.Helper()
	for _, sc := range rungtest.Rung7 {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return rungtest.ShardedScenario{}
}

// unchecked is a store whose Groups answer for any key they are asked
// about, trusting whoever sent the request to have sent it to the right
// place.
var unchecked = sharded(func(s *rungtest.ShardedStore) { s.Unchecked = true })

// flipping is a store where a Move changes the table at once and the Groups
// then follow the table.
var flipping = sharded(func(s *rungtest.ShardedStore) { s.FlipAtOnce = true })

// rung7Store is where stage 7a ends: each Group serves a Slot by its own
// Log, and a Move is confirmed by both Groups.
var rung7Store = sharded(func(*rungtest.ShardedStore) {})

// busy is rung7Store with clients that also give keys a time-to-live, run
// Transactions over two keys and scan, and with Sessions that are removed
// after 800 units unused.
var busy = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TTLPercent = 40
	s.Workload.TTL = [2]int64{100, 600}
	s.Workload.TxnPercent = 15
	s.Workload.ScanPercent = 10
	s.SessionTTL = 800
})

// Rung 7, "Exposed": a Group that answers for whatever it is asked. Once a
// Slot has moved, a Node with an old table still sends its keys to the Group
// that used to own it, and that Group answers from what it has left.
func TestUncheckedGroupIsExposed(t *testing.T) {
	if r := rungtest.RunSharded(unchecked, shardedScenario(t, "moves"), 1); r.Linearizable {
		t.Errorf("seed 1: expected a History that isn't Linearizable, got %v", r)
	}
	// Until something moves, nobody is ever sent to the wrong Group.
	for seed := uint64(1); seed <= 5; seed++ {
		if r := rungtest.RunSharded(unchecked, shardedScenario(t, "no-moves"), seed); !r.Passed() {
			t.Errorf("no Moves: %v %v", r, r.Diverged)
		}
	}
}

// Rung 7, "Exposed", part two: the Meta Group changes the table and the
// Groups follow it. Each checks that it owns what it serves, by its own
// Log, and still two of them serve one Slot at once: the new owner starts
// when it is given the Slot, and the old one stops only when it hears.
func TestFlippedTableIsExposed(t *testing.T) {
	if r := rungtest.RunSharded(flipping, shardedScenario(t, "moves"), 1); r.TwoOwners == "" {
		t.Errorf("seed 1: expected two Groups serving one Slot, got %v", r)
	}
	// And writes the old owner took after it sent the Slot are gone.
	if r := rungtest.RunSharded(flipping, shardedScenario(t, "moves-and-partitions"), 1); r.Linearizable {
		t.Errorf("moves-and-partitions seed 1: expected a History that isn't Linearizable, got %v", r)
	}
	for seed := uint64(1); seed <= 5; seed++ {
		if r := rungtest.RunSharded(flipping, shardedScenario(t, "no-moves"), seed); !r.Passed() {
			t.Errorf("no Moves: %v %v", r, r.Diverged)
		}
	}
}

// Stage 7a's promise (README): each key is owned by exactly one Group at
// any moment, and single-key operations stay Linearizable while keys move.
func TestRung7a(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 10
	}
	runs, moved := 0, 0
	for _, store := range []rungtest.ShardedStore{rung7Store, busy} {
		for _, sc := range rungtest.Rung7 {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.RunSharded(store, sc, seed)
				runs++
				moved += r.Moves
				if !r.Passed() || r.Recovery < 0 {
					t.Errorf("%v\n  end state: %v", r, r.Diverged)
				}
				if r.TimedOut {
					t.Errorf("the checker ran out of time: %v", r)
				}
			}
		}
	}
	t.Logf("%d runs, %d Moves finished", runs, moved)

	// Seeds that exposed the naive stores, or a bug on the way here.
	for _, tt := range []struct {
		store    rungtest.ShardedStore
		scenario string
		seed     uint64
	}{{rung7Store, "moves", 1}, {rung7Store, "moves-and-partitions", 1}, {rung7Store, "moves", 7}, {rung7Store, "moves-and-everything", 39}, {busy, "moves-and-crashes", 79}} {
		if r := rungtest.RunSharded(tt.store, shardedScenario(t, tt.scenario), tt.seed); !r.Passed() || r.Moves == 0 {
			t.Errorf("a seed that failed before still fails: %v %v", r, r.Diverged)
		}
	}
}
