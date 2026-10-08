package rungtest_test

import (
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
	"distributed-kv-store/internal/sim"
)

func newRaft(reads raft.ReadMode) rungtest.NewNode {
	return func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node {
		return raft.New(raft.Config{ID: id, Members: members, ElectionTicks: 10, HeartbeatTicks: 1, Rand: rng, Reads: reads})
	}
}

// rung1Store is the store Rung 1 ended with: reads go through the Log and
// clients never retry.
var rung1Store = rungtest.Store{NewNode: newRaft(raft.ReadsThroughLog), Workload: sim.DefaultWorkload}

// Rung 1's promise (README): at most one Leader per Term, no Acknowledged
// write lost and Linearizable Histories, under crashes and clean Partitions,
// on 3 and 5 Members.
func TestRaftRung1(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 20
	}
	for _, sc := range rungtest.Rung1 {
		for _, members := range []int{3, 5} {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(rung1Store, sc, members, seed)
				if !r.Passed() {
					t.Errorf("%v\n  diverged: %v", r, r.Diverged)
				}
				// Safety alone is easy: do nothing. The Group must also work
				// again once the Faults are repaired.
				if r.Recovery < 0 {
					t.Errorf("%v\n  no write succeeded after the Faults were repaired", r)
				}
			}
		}
	}
}

// The seeds that exposed the naive store (TestNaiveIsExposed) pass with Raft.
func TestRaftPassesTheNaiveSeeds(t *testing.T) {
	for _, tt := range []struct {
		scenario string
		members  int
	}{{"isolate-leader", 3}, {"isolate-leader", 5}, {"crash-leader", 3}, {"random", 5}} {
		if r := rungtest.Run(rung1Store, scenario(t, tt.scenario), tt.members, 1); !r.Passed() {
			t.Errorf("%v\n  diverged: %v", r, r.Diverged)
		}
	}
}

// The Rung 1 store, with reads through the Log and clients that never retry,
// already holds its guarantees under Rung 2's Faults. Rung 2 is about the
// shortcuts that break them.
func TestRaftUnderRung2Faults(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 20
	}
	for _, sc := range rungtest.Rung2 {
		for _, members := range []int{3, 5} {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(rung1Store, sc, members, seed)
				if !r.Passed() {
					t.Errorf("%v\n  diverged: %v", r, r.Diverged)
				}
				if r.Recovery < 0 {
					t.Errorf("%v\n  no write succeeded after the Faults were repaired", r)
				}
			}
		}
	}
}
