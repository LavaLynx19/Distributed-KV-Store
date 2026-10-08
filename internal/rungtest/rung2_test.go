package rungtest_test

import (
	"testing"

	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
	"distributed-kv-store/internal/sim"
)

// Rung 2's two naive shortcuts, each on top of the Rung 1 store.
var (
	// memoryReads: a Member that believes it leads answers reads from its
	// own state.
	memoryReads = rungtest.Store{
		NewNode:  newRaft(raft.ReadsFromMemory),
		Workload: with(func(w *sim.Workload) { w.ReadsBypassLog = true }),
	}
	// retries: clients resend a request that got no definite answer, and the
	// store has no way to recognise the repeat.
	retries = rungtest.Store{
		NewNode:  newRaft(raft.ReadsThroughLog),
		Workload: with(func(w *sim.Workload) { w.Retry = true }),
	}
)

// withSessions is the fix for retries: the same retrying clients, each in a
// Session.
var withSessions = rungtest.Store{
	NewNode:  newRaft(raft.ReadsThroughLog),
	Workload: with(func(w *sim.Workload) { w.Retry, w.Sessions = true, true }),
}

// rung2Store is where Rung 2 ends: reads by read index, and retrying clients
// in Sessions.
var rung2Store = rungtest.Store{
	NewNode: newRaft(raft.ReadsByIndex),
	Workload: with(func(w *sim.Workload) {
		w.ReadsBypassLog, w.Retry, w.Sessions = true, true, true
	}),
}

func with(change func(*sim.Workload)) sim.Workload {
	w := sim.DefaultWorkload
	change(&w)
	return w
}

// With no Faults, or with a merely slow and lossy network, both shortcuts
// pass every verdict.
func TestShortcutsPassWithoutPartitionsOrCrashes(t *testing.T) {
	for _, store := range []rungtest.Store{memoryReads, retries} {
		for _, name := range []string{"none", "messy"} {
			for _, members := range []int{3, 5} {
				for seed := uint64(1); seed <= 10; seed++ {
					if r := rungtest.Run(store, scenario(t, name), members, seed); !r.Passed() {
						t.Errorf("%v", r)
					}
				}
			}
		}
	}
}

// Rung 2, "Exposed": the seeds recorded in retros/rung-2.md. In each, the
// History isn't Linearizable while Members end identical and no Term has two
// Leaders, so only the History shows the failure.
func TestShortcutsAreExposed(t *testing.T) {
	tests := []struct {
		name     string
		store    rungtest.Store
		scenario string
		members  int
		seed     uint64
	}{
		// Stale reads.
		{"memory reads", memoryReads, "crash-leader", 3, 14},
		{"memory reads", memoryReads, "isolate-leader", 3, 78},
		{"memory reads", memoryReads, "leader-deaf", 5, 13},
		// A request applied twice.
		{"retries", retries, "crash-leader", 3, 4},
		{"retries", retries, "isolate-leader", 3, 1},
		{"retries", retries, "leader-deaf", 3, 1},
	}
	for _, tt := range tests {
		r := rungtest.Run(tt.store, scenario(t, tt.scenario), tt.members, tt.seed)
		t.Logf("%s: %v", tt.name, r)
		if r.Linearizable {
			t.Errorf("%s, %s seed %d: expected a History that isn't Linearizable", tt.name, tt.scenario, tt.seed)
		}
		if len(r.Diverged) != 0 || r.TwoLeaders != "" {
			t.Errorf("%s, %s seed %d: Members should still agree, with one Leader per Term: %v", tt.name, tt.scenario, tt.seed, r)
		}
	}
}

func allScenarios() []rungtest.Scenario {
	return append(append([]rungtest.Scenario{}, rungtest.Rung1...), rungtest.Rung2...)
}

// suite runs store through every Rung 1 and Rung 2 scenario on 3 and 5
// Members and requires every verdict, and recovery after repair.
func suite(t *testing.T, store rungtest.Store) {
	t.Helper()
	seeds := uint64(200)
	if testing.Short() {
		seeds = 20
	}
	for _, sc := range allScenarios() {
		for _, members := range []int{3, 5} {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(store, sc, members, seed)
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

// With Sessions, clients may retry: a repeated request takes effect once.
func TestSessionsMakeRetriesSafe(t *testing.T) {
	suite(t, withSessions)
	for _, tt := range []struct {
		scenario string
		seed     uint64
	}{{"crash-leader", 4}, {"isolate-leader", 1}, {"leader-deaf", 1}} {
		if r := rungtest.Run(withSessions, scenario(t, tt.scenario), 3, tt.seed); !r.Passed() {
			t.Errorf("a seed that exposed retries still fails: %v", r)
		}
	}
}

// Rung 2's promise (README): no Stale read and exactly-once effect, under
// one-way Partitions and delayed, reordered or duplicated messages, as well
// as everything Rung 1 survives.
func TestRung2(t *testing.T) {
	suite(t, rung2Store)
	for _, tt := range []struct {
		scenario string
		members  int
		seed     uint64
	}{
		{"crash-leader", 3, 14}, {"isolate-leader", 3, 78}, {"leader-deaf", 5, 13}, // Stale reads
		{"crash-leader", 3, 4}, {"isolate-leader", 3, 1}, {"leader-deaf", 3, 1}, // double apply
	} {
		if r := rungtest.Run(rung2Store, scenario(t, tt.scenario), tt.members, tt.seed); !r.Passed() {
			t.Errorf("a seed that exposed a shortcut still fails: %v", r)
		}
	}
}
