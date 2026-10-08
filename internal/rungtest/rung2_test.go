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
