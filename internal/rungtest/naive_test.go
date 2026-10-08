package rungtest_test

import (
	"os"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/naive"
	"distributed-kv-store/internal/rungtest"
	"distributed-kv-store/internal/sim"
)

// naiveStore is Rung 1's primary-backup store with Rung 1's clients.
var naiveStore = rungtest.Store{
	NewNode: func(id core.NodeID, members []core.NodeID, _ core.Rand) core.Node {
		return naive.New(id, members, naive.DefaultTimeout)
	},
	Workload: sim.DefaultWorkload,
}

func scenario(t *testing.T, name string) rungtest.Scenario {
	t.Helper()
	for _, set := range [][]rungtest.Scenario{rungtest.Rung1, rungtest.Rung2} {
		for _, sc := range set {
			if sc.Name == name {
				return sc
			}
		}
	}
	t.Fatalf("no scenario %q", name)
	return rungtest.Scenario{}
}

// With no Faults the naive store passes every verdict. That's the trap.
func TestNaivePassesWithoutFaults(t *testing.T) {
	for _, members := range []int{3, 5} {
		for seed := uint64(1); seed <= 10; seed++ {
			if r := rungtest.Run(naiveStore, scenario(t, "none"), members, seed); !r.Passed() {
				t.Errorf("%v", r)
			}
		}
	}
}

// Rung 1, "Exposed" (README → Success Criteria): the seeds recorded in
// retros/rung-1.md. Each shows the naive store breaking a Rung 1 guarantee.
// Set KV_EVIDENCE to a directory to write each History as an HTML timeline.
func TestNaiveIsExposed(t *testing.T) {
	tests := []struct {
		scenario string
		members  int
		seed     uint64
		// what must go wrong
		twoLeaders, nonLinearizable, diverged bool
	}{
		// Split brain: the cut-off primary and a backup both act as primary.
		// Clients on each side get acknowledgements; one side's are lost.
		{"isolate-leader", 3, 1, true, true, true},
		{"isolate-leader", 5, 1, true, true, true},
		// A lost Acknowledged write with no Partition at all: the primary
		// crashes, a backup takes over and acknowledges writes, then the old
		// primary returns and serves its older data.
		{"crash-leader", 3, 1, true, true, false},
		{"random", 5, 1, true, true, true},
	}
	for _, tt := range tests {
		r := rungtest.Run(naiveStore, scenario(t, tt.scenario), tt.members, tt.seed)
		t.Log(r)
		if tt.twoLeaders && r.TwoLeaders == "" {
			t.Errorf("%s seed %d: expected two primaries at once", tt.scenario, tt.seed)
		}
		if tt.nonLinearizable && r.Linearizable {
			t.Errorf("%s seed %d: expected a History that isn't Linearizable", tt.scenario, tt.seed)
		}
		if tt.diverged && len(r.Diverged) == 0 {
			t.Errorf("%s seed %d: expected Members to end with different data", tt.scenario, tt.seed)
		}
		if dir := os.Getenv("KV_EVIDENCE"); dir != "" {
			name := "naive-" + tt.scenario + "-" + string(rune('0'+tt.members)) + ".html"
			if err := r.Visualize(filepath.Join(dir, name)); err != nil {
				t.Error(err)
			}
		}
	}
}
