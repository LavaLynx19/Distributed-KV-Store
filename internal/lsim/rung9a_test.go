package lsim

import (
	"os"
	"testing"

	"distributed-kv-store/internal/leaderless"
)

// lastWriteWins is the store Rung 9 starts from in one respect: of two
// writes of a key made apart, the one with the higher number replaces the
// other, whole set and all. Hints are delivered and writes sent again, so
// that this is all that is wrong with it.
var lastWriteWins = Store{Nodes: 5, W: 2, R: 2, Mode: leaderless.LastWriteWins, DeliverHints: true, Resend: true, Workload: DefaultWorkload}

// hintsKept is the store Rung 9 starts from in another: a Node that takes a
// write for a Home Node it can't reach keeps it and never hands it over.
var hintsKept = Store{Nodes: 5, W: 2, R: 2, Mode: leaderless.AddWins, Resend: true, Workload: DefaultWorkload}

// rung9a is where stage 9a ends, at a given W and R.
func rung9a(w, r int) Store {
	return Store{Nodes: 5, W: w, R: r, Mode: leaderless.AddWins, DeliverHints: true, Resend: true, Workload: DefaultWorkload}
}

// settings are the W and R the results are reported for (A§13.2).
var settings = [][2]int{{1, 1}, {2, 2}, {3, 1}}

func scenario(t *testing.T, name string) Scenario {
	t.Helper()
	for _, sc := range Scenarios {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return Scenario{}
}

// Rung 9, "Exposed": last-write-wins. Two clients add to the same key on
// the two sides of a Partition. Each is told its addition succeeded. When
// the sides meet, the set with the higher number replaces the other, and
// what the other side added is gone.
//
// It doesn't take a Partition. A write here reads the set, changes it and
// writes it back whole, so two clients adding to one key at the same moment
// do the same to each other on a healthy store.
func TestLastWriteWinsLosesAcknowledgedAdds(t *testing.T) {
	for _, c := range []struct {
		scenario string
		seeds    []uint64
	}{{"long-partition", []uint64{2, 4, 5}}, {"no-faults", []uint64{10, 13, 20}}} {
		for _, seed := range c.seeds {
			if r := Run(lastWriteWins, scenario(t, c.scenario), seed); len(r.Lost) == 0 {
				t.Errorf("%s, seed %d: expected an Acknowledged add to be lost, got %v", c.scenario, seed, r)
			}
			if r := Run(rung9a(2, 2), scenario(t, c.scenario), seed); len(r.Lost) > 0 || len(r.Back) > 0 {
				t.Errorf("add wins: %v", r)
			}
		}
	}
}

// Rung 9, "Exposed": Hints kept and never delivered. A write made while a
// Home Node was cut off is told to succeed on the strength of a Node that
// is standing in for it. If the stand-in never hands the write over, the
// Home Nodes never have it: it is in the store and no read will find it.
func TestHintsNeverDeliveredLeaveHomeNodesWithoutTheWrite(t *testing.T) {
	for _, seed := range []uint64{3, 12, 13} {
		r := Run(hintsKept, scenario(t, "long-partition"), seed)
		if r.HintsLeft == 0 || len(r.Lost) == 0 {
			t.Errorf("seed %d: expected Hints left over and an Acknowledged add no Home Node has, got %v", seed, r)
		}
		if r := Run(rung9a(2, 2), scenario(t, "long-partition"), seed); r.HintsLeft > 0 || len(r.Lost) > 0 {
			t.Errorf("Hints delivered: %v", r)
		}
	}
}

// Stage 9a: at every setting, under Partitions and crashes, no Acknowledged
// add is lost, nothing removed comes back, and no Hint is left. With no
// Faults the store is also Converged. Under Faults it isn't always: that is
// stage 9b.
func TestRung9a(t *testing.T) {
	runs := 0
	for _, wr := range settings {
		for _, sc := range Scenarios {
			for seed := uint64(1); seed <= 100; seed++ {
				r := Run(rung9a(wr[0], wr[1]), sc, seed)
				runs++
				if len(r.Lost) > 0 || len(r.Back) > 0 || r.HintsLeft > 0 || (sc.Name == "no-faults" && !r.Passed()) {
					t.Errorf("W=%d R=%d: %v", wr[0], wr[1], r)
				}
			}
		}
	}
	t.Logf("%d runs", runs)
}

// TestMeasureRung9a prints what stage 9a's retro reports. It runs only when
// asked for: KV_MEASURE=1.
func TestMeasureRung9a(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("set KV_MEASURE=1")
	}
	const seeds = 300
	type named struct {
		name  string
		store Store
	}
	stores := []named{{"last-write-wins W=2 R=2", lastWriteWins}, {"Hints never delivered W=2 R=2", hintsKept}}
	for _, wr := range settings {
		stores = append(stores, named{"add wins " + string(rune('0'+wr[0])) + "/" + string(rune('0'+wr[1])), rung9a(wr[0], wr[1])})
	}
	for _, st := range stores {
		for _, sc := range Scenarios {
			var lost, back, apart, hints, ans, req, cans, creq, stale, reads int
			for seed := uint64(1); seed <= seeds; seed++ {
				r := Run(st.store, sc, seed)
				if len(r.Lost) > 0 {
					lost++
				}
				if len(r.Back) > 0 {
					back++
				}
				if len(r.Apart) > 0 {
					apart++
				}
				if r.HintsLeft > 0 {
					hints++
				}
				ans, req, cans, creq, stale, reads = ans+r.Answered, req+r.Requests, cans+r.CutAnswered, creq+r.CutRequests, stale+r.Stale, reads+r.Reads
			}
			pct := func(a, b int) float64 {
				if b == 0 {
					return 100
				}
				return 100 * float64(a) / float64(b)
			}
			t.Logf("%-30s %-23s runs of %d with: adds lost %3d, back %3d, Hints left %3d, not Converged %3d | answered %5.1f%%, during a Partition %5.1f%% (%d requests), stale reads %.2f%%",
				st.name, sc.Name, seeds, lost, back, hints, apart, pct(ans, req), pct(cans, creq), creq, pct(stale, reads))
		}
	}
}
