package lsim

import (
	"os"
	"slices"
	"testing"

	"distributed-kv-store/internal/leaderless"
)

// rung9b is where stage 9b ends, at a given W and R: Nodes exchange what
// they hold, and a read repairs the Home Nodes that had less.
func rung9b(w, r int) Store {
	s := rung9a(w, r)
	s.Exchange, s.Repair = true, true
	return s
}

// busy9b adds elements that expire, scans, and clocks that disagree.
func busy9b(w, r int) Store {
	s := rung9b(w, r)
	s.Workload.TTLPercent, s.Workload.TTL, s.Workload.ScanPercent = 30, [2]int64{100, 600}, 10
	s.Skew = map[leaderless.NodeID]int64{1: -80, 3: 120}
	return s
}

// Rung 9, "Exposed": no Exchange. A write is sent again for a while and a
// stand-in holds it for a Home Node that is away. After a Partition that
// outlasts the sending, with the stand-in on the far side too, nothing ever
// carries the write across, and the Home Nodes stay apart for good.
func TestWithoutTheExchangeHomeNodesStayApart(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		if r := Run(rung9a(2, 2), scenario(t, "long-partition"), seed); len(r.Apart) == 0 {
			t.Errorf("seed %d: expected Home Nodes left apart, got %v", seed, r)
		}
		if r := Run(rung9b(2, 2), scenario(t, "long-partition"), seed); !r.Passed() {
			t.Errorf("with the Exchange: %v", r)
		}
	}
}

// Rung 9, "Exposed": a remove forgotten at once. A Node that drops its
// record of a remove as soon as it has applied it can no longer tell an
// element it removed from one it has never heard of. The next Node to offer
// it that element, having missed the remove, is believed, and the element a
// client was told was removed is back. No Fault is needed: Home Nodes hear
// of things in different orders all the time.
func TestARemoveForgottenAtOnceLetsTheElementBack(t *testing.T) {
	forgetful := rung9b(2, 2)
	forgetful.Forgetful = true
	for _, seed := range []uint64{2, 4, 5} {
		if r := Run(forgetful, scenario(t, "no-faults"), seed); len(r.Back) == 0 {
			t.Errorf("seed %d: expected a removed element to be back, got %v", seed, r)
		}
		if r := Run(rung9b(2, 2), scenario(t, "no-faults"), seed); !r.Passed() {
			t.Errorf("with the record kept: %v", r)
		}
	}
}

// An element with a time-to-live stops showing when the clock of the Node
// asked passes its deadline. Nodes whose clocks disagree therefore
// disagree, for exactly as long as the clocks do, about whether it is
// there (A§13.8). This is Rung 5's naive store, kept on purpose.
func TestExpiryDisagreesByAsMuchAsTheClocks(t *testing.T) {
	skew := map[leaderless.NodeID]int64{1: -100, 3: 150}
	s := New(Config{Seed: 1, Nodes: 5, W: 3, R: 1, Mode: leaderless.AddWins, DeliverHints: true, Resend: true, Exchange: true, Skew: skew})
	added := false
	s.RequestTTL(2, leaderless.Add, "k", "x", 400, func(r leaderless.Reply) { added = r.OK })
	s.Run(100)
	if !added {
		t.Fatal("the add wasn't acknowledged")
	}
	gone := map[leaderless.NodeID]int64{}
	for at := int64(100); at < 900; at += 5 {
		s.At(at, func() {
			for _, id := range s.IDs() {
				s.Request(id, leaderless.Get, "k", "", func(r leaderless.Reply) {
					if _, done := gone[id]; r.OK && !done && !slices.Contains(r.Values, "x") {
						gone[id] = s.Now()
					}
				})
			}
		})
	}
	s.Run(1000)
	first, last := gone[3], gone[1]
	if len(gone) != 5 || last-first < 230 || last-first > 270 {
		t.Errorf("the element stopped showing at %v; the clocks are 250 apart", gone)
	}
	// What is stored is the same everywhere all the same.
	for _, home := range leaderless.NewRing(s.IDs()).Homes("k") {
		if got := s.Node(home).Holds("k").Values(); !slices.Equal(got, []string{"x"}) {
			t.Errorf("Home Node %d stores %v", home, got)
		}
	}
}

// Stage 9b: every run ends Converged, with no Acknowledged add lost and
// nothing back, at every setting and under every Fault here, including a
// Partition and an outage that outlast any sending again.
func TestRung9b(t *testing.T) {
	runs := 0
	stores := []Store{busy9b(2, 2)}
	for _, wr := range settings {
		stores = append(stores, rung9b(wr[0], wr[1]))
	}
	for _, store := range stores {
		for _, sc := range Scenarios {
			for seed := uint64(1); seed <= 100; seed++ {
				runs++
				if r := Run(store, sc, seed); !r.Passed() {
					t.Errorf("W=%d R=%d: %v", store.W, store.R, r)
				}
			}
		}
	}
	t.Logf("%d runs", runs)
}

// TestMeasureRung9b prints what stage 9b's retro reports. It runs only when
// asked for: KV_MEASURE=1.
func TestMeasureRung9b(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("set KV_MEASURE=1")
	}
	const seeds = 300
	type named struct {
		name  string
		store Store
	}
	forgetful := rung9b(2, 2)
	forgetful.Forgetful = true
	repairOnly := rung9a(1, 1)
	repairOnly.Repair = true
	exchangeOnly := rung9a(1, 1)
	exchangeOnly.Exchange = true
	stores := []named{
		{"no Exchange, no repair 1/1", rung9a(1, 1)}, {"repair only 1/1", repairOnly}, {"Exchange only 1/1", exchangeOnly},
		{"no Exchange 2/2", rung9a(2, 2)}, {"remove forgotten 2/2", forgetful},
		{"stage 9b 1/1", rung9b(1, 1)}, {"stage 9b 2/2", rung9b(2, 2)}, {"stage 9b 3/1", rung9b(3, 1)}, {"busy 2/2", busy9b(2, 2)},
	}
	for _, st := range stores {
		for _, sc := range Scenarios {
			var failed, lost, back, apart, stale, reads, missed, removes, scans int
			for seed := uint64(1); seed <= seeds; seed++ {
				r := Run(st.store, sc, seed)
				if !r.Passed() {
					failed++
				}
				if len(r.Lost) > 0 {
					lost++
				}
				if len(r.Back) > 0 {
					back++
				}
				if len(r.Apart) > 0 {
					apart++
				}
				stale, reads, missed, removes, scans = stale+r.Stale, reads+r.Reads, missed+r.Missed, removes+r.Removes, scans+r.Scans
			}
			t.Logf("%-27s %-23s runs of %d: failed %3d (not Converged %3d, add lost %3d, back %3d) | stale reads %.2f%% of %d, removes that missed %.2f%% of %d, scans %d",
				st.name, sc.Name, seeds, failed, apart, lost, back, 100*float64(stale)/float64(max(reads, 1)), reads, 100*float64(missed)/float64(max(removes, 1)), removes, scans)
		}
	}
}
