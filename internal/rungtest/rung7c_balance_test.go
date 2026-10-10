package rungtest_test

import (
	"fmt"
	"os"
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/rungtest"
	"distributed-kv-store/internal/shard"
)

// keysOf lists the workload's keys that fall in the Slots Group g starts
// with, in a store of 8 Slots and 3 Groups.
func keysOf(g shard.GroupID, keys int) []string {
	table := meta.New(8, 3).Table()
	var out []string
	for i := range keys {
		if key := fmt.Sprintf("k%d", i); table.OwnerOf(key) == g {
			out = append(out, key)
		}
	}
	return out
}

// balancing is the stage 7b store with load measured and evened out, and
// twenty clients working on 24 keys. With the five clients of the other
// stores a Group sees about four requests in each window its load is
// counted over, and the figures are mostly noise.
func balancing(tune func(*rungtest.ShardedStore)) rungtest.ShardedStore {
	return gossiping(cluster.GossipCounters, func(s *rungtest.ShardedStore) {
		s.Balancing.On, s.Balancing.Move = true, true
		s.Workload.Keys, s.Workload.Clients = 24, 20
		tune(s)
	})
}

// The loads the stores are put under.
var loads = []struct {
	name string
	tune func(*rungtest.ShardedStore)
}{
	// Every key as likely as any other.
	{"even", func(*rungtest.ShardedStore) {}},
	// Four requests in five go to the keys of the Slots Group 1 starts with.
	{"one-busy-group", func(s *rungtest.ShardedStore) {
		s.Workload.Skew.Percent, s.Workload.Skew.Sets = 80, [][]string{keysOf(1, 24)}
	}},
	// Four in five go to a single key: one Slot is busier than everything
	// else together, and no Move can change that.
	{"one-busy-slot", func(s *rungtest.ShardedStore) {
		s.Workload.Skew.Percent, s.Workload.Skew.Sets = 80, [][]string{{"k0"}}
	}},
	// The busy keys change every 1,200 units: Group 1's, then Group 2's,
	// then Group 3's, as the Slots were dealt at the start.
	{"shifting", func(s *rungtest.ShardedStore) {
		s.Workload.Skew.Percent, s.Workload.Skew.ShiftEvery = 80, 1200
		s.Workload.Skew.Sets = [][]string{keysOf(1, 24), keysOf(2, 24), keysOf(3, 24)}
	}},
}

// TestMeasureBalancing prints what rebalancing does, for the retro.
func TestMeasureBalancing(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("a measurement, not a check: set KV_MEASURE=1 to run it")
	}
	quiet := shardedScenario(t, "no-moves")
	for _, kind := range []struct {
		name string
		tune func(*rungtest.ShardedStore)
	}{
		{"off", func(s *rungtest.ShardedStore) { s.Balancing.Move = false }},
		{"naive", func(s *rungtest.ShardedStore) { s.Balancing.Naive = true }},
		{"damped", func(*rungtest.ShardedStore) {}},
		{"1.3/1.1", func(s *rungtest.ShardedStore) { s.Balancing.High, s.Balancing.Low = 1.3, 1.1 }},
	} {
		for _, load := range loads {
			store := balancing(func(s *rungtest.ShardedStore) { load.tune(s); kind.tune(s) })
			const seeds = 30
			fails, moves, late, answered := 0, 0, 0, 0
			var frozen int64
			var busiest float64
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.RunSharded(store, quiet, seed)
				if !r.Passed() {
					fails++
					if fails == 1 {
						t.Logf("%v %v", r, r.Diverged)
					}
				}
				moves += len(r.MovedAt)
				for _, at := range r.MovedAt {
					if at > 2300 {
						late++
					}
				}
				for _, p := range r.Pauses {
					frozen += p
				}
				busiest += r.Busiest
				answered += r.Signals.Answered
			}
			t.Logf("%-6s %-14s: %d/%d fail; per run %.1f Moves (%.1f after t=2300), %d units frozen, %d answered; busiest Group at %.2f of the mean",
				kind.name, load.name, fails, seeds, float64(moves)/seeds, float64(late)/seeds, frozen/seeds, answered/seeds, busiest/seeds)
		}
	}
}

// Stage 7c, "Exposed", part three: the Meta Leader moves the busiest Slot
// off the busiest Group whenever that Group is over the line, going by the
// last count it heard. One Slot here is busier than everything else put
// together, so whichever Group holds it is the busiest, and it is passed
// from Group to Group for as long as the run lasts. Under even load the
// counts wobble enough to do the same.
func TestNaiveRebalancingIsExposed(t *testing.T) {
	quiet := shardedScenario(t, "no-moves")
	for _, load := range []int{0, 2} {
		naive := balancing(func(s *rungtest.ShardedStore) { loads[load].tune(s); s.Balancing.Naive = true })
		damped := balancing(loads[load].tune)
		a, b := rungtest.RunSharded(naive, quiet, 1), rungtest.RunSharded(damped, quiet, 1)
		if len(a.MovedAt) < 50 || a.Signals.Answered > b.Signals.Answered*9/10 {
			t.Errorf("%s load, seed 1: expected Slots moved back and forth at the clients' cost; got %d Moves and %d requests answered, against %d and %d damped",
				loads[load].name, len(a.MovedAt), a.Signals.Answered, len(b.MovedAt), b.Signals.Answered)
		}
		if !a.Passed() {
			t.Errorf("%s load, seed 1: churn must cost time and nothing else: %v %v", loads[load].name, a, a.Diverged)
		}
	}
}

// Stage 7c's promise for load (A§11.11). A Group that carries far more than
// its share is relieved, in a handful of Moves. Load that is even to begin
// with, or that no Move can even out, is left alone after the first few.
// And with Nodes being lost, cut off and replaced at the same time, every
// earlier guarantee still holds.
func TestRung7cBalancing(t *testing.T) {
	seeds := uint64(50)
	if testing.Short() {
		seeds = 5
	}
	quiet := shardedScenario(t, "no-moves")
	for _, load := range loads {
		store := balancing(load.tune)
		var busiest float64
		moves, late := 0, 0
		for seed := uint64(1); seed <= seeds; seed++ {
			r := rungtest.RunSharded(store, quiet, seed)
			if !r.Passed() || r.Recovery < 0 {
				t.Errorf("%s: %v\n  end state: %v", load.name, r, r.Diverged)
			}
			if len(r.MovedAt) > 12 {
				t.Errorf("%s: %v: %d Moves", load.name, r, len(r.MovedAt))
			}
			busiest += r.Busiest
			moves += len(r.MovedAt)
			for _, at := range r.MovedAt {
				if at > 2300 {
					late++
				}
			}
		}
		mean := busiest / float64(seeds)
		t.Logf("%s: busiest Group at %.2f of the mean; per run %.1f Moves, %.1f of them after t=2300", load.name, mean, float64(moves)/float64(seeds), float64(late)/float64(seeds))
		switch load.name {
		case "one-busy-group":
			if mean > 1.3 {
				t.Errorf("%s: the busiest Group ends at %.2f of the mean", load.name, mean)
			}
		case "even", "one-busy-slot":
			if float64(late) > 0.5*float64(seeds) {
				t.Errorf("%s: %d Moves after t=2300 in %d runs: it should have settled", load.name, late, seeds)
			}
		}
	}

	// Everything at once: load that shifts, Slots moved by hand, Nodes
	// crashing, cut off, lost for good and replaced.
	all := balancing(func(s *rungtest.ShardedStore) {
		loads[3].tune(s)
		s.Spares = 2
		s.Replacing.On = true
	})
	runs := 0
	for _, sc := range append(append(rungtest.Rung7, rungtest.Rung7b...), rungtest.Rung7c...) {
		for seed := uint64(1); seed <= seeds; seed++ {
			r := rungtest.RunSharded(all, sc, seed)
			runs++
			if !r.Passed() || r.Recovery < 0 {
				t.Errorf("%v\n  end state: %v", r, r.Diverged)
			}
		}
	}
	t.Logf("%d runs with everything on", runs)
}
