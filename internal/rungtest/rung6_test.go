package rungtest_test

import (
	"testing"

	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
)

// changing builds a Rung 5 Store with two spare Nodes, whose Members are
// configured by tune.
func changing(tune func(*raft.Config)) rungtest.Store {
	s := timedFrom(durable(tune))
	s.Spares = 2
	return s
}

// swapping is the store Rung 6 starts from: asked for a new Member list, the
// Leader puts it in force in one step, whatever it is.
var swapping = changing(func(c *raft.Config) { c.SwapMembersAtOnce = true })

// rung6Store changes one Member at a time, under both rules of A§6.5, and
// brings a new Member up to date before it counts.
var rung6Store = changing(func(*raft.Config) {})

// countsAtOnce adds a Member with a single change, correctly, but the new
// Member counts toward the Majority from that moment, holding nothing.
var countsAtOnce = changing(func(c *raft.Config) { c.AddWithoutCatchUp = true })

// Rung 6, "Exposed": a Member list swapped in one step. While some Members
// hold the new list and some the old, each side can have a Majority of the
// list it knows.
func TestSwappingMembersIsExposed(t *testing.T) {
	// A founder and a newcomer are both elected in Term 2: one by the two
	// founders that never heard of the change, one by the three Nodes that
	// did.
	r := rungtest.Run(swapping, scenario(t, "change-in-a-partition"), 3, 11)
	if r.TwoLeaders == "" {
		t.Errorf("seed 11: expected two Leaders in one Term, got %v", r)
	}
	// Both sides take writes, and clients see answers no single order of
	// requests could give.
	if r := rungtest.Run(swapping, scenario(t, "change-in-a-partition"), 3, 1); r.Linearizable {
		t.Errorf("seed 1: expected a History that isn't Linearizable, got %v", r)
	}
	// With nothing else going wrong the swap looks fine.
	for seed := uint64(1); seed <= 10; seed++ {
		if r := rungtest.Run(swapping, scenario(t, "grow-and-shrink"), 3, seed); !r.Passed() || r.Changes == 0 {
			t.Errorf("no Faults: %v (%d changes)", r, r.Changes)
		}
	}
}

// Rung 6, "Exposed", part two: a Member that counts before it has caught
// up. Adding a dead Node to a Group of three makes it a Group of four that
// needs three, with one of them gone already. One more loss and it stops.
// A Member that must catch up first is never added, and the Group of three
// carries on.
func TestCountingBeforeCatchingUpIsExposed(t *testing.T) {
	const firstFault = 300 // rungtest's warm-up
	stopped := rungtest.Run(countsAtOnce, scenario(t, "add-the-dead"), 3, 1)
	carried := rungtest.Run(rung6Store, scenario(t, "add-the-dead"), 3, 1)
	if !stopped.Safe() || !carried.Passed() {
		t.Fatalf("neither run should be unsafe: %v / %v", stopped, carried)
	}
	if pause := stopped.Signals.LongestPauseAfter(firstFault); pause < 1500 {
		t.Errorf("counting at once: writes paused for %d at most, expected the Group to stop for about 2000", pause)
	}
	if pause := carried.Signals.LongestPauseAfter(firstFault); pause > 400 {
		t.Errorf("catching up first: writes paused for %d", pause)
	}
	if len(carried.Final) != 3 {
		t.Errorf("catching up first: the dead Node was added: %v", carried.Final)
	}
}

// rung6Scenarios is every earlier Fault plus the Membership changes.
func rung6Scenarios() []rungtest.Scenario {
	return append(rung5Scenarios(), rungtest.Rung6...)
}

// Rung 6's promise (README): never two Leaders during a change, including
// changes that straddle Terms. Every run has two spare Nodes, and the
// Members compared at the end are the ones the Group ends with.
func TestRung6(t *testing.T) {
	seeds := uint64(100)
	if testing.Short() {
		seeds = 10
	}
	runs, changes := 0, 0
	for _, sc := range rung6Scenarios() {
		for _, members := range []int{3, 4, 5} {
			if members == 4 && sc.Name != "straddle" {
				continue // only that scenario needs four founders
			}
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(rung6Store, sc, members, seed)
				runs++
				changes += r.Changes
				if !r.Safe() || (!r.Stalled && len(r.Diverged) > 0) {
					t.Errorf("%v\n  diverged: %v", r, r.Diverged)
				}
			}
		}
	}
	t.Logf("%d runs, %d Membership changes Committed", runs, changes)

	// The seeds that exposed the swap.
	for _, seed := range []uint64{11, 1} {
		if r := rungtest.Run(rung6Store, scenario(t, "change-in-a-partition"), 3, seed); !r.Passed() || r.Changes == 0 {
			t.Errorf("a seed that exposed the swap still fails: %v (%d changes)", r, r.Changes)
		}
	}
}
