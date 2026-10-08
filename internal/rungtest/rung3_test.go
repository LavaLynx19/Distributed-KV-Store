package rungtest_test

import (
	"strings"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
)

// forgetful is the Rung 2 store under Rung 3's crashes: it stores nothing,
// so a Member that restarts comes back empty, in Term 0, having forgotten
// its vote and its Log.
var forgetful = func() rungtest.Store {
	s := rung2Store
	fresh := newRaft(raft.ReadsByIndex)
	s.Restart = func(id core.NodeID, members []core.NodeID, rng core.Rand, _ core.Stored) core.Node {
		return fresh(id, members, rng)
	}
	return s
}()

// A single Member losing its memory is survivable by luck: the others still
// hold everything, and it catches up from them.
func TestForgetfulSurvivesOneCrash(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		if r := rungtest.Run(forgetful, scenario(t, "crash-leader"), 3, seed); !r.Passed() {
			t.Errorf("%v", r)
		}
	}
}

// Rung 3, "Exposed": the seeds recorded in retros/rung-3.md.
func TestForgettingIsExposed(t *testing.T) {
	// Every Member restarts at once: everything is gone.
	for _, members := range []int{3, 5} {
		r := rungtest.Run(forgetful, scenario(t, "full-restart"), members, 1)
		t.Log(r)
		if r.Linearizable {
			t.Errorf("full restart, %d Members: expected Acknowledged writes to be lost", members)
		}
	}
	// Members restart one at a time. A Majority that has forgotten can elect
	// a Leader without the Committed Entries, and the core's own check trips.
	r := rungtest.Run(forgetful, scenario(t, "rolling-crashes"), 5, 1)
	t.Log(r)
	if r.Passed() {
		t.Error("rolling crashes: expected a failure")
	}
	var tripped bool
	for seed := uint64(1); seed <= 20 && !tripped; seed++ {
		r := rungtest.Run(forgetful, scenario(t, "rolling-crashes"), 5, seed)
		tripped = strings.Contains(r.Panic, "asked to replace Committed Entry")
	}
	if !tripped {
		t.Error("rolling crashes: expected some run to try to replace a Committed Entry")
	}
}
