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
	fresh := func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node {
		cfg := raftConfig(id, members, rng, raft.ReadsByIndex)
		cfg.Volatile = true
		return raft.New(cfg)
	}
	s.NewNode = fresh
	s.Restart = func(id core.NodeID, members []core.NodeID, rng core.Rand, _ core.Stored) core.Node {
		return fresh(id, members, rng)
	}
	return s
}()

// rung3Store keeps its Term, vote and Log on disk, and a Member that
// restarts is rebuilt from what its disk holds. Writes take 1 to 6 units to
// become durable, so crashes often land in the middle of one. It takes a
// Snapshot every 20 Entries, so Logs are always being trimmed and Members
// that fall behind are caught up by Snapshot.
var rung3Store = func() rungtest.Store {
	s := rung2Store
	s.DiskDelay = [2]int64{1, 6}
	s.SnapshotEvery = 20
	s.Restart = func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
		cfg := raftConfig(id, members, rng, raft.ReadsByIndex)
		cfg.Stored = stored
		return raft.New(cfg)
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

// stranding takes Snapshots and trims its Log, but a Leader has no way to
// send a Snapshot to a Member that needs what was trimmed.
var stranding = func() rungtest.Store {
	s := rung3Store
	s.SnapshotEvery = 20
	build := func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
		cfg := raftConfig(id, members, rng, raft.ReadsByIndex)
		cfg.Stored, cfg.NoSnapshotTransfer = stored, true
		return raft.New(cfg)
	}
	s.NewNode = func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node {
		return build(id, members, rng, core.Stored{})
	}
	s.Restart = build
	return s
}()

// Rung 3, "Exposed", part two: once the Log is trimmed, a Member that was
// away can't be brought up to date. The History stays Linearizable, since a
// Majority carries on without it. Only the End-state comparison notices.
func TestTrimmedLogStrandsAMember(t *testing.T) {
	for _, tt := range []struct {
		scenario string
		members  int
	}{{"isolate-leader", 3}, {"crash-leader", 5}, {"rolling-crashes", 3}} {
		r := rungtest.Run(stranding, scenario(t, tt.scenario), tt.members, 1)
		t.Log(r)
		if !r.Linearizable || r.Panic != "" || r.TwoLeaders != "" {
			t.Errorf("%s: the History should still be fine: %v", tt.scenario, r)
		}
		if len(r.Diverged) == 0 {
			t.Errorf("%s: expected a Member left behind with different data", tt.scenario)
		}
	}
	// With nobody falling behind there is nothing to strand.
	if r := rungtest.Run(stranding, scenario(t, "none"), 3, 1); !r.Passed() {
		t.Errorf("no Faults: %v", r)
	}
}

func everyScenario() []rungtest.Scenario {
	return append(allScenarios(), rungtest.Rung3...)
}

// Rung 3's promise (README): every Acknowledged write survives a full
// restart, a crash in the middle of a write and a stalled disk, on top of
// everything Rungs 1 and 2 survive.
func TestRung3(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 20
	}
	for _, sc := range everyScenario() {
		for _, members := range []int{3, 5} {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(rung3Store, sc, members, seed)
				if !r.Passed() {
					t.Errorf("%v\n  diverged: %v", r, r.Diverged)
				}
				if r.Recovery < 0 {
					t.Errorf("%v\n  no write succeeded after the Faults were repaired", r)
				}
			}
		}
	}
	for _, members := range []int{3, 5} {
		if r := rungtest.Run(rung3Store, scenario(t, "full-restart"), members, 1); !r.Passed() {
			t.Errorf("a seed that exposed forgetting still fails: %v", r)
		}
	}
	for _, tt := range []struct {
		scenario string
		members  int
	}{{"isolate-leader", 3}, {"crash-leader", 5}, {"rolling-crashes", 3}} {
		if r := rungtest.Run(rung3Store, scenario(t, tt.scenario), tt.members, 1); !r.Passed() {
			t.Errorf("a seed that stranded a Member still fails: %v", r)
		}
	}
}
