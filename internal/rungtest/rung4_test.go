package rungtest_test

import (
	"strings"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/rungtest"
)

// trusting is the Rung 3 store on a disk that lies: its files have no
// checksums, so it believes whatever it reads back.
var trusting = func() rungtest.Store {
	s := rung3Store
	s.UncheckedDisk = true
	return s
}()

// trustingTorn is the same store when a crash can leave part of a write in
// progress on disk.
var trustingTorn = func() rungtest.Store {
	s := trusting
	s.TearWrites = true
	return s
}()

// durable builds a Rung 3-and-later Store whose Members are configured by
// tune, on first start and on every restart.
func durable(tune func(*raft.Config)) rungtest.Store {
	s := rung3Store
	build := func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
		cfg := raftConfig(id, members, rng, raft.ReadsByIndex)
		cfg.Stored = stored
		tune(&cfg)
		return raft.New(cfg)
	}
	s.NewNode = func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node {
		return build(id, members, rng, core.Stored{})
	}
	s.Restart = build
	s.TearWrites = true
	return s
}

// careless checks its disk and repairs what it finds damaged, then carries
// on as if nothing had happened: a Member that has lost Entries it
// acknowledged still votes and stands for election.
var careless = durable(func(c *raft.Config) { c.RepairWithoutAbstaining = true })

// rung4Store is where Rung 4 ends: checksums, repair, and a Member that
// found damage stays out of elections until it has caught up.
var rung4Store = durable(func(*raft.Config) {})

// phantomKeys lists keys in the differences that no client ever wrote.
// Clients only use k0, k1 and k2.
func phantomKeys(diverged []string) []string {
	var found []string
	for _, d := range diverged {
		if !strings.Contains(d, `key "k0"`) && !strings.Contains(d, `key "k1"`) && !strings.Contains(d, `key "k2"`) {
			found = append(found, d)
		}
	}
	return found
}

// Rung 4, "Exposed": the seeds recorded in retros/rung-4.md.
func TestLyingDiskIsExposed(t *testing.T) {
	// One flipped bit, replayed as valid: a Member ends up holding a key no
	// client ever wrote. The History is Linearizable and nothing crashes.
	r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 87)
	t.Log(r, r.Diverged)
	if !r.Safe() || len(r.Unreadable) != 0 || len(phantomKeys(r.Diverged)) == 0 {
		t.Errorf("seed 87: expected a Member silently holding a key nobody wrote, got %v %v", r, r.Diverged)
	}
	// A flipped bit that reaches clients.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 7); r.Linearizable {
		t.Errorf("seed 7: expected a History that isn't Linearizable, got %v", r)
	}
	// One that makes the core stop on an impossible state.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 9); r.Panic == "" {
		t.Errorf("seed 9: expected the core's own check to trip, got %v", r)
	}
	// And one that leaves a Member unable to read its own disk.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 1); len(r.Unreadable) == 0 {
		t.Errorf("seed 1: expected a Member that can't restart, got %v", r)
	}
	// A torn write does the same without any bit being flipped.
	if r := rungtest.Run(trustingTorn, scenario(t, "rolling-crashes"), 3, 2); len(r.Unreadable) == 0 {
		t.Errorf("torn write, seed 2: expected a Member that can't restart, got %v", r)
	}
}

// Rung 4, "Exposed", part two: checksums catch the damage and the Member
// drops what it can't verify. If it then carries on as a full Member, it
// votes with a Log that is missing Entries it acknowledged, and a Leader can
// be elected without something that was Committed.
func TestCarelessRepairIsExposed(t *testing.T) {
	if r := rungtest.Run(careless, scenario(t, "rot-and-everything"), 3, 6); r.Linearizable {
		t.Errorf("seed 6: expected a History that isn't Linearizable, got %v", r)
	}
	// Here a follower has Committed further than its Leader's Log reaches,
	// and the Leader's core stops on an Index it doesn't hold.
	if r := rungtest.Run(careless, scenario(t, "rot-and-everything"), 3, 27); r.Panic == "" {
		t.Errorf("seed 27: expected the core's own check to trip, got %v", r)
	}
	// One Member damaged at a time, with the others healthy, is repaired
	// before it matters. The careless store usually gets away with it.
	passed := 0
	for seed := uint64(1); seed <= 20; seed++ {
		if rungtest.Run(careless, scenario(t, "bit-flips"), 5, seed).Passed() {
			passed++
		}
	}
	if passed < 18 {
		t.Errorf("careless repair passed only %d of 20 runs with occasional flips; it should look fine", passed)
	}
}

// Rung 4's promise (README): corruption is always detected and never
// applied, and Members stay identical.
//
// Every run must be Safe. A run may end Stalled: with most Members damaged at
// once, too few are fit to vote, and the Group stops on purpose (A§6.8). A
// run that isn't Stalled must also converge and work again. A Member whose
// two copies of its Term and vote are both damaged stays down, by design.
func TestRung4(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 15
	}
	runs, stalled, down := 0, 0, 0
	for _, sc := range append(everyScenario(), rungtest.Rung4...) {
		for _, members := range []int{3, 5} {
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.Run(rung4Store, sc, members, seed)
				runs++
				if !r.Safe() {
					t.Errorf("%v", r)
				}
				if len(r.Unreadable) > 0 {
					down++
				}
				if r.Stalled {
					stalled++
					continue
				}
				if len(r.Diverged) > 0 || r.Recovery < 0 {
					t.Errorf("%v\n  diverged: %v", r, r.Diverged)
				}
			}
		}
	}
	t.Logf("%d runs: %d ended Stalled, %d had a Member that couldn't start", runs, stalled, down)

	// The seeds that exposed the trusting and the careless stores.
	for _, tt := range []struct {
		scenario string
		seed     uint64
	}{{"bit-flips", 87}, {"bit-flips", 7}, {"bit-flips", 9}, {"bit-flips", 1}, {"rolling-crashes", 2}, {"rot-and-everything", 6}, {"rot-and-everything", 27}} {
		if r := rungtest.Run(rung4Store, scenario(t, tt.scenario), 3, tt.seed); !r.Safe() || (!r.Stalled && len(r.Diverged) > 0) {
			t.Errorf("a seed that exposed an earlier store still fails: %v", r)
		}
	}
}

// With no crash to tear a write and no bit flipped, the same store is fine.
func TestTrustingStoreIsFineOnAnHonestDisk(t *testing.T) {
	for seed := uint64(1); seed <= 10; seed++ {
		if r := rungtest.Run(trustingTorn, scenario(t, "none"), 3, seed); !r.Passed() {
			t.Errorf("%v", r)
		}
	}
}
