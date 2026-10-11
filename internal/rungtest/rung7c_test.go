package rungtest_test

import (
	"os"
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/rungtest"
)

// replacing is the stage 7b store with two Spares and dead Nodes replaced.
func replacing(tune func(*rungtest.ShardedStore)) rungtest.ShardedStore {
	return gossiping(cluster.GossipCounters, func(s *rungtest.ShardedStore) {
		s.Spares = 2
		s.Replacing.On = true
		tune(s)
	})
}

// naiveReplacing is the store stage 7c starts from: the Meta Leader's own
// word is enough to replace a Node, and every Node starts and drops
// replicas by the table it happens to hold.
var naiveReplacing = replacing(func(s *rungtest.ShardedStore) {
	s.Replacing.OneWord, s.Replacing.FollowTable = true, true
})

// rung7cStore is where replacement ends up: a Majority of the Meta Group's
// Members and a wait before a Node is replaced, and a replica dropped only
// on its Group's say-so.
var rung7cStore = replacing(func(*rungtest.ShardedStore) {})

func shardedScenario7c(t *testing.T, name string) rungtest.ShardedScenario {
	t.Helper()
	for _, sc := range append(rungtest.Rung7c, append(rungtest.Rung7, rungtest.Rung7b...)...) {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return rungtest.ShardedScenario{}
}

// Stage 7c, "Exposed": a Node keeps the replicas the table it holds says it
// should. A Node that restarts holds the table the store was founded with
// until gossip brings it the news, and a Node that took a lost Node's place
// isn't in that one. So it drops data its Group is counting on, and when
// the news arrives starts again, empty, as a Member in good standing. With
// one other Member behind and the third away, the empty one and the one
// behind elect a Leader that never saw what the Group had Committed.
func TestReplicasThatFollowTheTableAreExposed(t *testing.T) {
	rolling := shardedScenario7c(t, "lost-node-then-rolling-restarts")
	if r := rungtest.RunSharded(naiveReplacing, rolling, 4); r.WrongDrop == "" {
		t.Errorf("seed 4: expected a replica dropped while still a Member, got %v", r)
	}
	if r := rungtest.RunSharded(naiveReplacing, rolling, 3480); r.WrongDrop == "" || r.Linearizable {
		t.Errorf("seed 3480: expected a History that isn't Linearizable, got %v", r)
	}
	if r := rungtest.RunSharded(naiveReplacing, rolling, 1585); r.Panic == "" {
		t.Errorf("seed 1585: expected a core to trip its own check, got %v", r)
	}
	// A Node lost for good, with nothing restarting, is replaced cleanly.
	for seed := uint64(1); seed <= 5; seed++ {
		if r := rungtest.RunSharded(naiveReplacing, shardedScenario7c(t, "lost-node"), seed); !r.Passed() || r.Replacements != 1 {
			t.Errorf("a lost Node and nothing else: %v %v", r, r.Diverged)
		}
	}
}

// Stage 7c, "Exposed", part two: the Meta Leader replaces a Node the moment
// its own gossip gives the Node up. Every Node in this scenario is back
// within 350 units of crashing, and some are replaced all the same: a copy
// of every Group they were in, for a Node that was only restarting.
func TestOneWordReplacementIsExposed(t *testing.T) {
	crashes := shardedScenario7c(t, "moves-and-crashes")
	if r := rungtest.RunSharded(naiveReplacing, crashes, 1); r.Replacements == 0 {
		t.Errorf("seed 1: expected a restarting Node to be replaced, got %v", r)
	}
}

// TestMeasureReplacing prints what replacing costs, for the retro.
func TestMeasureReplacing(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("a measurement, not a check: set KV_MEASURE=1 to run it")
	}
	for _, store := range []struct {
		name  string
		store rungtest.ShardedStore
	}{{"naive", naiveReplacing}, {"fixed", rung7cStore}} {
		for _, name := range []string{"lost-node", "lost-node-and-crashes", "long-outages", "long-partitions", "moves-and-crashes", "lost-node-then-rolling-restarts"} {
			fails, wrong, repl, running, drops := 0, 0, 0, 0, 0
			for seed := uint64(1); seed <= 30; seed++ {
				r := rungtest.RunSharded(store.store, shardedScenario7c(t, name), seed)
				if !r.Passed() {
					fails++
				}
				if r.WrongDrop != "" {
					wrong++
				}
				repl += r.Replacements
				running += r.ReplacedRunning
				drops += r.Drops
			}
			t.Logf("%s %s: %d/30 fail, a wrong drop in %d; %d replacements (%d of running Nodes), %d drops", store.name, name, fails, wrong, repl, running, drops)
		}
	}
}

// Stage 7c's promise for dead Nodes (A§11.11). A Node lost for good is
// replaced in every Group it was in, the Meta Group included, with nobody
// asking. A Node that is only restarting is left alone. A Node that was
// replaced and comes back drops its replicas only once each Group says it
// may, and holds none at the end. All of stage 7b's guarantees still hold.
func TestRung7cReplacing(t *testing.T) {
	seeds := uint64(100)
	if testing.Short() {
		seeds = 5
	}
	stores := []rungtest.ShardedStore{
		rung7cStore,
		replacing(func(s *rungtest.ShardedStore) { s.Gossip = cluster.GossipSWIM }),
		replacing(func(s *rungtest.ShardedStore) { s.Workload = busy.Workload; s.SessionTTL = busy.SessionTTL }),
	}
	// Scenarios in which no Node is away at all, and those in which one is
	// lost for good.
	brief := map[string]bool{"no-moves": true, "moves": true}
	lost := map[string]bool{"lost-node": true, "lost-node-and-crashes": true, "lost-node-then-rolling-restarts": true}
	runs, replaced, drops, restarting := 0, 0, 0, 0
	for i, store := range stores {
		for _, sc := range append(append(rungtest.Rung7, rungtest.Rung7b...), rungtest.Rung7c...) {
			n := seeds
			if i > 0 {
				n = (seeds + 1) / 2 // the two variants get half the seeds
			}
			for seed := uint64(1); seed <= n; seed++ {
				r := rungtest.RunSharded(store, sc, seed)
				runs++
				replaced += r.Replacements
				drops += r.Drops
				if !r.Passed() || r.Recovery < 0 {
					t.Errorf("%v\n  end state: %v", r, r.Diverged)
				}
				if sc.Name == "moves-and-crashes" {
					restarting += r.Replacements
				}
				if brief[sc.Name] && r.Replacements != 0 {
					t.Errorf("%v: %d Nodes replaced, though none was away", r, r.Replacements)
				}
				if lost[sc.Name] && r.Replacements == 0 {
					t.Errorf("%v: the lost Node wasn't replaced", r)
				}
			}
		}
	}
	t.Logf("%d runs, %d Nodes replaced, %d replicas dropped", runs, replaced, drops)
	// A Node that crashes again just after restarting is away for both
	// spells as far as anyone can tell, so a few are replaced even here.
	t.Logf("with every crashed Node back within 350 units: %d replaced", restarting)

	// The seeds that exposed the naive store.
	rolling := shardedScenario7c(t, "lost-node-then-rolling-restarts")
	for _, seed := range []uint64{4, 1585, 3480} {
		if r := rungtest.RunSharded(rung7cStore, rolling, seed); !r.Passed() {
			t.Errorf("a seed that exposed the naive store still fails: %v %v", r, r.Diverged)
		}
	}
	if r := rungtest.RunSharded(rung7cStore, shardedScenario7c(t, "moves-and-crashes"), 1); !r.Passed() || r.Replacements != 0 {
		t.Errorf("a seed that exposed the naive store still fails: %v", r)
	}
}
