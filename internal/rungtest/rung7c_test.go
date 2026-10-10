package rungtest_test

import (
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
	if testing.Short() {
		t.Skip("a measurement, not a check")
	}
	for _, store := range []struct {
		name  string
		store rungtest.ShardedStore
	}{{"naive", naiveReplacing}} {
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
