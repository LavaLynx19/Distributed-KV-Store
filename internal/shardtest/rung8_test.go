package shardtest

import (
	"os"
	"testing"

	"distributed-kv-store/internal/rungtest"
)

// memoryTxn is the store Rung 8 starts from, with one of its three
// shortcuts: a Transaction across Groups is carried out by the Node that
// was asked, which keeps what it decided in memory (A§12.1).
var memoryTxn = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Transactions.On = true
	s.Transactions.Memory = true
})

func scenario8(t *testing.T, name string) rungtest.ShardedScenario {
	t.Helper()
	for _, sc := range rungtest.Rung8 {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return rungtest.ShardedScenario{}
}

// With nothing going wrong, a decision kept in memory is enough: the
// Transactions across Groups commit and the History is Linearizable.
func TestMemoryTxnWithNoFaults(t *testing.T) {
	committed := 0
	for seed := uint64(1); seed <= 10; seed++ {
		r := rungtest.RunSharded(memoryTxn, shardedScenario(t, "no-moves"), seed)
		if !r.Passed() {
			t.Errorf("%v %v", r, r.Diverged)
		}
		committed += r.Routing.TxnCommitted
	}
	if committed < 200 {
		t.Errorf("only %d Transactions across Groups committed in 10 runs", committed)
	}
}

// memoryExposed are the runs in which a decision kept in memory left a
// Transaction half-committed and the History not Linearizable.
var memoryExposed = []struct {
	scenario string
	seeds    []uint64
}{
	{"stopped-mid-commit", []uint64{66, 72, 89}},
	{"crashes", []uint64{24, 25}},
	{"partitions", []uint64{67, 94}},
}

// Rung 8, "Exposed": the decision is kept in the memory of the Node that
// was asked. Stop that Node after it has told one Group to commit and
// before it has told the other, and the Transaction is half-committed: one
// Group has its writes and the other never will. The client, hearing
// nothing, asks again through another Node, which knows nothing of the
// first attempt. It finds the Coordinator Group saying the request was
// carried out, tells the client so, and lets the other Group's Locks go.
//
// It doesn't take a crash at the worst moment: any crash will do. Nor a
// crash at all: under a Partition a client that gives up waiting asks
// again, and two Nodes then carry out the same Transaction, neither
// knowing of the other.
func TestDecisionInMemoryIsExposed(t *testing.T) {
	for _, e := range memoryExposed {
		for _, seed := range e.seeds {
			r := rungtest.RunSharded(memoryTxn, scenario8(t, e.scenario), seed)
			if len(r.HalfCommitted) == 0 || r.Linearizable {
				t.Errorf("%s, seed %d: expected a half-committed Transaction and a History that isn't Linearizable, got %v", e.scenario, seed, r)
			}
		}
	}
}

// rung8Store keeps every step of a Transaction across Groups in the
// Coordinator Group's Log, whose Leader carries it on (A§12.2).
var rung8Store = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Transactions.On = true
})

// The runs that exposed the decision in memory pass once it is in a Log.
func TestDecisionInTheLogSurvives(t *testing.T) {
	for _, e := range memoryExposed {
		for _, seed := range e.seeds {
			if r := rungtest.RunSharded(rung8Store, scenario8(t, e.scenario), seed); !r.Passed() {
				t.Errorf("%v %v", r, r.Diverged)
			}
		}
	}
	committed := 0
	for seed := uint64(1); seed <= 10; seed++ {
		r := rungtest.RunSharded(rung8Store, shardedScenario(t, "no-moves"), seed)
		if !r.Passed() {
			t.Errorf("%v %v", r, r.Diverged)
		}
		committed += r.Routing.TxnCommitted
	}
	if committed < 200 {
		t.Errorf("only %d Transactions across Groups committed in 10 runs", committed)
	}
}

// readLocked is rung8Store with one shortcut left in: a Locked key can be
// read.
var readLocked = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Transactions.On = true
	s.Transactions.ReadLocked = true
})

// Rung 8, "Exposed": a Locked key that can still be read. The client is
// told a Transaction committed as soon as the Coordinator Group has decided,
// and another Group may not have heard yet. A read of its key in between
// gets the old value: a Stale read. No Fault is needed.
func TestReadableLockIsExposed(t *testing.T) {
	for _, seed := range []uint64{35, 39, 42} {
		if r := rungtest.RunSharded(readLocked, shardedScenario(t, "no-moves"), seed); r.Linearizable {
			t.Errorf("seed %d: expected a History that isn't Linearizable, got %v", seed, r)
		}
		if r := rungtest.RunSharded(rung8Store, shardedScenario(t, "no-moves"), seed); !r.Passed() {
			t.Errorf("with Locked keys unreadable: %v %v", r, r.Diverged)
		}
	}
}

// noOffset is rung8Store with another shortcut left in: a Group that is
// given a version above its own does nothing about it.
var noOffset = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Transactions.On = true
	s.Transactions.NoOffset = true
})

// Rung 8, "Exposed": a version with no offset. A Transaction's keys all get
// the highest version any of its Groups offered. In a Group whose Log is
// shorter, the next write to such a key gets a lower version than the key
// already had, so versions go down and a compare-and-set made against the
// old version can succeed against the wrong value. Every run shows it.
func TestVersionWithNoOffsetIsExposed(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		if r := rungtest.RunSharded(noOffset, shardedScenario(t, "no-moves"), seed); r.Linearizable {
			t.Errorf("seed %d: expected a History that isn't Linearizable, got %v", seed, r)
		}
	}
}

// noAsking is rung8Store with Groups that wait to be told how a Transaction
// ended and never ask.
var noAsking = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Transactions.On = true
	s.Transactions.NoAsking = true
})

// A prepare that is sent twice can arrive a second time after the
// Transaction was aborted and finished. The Group Locks the keys again, and
// nobody will ever tell it to let go: the Coordinator Group has heard that
// Group finish. Asking is what ends it (A§12.3).
func TestLocksNobodyAsksAboutAreLeft(t *testing.T) {
	for _, c := range []struct {
		scenario string
		seed     uint64
	}{{"crashes", 82}, {"stopped-mid-commit", 76}, {"stopped-mid-commit", 150}} {
		if r := rungtest.RunSharded(noAsking, scenario8(t, c.scenario), c.seed); len(r.Diverged) == 0 {
			t.Errorf("%s, seed %d: expected keys left Locked, got %v", c.scenario, c.seed, r)
		}
		if r := rungtest.RunSharded(rung8Store, scenario8(t, c.scenario), c.seed); !r.Passed() {
			t.Errorf("with asking: %v %v", r, r.Diverged)
		}
	}
}

// busy8 is rung8Store with clients that also give keys a time-to-live and
// scan, and with Sessions that are removed after 800 units unused.
var busy8 = sharded(func(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Workload.TTLPercent = 40
	s.Workload.TTL = [2]int64{100, 600}
	s.Workload.ScanPercent = 10
	s.SessionTTL = 800
	s.Transactions.On = true
})

// scenarios8a are the Faults of stage 8a: Rung 8's own, and Rung 7's with
// Moves, since a Move must now wait for a Slot's Locks.
func scenarios8a() []rungtest.ShardedScenario {
	return append(append([]rungtest.ShardedScenario{}, rungtest.Rung8...), rungtest.Rung7[:6]...)
}

// Runs that failed while stage 8a was being built, each for a different
// reason, kept as they were found.
func TestRung8aRegressions(t *testing.T) {
	for _, c := range []struct {
		store    rungtest.ShardedStore
		scenario rungtest.ShardedScenario
		seed     uint64
	}{
		// A retry after a Move had split the request's keys was answered
		// with the stored names of the keys.
		{rung8Store, shardedScenario(t, "moves-interrupted"), 1},
		{rung8Store, shardedScenario(t, "moves-and-crashes"), 54},
		{rung8Store, shardedScenario(t, "moves-and-partitions"), 20},
		// A Leader that had been replaced, and didn't know it, sent a
		// prepare long after the Transaction had committed and the Group
		// had forgotten the Session. The Group committed it again.
		{busy8, shardedScenario(t, "moves-and-everything"), 56},
	} {
		if r := rungtest.RunSharded(c.store, c.scenario, c.seed); !r.Passed() {
			t.Errorf("%v %v", r, r.Diverged)
		}
	}
}

// Stage 8a: Transactions across Groups stay atomic and the History
// Linearizable under crashes, Partitions and Moves, with nothing left
// Locked or undecided at the end (A§12.9).
func TestRung8a(t *testing.T) {
	runs, committed := 0, 0
	for name, store := range map[string]rungtest.ShardedStore{"plain": rung8Store, "busy": busy8} {
		for _, sc := range scenarios8a() {
			for seed := uint64(1); seed <= 50; seed++ {
				r := rungtest.RunSharded(store, sc, seed)
				runs++
				committed += r.Routing.TxnCommitted
				if !r.Passed() {
					t.Errorf("%s: %v %v", name, r, r.Diverged)
				}
			}
		}
	}
	t.Logf("%d runs, %d Transactions across Groups committed", runs, committed)
}

// TestMeasureRung8a prints what stage 8a's retro reports. It is long, so it
// runs only when asked for: KV_MEASURE=1.
func TestMeasureRung8a(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("set KV_MEASURE=1")
	}
	const seeds = 300
	naive := []struct {
		name  string
		store rungtest.ShardedStore
	}{{"decision in memory", memoryTxn}, {"Locked keys readable", readLocked}, {"no version offset", noOffset}, {"no asking", noAsking}}
	scs := append([]rungtest.ShardedScenario{shardedScenario(t, "no-moves")}, rungtest.Rung8...)
	for _, n := range naive {
		for _, sc := range scs {
			failed, half, stale, locked := 0, 0, 0, 0
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.RunSharded(n.store, sc, seed)
				if !r.Passed() {
					failed++
				}
				if len(r.HalfCommitted) > 0 {
					half++
				}
				if !r.Linearizable {
					stale++
				}
				if len(r.Diverged) > 0 {
					locked++
				}
			}
			t.Logf("%-22s %-20s failed %3d of %d: half-committed %3d, not Linearizable %3d, left Locked or diverged %3d", n.name, sc.Name, failed, seeds, half, stale, locked)
		}
	}
	for name, store := range map[string]rungtest.ShardedStore{"plain": rung8Store, "busy": busy8} {
		for _, sc := range scenarios8a() {
			failed, committed, conflict, locked, moves := 0, 0, 0, 0, 0
			var pause int64
			for seed := uint64(1); seed <= seeds; seed++ {
				r := rungtest.RunSharded(store, sc, seed)
				if !r.Passed() {
					failed++
				}
				committed += r.Routing.TxnCommitted
				conflict += r.Routing.TxnConflict
				locked += r.Routing.Locked
				moves += r.Moves
				for _, p := range r.Pauses {
					pause = max(pause, p)
				}
			}
			t.Logf("%-5s %-22s failed %d of %d: committed %5d, aborted over a conflict %4d, requests refused locked %4d, Moves %4d, longest Move pause %d", name, sc.Name, failed, seeds, committed, conflict, locked, moves, pause)
		}
	}
}
