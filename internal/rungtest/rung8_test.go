package rungtest_test

import (
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
	for _, seed := range []uint64{1, 15, 17} {
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
