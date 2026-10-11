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

// With nothing going wrong, a decision kept in memory is nearly always
// enough: the Transactions across Groups commit and the History is
// Linearizable.
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

// Rung 8, "Exposed": the decision is kept in the memory of the Node that
// was asked. Stop that Node after it has told one Group to commit and
// before it has told the other, and the Transaction is half-committed: one
// Group has its writes and the other never will. The client, hearing
// nothing, asks again through another Node, which knows nothing of the
// first attempt. It finds the Coordinator Group saying the request was
// carried out, tells the client so, and lets the other Group's Locks go.
func TestDecisionInMemoryIsExposed(t *testing.T) {
	for _, seed := range []uint64{1, 2} {
		r := rungtest.RunSharded(memoryTxn, scenario8(t, "stopped-mid-commit"), seed)
		if len(r.HalfCommitted) == 0 || r.Linearizable {
			t.Errorf("stopped mid-commit, seed %d: expected a half-committed Transaction and a History that isn't Linearizable, got %v", seed, r)
		}
	}
	// It doesn't take a crash at the worst moment. Any crash will do.
	if r := rungtest.RunSharded(memoryTxn, scenario8(t, "crashes"), 4); len(r.HalfCommitted) == 0 || r.Linearizable {
		t.Errorf("crashes, seed 4: expected a half-committed Transaction, got %v", r)
	}
	// Nor a crash at all. A client that gives up waiting asks again, and
	// two Nodes then carry out the same Transaction, neither knowing of
	// the other: under a Partition, and now and then with no Fault.
	if r := rungtest.RunSharded(memoryTxn, scenario8(t, "partitions"), 1); len(r.HalfCommitted) == 0 || r.Linearizable {
		t.Errorf("partitions, seed 1: expected a half-committed Transaction, got %v", r)
	}
	if r := rungtest.RunSharded(memoryTxn, shardedScenario(t, "no-moves"), 20); len(r.HalfCommitted) == 0 || r.Linearizable {
		t.Errorf("no Faults, seed 20: expected a half-committed Transaction, got %v", r)
	}
}
