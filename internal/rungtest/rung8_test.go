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
