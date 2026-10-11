package shardtest

import (
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/rungtest"
)

// contended is a store whose clients fight over keys: 8 of them, half of
// whose requests are Transactions over the given number of keys, sent
// again up to retry times when aborted over a conflict.
func contended(mode cluster.ConflictMode, keys, retry int) rungtest.ShardedStore {
	return sharded(func(s *rungtest.ShardedStore) {
		s.Workload.Clients = 8
		s.Workload.TxnPercent = 50
		s.Workload.TxnKeys = keys
		s.Workload.RetryTxn = retry
		s.Transactions.On = true
		s.Transactions.Conflicts = mode
	})
}

// Rung 8, "Exposed": Transactions that wait for each other's Locks with no
// order between them. One holds its keys in Group 2 and waits for Group 3;
// another holds Group 3 and waits for Group 2. Neither is ever decided, the
// keys stay Locked, and every client that touches them waits too. No Fault
// is needed.
func TestWaitingWithNoOrderIsExposed(t *testing.T) {
	waiting := contended(cluster.WaitInTurn, 3, 0)
	for _, seed := range []uint64{1, 2} {
		r := rungtest.RunSharded(waiting, shardedScenario(t, "no-moves"), seed)
		if len(r.Diverged) == 0 || r.Signals.Lost == 0 {
			t.Errorf("seed %d: expected keys left Locked and clients left waiting, got %v", seed, r)
		}
		if r := rungtest.RunSharded(contended(cluster.OlderWins, 3, 5), shardedScenario(t, "no-moves"), seed); !r.Passed() {
			t.Errorf("older wins: %v %v", r, r.Diverged)
		}
	}
}

// A Transaction over two keys can't be caught this way. Its Coordinator
// Group owns its lowest key and Locks its own keys first, so each one
// waits only for a key higher than one it holds, and no ring of them can
// close. It takes three Groups, prepared side by side.
func TestTwoKeysNeverWaitForEachOther(t *testing.T) {
	waiting := contended(cluster.WaitInTurn, 2, 0)
	for seed := uint64(1); seed <= 10; seed++ {
		if r := rungtest.RunSharded(waiting, shardedScenario(t, "no-moves"), seed); !r.Passed() {
			t.Errorf("%v %v", r, r.Diverged)
		}
	}
}

// With the older Transaction winning, every Transaction is decided, and
// one that is aborted and sent again with its age gets through.
func TestOlderWins(t *testing.T) {
	store := contended(cluster.OlderWins, 3, 5)
	committed, gaveUp := 0, 0
	for seed := uint64(1); seed <= 20; seed++ {
		r := rungtest.RunSharded(store, shardedScenario(t, "no-moves"), seed)
		if !r.Passed() {
			t.Errorf("%v %v", r, r.Diverged)
		}
		committed += r.Routing.TxnCommitted
		gaveUp += r.Routing.TxnGaveUp
	}
	if committed < 500 || gaveUp*50 > committed {
		t.Errorf("%d committed, %d given up after 6 attempts", committed, gaveUp)
	}
}
