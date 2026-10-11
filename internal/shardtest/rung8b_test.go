package shardtest

import (
	"os"
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

// tune8b is where Rung 8 ends: the older Transaction wins, a Move aborts
// the undecided Transactions in its way, and a record is forgotten once
// every Group has confirmed. Clients send Transactions over three keys,
// and send an aborted one again with its age.
func tune8b(s *rungtest.ShardedStore) {
	s.Workload.TxnPercent = 30
	s.Workload.TxnKeys = 3
	s.Workload.RetryTxn = 5
	s.Transactions = cluster.Transactions{On: true, Conflicts: cluster.OlderWins, MovesAbort: true, Forget: true}
}

var rung8bStore = sharded(tune8b)

// busy8b adds time-to-live, scans, and Sessions removed after 800 units.
var busy8b = sharded(func(s *rungtest.ShardedStore) {
	tune8b(s)
	s.Workload.TTLPercent = 40
	s.Workload.TTL = [2]int64{100, 600}
	s.Workload.ScanPercent = 10
	s.SessionTTL = 800
})

// movesWait is rung8bStore with Moves that wait for Transactions to end.
var movesWait = sharded(func(s *rungtest.ShardedStore) {
	tune8b(s)
	s.Transactions.MovesAbort = false
})

// balancing8b has the balancer of stage 7c at work, with most requests
// sent to Group 1's keys so that it has Slots to move. It has 6 clients,
// not the 20 that stage 7c measured load with: a History with
// Transactions is checked over the whole store at once, and 20 clients
// are more than that check can get through.
var balancing8b = balancing(func(s *rungtest.ShardedStore) {
	tune8b(s)
	s.Workload.Clients, s.Workload.Keys = 6, 12
	s.Workload.Skew.Percent, s.Workload.Skew.Sets = 70, [][]string{keysOf(1, 12)}
})

func longestLockWait(store rungtest.ShardedStore, sc rungtest.ShardedScenario, seeds uint64) (longest int64, waited int) {
	for seed := uint64(1); seed <= seeds; seed++ {
		for _, w := range rungtest.RunSharded(store, sc, seed).LockWaits {
			longest = max(longest, w)
			waited++
		}
	}
	return longest, waited
}

// A Slot that is ready to move and has Locked keys asks for the
// Transactions that hold them to be aborted, and is on its way by the next
// time or two its Group looks.
func TestAMoveAbortsWhatIsInItsWay(t *testing.T) {
	longest, waited := longestLockWait(rung8bStore, shardedScenario(t, "moves"), 50)
	if waited == 0 || longest > 60 {
		t.Errorf("%d Moves waited for Locks, the longest for %d units", waited, longest)
	}
}

// Stage 8b: the whole of Rung 8, under the Faults of stage 8a, and with
// stage 7c's replacement and balancing at work (A§12.9).
func TestRung8b(t *testing.T) {
	runs, committed, moves, replaced := 0, 0, 0, 0
	run := func(name string, store rungtest.ShardedStore, sc rungtest.ShardedScenario, seeds uint64) {
		for seed := uint64(1); seed <= seeds; seed++ {
			r := rungtest.RunSharded(store, sc, seed)
			runs++
			committed += r.Routing.TxnCommitted
			moves += r.Moves
			replaced += r.Replacements
			if !r.Passed() {
				t.Errorf("%s: %v %v", name, r, r.Diverged)
			}
		}
	}
	for _, sc := range scenarios8a() {
		run("plain", rung8bStore, sc, 40)
		run("busy", busy8b, sc, 40)
	}
	for _, sc := range rungtest.Rung7c {
		run("replacing", replacing(tune8b), sc, 20)
	}
	run("balancing", balancing8b, shardedScenario(t, "no-moves"), 40)
	t.Logf("%d runs: %d Transactions across Groups committed, %d Moves, %d replacements", runs, committed, moves, replaced)
}

// TestMeasureRung8b prints what stage 8b's retro reports. It is long, so it
// runs only when asked for: KV_MEASURE=1.
func TestMeasureRung8b(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("set KV_MEASURE=1")
	}
	// What a Transaction does about a key another has Locked, with no
	// Faults.
	for _, keys := range []int{2, 3} {
		for _, m := range []struct {
			name string
			mode cluster.ConflictMode
			try  int
		}{{"refused at once", cluster.RefuseAtOnce, 5}, {"waits, no order", cluster.WaitInTurn, 0}, {"older wins", cluster.OlderWins, 5}} {
			failed, committed, aborted, gaveUp, lost, tries := 0, 0, 0, 0, 0, 0
			for seed := uint64(1); seed <= 100; seed++ {
				r := rungtest.RunSharded(contended(m.mode, keys, m.try), shardedScenario(t, "no-moves"), seed)
				if !r.Passed() {
					failed++
				}
				committed += r.Routing.TxnCommitted
				aborted += r.Routing.TxnConflict
				gaveUp += r.Routing.TxnGaveUp
				lost += r.Signals.Lost
				tries = max(tries, r.Routing.TxnMaxTries)
			}
			t.Logf("%d keys, %-16s failed %3d of 100: committed %5d, aborted over a conflict %5d, given up %4d, most attempts %d, requests never answered %4d", keys, m.name, failed, committed, aborted, gaveUp, tries, lost)
		}
	}
	// How long a Slot ready to move waits for its Locks.
	for _, name := range []string{"moves", "moves-and-crashes", "moves-and-partitions", "moves-interrupted", "moves-and-everything"} {
		w, nw := longestLockWait(movesWait, shardedScenario(t, name), 300)
		a, na := longestLockWait(rung8bStore, shardedScenario(t, name), 300)
		t.Logf("%-22s Moves that waited for Locks: %3d, longest %4d (waiting); %3d, longest %4d (aborting)", name, nw, w, na, a)
	}
	// The suite, at length.
	measure := func(name string, store rungtest.ShardedStore, sc rungtest.ShardedScenario, seeds uint64) {
		failed, committed, aborted, gaveUp, moves, replaced, tries := 0, 0, 0, 0, 0, 0, 0
		for seed := uint64(1); seed <= seeds; seed++ {
			r := rungtest.RunSharded(store, sc, seed)
			if !r.Passed() {
				failed++
			}
			committed += r.Routing.TxnCommitted
			aborted += r.Routing.TxnConflict
			gaveUp += r.Routing.TxnGaveUp
			moves += r.Moves
			replaced += r.Replacements
			tries = max(tries, r.Routing.TxnMaxTries)
		}
		t.Logf("%-9s %-32s failed %d of %d: committed %5d, aborted over a conflict %5d, given up %3d, most attempts %d, Moves %4d, replacements %3d", name, sc.Name, failed, seeds, committed, aborted, gaveUp, tries, moves, replaced)
	}
	for _, sc := range scenarios8a() {
		measure("plain", rung8bStore, sc, 300)
		measure("busy", busy8b, sc, 300)
	}
	for _, sc := range rungtest.Rung7c {
		measure("replacing", replacing(tune8b), sc, 100)
	}
	measure("balancing", balancing8b, shardedScenario(t, "no-moves"), 300)
	// A crowd: 20 clients on 24 keys. Too many for the checker, so only
	// what got done is counted.
	crowd := gossiping(cluster.GossipCounters, func(s *rungtest.ShardedStore) {
		tune8b(s)
		s.Workload.Keys, s.Workload.Clients = 24, 20
	})
	for seed := uint64(1); seed <= 3; seed++ {
		r := rungtest.RunSharded(crowd, shardedScenario(t, "no-moves"), seed)
		t.Logf("crowd seed %d: committed %d, aborted over a conflict %d, requests never answered %d, left at the end: %d", seed, r.Routing.TxnCommitted, r.Routing.TxnConflict, r.Signals.Lost, len(r.Diverged))
	}
}
