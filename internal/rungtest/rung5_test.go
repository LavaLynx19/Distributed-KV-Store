package rungtest_test

import (
	"testing"

	"distributed-kv-store/internal/rungtest"
)

// timed is rung4Store with clients that give some keys a time-to-live, and
// every request stamped with the receiving Member's clock.
func timed() rungtest.Store {
	s := rung4Store
	s.Stamped = true
	s.Workload.TTLPercent = 40
	s.Workload.TTL = [2]int64{100, 600}
	return s
}

// ownClock is the store Rung 5 starts from: a key's deadline is the
// Leader's clock reading plus its time-to-live, and each Member decides
// whether the key is still there by looking at its own clock.
var ownClock = func() rungtest.Store {
	s := timed()
	s.OwnClock = true
	return s
}()

// rung5Store judges Expiry by Log time: the highest Leader stamp applied.
var rung5Store = timed()

// Rung 5, "Exposed": Members that each consult their own clock disagree
// about which keys exist.
func TestOwnClockIsExposed(t *testing.T) {
	// A client is told a key is gone and then, by a Leader whose clock is
	// behind, that it is there.
	for _, tt := range []struct {
		scenario string
		seed     uint64
	}{{"clock-skew", 1}, {"clock-jumps", 3}} {
		if r := rungtest.Run(ownClock, scenario(t, tt.scenario), 3, tt.seed); r.Linearizable {
			t.Errorf("%s seed %d: expected a History that isn't Linearizable, got %v", tt.scenario, tt.seed, r)
		}
	}
	// Every History is fine and the Members still end up holding different
	// keys.
	if r := rungtest.Run(ownClock, scenario(t, "clock-skew"), 5, 1); !r.Safe() || len(r.Diverged) == 0 {
		t.Errorf("clock-skew, 5 Members, seed 1: expected Members to diverge quietly, got %v", r)
	}
	// It doesn't take a bad clock. A Member that restarts applies its Log
	// again, later than the first time, and a compare-and-set that found the
	// key alive then finds it expired now.
	if r := rungtest.Run(ownClock, scenario(t, "crash-leader"), 3, 99); r.Linearizable {
		t.Errorf("crash-leader seed 99: expected a History that isn't Linearizable, got %v", r)
	}
	// With good clocks and no restarts it looks fine.
	for seed := uint64(1); seed <= 10; seed++ {
		if r := rungtest.Run(ownClock, scenario(t, "none"), 3, seed); !r.Passed() {
			t.Errorf("no Faults: %v", r)
		}
	}
}
