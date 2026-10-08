package rungtest_test

import (
	"strings"
	"testing"

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

// Rung 4, "Exposed": the seeds recorded in retros/rung-4.md.
func TestLyingDiskIsExposed(t *testing.T) {
	// One flipped bit, replayed as valid: a Member ends up holding a key no
	// client ever wrote. Every History is Linearizable and nothing crashes.
	r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 4)
	t.Log(r, r.Diverged)
	if len(r.Diverged) == 0 || r.Panic != "" || len(r.Unreadable) != 0 {
		t.Errorf("seed 4: expected Members to diverge silently, got %v", r)
	}
	var phantom bool
	for _, d := range r.Diverged {
		phantom = phantom || strings.Contains(d, `key "j0"`)
	}
	if !phantom {
		t.Errorf("seed 4: expected the phantom key j0 (k0 with one bit flipped), got %v", r.Diverged)
	}

	// A flipped bit that reaches clients.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 14); r.Linearizable {
		t.Errorf("seed 14: expected a History that isn't Linearizable, got %v", r)
	}
	// One that makes a Member contradict what is Committed.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 3); !strings.Contains(r.Panic, "asked to replace Committed Entry") {
		t.Errorf("seed 3: expected the core's safety check to trip, got %v", r)
	}
	// And one that leaves a Member unable to read its own disk.
	if r := rungtest.Run(trusting, scenario(t, "bit-flips"), 3, 2); len(r.Unreadable) == 0 {
		t.Errorf("seed 2: expected a Member that can't restart, got %v", r)
	}
	// A torn write does the same without any bit being flipped.
	if r := rungtest.Run(trustingTorn, scenario(t, "rolling-crashes"), 3, 2); len(r.Unreadable) == 0 {
		t.Errorf("torn write, seed 2: expected a Member that can't restart, got %v", r)
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
