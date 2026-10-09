package fsm

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
)

func entryAt(index core.Index, c Command) core.Entry {
	return core.Entry{Index: index, Kind: core.EntryCommand, Payload: c.Encode()}
}

// at stamps a Command with the Leader's clock reading.
func at(stamp int64, c Command) Command { c.Stamp = stamp; return c }

func ttl(c Command, d int64) Command { c.TTL = d; return c }

func TestKeyExpiresWhenLogTimeReachesItsDeadline(t *testing.T) {
	m := New()
	got := run(t, m,
		at(1000, ttl(put("a", "1"), 100)),
		at(1099, get("a")),
		at(1100, get("a")),
		at(1100, cas("a", "2", 0)), // gone means version 0 again
	)
	if got[1].Status != StatusOK || string(got[1].Value) != "1" {
		t.Errorf("one unit before its deadline the key read as %+v", got[1])
	}
	if got[2].Status != StatusNotFound {
		t.Errorf("at its deadline the key read as %+v, want not found", got[2])
	}
	if got[3].Status != StatusOK {
		t.Errorf("a create-if-absent after Expiry got %+v", got[3])
	}
	if want := []Item{{Key: "a", Value: []byte("2"), Version: 4}}; !reflect.DeepEqual(m.Items(), want) {
		t.Errorf("items = %+v, want %+v", m.Items(), want)
	}
}

// Log time is the highest Stamp so far. A Leader whose clock is behind the
// last one's doesn't move it back, and counts a time-to-live from where it
// stands.
func TestLogTimeNeverGoesBack(t *testing.T) {
	m := New()
	run(t, m, at(5000, put("x", "1")), at(200, ttl(put("a", "1"), 100)))
	if m.LogTime() != 5000 {
		t.Fatalf("log time = %d, want 5000", m.LogTime())
	}
	if d, ok := m.NextDeadline(); !ok || d != 5100 {
		t.Fatalf("deadline = %d %v, want 5100", d, ok)
	}
	// Commands with no Stamp leave time alone.
	run(t, m, get("a"), put("y", "1"))
	if m.LogTime() != 5000 {
		t.Fatalf("an unstamped Command moved log time to %d", m.LogTime())
	}
}

func TestOverwriteAndDeleteDropTheDeadline(t *testing.T) {
	m := New()
	run(t, m,
		at(10, ttl(put("a", "1"), 100)),
		at(10, ttl(put("b", "1"), 100)),
		at(10, ttl(put("c", "1"), 100)),
		at(20, put("a", "for good")),
		at(20, del("b")),
		at(30, ttl(put("c", "2"), 500)), // a later deadline replaces the first
		at(200, put("b", "back")),
		at(400, get("c")),
	)
	want := []Item{
		{Key: "a", Value: []byte("for good"), Version: 4},
		{Key: "b", Value: []byte("back"), Version: 7},
		{Key: "c", Value: []byte("2"), Version: 6, Expires: 530},
	}
	if !reflect.DeepEqual(m.Items(), want) {
		t.Fatalf("items = %+v\nwant   %+v", m.Items(), want)
	}
	if d, ok := m.NextDeadline(); !ok || d != 530 {
		t.Fatalf("next deadline = %d %v, want 530", d, ok)
	}
}

// Members that apply the same Entries agree, whatever their own clocks say.
func TestMembersAgreeWhateverTheirClocks(t *testing.T) {
	cmds := []Command{at(100, ttl(put("a", "1"), 50)), at(120, ttl(put("b", "1"), 500)), at(160, put("c", "1"))}
	fast, slow := New(), New()
	fast.Observe(1_000_000)
	run(t, fast, cmds...)
	slow.Observe(-7)
	run(t, slow, cmds...)
	if !reflect.DeepEqual(fast.Items(), slow.Items()) {
		t.Fatalf("Members disagree:\n%+v\n%+v", fast.Items(), slow.Items())
	}
	if len(fast.Items()) != 2 {
		t.Fatalf("items = %+v, want b and c", fast.Items())
	}
}

// The store Rung 5 starts from: each Member compares a deadline with its own
// clock, so two Members holding the same Log give different answers.
func TestOwnClockMembersDisagree(t *testing.T) {
	cmds := []Command{at(100, ttl(put("a", "1"), 50))}
	ahead, behind := NewOwnClock(), NewOwnClock()
	run(t, ahead, cmds...)
	run(t, behind, cmds...)
	ahead.Observe(160)
	behind.Observe(140)
	a, _ := DecodeResponse(ahead.Read(get("a").Encode()))
	b, _ := DecodeResponse(behind.Read(get("a").Encode()))
	if a.Status != StatusNotFound || b.Status != StatusOK {
		t.Fatalf("ahead answered %+v, behind answered %+v", a, b)
	}
}

func TestCaptureKeepsTime(t *testing.T) {
	m := New()
	run(t, m, at(100, ttl(put("a", "1"), 50)), at(110, ttl(put("b", "2"), 500)), at(120, put("c", "3")))
	restored := New()
	if err := restored.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Items(), m.Items()) || restored.LogTime() != 120 {
		t.Fatalf("restored %+v at %d, want %+v at 120", restored.Items(), restored.LogTime(), m.Items())
	}
	// The restored Machine expires keys exactly as the original does.
	for _, x := range []*Machine{m, restored} {
		x.Apply(entryAt(4, at(150, get("a"))))
	}
	if !reflect.DeepEqual(restored.Items(), m.Items()) || len(m.Items()) != 2 {
		t.Fatalf("after the deadline: restored %+v, original %+v", restored.Items(), m.Items())
	}
	if !reflect.DeepEqual(restored.Capture()(), m.Capture()()) {
		t.Fatal("the same state encoded differently")
	}
}

// A Command with no Stamp and no time-to-live encodes exactly as it did
// before Rung 5, and so does a Machine that has never seen one.
func TestUntimedEncodingIsUnchanged(t *testing.T) {
	plain := put("a", "1").Encode()
	timed := at(1, put("a", "1")).Encode()
	if len(timed) != len(plain)+2 || plain[1] != 0 {
		t.Fatalf("plain %v, timed %v", plain, timed)
	}
	for _, c := range []Command{at(-5, put("a", "1")), ttl(put("a", "1"), 9), at(1<<40, ttl(cas("a", "1", 3), 77))} {
		got, err := DecodeCommand(c.Encode())
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("round trip of %+v gave %+v, %v", c, got, err)
		}
	}
	if got, _ := DecodeCommand(Stamp(put("a", "1").Encode(), 42)); got.Stamp != 42 {
		t.Errorf("Stamp set %d, want 42", got.Stamp)
	}
}

// Due tells a Leader when an OpTick would remove something, and stops
// telling it once the OpTick has been applied.
func TestDueUntilTheTickIsApplied(t *testing.T) {
	m := New()
	run(t, m, at(100, ttl(put("a", "1"), 50)))
	if m.Due(149) {
		t.Fatal("due before the deadline")
	}
	if !m.Due(150) {
		t.Fatal("not due at the deadline")
	}
	if r, _ := DecodeResponse(m.Apply(entryAt(2, at(150, Command{Op: OpTick})))); r.Status != StatusOK {
		t.Fatalf("tick answered %+v", r)
	}
	if len(m.Items()) != 0 || m.Due(1000) {
		t.Fatalf("after the tick: items %+v, due %v", m.Items(), m.Due(1000))
	}
	// A Leader whose clock is behind Log time can't move it, so isn't asked.
	run(t, New(), at(100, put("x", "1")))
	behind := New()
	run(t, behind, at(1000, ttl(put("a", "1"), 50)))
	if behind.Due(900) {
		t.Fatal("due by a clock that is behind Log time")
	}
}
