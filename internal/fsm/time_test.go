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

func TestIdleSessionIsRemovedByLogTime(t *testing.T) {
	m := New()
	m.SessionTTL = 1000
	got := run(t, m,
		at(100, Command{Op: OpOpenSession}),      // Session 1
		at(100, Command{Op: OpOpenSession}),      // Session 2
		at(900, inSession(put("a", "1"), 1, 1)),  // Session 1 is used
		at(1200, put("x", "1")),                  // Session 2 has now been idle for 1100
		at(1200, inSession(put("b", "1"), 2, 1)), // so this is refused
		at(1300, inSession(put("a", "2"), 1, 2)), // Session 1 lives on
		at(1300, inSession(put("a", "2"), 1, 2)), // and still recognises a retry
		at(2400, put("x", "2")),                  // idle since 1300
		at(2400, inSession(put("a", "3"), 1, 3)),
	)
	if got[4].Status != StatusSessionExpired {
		t.Errorf("a request in a Session idle for longer than SessionTTL got %+v", got[4])
	}
	if got[5].Status != StatusOK || got[6].Version != got[5].Version {
		t.Errorf("the Session in use: %+v, retry %+v", got[5], got[6])
	}
	if got[8].Status != StatusSessionExpired || m.Sessions() != 0 {
		t.Errorf("after 1100 idle: %+v, %d Sessions left", got[8], m.Sessions())
	}
	if want := []Item{{Key: "a", Value: []byte("2"), Version: 6}, {Key: "x", Value: []byte("2"), Version: 8}}; !reflect.DeepEqual(m.Items(), want) {
		t.Errorf("items = %+v, want %+v", m.Items(), want)
	}
}

// A restored Machine removes Sessions at the same Entry as the original.
func TestCaptureKeepsWhenSessionsWereUsed(t *testing.T) {
	m := New()
	m.SessionTTL = 1000
	run(t, m, at(100, Command{Op: OpOpenSession}), at(100, Command{Op: OpOpenSession}), at(700, inSession(put("a", "1"), 2, 1)))
	restored := New()
	restored.SessionTTL = 1000
	if err := restored.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	for _, x := range []*Machine{m, restored} {
		x.Apply(entryAt(4, at(1300, put("x", "1"))))
		if x.Sessions() != 1 {
			t.Fatalf("%d Sessions left at 1300, want only the one used at 700", x.Sessions())
		}
	}
	if !reflect.DeepEqual(restored.Capture()(), m.Capture()()) {
		t.Fatal("the same state encoded differently")
	}
}

// A Session can be registered part-way through its life (A§11.6): the
// record starts at the request that registers it and answers for nothing
// earlier.
func TestRegisterASessionWithAFloor(t *testing.T) {
	first := inSession(put("a", "1"), 77, 6)
	first.Register = true
	m := New()
	got := run(t, m,
		inSession(put("a", "0"), 77, 6),   // not registered, and doesn't ask to be
		first,                             // registers at request 6 and is applied
		inSession(put("a", "1"), 77, 6),   // a retry of it: answered as before
		inSession(put("a", "old"), 77, 5), // below the floor: refused
		inSession(put("a", "2"), 77, 7),
	)
	want := []Status{StatusSessionExpired, StatusOK, StatusOK, StatusSessionExpired, StatusOK}
	for i, r := range got {
		if r.Status != want[i] {
			t.Errorf("request %d: got %v, want %v", i, r.Status, want[i])
		}
	}
	if got[2].Version != got[1].Version {
		t.Errorf("the retry was applied again: versions %d and %d", got[1].Version, got[2].Version)
	}
	// The floor survives a Snapshot.
	restored := New()
	if err := restored.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	if r, _ := restored.SessionRecord(77); r.Floor != 6 || r.LastSeq != 7 {
		t.Fatalf("restored record %+v, want floor 6 and last request 7", r)
	}
	if !reflect.DeepEqual(restored.Capture()(), m.Capture()()) {
		t.Fatal("the same state encoded differently")
	}
	// The flag travels in the encoding, and costs nothing when it is off.
	if got, err := DecodeCommand(first.Encode()); err != nil || !got.Register {
		t.Fatalf("round trip lost Register: %+v, %v", got, err)
	}
}

func TestRawAccess(t *testing.T) {
	m := New()
	run(t, m, at(100, put("x", "1")))
	m.SetRaw(Raw{Key: "b", Value: []byte("2"), Version: 9, Deadline: 150})
	m.SetRaw(Raw{Key: "a", Value: []byte("1"), Version: 8})
	var keys []string
	m.Range("a", "c", func(r Raw) bool { keys = append(keys, r.Key); return true })
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("Range gave %v", keys)
	}
	if r, ok := m.GetRaw("b"); !ok || r.Version != 9 || r.Deadline != 150 {
		t.Fatalf("GetRaw(b) = %+v, %v", r, ok)
	}
	// Catching up with another Group's time removes what is due.
	m.AdvanceTime(150)
	if _, ok := m.GetRaw("b"); ok || m.LogTime() != 150 {
		t.Fatalf("after AdvanceTime(150): b still there, or log time %d", m.LogTime())
	}
	m.DeleteRaw("a")
	if len(m.Items()) != 1 {
		t.Fatalf("items = %+v, want only x", m.Items())
	}
}
