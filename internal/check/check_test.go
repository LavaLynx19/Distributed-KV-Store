package check

import (
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
)

// op is one hand-written request in a test History.
type op struct {
	cmd        fsm.Command
	start, end int64
	result     Result
	resp       fsm.Response
}

func history(ops ...op) *History {
	h := &History{}
	for i, o := range ops {
		id := h.Begin(i, o.cmd, o.start)
		h.End(id, o.result, o.resp, o.end)
	}
	return h
}

func put(k, v string) fsm.Command { return fsm.Command{Op: fsm.OpPut, Key: k, Value: []byte(v)} }
func get(k string) fsm.Command    { return fsm.Command{Op: fsm.OpGet, Key: k} }
func putTTL(k, v string) fsm.Command {
	return fsm.Command{Op: fsm.OpPut, Key: k, Value: []byte(v), TTL: 100}
}
func del(k string) fsm.Command { return fsm.Command{Op: fsm.OpDelete, Key: k} }
func cas(k, v string, ifVersion uint64) fsm.Command {
	return fsm.Command{Op: fsm.OpPut, Key: k, Value: []byte(v), Conditional: true, IfVersion: ifVersion}
}

func wrote(version uint64) fsm.Response {
	return fsm.Response{Status: fsm.StatusOK, Version: version}
}
func read(v string, version uint64) fsm.Response {
	return fsm.Response{Status: fsm.StatusOK, Value: []byte(v), Version: version}
}

var (
	notFound = fsm.Response{Status: fsm.StatusNotFound}
	deleted  = fsm.Response{Status: fsm.StatusOK}
)

func mismatch(found uint64) fsm.Response {
	return fsm.Response{Status: fsm.StatusVersionMismatch, Version: found}
}

func TestLinearizable(t *testing.T) {
	tests := []struct {
		name string
		ops  []op
		want bool
	}{
		{"sequential put then get", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{get("a"), 20, 30, Answered, read("1", 1)},
		}, true},
		{"lost Acknowledged write: a read after the write finds nothing", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{get("a"), 20, 30, Answered, notFound},
		}, false},
		{"Stale read: an older value after a newer write finished", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 30, Answered, wrote(2)},
			{get("a"), 40, 50, Answered, read("1", 1)},
		}, false},
		{"a read overlapping a write may see the old value", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 60, Answered, wrote(2)},
			{get("a"), 30, 40, Answered, read("1", 1)},
		}, true},
		{"a read overlapping a write may see the new value", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 60, Answered, wrote(2)},
			{get("a"), 30, 40, Answered, read("2", 2)},
		}, true},
		{"reads can't go back in time", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 90, Answered, wrote(2)},
			{get("a"), 30, 40, Answered, read("2", 2)},
			{get("a"), 50, 60, Answered, read("1", 1)},
		}, false},
		{"a rejected write had no effect", []op{
			{put("a", "1"), 0, 10, Rejected, fsm.Response{}},
			{get("a"), 20, 30, Answered, notFound},
		}, true},
		{"a lost write may have taken effect", []op{
			{put("a", "1"), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, read("1", 7)},
		}, true},
		{"a lost write may never have taken effect", []op{
			{put("a", "1"), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, notFound},
		}, true},
		{"a lost write may take effect late", []op{
			{put("a", "1"), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, notFound},
			{get("a"), 40, 50, Answered, read("1", 7)},
		}, true},
		{"but a lost write can't take effect and then vanish", []op{
			{put("a", "1"), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, read("1", 7)},
			{get("a"), 40, 50, Answered, notFound},
		}, false},
		{"compare-and-set succeeds on the version read", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{cas("a", "2", 1), 20, 30, Answered, wrote(2)},
			{get("a"), 40, 50, Answered, read("2", 2)},
		}, true},
		{"compare-and-set can't succeed on a stale version", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 30, Answered, wrote(2)},
			{cas("a", "3", 1), 40, 50, Answered, wrote(3)},
		}, false},
		{"a mismatch must report the real version", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{cas("a", "2", 5), 20, 30, Answered, mismatch(1)},
			{cas("a", "2", 5), 40, 50, Answered, mismatch(9)},
		}, false},
		{"a lost compare-and-set applied at most once", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{cas("a", "2", 1), 20, 30, Lost, fsm.Response{}},
			{get("a"), 40, 50, Answered, read("2", 4)},
			{cas("a", "3", 4), 60, 70, Answered, wrote(5)},
		}, true},
		{"versions only grow", []op{
			{put("a", "1"), 0, 10, Answered, wrote(5)},
			{put("a", "2"), 20, 30, Answered, wrote(3)},
		}, false},
		{"delete, then recreate with a new version", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{del("a"), 20, 30, Answered, deleted},
			{get("a"), 40, 50, Answered, notFound},
			{cas("a", "2", 0), 60, 70, Answered, wrote(4)},
			{del("a"), 80, 90, Answered, deleted},
			{del("a"), 100, 110, Answered, notFound},
		}, true},
		{"keys are independent", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 0, 10, Answered, wrote(2)},
			{get("a"), 20, 30, Answered, read("1", 1)},
			{get("b"), 20, 30, Answered, read("2", 2)},
		}, true},
		{"a key with a time-to-live may be found gone", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{get("a"), 20, 30, Answered, read("1", 1)},
			{get("a"), 40, 50, Answered, notFound},
			{get("a"), 60, 70, Answered, notFound},
		}, true},
		{"a key that expired doesn't come back", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{get("a"), 20, 30, Answered, notFound},
			{get("a"), 40, 50, Answered, read("1", 1)},
		}, false},
		{"a key without a time-to-live never just goes", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 30, Answered, wrote(2)},
			{get("a"), 40, 50, Answered, notFound},
		}, false},
		{"create-if-absent succeeds once the old key has expired", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{cas("a", "2", 0), 20, 30, Answered, wrote(2)},
			{get("a"), 40, 50, Answered, read("2", 2)},
		}, true},
		{"a compare-and-set on an expired key reports version 0", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{cas("a", "2", 1), 20, 30, Answered, mismatch(0)},
			{get("a"), 40, 50, Answered, read("1", 1)},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := history(tt.ops...).Linearizable(0)
			if v.Linearizable != tt.want || v.TimedOut {
				t.Errorf("Linearizable = %v (timed out %v), want %v", v.Linearizable, v.TimedOut, tt.want)
			}
		})
	}
}

// A request that never ended is treated like a lost one.
func TestOpenRequestStaysOpen(t *testing.T) {
	h := &History{}
	h.Begin(0, put("a", "1"), 0)
	id := h.Begin(1, get("a"), 20)
	h.End(id, Answered, read("1", 3), 30)
	if v := h.Linearizable(0); !v.Linearizable {
		t.Error("an open write followed by a read of its value should be Linearizable")
	}
	if n := len(h.Operations()); n != 2 {
		t.Errorf("want 2 operations, got %d", n)
	}
}

func TestSignals(t *testing.T) {
	h := history(
		op{put("a", "1"), 0, 10, Answered, wrote(1)},
		op{get("a"), 20, 25, Answered, read("1", 1)},
		op{put("a", "2"), 30, 40, Rejected, fsm.Response{}},
		op{put("a", "2"), 50, 60, Lost, fsm.Response{}},
		op{put("a", "3"), 70, 95, Answered, wrote(4)},
	)
	s := h.Signals
	if s.Answered != 3 || s.Rejected != 1 || s.Lost != 1 {
		t.Errorf("signals = %+v", s)
	}
	if got := s.RecoveryAfter(30); got != 65 {
		t.Errorf("RecoveryAfter(30) = %d, want 65 (next OK write at 95)", got)
	}
	if got := s.RecoveryAfter(100); got != -1 {
		t.Errorf("RecoveryAfter(100) = %d, want -1", got)
	}
	// OK writes at 10 and 95. A mark at 30 falls inside the pause between
	// them, and the whole pause counts.
	if got := s.LongestPauseAfter(30); got != 85 {
		t.Errorf("LongestPauseAfter(30) = %d, want 85", got)
	}
	if got := s.LongestPauseAfter(5); got != 85 {
		t.Errorf("LongestPauseAfter(5) = %d, want 85 (the longer of 5 and 85)", got)
	}
	if got := s.LongestPauseAfter(100); got != -1 {
		t.Errorf("LongestPauseAfter(100) = %d, want -1", got)
	}
}

func TestDiverged(t *testing.T) {
	a := []fsm.Item{{Key: "a", Value: []byte("1"), Version: 1}, {Key: "b", Value: []byte("2"), Version: 2}}
	same := map[core.NodeID][]fsm.Item{1: a, 2: a, 3: a}
	if d := Diverged(same); d != nil {
		t.Errorf("identical Members reported as diverged: %v", d)
	}
	b := []fsm.Item{{Key: "a", Value: []byte("1"), Version: 1}, {Key: "c", Value: []byte("3"), Version: 3}}
	d := Diverged(map[core.NodeID][]fsm.Item{1: a, 2: b})
	if len(d) != 2 {
		t.Errorf("want 2 differences (b missing on node 2, c missing on node 1), got %v", d)
	}
}
