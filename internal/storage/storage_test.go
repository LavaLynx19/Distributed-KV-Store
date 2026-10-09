package storage

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
)

func es(from, to core.Index, term core.Term) []core.Entry {
	var out []core.Entry
	for i := from; i <= to; i++ {
		out = append(out, core.Entry{Index: i, Term: term, Kind: core.EntryCommand, Payload: []byte{byte(i), byte(i >> 8)}})
	}
	return out
}

// reopen closes s and opens the directory again, as after a restart.
func reopen(t *testing.T, s *Store, dir string, segmentBytes int64) (*Store, core.Stored) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, stored, err := Open(dir, segmentBytes)
	if err != nil {
		t.Fatal(err)
	}
	return s2, stored
}

func save(t *testing.T, s *Store, p core.Persist) {
	t.Helper()
	if err := s.Save(&p); err != nil {
		t.Fatal(err)
	}
}

func segmentCount(t *testing.T, dir string) int {
	t.Helper()
	names, err := os.ReadDir(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	return len(names)
}

func TestEmptyDirectory(t *testing.T) {
	_, stored, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if stored.HardState != (core.HardState{}) || stored.Snapshot != nil || len(stored.Entries) != 0 {
		t.Fatalf("a new directory holds %+v", stored)
	}
}

func TestSaveThenOpen(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{HardState: &core.HardState{Term: 3, VotedFor: 2}, Entries: es(1, 5, 3)})
	save(t, s, core.Persist{Entries: es(6, 6, 3)})
	s, stored := reopen(t, s, dir, 0)
	defer s.Close()

	if stored.HardState != (core.HardState{Term: 3, VotedFor: 2}) {
		t.Errorf("hard state = %+v", stored.HardState)
	}
	if !reflect.DeepEqual(stored.Entries, es(1, 6, 3)) {
		t.Errorf("entries = %+v", stored.Entries)
	}
	// The reopened Store carries on where the old one stopped.
	save(t, s, core.Persist{Entries: es(7, 7, 4)})
	_, stored = reopen(t, s, dir, 0)
	if len(stored.Entries) != 7 || stored.Entries[6].Term != 4 {
		t.Errorf("after appending to a reopened Store: %d Entries", len(stored.Entries))
	}
}

func TestTruncateAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	const small = 64 // a few records per segment
	s, _, err := Open(dir, small)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{Entries: es(1, 40, 1)})
	if n := segmentCount(t, dir); n < 4 {
		t.Fatalf("expected several segments, got %d", n)
	}
	save(t, s, core.Persist{TruncateFrom: 13, Entries: es(13, 15, 2)})
	s, stored := reopen(t, s, dir, small)
	defer s.Close()
	want := append(es(1, 12, 1), es(13, 15, 2)...)
	if !reflect.DeepEqual(stored.Entries, want) {
		t.Fatalf("after truncating at 13: got %d Entries ending at Term %d", len(stored.Entries), stored.Entries[len(stored.Entries)-1].Term)
	}
}

func TestSnapshotTrimsTheLog(t *testing.T) {
	dir := t.TempDir()
	const small = 64
	s, _, err := Open(dir, small)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{Entries: es(1, 40, 1)})
	before := segmentCount(t, dir)
	save(t, s, core.Persist{Snapshot: &core.Snapshot{Index: 30, Term: 1, Data: []byte("state at 30")}})
	if after := segmentCount(t, dir); after >= before {
		t.Fatalf("a Snapshot at 30 of 40 should delete old segments: %d before, %d after", before, after)
	}
	s, stored := reopen(t, s, dir, small)
	if stored.Snapshot == nil || stored.Snapshot.Index != 30 || string(stored.Snapshot.Data) != "state at 30" {
		t.Fatalf("snapshot = %+v", stored.Snapshot)
	}
	if !reflect.DeepEqual(stored.Entries, es(31, 40, 1)) {
		t.Fatalf("entries after the Snapshot: %d, first %d", len(stored.Entries), stored.Entries[0].Index)
	}

	// An installed Snapshot replaces the whole Log.
	save(t, s, core.Persist{Snapshot: &core.Snapshot{Index: 100, Term: 5, Data: []byte("installed")}, ResetLog: true, Entries: es(101, 102, 5)})
	s, stored = reopen(t, s, dir, small)
	defer s.Close()
	if stored.Snapshot.Index != 100 || !reflect.DeepEqual(stored.Entries, es(101, 102, 5)) {
		t.Fatalf("after installing: snapshot %d, %d Entries", stored.Snapshot.Index, len(stored.Entries))
	}
}

// A crash part way through a write leaves an incomplete record at the end.
// It is dropped; everything before it is kept.
func TestIncompleteLastRecordIsDropped(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{Entries: es(1, 3, 1)})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := s.segmentPath(1)
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{1, 3, 5, len(encodeRecord(es(3, 3, 1)[0], true)) - 1} {
		if err := os.WriteFile(path, whole[:len(whole)-cut], 0o644); err != nil {
			t.Fatal(err)
		}
		s2, stored, err := Open(dir, 0)
		if err != nil {
			t.Fatalf("cut %d bytes: %v", cut, err)
		}
		if !reflect.DeepEqual(stored.Entries, es(1, 2, 1)) {
			t.Fatalf("cut %d bytes: %d Entries survive, want 2", cut, len(stored.Entries))
		}
		// The Store is usable, and the dropped Entry can be written again.
		save(t, s2, core.Persist{Entries: es(3, 3, 2)})
		_, stored = reopen(t, s2, dir, 0)
		if len(stored.Entries) != 3 || stored.Entries[2].Term != 2 {
			t.Fatalf("cut %d bytes: rewrite gave %+v", cut, stored.Entries)
		}
	}
}

// Whatever sequence of changes a core asks for, the files and core.Stored
// must agree on the result.
func TestMatchesStoredApply(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for round := range 30 {
		dir := t.TempDir()
		const small = 96
		s, _, err := Open(dir, small)
		if err != nil {
			t.Fatal(err)
		}
		var model core.Stored
		last := func() core.Index {
			first := core.Index(1)
			if model.Snapshot != nil {
				first = model.Snapshot.Index + 1
			}
			return first + core.Index(len(model.Entries)) - 1
		}
		for step := range 60 {
			var p core.Persist
			first := last() - core.Index(len(model.Entries)) + 1
			switch roll := rng.IntN(10); {
			case roll < 5:
				p.Entries = es(last()+1, last()+core.Index(1+rng.IntN(6)), core.Term(step+1))
			case roll < 7 && len(model.Entries) > 0:
				at := first + core.Index(rng.IntN(len(model.Entries)))
				p.TruncateFrom = at
				p.Entries = es(at, at+core.Index(rng.IntN(3)), core.Term(step+1))
			case roll < 8 && len(model.Entries) > 0:
				at := first + core.Index(rng.IntN(len(model.Entries)))
				p.Snapshot = &core.Snapshot{Index: at, Term: model.Entries[at-first].Term, Data: []byte{byte(step)}}
			case roll < 9:
				at := last() + core.Index(rng.IntN(5))
				p.Snapshot = &core.Snapshot{Index: at, Term: core.Term(step + 1), Data: []byte{byte(step)}}
				p.ResetLog = true
			default:
				p.HardState = &core.HardState{Term: core.Term(step + 1), VotedFor: core.NodeID(rng.IntN(5))}
			}
			model.Apply(&p)
			save(t, s, p)
			if rng.IntN(4) == 0 {
				var got core.Stored
				s, got = reopen(t, s, dir, small)
				want := model.Clone()
				if len(want.Entries) == 0 {
					want.Entries = nil
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("round %d step %d: files hold\n%+v\nbut the model says\n%+v", round, step, got, want)
				}
			}
		}
		s.Close()
	}
}

// A Snapshot's Member list comes back with it, and a Membership change Entry
// comes back as what it was. A Snapshot with no list is stored exactly as it
// was before lists existed.
func TestMemberListsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	change := core.Entry{Index: 3, Term: 2, Kind: core.EntryMembers, Payload: []byte{2, 1, 4}}
	save(t, s, core.Persist{Entries: append(es(1, 2, 1), change)})
	save(t, s, core.Persist{Snapshot: &core.Snapshot{Index: 2, Term: 1, Data: []byte("state"), Members: []core.NodeID{1, 2, 5}}})
	s, stored := reopen(t, s, dir, 0)
	want := &core.Snapshot{Index: 2, Term: 1, Data: []byte("state"), Members: []core.NodeID{1, 2, 5}}
	if !reflect.DeepEqual(stored.Snapshot, want) {
		t.Fatalf("snapshot = %+v, want %+v", stored.Snapshot, want)
	}
	if len(stored.Entries) != 1 || !reflect.DeepEqual(stored.Entries[0], change) {
		t.Fatalf("entries = %+v, want the Membership change", stored.Entries)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	plain := &core.Snapshot{Index: 7, Term: 3, Data: []byte("state")}
	if raw := encodeSnapshot(plain); len(raw) != 16+5 || raw[8] != 0 {
		t.Fatalf("a Snapshot with no Member list encoded as %v", raw)
	}
	if got := decodeSnapshot(encodeSnapshot(plain)); !reflect.DeepEqual(got, plain) {
		t.Fatalf("round trip gave %+v", got)
	}
	// A header that promises more Members than the file holds is damage.
	short := encodeSnapshot(want)[:20]
	if readableSnapshot(short) {
		t.Fatal("a Snapshot cut off inside its Member list was accepted")
	}
}
