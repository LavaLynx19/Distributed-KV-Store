package core

import (
	"reflect"
	"testing"
)

func es(from, to Index, term Term) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term})
	}
	return out
}

func indexes(entries []Entry) []Index {
	out := []Index{}
	for _, e := range entries {
		out = append(out, e.Index)
	}
	return out
}

func TestStoredApply(t *testing.T) {
	var s Stored
	s.Apply(&Persist{HardState: &HardState{Term: 2, VotedFor: 3}, Entries: es(1, 5, 1)})
	if s.HardState != (HardState{Term: 2, VotedFor: 3}) || !reflect.DeepEqual(indexes(s.Entries), []Index{1, 2, 3, 4, 5}) {
		t.Fatalf("after first write: %+v", s)
	}

	// A conflict: drop 4 and 5, then append new ones.
	s.Apply(&Persist{TruncateFrom: 4, Entries: es(4, 6, 2)})
	if got := indexes(s.Entries); !reflect.DeepEqual(got, []Index{1, 2, 3, 4, 5, 6}) || s.Entries[3].Term != 2 {
		t.Fatalf("after truncate and append: %v, term at 4 = %d", got, s.Entries[3].Term)
	}

	// A Snapshot at 4 covers Entries 1 to 4; 5 and 6 remain.
	s.Apply(&Persist{Snapshot: &Snapshot{Index: 4, Term: 2}})
	if got := indexes(s.Entries); !reflect.DeepEqual(got, []Index{5, 6}) || s.Snapshot.Index != 4 {
		t.Fatalf("after Snapshot: %v", got)
	}
	s.Apply(&Persist{Entries: es(7, 7, 2)})
	s.Apply(&Persist{TruncateFrom: 6})
	if got := indexes(s.Entries); !reflect.DeepEqual(got, []Index{5}) {
		t.Fatalf("after truncating behind a Snapshot: %v", got)
	}

	// An installed Snapshot replaces everything.
	s.Apply(&Persist{Snapshot: &Snapshot{Index: 20, Term: 3}, ResetLog: true, Entries: es(21, 22, 3)})
	if got := indexes(s.Entries); !reflect.DeepEqual(got, []Index{21, 22}) {
		t.Fatalf("after installing a Snapshot: %v", got)
	}
}

func TestStoredApplyRejectsAGap(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("appending Entry 3 to an empty Log should panic")
		}
	}()
	var s Stored
	s.Apply(&Persist{Entries: es(3, 3, 1)})
}

func TestStoredCloneIsIndependent(t *testing.T) {
	var s Stored
	s.Apply(&Persist{Entries: es(1, 2, 1), Snapshot: nil})
	c := s.Clone()
	c.Entries[0].Term = 9
	c.Apply(&Persist{Entries: es(3, 3, 1)})
	if s.Entries[0].Term != 1 || len(s.Entries) != 2 {
		t.Fatalf("changing the clone changed the original: %+v", s.Entries)
	}
}
