package storage

import (
	"math/rand/v2"
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
)

// The same Store code runs on MemFS: a crash keeps what was synced and
// nothing else.
func TestMemFSCrashKeepsOnlyWhatWasSynced(t *testing.T) {
	fs := NewMemFS()
	s, _, err := OpenFS(fs, "data", 0)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{HardState: &core.HardState{Term: 2, VotedFor: 1}, Entries: es(1, 3, 2)})
	// Written but not synced: a write in progress when the power goes.
	if err := s.Write(&core.Persist{HardState: &core.HardState{Term: 9}, Entries: es(4, 6, 2)}); err != nil {
		t.Fatal(err)
	}
	fs.Crash(nil, false)

	_, stored, err := OpenFS(fs, "data", 0)
	if err != nil {
		t.Fatal(err)
	}
	if stored.HardState != (core.HardState{Term: 2, VotedFor: 1}) || !reflect.DeepEqual(stored.Entries, es(1, 3, 2)) {
		t.Fatalf("after the crash: %+v with %d Entries; want Term 2 and Entries 1-3", stored.HardState, len(stored.Entries))
	}
}

// With tearing on, an unsynced write can partly survive. Whatever survives,
// everything synced before it is intact, and no synced Entry is lost.
func TestMemFSTornWritesNeverTouchSyncedData(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	sawPartial := false
	for range 300 {
		fs := NewMemFS()
		s, _, err := OpenFS(fs, "data", 0)
		if err != nil {
			t.Fatal(err)
		}
		save(t, s, core.Persist{Entries: es(1, 3, 1)})
		if err := s.Write(&core.Persist{Entries: es(4, 8, 1)}); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
		before := len(fs.files["data/log/0000000000000001.seg"].synced)
		fs.Crash(rng, true)
		after := fs.files["data/log/0000000000000001.seg"].data
		if len(after) < before {
			t.Fatal("a torn write shortened the synced part of the file")
		}
		if len(after) > before {
			sawPartial = true
		}
		_, stored, err := OpenFS(fs, "data", 0)
		if err != nil {
			continue // without checksums a torn record can be unreadable: Rung 4's subject
		}
		if len(stored.Entries) < 3 || !reflect.DeepEqual(stored.Entries[:3], es(1, 3, 1)) {
			t.Fatalf("synced Entries were damaged: %+v", stored.Entries)
		}
	}
	if !sawPartial {
		t.Fatal("tearing never kept any part of an unsynced write")
	}
}

func TestMemFSFlipBitChangesExactlyOneBit(t *testing.T) {
	fs := NewMemFS()
	s, _, err := OpenFS(fs, "data", 0)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{HardState: &core.HardState{Term: 1}, Entries: es(1, 5, 1)})
	before := fs.Durable()
	if path := fs.FlipBit(rand.New(rand.NewPCG(1, 1))); path == "" {
		t.Fatal("nothing was flipped")
	}
	differing := 0
	for path, f := range fs.durable {
		old := before.durable[path].synced
		for i := range f.synced {
			for b := f.synced[i] ^ old[i]; b != 0; b &= b - 1 {
				differing++
			}
		}
	}
	if differing != 1 {
		t.Fatalf("%d bits differ, want 1", differing)
	}
	if NewMemFS().FlipBit(rand.New(rand.NewPCG(1, 1))) != "" {
		t.Fatal("flipped a bit in an empty filesystem")
	}
}

// The randomized comparison against core.Stored, on MemFS, with a clean
// crash before every reopen.
func TestMemFSMatchesStoredApply(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for round := range 200 {
		fs := NewMemFS()
		const small = 96
		s, _, err := OpenFS(fs, "data", small)
		if err != nil {
			t.Fatal(err)
		}
		var model core.Stored
		for step := range 60 {
			first := core.Index(1)
			if model.Snapshot != nil {
				first = model.Snapshot.Index + 1
			}
			last := first + core.Index(len(model.Entries)) - 1
			var p core.Persist
			switch roll := rng.IntN(10); {
			case roll < 5:
				p.Entries = es(last+1, last+core.Index(1+rng.IntN(6)), core.Term(step+1))
			case roll < 7 && len(model.Entries) > 0:
				at := first + core.Index(rng.IntN(len(model.Entries)))
				p.TruncateFrom = at
				p.Entries = es(at, at+core.Index(rng.IntN(3)), core.Term(step+1))
			case roll < 8 && len(model.Entries) > 0:
				at := first + core.Index(rng.IntN(len(model.Entries)))
				p.Snapshot = &core.Snapshot{Index: at, Term: model.Entries[at-first].Term, Data: []byte{byte(step)}}
			case roll < 9:
				p.Snapshot = &core.Snapshot{Index: last + core.Index(rng.IntN(5)), Term: core.Term(step + 1), Data: []byte{byte(step)}}
				p.ResetLog = true
			default:
				p.HardState = &core.HardState{Term: core.Term(step + 1), VotedFor: core.NodeID(rng.IntN(5))}
			}
			model.Apply(&p)
			save(t, s, p)
			if rng.IntN(4) == 0 {
				fs.Crash(nil, false)
				var got core.Stored
				if s, got, err = OpenFS(fs, "data", small); err != nil {
					t.Fatalf("round %d step %d: %v", round, step, err)
				}
				want := model.Clone()
				if len(want.Entries) == 0 {
					want.Entries = nil
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("round %d step %d: files hold\n%+v\nbut the model says\n%+v", round, step, got, want)
				}
			}
		}
	}
}
