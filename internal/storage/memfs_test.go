package storage

import (
	"errors"
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
// the Store opens without complaint, everything synced before is intact, and
// anything it returns beyond that is a whole Entry that really was written.
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
			t.Fatalf("a torn write was reported as damage: %v", err)
		}
		all := es(1, 8, 1)
		if len(stored.Entries) < 3 || len(stored.Entries) > 8 || !reflect.DeepEqual(stored.Entries, all[:len(stored.Entries)]) {
			t.Fatalf("after a torn write the Store returned %+v", stored.Entries)
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

// Flip every bit of every file, one at a time. Whatever the Store then
// returns must be true: either an error, or the original state, or the
// original state with Entries missing from the end. It must never return an
// Entry, a Term, a vote or a Snapshot that wasn't written.
func TestEveryBitFlipIsDetectedOrHarmless(t *testing.T) {
	fs := NewMemFS()
	const small = 128
	s, _, err := OpenFS(fs, "data", small)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{HardState: &core.HardState{Term: 3, VotedFor: 2}, Entries: es(1, 12, 3)})
	save(t, s, core.Persist{Snapshot: &core.Snapshot{Index: 4, Term: 3, Data: []byte("state at four")}})
	save(t, s, core.Persist{Entries: es(13, 20, 3)})
	_, want, err := OpenFS(fs.Durable(), "data", small)
	if err != nil {
		t.Fatal(err)
	}

	flips, detected, shortened := 0, 0, 0
	for _, path := range fs.paths() {
		for at := range fs.durable[path].synced {
			for bit := range 8 {
				damaged := fs.Durable()
				damaged.durable[path].synced[at] ^= 1 << bit
				damaged.durable[path].data[at] ^= 1 << bit
				flips++
				_, got, err := OpenFS(damaged, "data", small)
				if err != nil {
					detected++
					continue
				}
				if got.HardState != want.HardState || !reflect.DeepEqual(got.Snapshot, want.Snapshot) {
					t.Fatalf("flipping bit %d of byte %d in %s changed the Term, vote or Snapshot without an error", bit, at, path)
				}
				if len(got.Entries) > len(want.Entries) || !reflect.DeepEqual(got.Entries, want.Entries[:len(got.Entries)]) {
					t.Fatalf("flipping bit %d of byte %d in %s returned an Entry that was never written", bit, at, path)
				}
				if len(got.Entries) < len(want.Entries) {
					shortened++
				}
			}
		}
	}
	t.Logf("%d single-bit flips: %d reported as damage, %d taken for a write cut short (Entries dropped from the end)", flips, detected, shortened)
	if detected == 0 || detected+shortened != flips {
		t.Fatalf("%d flips went unnoticed", flips-detected-shortened)
	}
}

// The unchecked format believes a flipped bit. This is what Rung 4 exposed.
func TestUncheckedFormatBelievesDamage(t *testing.T) {
	fs := NewMemFS()
	s, _, err := OpenWith(fs, "data", Options{Unchecked: true})
	if err != nil {
		t.Fatal(err)
	}
	e := core.Entry{Index: 1, Term: 1, Kind: core.EntryCommand, Payload: []byte("k0")}
	save(t, s, core.Persist{Entries: []core.Entry{e}})
	f := fs.durable["data/log/0000000000000001.seg"]
	f.synced[len(f.synced)-2] ^= 1 // 'k' becomes 'j'
	f.data[len(f.data)-2] ^= 1
	_, stored, err := OpenWith(fs.Durable(), "data", Options{Unchecked: true})
	if err != nil || len(stored.Entries) != 1 || string(stored.Entries[0].Payload) != "j0" {
		t.Fatalf("expected the damaged Entry to be read back as j0, got %+v, %v", stored.Entries, err)
	}
	// The checked format refuses the same damage.
	checked := NewMemFS()
	s2, _, _ := OpenFS(checked, "data", 0)
	save(t, s2, core.Persist{Entries: []core.Entry{e}})
	g := checked.durable["data/log/0000000000000001.seg"]
	g.synced[len(g.synced)-2] ^= 1
	g.data[len(g.data)-2] ^= 1
	var corrupt *CorruptError
	if _, _, err := OpenFS(checked.Durable(), "data", 0); !errors.As(err, &corrupt) {
		t.Fatalf("the checked format should report the damage, got %v", err)
	}
}
