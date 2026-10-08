package storage

import (
	"errors"
	"fmt"
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

// Flip every bit of every file, one at a time, and open the directory. What
// comes back must be true and must own up to anything missing:
//   - the Term and vote are always the ones written, since there are two
//     copies;
//   - the Snapshot is the one written, or absent;
//   - the Entries are the ones written, possibly with some missing from the
//     end, and never one that wasn't written;
//   - if anything is missing, Damaged is set.
func TestEveryBitFlipIsRepairedAndOwnedUpTo(t *testing.T) {
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
	if err != nil || want.Damaged {
		t.Fatalf("setup: %v, damaged %v", err, want.Damaged)
	}

	flips, harmless, logCut, snapshotLost := 0, 0, 0, 0
	for _, path := range fs.paths() {
		for at := range fs.durable[path].synced {
			for bit := range 8 {
				damaged := fs.Durable()
				damaged.durable[path].synced[at] ^= 1 << bit
				damaged.durable[path].data[at] ^= 1 << bit
				flips++
				where := fmt.Sprintf("bit %d of byte %d in %s", bit, at, path)

				reopened, got, err := OpenFS(damaged, "data", small)
				if err != nil {
					t.Fatalf("%s: one flipped bit made the directory unopenable: %v", where, err)
				}
				if got.HardState != want.HardState {
					t.Fatalf("%s: Term and vote came back as %+v", where, got.HardState)
				}
				complete := reflect.DeepEqual(got.Snapshot, want.Snapshot) && reflect.DeepEqual(got.Entries, want.Entries)
				switch {
				case complete:
					harmless++
				case got.Snapshot == nil:
					snapshotLost++
					if len(got.Entries) != 0 {
						t.Fatalf("%s: the Snapshot is gone but %d Entries remain", where, len(got.Entries))
					}
				default:
					logCut++
					if !reflect.DeepEqual(got.Snapshot, want.Snapshot) || len(got.Entries) >= len(want.Entries) ||
						(len(got.Entries) > 0 && !reflect.DeepEqual(got.Entries, want.Entries[:len(got.Entries)])) {
						t.Fatalf("%s: returned something that was never written", where)
					}
				}
				if complete == got.Damaged {
					t.Fatalf("%s: complete=%v but Damaged=%v", where, complete, got.Damaged)
				}

				// The repaired directory is usable, and stays marked until
				// the core says it has recovered.
				last := core.Index(0)
				if got.Snapshot != nil {
					last = got.Snapshot.Index
				}
				last += core.Index(len(got.Entries))
				save(t, reopened, core.Persist{Entries: es(last+1, last+1, 9)})
				_, again, err := OpenFS(damaged.Durable(), "data", small)
				if err != nil || again.Damaged != got.Damaged || len(again.Entries) != len(got.Entries)+1 {
					t.Fatalf("%s: after repair and one more write: %v, damaged %v, %d Entries", where, err, again.Damaged, len(again.Entries))
				}
				if got.Damaged {
					save(t, reopened, core.Persist{Recovered: true})
					if _, cleared, _ := OpenFS(damaged.Durable(), "data", small); cleared.Damaged {
						t.Fatalf("%s: still marked damaged after Recovered", where)
					}
				}
			}
		}
	}
	t.Logf("%d single-bit flips: %d harmless (a spare copy of the Term and vote), %d cut the Log, %d lost the Snapshot", flips, harmless, logCut, snapshotLost)
	if harmless == 0 || logCut == 0 || snapshotLost == 0 {
		t.Fatal("expected all three outcomes")
	}
}

// Both copies of the Term and vote damaged: the directory must not open.
func TestBothStateCopiesDamagedRefusesToOpen(t *testing.T) {
	fs := NewMemFS()
	s, _, err := OpenFS(fs, "data", 0)
	if err != nil {
		t.Fatal(err)
	}
	save(t, s, core.Persist{HardState: &core.HardState{Term: 3, VotedFor: 2}, Entries: es(1, 3, 3)})
	for _, name := range stateCopies {
		f := fs.durable["data/"+name]
		f.synced[0] ^= 1
		f.data[0] ^= 1
	}
	var corrupt *CorruptError
	if _, _, err := OpenFS(fs.Durable(), "data", 0); !errors.As(err, &corrupt) {
		t.Fatalf("want a CorruptError, got %v", err)
	}
}

// A crash between replacing the two copies leaves them different. The newer
// one wins: the higher Term, or the one that has a vote.
func TestNewerStateCopyWins(t *testing.T) {
	for _, tt := range []struct {
		name     string
		old, new core.HardState
	}{
		{"higher Term", core.HardState{Term: 3, VotedFor: 2}, core.HardState{Term: 4}},
		{"vote added", core.HardState{Term: 4}, core.HardState{Term: 4, VotedFor: 1}},
	} {
		fs := NewMemFS()
		s, _, err := OpenFS(fs, "data", 0)
		if err != nil {
			t.Fatal(err)
		}
		save(t, s, core.Persist{HardState: &tt.old})
		// Only the first copy gets the new value before the "crash".
		if err := s.replaceFile(stateCopies[0], encodeState(tt.new)); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		_, stored, err := OpenFS(fs.Durable(), "data", 0)
		if err != nil || stored.HardState != tt.new || stored.Damaged {
			t.Fatalf("%s: got %+v, damaged %v, %v; want %+v", tt.name, stored.HardState, stored.Damaged, err, tt.new)
		}
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
	_, got, err := OpenFS(checked.Durable(), "data", 0)
	if err != nil || !got.Damaged || len(got.Entries) != 0 {
		t.Fatalf("the checked format should drop the damaged Entry and say so, got %+v, %v", got, err)
	}
}
