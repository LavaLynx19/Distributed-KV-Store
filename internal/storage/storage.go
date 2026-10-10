// Package storage keeps a Member's durable state in files (A§5.4): its Term
// and vote, its Snapshot, and its Log.
//
// Layout of a data directory:
//
//	state.a state.b  Term and vote, 16 bytes, twice: either copy is enough
//	snapshot         Index, Term, then the state machine's data
//	log/<first>.seg  Log segments, named by the Index of their first Entry
//	damaged          present from when damage is found until the Member has
//	                 recovered (A§6.8)
//
// state and snapshot are replaced whole: written to a temporary file, synced,
// and renamed over the old one, and end with a CRC-32C of their contents.
//
// A segment is a run of records. Each record is (A§5.4):
//
//	length    4 bytes   size of the body
//	checksum  4 bytes   CRC-32C of the length
//	checksum  4 bytes   CRC-32C of the body
//	body                one encoded Entry
//	end mark  1 byte    0xA5
//
// Open verifies everything. It tells two cases apart:
//
//   - A record that was never completely written: the file ends inside it,
//     or zeros sit where its end should be, with nothing after. Only the last
//     write before a crash can look like this. It was never synced, so
//     nothing in it was acknowledged, and it is dropped.
//   - A record that was complete and no longer matches its checksums. This
//     is damage, and Open reports it as a *CorruptError without returning
//     the record as data.
//
// The length has its own checksum because a damaged length would otherwise
// make a record seem to run past the end of the file, and so pass for the
// first case. The end mark is non-zero because a file can grow before its
// data arrives, leaving zeros that a crash then makes permanent.
//
// Options.Unchecked selects the format Rung 3 used, with no checksums, in
// which a complete but damaged record is read back as if valid. It exists so
// that Rung 4's exposure of that stays reproducible.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"distributed-kv-store/internal/core"
)

// DefaultSegmentBytes is the size at which a new segment is started.
const DefaultSegmentBytes = 4 << 20

// Options adjust how a data directory is opened.
type Options struct {
	// SegmentBytes is the size at which a new segment is started. Zero means
	// DefaultSegmentBytes.
	SegmentBytes int64
	// Unchecked reads and writes the Rung 3 format, with no checksums.
	Unchecked bool
}

// CorruptError reports stored data that doesn't match its checksum, or that
// can't be what the Store wrote.
type CorruptError struct {
	Path   string
	Offset int64
	Detail string

	// For a damaged Log: the segment it is in, and whether the damage is to
	// the Log's structure, so that none of it can be kept.
	segment  core.Index
	wholeLog bool
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("storage: %s is damaged at byte %d: %s", e.Path, e.Offset, e.Detail)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// sum is the CRC-32C of the parts, as if they were one run of bytes.
func sum(parts ...[]byte) uint32 {
	var c uint32
	for _, p := range parts {
		c = crc32.Update(c, castagnoli, p)
	}
	return c
}

// position is where an Entry's record starts.
type position struct {
	segment core.Index // the segment's first Index, which names its file
	offset  int64
}

// Store is one Member's data directory. It is not safe for concurrent use.
type Store struct {
	fs           FS
	dir          string
	segmentBytes int64
	checked      bool

	snapshotTerm core.Term // Term the Snapshot ends on, read at Open

	first     core.Index // Index of positions[0]
	positions []position // one per stored Entry still in use

	active     File // the last segment, open for appending
	activeName core.Index
	activeSize int64
	out        *bufio.Writer

	dirtyFile bool // active has unsynced writes
	dirtyDir  bool // files were created, renamed or removed
}

// Open reads a data directory on the operating system's filesystem, creating
// it if needed, and returns what it holds. segmentBytes of 0 means
// DefaultSegmentBytes.
func Open(dir string, segmentBytes int64) (*Store, core.Stored, error) {
	return OpenWith(OSFS{}, dir, Options{SegmentBytes: segmentBytes})
}

// OpenFS is Open on any filesystem.
func OpenFS(fs FS, dir string, segmentBytes int64) (*Store, core.Stored, error) {
	return OpenWith(fs, dir, Options{SegmentBytes: segmentBytes})
}

// OpenWith is Open on any filesystem, with Options.
//
// If part of what the directory holds is damaged, OpenWith repairs the
// directory by removing what it can't verify, and returns the rest with
// Stored.Damaged set (A§6.8):
//
//   - A damaged Log record: that record and everything after it go.
//   - A damaged Snapshot: it goes, and the whole Log with it, since the Log
//     means nothing without the Snapshot it follows.
//
// Before removing anything it leaves a durable mark, so that a crash during
// the repair, or a restart after it, still reports the damage. The mark
// stays until a Persist with Recovered set removes it.
//
// The Term and vote are kept in two copies, and either one is enough. If
// both are damaged OpenWith returns a *CorruptError and no Store: a Member
// that can't say how it voted must not start (Decision Log).
func OpenWith(fs FS, dir string, opts Options) (*Store, core.Stored, error) {
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if err := fs.MkdirAll(filepath.Join(dir, "log")); err != nil {
		return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
	}
	s := &Store{fs: fs, dir: dir, segmentBytes: opts.SegmentBytes, checked: !opts.Unchecked, first: 1}
	var stored core.Stored
	var err error

	if stored.HardState, err = s.readState(); err != nil {
		return nil, core.Stored{}, err
	}
	if s.checked {
		if _, err := fs.ReadFile(filepath.Join(dir, damageMark)); err == nil {
			stored.Damaged = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
		}
	}

	raw, err := s.readWhole("snapshot")
	var corrupt *CorruptError
	switch {
	case errors.As(err, &corrupt) || (err == nil && raw != nil && !readableSnapshot(raw)):
		if err := s.markDamaged(&stored); err != nil {
			return nil, core.Stored{}, err
		}
		if err := s.fs.Remove(filepath.Join(dir, "snapshot")); err != nil {
			return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
		}
		if err := s.dropThrough(0, true); err != nil {
			return nil, core.Stored{}, err
		}
		s.first = 1
	case err != nil:
		return nil, core.Stored{}, err
	case raw != nil:
		stored.Snapshot = decodeSnapshot(raw)
		s.first = stored.Snapshot.Index + 1
		s.snapshotTerm = stored.Snapshot.Term
	}

	stored.Entries, err = s.load()
	if errors.As(err, &corrupt) && s.checked {
		// Cut the Log just before the damage and read it again.
		if err := s.markDamaged(&stored); err != nil {
			return nil, core.Stored{}, err
		}
		if err := s.cutLog(corrupt); err != nil {
			return nil, core.Stored{}, err
		}
		stored.Entries, err = s.load()
	}
	if err != nil {
		return nil, core.Stored{}, err
	}
	if stored.Forced, err = s.readForced(); err != nil {
		return nil, core.Stored{}, err
	}
	if err := s.Sync(); err != nil {
		return nil, core.Stored{}, err
	}
	return s, stored, nil
}

// forcedFile holds the Member list an operator imposed by Unsafe recovery
// (A§6.6): the Index of the last Entry the Member held at the time, a
// 4-byte count, and 8 bytes per Member.
const forcedFile = "forced"

func (s *Store) readForced() (*core.ForcedMembers, error) {
	raw, err := s.readWhole(forcedFile)
	if err != nil || raw == nil {
		return nil, err // a damaged file is an error: nothing else says who the Members are
	}
	if len(raw) < 12 || uint64(len(raw)-12) != 8*uint64(binary.BigEndian.Uint32(raw[8:12])) {
		return nil, &CorruptError{Path: filepath.Join(s.dir, forcedFile), Detail: "not a forced Member list"}
	}
	f := &core.ForcedMembers{At: core.Index(binary.BigEndian.Uint64(raw[:8]))}
	for rest := raw[12:]; len(rest) > 0; rest = rest[8:] {
		f.Members = append(f.Members, core.NodeID(binary.BigEndian.Uint64(rest[:8])))
	}
	return f, nil
}

// Recovery is what ForceMembers found and did.
type Recovery struct {
	// LastIndex and LastTerm are the last Entry this Member holds. Anything
	// the Group Committed after it is gone unless another survivor has it.
	LastIndex core.Index
	LastTerm  core.Term
	// Term is the latest Term the Member had seen.
	Term core.Term
	// WasDamaged: the Member had found damage on its disk and not yet
	// recovered (A§6.8). It may be missing Entries it once acknowledged,
	// and it will now vote all the same.
	WasDamaged bool
	// Stored is everything the Member held, for the caller to describe.
	Stored core.Stored
}

// ForceMembers is Unsafe recovery (A§6.6). It writes members to the
// directory of a stopped Member as its Member list, overriding every
// Membership change in its Log, and clears the mark that keeps a damaged
// Member out of elections. The Member will then act on that list when it
// starts. Nothing is removed from its Log.
//
// It is unsafe because it can't know what the rest of the Group Committed.
// It must never be run while the Member is running, and the Members left
// out must never be started again with their old data.
func ForceMembers(fs FS, dir string, members []core.NodeID, opts Options) (Recovery, error) {
	if len(members) == 0 {
		return Recovery{}, errors.New("storage: a Group needs at least one Member")
	}
	s, stored, err := OpenWith(fs, dir, opts)
	if err != nil {
		return Recovery{}, err
	}
	rec := Recovery{Term: stored.HardState.Term, WasDamaged: stored.Damaged, Stored: stored}
	if snap := stored.Snapshot; snap != nil {
		rec.LastIndex, rec.LastTerm = snap.Index, snap.Term
	}
	if n := len(stored.Entries); n > 0 {
		rec.LastIndex, rec.LastTerm = stored.Entries[n-1].Index, stored.Entries[n-1].Term
	}
	raw := binary.BigEndian.AppendUint64(nil, uint64(rec.LastIndex))
	raw = binary.BigEndian.AppendUint32(raw, uint32(len(members)))
	for _, m := range members {
		raw = binary.BigEndian.AppendUint64(raw, uint64(m))
	}
	if err := s.replaceFile(forcedFile, raw); err != nil {
		return Recovery{}, err
	}
	if err := s.Write(&core.Persist{Recovered: true}); err != nil {
		return Recovery{}, err
	}
	if err := s.Sync(); err != nil {
		return Recovery{}, err
	}
	return rec, s.Close()
}

// DropData empties a Member's directory of its Log and Snapshot and keeps
// its Term and vote. It is for a Node that is no longer a Member of the
// Group (A§11.11). The Term and vote stay because the Node may one day be
// added to the same Group again under the same id, and must not then vote a
// second time in a Term it voted in before. The Member must not be running.
func DropData(fs FS, dir string, opts Options) error {
	// Opening first makes sure the Term and vote can be read back.
	s, _, err := OpenWith(fs, dir, opts)
	if err != nil {
		return err
	}
	if err := s.Close(); err != nil {
		return err
	}
	names, err := fs.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasPrefix(name, "state") {
			continue
		}
		if err := fs.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return fs.SyncDir(dir)
}

// damageMark is the file whose presence says the directory was found
// damaged and the Member hasn't recovered yet.
const damageMark = "damaged"

// markDamaged records, durably, that damage was found. It must be on disk
// before anything is removed: otherwise a crash during the repair would
// leave a directory that looks whole and is missing data.
func (s *Store) markDamaged(stored *core.Stored) error {
	if stored.Damaged {
		return nil
	}
	stored.Damaged = true
	if err := s.replaceFile(damageMark, nil); err != nil {
		return err
	}
	return s.Sync()
}

// cutLog removes the damaged record at c and everything after it: later
// segments first, then the rest of the segment holding it.
func (s *Store) cutLog(c *CorruptError) error {
	if err := s.closeActive(); err != nil {
		return err
	}
	firsts, err := s.segments()
	if err != nil {
		return err
	}
	for i := len(firsts) - 1; i >= 0 && firsts[i] > c.segment; i-- {
		if err := s.fs.Remove(s.segmentPath(firsts[i])); err != nil {
			return fmt.Errorf("storage: %w", err)
		}
	}
	if c.wholeLog {
		for _, first := range firsts {
			if first <= c.segment {
				if err := s.fs.Remove(s.segmentPath(first)); err != nil {
					return fmt.Errorf("storage: %w", err)
				}
			}
		}
	} else if err := s.fs.Truncate(s.segmentPath(c.segment), c.Offset); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	s.positions = nil
	s.dirtyDir = true
	return nil
}

// The Term and vote are stored twice, in files replaced one after the other,
// so that damage to one leaves the other (A§5.4).
var stateCopies = [2]string{"state.a", "state.b"}

func decodeState(raw []byte) (core.HardState, bool) {
	if len(raw) != 16 {
		return core.HardState{}, false
	}
	return core.HardState{
		Term:     core.Term(binary.BigEndian.Uint64(raw[:8])),
		VotedFor: core.NodeID(binary.BigEndian.Uint64(raw[8:])),
	}, true
}

func encodeState(h core.HardState) []byte {
	raw := make([]byte, 16)
	binary.BigEndian.PutUint64(raw[:8], uint64(h.Term))
	binary.BigEndian.PutUint64(raw[8:], uint64(h.VotedFor))
	return raw
}

// readState returns the Term and vote. With checksums it reads both copies,
// takes the newer of those that verify, and rewrites a copy that is damaged
// or behind. It fails only if a copy exists and none verifies.
func (s *Store) readState() (core.HardState, error) {
	if !s.checked {
		raw, err := s.readWhole("state")
		if err != nil || raw == nil {
			return core.HardState{}, err
		}
		h, _ := decodeState(raw)
		return h, nil
	}
	var best core.HardState
	var found, bad int
	var good [2]bool
	var states [2]core.HardState
	for i, name := range stateCopies {
		raw, err := s.readWhole(name)
		var corrupt *CorruptError
		switch {
		case errors.As(err, &corrupt):
			bad++
			continue
		case err != nil:
			return core.HardState{}, err
		case raw == nil:
			continue
		}
		h, ok := decodeState(raw)
		if !ok {
			bad++
			continue
		}
		good[i], states[i] = true, h
		// A vote is only ever added within a Term, so the copy with the
		// higher Term, or with a vote where the other has none, is newer.
		if found == 0 || h.Term > best.Term || (h.Term == best.Term && best.VotedFor == 0) {
			best = h
		}
		found++
	}
	if found == 0 {
		if bad > 0 {
			return core.HardState{}, &CorruptError{Path: filepath.Join(s.dir, "state.*"), Detail: "no copy of the Term and vote can be verified"}
		}
		return core.HardState{}, nil
	}
	for i, name := range stateCopies {
		if !good[i] || states[i] != best {
			if err := s.replaceFile(name, encodeState(best)); err != nil {
				return core.HardState{}, err
			}
		}
	}
	return best, nil
}

// readWhole returns the contents of a file that is replaced whole, with its
// checksum verified and removed, or nil if the file doesn't exist.
func (s *Store) readWhole(name string) ([]byte, error) {
	path := filepath.Join(s.dir, name)
	raw, err := s.fs.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	if !s.checked {
		return raw, nil
	}
	if len(raw) < 4 {
		return nil, &CorruptError{Path: path, Detail: "too short to hold a checksum"}
	}
	body, want := raw[:len(raw)-4], binary.BigEndian.Uint32(raw[len(raw)-4:])
	if sum(body) != want {
		return nil, &CorruptError{Path: path, Detail: "checksum mismatch"}
	}
	return body, nil
}

// segments lists the first Index of every segment file, in order.
func (s *Store) segments() ([]core.Index, error) {
	names, err := s.fs.ReadDir(filepath.Join(s.dir, "log"))
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	var firsts []core.Index
	for _, name := range names {
		var first uint64
		if strings.HasSuffix(name, ".seg") {
			if _, err := fmt.Sscanf(name, "%016x.seg", &first); err == nil {
				firsts = append(firsts, core.Index(first))
			}
		}
	}
	slices.Sort(firsts)
	return firsts, nil
}

func (s *Store) segmentPath(first core.Index) string {
	return filepath.Join(s.dir, "log", fmt.Sprintf("%016x.seg", uint64(first)))
}

// load reads every segment, drops Entries the Snapshot already covers and an
// incomplete record at the very end, and opens the last segment for
// appending.
func (s *Store) load() ([]core.Entry, error) {
	firsts, err := s.segments()
	if err != nil {
		return nil, err
	}
	var entries []core.Entry
	next := core.Index(0) // the Index the next record must have; 0 before the first
	for i, first := range firsts {
		raw, err := s.fs.ReadFile(s.segmentPath(first))
		if err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
		var offset int64
		for offset < int64(len(raw)) {
			e, size, err := readRecord(raw[offset:], s.checked)
			if errors.Is(err, errChecksum) || (errors.Is(err, errShort) && s.checked && i < len(firsts)-1) {
				// Damage, or an incomplete record that isn't the last
				// thing in the Log, which no crash can produce.
				return nil, &CorruptError{Path: s.segmentPath(first), Offset: offset, Detail: err.Error(), segment: first}
			}
			if errors.Is(err, errShort) && i == len(firsts)-1 {
				// A crash cut the last write short. Nothing after it can
				// have been acknowledged, so it is dropped.
				if err := s.fs.Truncate(s.segmentPath(first), offset); err != nil {
					return nil, fmt.Errorf("storage: %w", err)
				}
				break
			}
			if err != nil {
				return nil, &CorruptError{Path: s.segmentPath(first), Offset: offset, Detail: err.Error(), segment: first}
			}
			if next != 0 && e.Index != next {
				return nil, &CorruptError{Path: s.segmentPath(first), Offset: offset, segment: first,
					Detail: fmt.Sprintf("holds Entry %d where %d was expected", e.Index, next)}
			}
			next = e.Index + 1
			if e.Index >= s.first {
				if len(entries) == 0 && e.Index != s.first {
					// Records that verify but don't follow the Snapshot:
					// files are missing, and nothing here can be placed.
					return nil, &CorruptError{Path: s.segmentPath(first), Offset: offset, segment: first, wholeLog: true,
						Detail: fmt.Sprintf("the Log starts at Entry %d but the Snapshot ends at %d", e.Index, s.first-1)}
				}
				entries = append(entries, e)
				s.positions = append(s.positions, position{segment: first, offset: offset})
			}
			offset += size
		}
		if i == len(firsts)-1 {
			if err := s.openActive(first, offset); err != nil {
				return nil, err
			}
		}
	}

	// A crash between saving a Snapshot and trimming the Log can leave
	// Entries the Snapshot has made obsolete: all of them covered, or a tail
	// from an older Term than the Snapshot ends on, which can't follow it.
	stale := len(entries) > 0 && s.first > 1 && entries[0].Term < s.snapshotTerm
	if len(firsts) > 0 && (len(entries) == 0 || stale) {
		if err := s.dropThrough(s.first-1, true); err != nil {
			return nil, err
		}
		entries = nil
	}
	return entries, nil
}

func (s *Store) openActive(first core.Index, size int64) error {
	f, err := s.fs.Append(s.segmentPath(first), size)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	s.active, s.activeName, s.activeSize = f, first, size
	s.out = bufio.NewWriter(f)
	return nil
}

func (s *Store) closeActive() error {
	if s.active == nil {
		return nil
	}
	if err := s.out.Flush(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	if s.dirtyFile {
		if err := s.active.Sync(); err != nil {
			return fmt.Errorf("storage: %w", err)
		}
		s.dirtyFile = false
	}
	err := s.active.Close()
	s.active, s.out = nil, nil
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return nil
}

// Save makes p durable. When it returns nil, a later Open will see the change.
func (s *Store) Save(p *core.Persist) error {
	if err := s.Write(p); err != nil {
		return err
	}
	return s.Sync()
}

// Write applies p to the files without forcing it to disk. Several Writes
// can share one Sync.
func (s *Store) Write(p *core.Persist) error {
	if p.HardState != nil {
		names := stateCopies[:]
		if !s.checked {
			names = []string{"state"}
		}
		for _, name := range names {
			if err := s.replaceFile(name, encodeState(*p.HardState)); err != nil {
				return err
			}
		}
	}
	if p.Snapshot != nil {
		if err := s.replaceFile("snapshot", encodeSnapshot(p.Snapshot)); err != nil {
			return err
		}
		if err := s.dropThrough(p.Snapshot.Index, p.ResetLog); err != nil {
			return err
		}
	}
	if p.TruncateFrom != 0 {
		if err := s.truncateFrom(p.TruncateFrom); err != nil {
			return err
		}
	}
	for _, e := range p.Entries {
		if err := s.append(e); err != nil {
			return err
		}
	}
	if p.Recovered {
		err := s.fs.Remove(filepath.Join(s.dir, damageMark))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: %w", err)
		}
		s.dirtyDir = true
	}
	return nil
}

// Flush hands everything written so far to the filesystem without forcing it
// to disk. Nothing needs it for correctness: Sync flushes first. It marks
// the moment a write is on its way but not yet durable, which is when a
// crash can tear it, and the Simulation uses it for that.
func (s *Store) Flush() error {
	if s.out != nil {
		if err := s.out.Flush(); err != nil {
			return fmt.Errorf("storage: %w", err)
		}
	}
	return nil
}

// Sync forces everything written so far to disk.
func (s *Store) Sync() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if s.dirtyFile {
		if err := s.active.Sync(); err != nil {
			return fmt.Errorf("storage: %w", err)
		}
		s.dirtyFile = false
	}
	if s.dirtyDir {
		for _, d := range []string{s.dir, filepath.Join(s.dir, "log")} {
			if err := s.fs.SyncDir(d); err != nil {
				return fmt.Errorf("storage: %w", err)
			}
		}
		s.dirtyDir = false
	}
	return nil
}

// Close syncs and closes the files.
func (s *Store) Close() error {
	if err := s.Sync(); err != nil {
		return err
	}
	return s.closeActive()
}

// replaceFile swaps in new contents for a whole file, so a crash leaves
// either the old contents or the new, never a mixture.
func (s *Store) replaceFile(name string, data []byte) error {
	tmp := filepath.Join(s.dir, name+".tmp")
	f, err := s.fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	if s.checked {
		data = binary.BigEndian.AppendUint32(slices.Clone(data), sum(data))
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("storage: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	if err := s.fs.Rename(tmp, filepath.Join(s.dir, name)); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	s.dirtyDir = true
	return nil
}

func (s *Store) last() core.Index { return s.first + core.Index(len(s.positions)) - 1 }

func (s *Store) append(e core.Entry) error {
	if e.Index != s.last()+1 {
		return fmt.Errorf("storage: appending Entry %d after %d", e.Index, s.last())
	}
	if s.active == nil || s.activeSize >= s.segmentBytes {
		if err := s.closeActive(); err != nil {
			return err
		}
		if err := s.openActive(e.Index, 0); err != nil {
			return err
		}
		s.dirtyDir = true
	}
	record := encodeRecord(e, s.checked)
	if _, err := s.out.Write(record); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	s.positions = append(s.positions, position{segment: s.activeName, offset: s.activeSize})
	s.activeSize += int64(len(record))
	s.dirtyFile = true
	return nil
}

// truncateFrom removes the Entry at index and everything after it. Later
// segments are deleted before the one holding index is cut, so a crash part
// way through still leaves a Log with no gap.
func (s *Store) truncateFrom(index core.Index) error {
	if index < s.first {
		return fmt.Errorf("storage: truncating at %d, inside the Snapshot", index)
	}
	if index > s.last() {
		return nil
	}
	at := s.positions[index-s.first]
	if err := s.closeActive(); err != nil {
		return err
	}
	firsts, err := s.segments()
	if err != nil {
		return err
	}
	for i := len(firsts) - 1; i >= 0 && firsts[i] > at.segment; i-- {
		if err := s.fs.Remove(s.segmentPath(firsts[i])); err != nil {
			return fmt.Errorf("storage: %w", err)
		}
	}
	if err := s.fs.Truncate(s.segmentPath(at.segment), at.offset); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	s.positions = s.positions[:index-s.first]
	s.dirtyDir = true
	if err := s.openActive(at.segment, at.offset); err != nil {
		return err
	}
	s.dirtyFile = true // the cut itself must reach the disk
	return nil
}

// dropThrough forgets Entries up to index, which a Snapshot now covers, and
// deletes the segments that hold nothing else. With all set, every Entry
// goes.
func (s *Store) dropThrough(index core.Index, all bool) error {
	if all || index >= s.last() {
		if err := s.closeActive(); err != nil {
			return err
		}
		firsts, err := s.segments()
		if err != nil {
			return err
		}
		for _, first := range firsts {
			if err := s.fs.Remove(s.segmentPath(first)); err != nil {
				return fmt.Errorf("storage: %w", err)
			}
		}
		s.positions, s.first = nil, index+1
		s.dirtyDir = true
		return nil
	}
	if index < s.first {
		return nil
	}
	s.positions = slices.Clone(s.positions[index+1-s.first:])
	s.first = index + 1
	keep := s.positions[0].segment // the oldest segment still needed
	firsts, err := s.segments()
	if err != nil {
		return err
	}
	for _, first := range firsts {
		if first < keep {
			if err := s.fs.Remove(s.segmentPath(first)); err != nil {
				return fmt.Errorf("storage: %w", err)
			}
			s.dirtyDir = true
		}
	}
	return nil
}

// withMembers is set in a Snapshot file's Term field when a Member list
// follows the header. No Term comes near it.
const withMembers = 1 << 63

// encodeSnapshot lays a Snapshot out as Index and Term, 8 bytes each, then
// the state machine's data. If the Snapshot carries a Member list (A§6.5),
// the Term's top bit is set and the list comes between the two: a 4-byte
// count, then 8 bytes per Member.
func encodeSnapshot(snap *core.Snapshot) []byte {
	raw := make([]byte, 16, 16+12+8*len(snap.Members)+len(snap.Data))
	binary.BigEndian.PutUint64(raw[:8], uint64(snap.Index))
	term := uint64(snap.Term)
	if snap.Members != nil {
		term |= withMembers
		raw = binary.BigEndian.AppendUint32(raw, uint32(len(snap.Members)))
		raw = binary.BigEndian.AppendUint64(raw, uint64(snap.MembersAt))
		for _, m := range snap.Members {
			raw = binary.BigEndian.AppendUint64(raw, uint64(m))
		}
	}
	binary.BigEndian.PutUint64(raw[8:16], term)
	return append(raw, snap.Data...)
}

// readableSnapshot reports whether raw is long enough for what its header
// says it holds.
func readableSnapshot(raw []byte) bool {
	if len(raw) < 16 {
		return false
	}
	if binary.BigEndian.Uint64(raw[8:16])&withMembers == 0 {
		return true
	}
	return len(raw) >= 28 && uint64(len(raw)-28) >= 8*uint64(binary.BigEndian.Uint32(raw[16:20]))
}

func decodeSnapshot(raw []byte) *core.Snapshot {
	term := binary.BigEndian.Uint64(raw[8:16])
	snap := &core.Snapshot{Index: core.Index(binary.BigEndian.Uint64(raw[:8])), Term: core.Term(term &^ withMembers)}
	rest := raw[16:]
	if term&withMembers != 0 {
		count := binary.BigEndian.Uint32(rest[:4])
		snap.MembersAt = core.Index(binary.BigEndian.Uint64(rest[4:12]))
		rest = rest[12:]
		snap.Members = make([]core.NodeID, count)
		for i := range snap.Members {
			snap.Members[i] = core.NodeID(binary.BigEndian.Uint64(rest[:8]))
			rest = rest[8:]
		}
	}
	snap.Data = rest
	return snap
}

// endMark closes every checked record. It is non-zero on purpose.
const endMark = 0xA5

// encodeRecord lays an Entry out as a record. The body is Index and Term as
// unsigned varints, the kind, and the payload. A checked record wraps it as
// the package comment describes; an unchecked one is just length and body.
func encodeRecord(e core.Entry, checked bool) []byte {
	body := make([]byte, 0, 2*binary.MaxVarintLen64+1+len(e.Payload))
	body = binary.AppendUvarint(body, uint64(e.Index))
	body = binary.AppendUvarint(body, uint64(e.Term))
	body = append(body, byte(e.Kind))
	body = append(body, e.Payload...)
	record := make([]byte, 4, 13+len(body))
	binary.BigEndian.PutUint32(record, uint32(len(body)))
	if !checked {
		return append(record, body...)
	}
	record = binary.BigEndian.AppendUint32(record, sum(record[:4]))
	record = binary.BigEndian.AppendUint32(record, sum(body))
	record = append(record, body...)
	return append(record, endMark)
}

var (
	// errShort means a record was never completely written: the file ends
	// inside it, or it trails off into zeros.
	errShort = errors.New("record cut short")
	// errChecksum means a record is all there but doesn't match its checksum.
	errChecksum = errors.New("checksum mismatch")
)

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// readRecord decodes the record at the start of b, which runs to the end of
// its segment, and reports how many bytes the record took. It returns
// errShort for a record that was never completely written, and errChecksum
// for one that was complete and is now damaged.
func readRecord(b []byte, checked bool) (core.Entry, int64, error) {
	if !checked {
		if len(b) < 4 || len(b)-4 < int(binary.BigEndian.Uint32(b)) {
			return core.Entry{}, 0, errShort
		}
		n := int(binary.BigEndian.Uint32(b))
		e, err := decodeBody(b[4 : 4+n])
		return e, int64(4 + n), err
	}

	const head = 12
	if len(b) < head {
		// Fewer bytes than a header. The record before this one checked
		// out, so this is where a record starts, and no complete record is
		// this small: it was never finished.
		return core.Entry{}, 0, errShort
	}
	if sum(b[:4]) != binary.BigEndian.Uint32(b[4:8]) {
		// The length can't be trusted, so neither can where the record
		// ends. If nothing but zeros follows the first few bytes, the write
		// stopped inside the header. A complete record always has non-zero
		// bytes further on, its end mark at the least.
		if allZero(b[8:]) {
			return core.Entry{}, 0, errShort
		}
		return core.Entry{}, 0, errChecksum
	}
	n := int(binary.BigEndian.Uint32(b))
	if len(b)-head < n+1 {
		return core.Entry{}, 0, errShort // the length is sound and the file ends first
	}
	body, mark := b[head:head+n], b[head+n]
	if sum(body) != binary.BigEndian.Uint32(b[8:12]) || mark != endMark {
		// A record whose end never arrived has zeros there, and nothing
		// after it. One that was complete has its end mark.
		if mark == 0 && allZero(b[head+n:]) {
			return core.Entry{}, 0, errShort
		}
		return core.Entry{}, 0, errChecksum
	}
	e, err := decodeBody(body)
	return e, int64(head + n + 1), err
}

func decodeBody(body []byte) (core.Entry, error) {
	index, i := binary.Uvarint(body)
	if i <= 0 {
		return core.Entry{}, errors.New("unreadable record")
	}
	term, j := binary.Uvarint(body[i:])
	if j <= 0 || i+j >= len(body) {
		return core.Entry{}, errors.New("unreadable record")
	}
	e := core.Entry{Index: core.Index(index), Term: core.Term(term), Kind: core.EntryKind(body[i+j])}
	if payload := body[i+j+1:]; len(payload) > 0 {
		e.Payload = slices.Clone(payload)
	}
	return e, nil
}
