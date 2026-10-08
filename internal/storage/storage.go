// Package storage keeps a Member's durable state in files (A§5.4): its Term
// and vote, its Snapshot, and its Log.
//
// Layout of a data directory:
//
//	state            Term and vote, 16 bytes
//	snapshot         Index, Term, then the state machine's data
//	log/<first>.seg  Log segments, named by the Index of their first Entry
//
// state and snapshot are replaced whole: written to a temporary file, synced,
// and renamed over the old one. A segment is a run of records, each a 4-byte
// length followed by one encoded Entry. A crash can leave the last record of
// the last segment incomplete; Open drops it.
//
// Records carry no checksum (Decision Log: "No checksums on disk until
// Rung 4"). A record that is complete but damaged is read back as if valid.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"distributed-kv-store/internal/core"
)

// DefaultSegmentBytes is the size at which a new segment is started.
const DefaultSegmentBytes = 4 << 20

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
	return OpenFS(OSFS{}, dir, segmentBytes)
}

// OpenFS is Open on any filesystem.
func OpenFS(fs FS, dir string, segmentBytes int64) (*Store, core.Stored, error) {
	if segmentBytes == 0 {
		segmentBytes = DefaultSegmentBytes
	}
	if err := fs.MkdirAll(filepath.Join(dir, "log")); err != nil {
		return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
	}
	s := &Store{fs: fs, dir: dir, segmentBytes: segmentBytes, first: 1}
	var stored core.Stored

	if raw, err := fs.ReadFile(filepath.Join(dir, "state")); err == nil && len(raw) == 16 {
		stored.HardState = core.HardState{
			Term:     core.Term(binary.BigEndian.Uint64(raw[:8])),
			VotedFor: core.NodeID(binary.BigEndian.Uint64(raw[8:])),
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
	}

	if raw, err := fs.ReadFile(filepath.Join(dir, "snapshot")); err == nil && len(raw) >= 16 {
		stored.Snapshot = &core.Snapshot{
			Index: core.Index(binary.BigEndian.Uint64(raw[:8])),
			Term:  core.Term(binary.BigEndian.Uint64(raw[8:16])),
			Data:  raw[16:],
		}
		s.first = stored.Snapshot.Index + 1
		s.snapshotTerm = stored.Snapshot.Term
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, core.Stored{}, fmt.Errorf("storage: %w", err)
	}

	entries, err := s.load()
	if err != nil {
		return nil, core.Stored{}, err
	}
	stored.Entries = entries
	return s, stored, nil
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
			e, size, err := readRecord(raw[offset:])
			if errors.Is(err, errShort) && i == len(firsts)-1 {
				// A crash cut the last write short. Nothing after it can
				// have been acknowledged, so it is dropped.
				if err := s.fs.Truncate(s.segmentPath(first), offset); err != nil {
					return nil, fmt.Errorf("storage: %w", err)
				}
				break
			}
			if err != nil {
				return nil, fmt.Errorf("storage: segment %016x at offset %d: %w", uint64(first), offset, err)
			}
			if next != 0 && e.Index != next {
				return nil, fmt.Errorf("storage: segment %016x holds Entry %d where %d was expected", uint64(first), e.Index, next)
			}
			next = e.Index + 1
			if e.Index >= s.first {
				if len(entries) == 0 && e.Index != s.first {
					return nil, fmt.Errorf("storage: the Log starts at Entry %d but the Snapshot ends at %d", e.Index, s.first-1)
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
		var raw [16]byte
		binary.BigEndian.PutUint64(raw[:8], uint64(p.HardState.Term))
		binary.BigEndian.PutUint64(raw[8:], uint64(p.HardState.VotedFor))
		if err := s.replaceFile("state", raw[:]); err != nil {
			return err
		}
	}
	if p.Snapshot != nil {
		raw := make([]byte, 16, 16+len(p.Snapshot.Data))
		binary.BigEndian.PutUint64(raw[:8], uint64(p.Snapshot.Index))
		binary.BigEndian.PutUint64(raw[8:], uint64(p.Snapshot.Term))
		if err := s.replaceFile("snapshot", append(raw, p.Snapshot.Data...)); err != nil {
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
	record := encodeRecord(e)
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

// encodeRecord lays an Entry out as: a 4-byte length, then Index and Term as
// unsigned varints, the kind, and the payload.
func encodeRecord(e core.Entry) []byte {
	body := make([]byte, 0, 2*binary.MaxVarintLen64+1+len(e.Payload))
	body = binary.AppendUvarint(body, uint64(e.Index))
	body = binary.AppendUvarint(body, uint64(e.Term))
	body = append(body, byte(e.Kind))
	body = append(body, e.Payload...)
	record := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(record, uint32(len(body)))
	return append(record, body...)
}

// errShort means a record is incomplete: the file ends before it does.
var errShort = errors.New("record cut short")

// readRecord decodes the record at the start of b and reports how many bytes
// it took. It returns errShort if b ends part way through the record.
func readRecord(b []byte) (core.Entry, int64, error) {
	if len(b) < 4 {
		return core.Entry{}, 0, errShort
	}
	n := int(binary.BigEndian.Uint32(b))
	if len(b)-4 < n {
		return core.Entry{}, 0, errShort
	}
	body := b[4 : 4+n]
	index, i := binary.Uvarint(body)
	if i <= 0 {
		return core.Entry{}, 0, errors.New("unreadable record")
	}
	term, j := binary.Uvarint(body[i:])
	if j <= 0 || i+j >= len(body) {
		return core.Entry{}, 0, errors.New("unreadable record")
	}
	e := core.Entry{Index: core.Index(index), Term: core.Term(term), Kind: core.EntryKind(body[i+j])}
	if payload := body[i+j+1:]; len(payload) > 0 {
		e.Payload = slices.Clone(payload)
	}
	return e, int64(4 + n), nil
}
