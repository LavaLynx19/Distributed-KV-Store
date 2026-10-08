package storage

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
)

// MemFS is a filesystem held in memory, for the Simulation (A§8.1). It keeps
// two views of every file: what has been written, and what has been synced.
// A crash throws away everything that wasn't synced, and can be told to do
// what real disks do on the way: keep part of an unsynced write, or leave
// zeros where data should be. A bit can be flipped at any time.
//
// It is not safe for concurrent use.
type MemFS struct {
	files   map[string]*memFile // every file, by path, as last written
	durable map[string]*memFile // the names that would survive a crash
}

type memFile struct {
	data   []byte // as last written
	synced []byte // as last synced
}

func NewMemFS() *MemFS {
	return &MemFS{files: map[string]*memFile{}, durable: map[string]*memFile{}}
}

func notExist(op, path string) error {
	return &os.PathError{Op: op, Path: path, Err: os.ErrNotExist}
}

func (m *MemFS) MkdirAll(string) error { return nil }

func (m *MemFS) ReadFile(path string) ([]byte, error) {
	f, ok := m.files[path]
	if !ok {
		return nil, notExist("open", path)
	}
	return slices.Clone(f.data), nil
}

func (m *MemFS) ReadDir(dir string) ([]string, error) {
	var names []string
	for path := range m.files {
		if filepath.Dir(path) == dir {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (m *MemFS) Create(path string) (File, error) {
	f := &memFile{}
	m.files[path] = f
	return memHandle{f}, nil
}

func (m *MemFS) Append(path string, size int64) (File, error) {
	f, ok := m.files[path]
	if !ok {
		f = &memFile{}
		m.files[path] = f
	}
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	return memHandle{f}, nil
}

func (m *MemFS) Truncate(path string, size int64) error {
	f, ok := m.files[path]
	if !ok {
		return notExist("truncate", path)
	}
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	return nil
}

func (m *MemFS) Remove(path string) error {
	if _, ok := m.files[path]; !ok {
		return notExist("remove", path)
	}
	delete(m.files, path)
	return nil
}

func (m *MemFS) Rename(from, to string) error {
	f, ok := m.files[from]
	if !ok {
		return notExist("rename", from)
	}
	m.files[to] = f
	delete(m.files, from)
	return nil
}

// SyncDir makes dir's current list of names the one that survives a crash.
func (m *MemFS) SyncDir(dir string) error {
	for path := range m.durable {
		if filepath.Dir(path) == dir {
			delete(m.durable, path)
		}
	}
	for path, f := range m.files {
		if filepath.Dir(path) == dir {
			m.durable[path] = f
		}
	}
	return nil
}

type memHandle struct{ f *memFile }

func (h memHandle) Write(p []byte) (int, error) {
	h.f.data = append(h.f.data, p...)
	return len(p), nil
}

func (h memHandle) Sync() error {
	h.f.synced = slices.Clone(h.f.data)
	return nil
}

func (memHandle) Close() error { return nil }

// paths lists the durable files in a fixed order, so that faults chosen with
// a seeded source land the same way every run.
func (m *MemFS) paths() []string {
	paths := make([]string, 0, len(m.durable))
	for p := range m.durable {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

// Crash leaves the filesystem as a power cut would: only synced names and
// synced contents remain.
//
// With tear set, an unsynced write to the end of a file may partly survive,
// as on a real disk: some of its bytes are kept, and some of those may be
// zeros, where the file grew but the data never reached it. rng decides how
// much.
func (m *MemFS) Crash(rng *rand.Rand, tear bool) {
	for _, path := range m.paths() {
		f := m.durable[path]
		kept := f.synced
		if extra := len(f.data) - len(f.synced); tear && extra > 0 && slices.Equal(f.data[:len(f.synced)], f.synced) {
			n := rng.IntN(extra + 1)
			tail := slices.Clone(f.data[len(f.synced) : len(f.synced)+n])
			if n > 0 && rng.IntN(2) == 0 {
				clear(tail[rng.IntN(n):])
			}
			kept = append(slices.Clone(f.synced), tail...)
		}
		f.data, f.synced = slices.Clone(kept), slices.Clone(kept)
	}
	m.files = map[string]*memFile{}
	for path, f := range m.durable {
		m.files[path] = f
	}
}

// FlipBit inverts one bit, chosen by rng, in one durable file, and reports
// which file. It returns "" if there is nothing to damage.
func (m *MemFS) FlipBit(rng *rand.Rand) string {
	var candidates []string
	for _, path := range m.paths() {
		if len(m.durable[path].synced) > 0 {
			candidates = append(candidates, path)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	path := candidates[rng.IntN(len(candidates))]
	f := m.durable[path]
	at, bit := rng.IntN(len(f.synced)), byte(1)<<rng.IntN(8)
	f.synced[at] ^= bit
	if at < len(f.data) {
		f.data[at] ^= bit
	}
	return path
}

// Durable returns an independent filesystem holding only what would survive
// a clean crash right now. Opening a Store on it shows what is on disk
// without disturbing this one.
func (m *MemFS) Durable() *MemFS {
	c := NewMemFS()
	for path, f := range m.durable {
		copied := &memFile{data: slices.Clone(f.synced), synced: slices.Clone(f.synced)}
		c.files[path], c.durable[path] = copied, copied
	}
	return c
}
