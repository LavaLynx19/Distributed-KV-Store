package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// FS is the part of a filesystem the Store uses. The real one is OSFS; the
// Simulation supplies MemFS, which can lose, tear and corrupt what it holds
// (A§8.1).
type FS interface {
	MkdirAll(dir string) error
	// ReadFile returns a file's contents, or an error satisfying
	// errors.Is(err, os.ErrNotExist).
	ReadFile(path string) ([]byte, error)
	// ReadDir lists the names of the files directly inside dir.
	ReadDir(dir string) ([]string, error)
	// Create opens a new, empty file for writing, replacing any existing one.
	Create(path string) (File, error)
	// Append opens a file for writing at its end, creating it if needed and
	// first cutting it to size bytes.
	Append(path string, size int64) (File, error)
	Truncate(path string, size int64) error
	Remove(path string) error
	Rename(from, to string) error
	// SyncDir makes the creations, renames and removals inside dir durable.
	SyncDir(dir string) error
}

// File is a file open for writing. Sync makes what has been written durable.
type File interface {
	io.Writer
	Sync() error
	Close() error
}

// OSFS is the operating system's filesystem.
type OSFS struct{}

func (OSFS) MkdirAll(dir string) error            { return os.MkdirAll(dir, 0o755) }
func (OSFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (OSFS) Truncate(path string, n int64) error  { return os.Truncate(path, n) }
func (OSFS) Remove(path string) error             { return os.Remove(path) }
func (OSFS) Rename(from, to string) error         { return os.Rename(from, to) }
func (OSFS) Create(path string) (File, error)     { return os.Create(path) }

func (OSFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}

func (OSFS) Append(path string, size int64) (File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if _, err := f.Seek(size, io.SeekStart); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

func (OSFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync %s: %w", dir, err), d.Close())
	}
	return d.Close()
}
