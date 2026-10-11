package storage

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

// SharedSyncFS is OSFS for a process that keeps several Stores on one disk
// (A§11.9). A sync of any file is split in two: the file's data is handed to
// the drive, which is cheap, and then the drive is told to put everything it
// holds on permanent storage, which is slow and covers every file at once.
// The second step is shared: every Store waiting for it rides on the same
// one.
//
// It relies on the drive-wide flush making durable whatever was handed to
// the drive before the flush began, for any file on the volume. That is
// what F_FULLFSYNC does on macOS, where os.File.Sync uses it. All the files
// must be on one volume, with the barrier file.
type SharedSyncFS struct {
	OSFS
	barrier *os.File

	mu       sync.Mutex
	cond     *sync.Cond
	started  int64 // drive-wide flushes begun
	finished int64 // and completed
	wanted   bool  // someone is waiting for one that hasn't begun
	err      error
}

// NewSharedSyncFS returns a SharedSyncFS whose flushes go through a file at
// barrierPath, which must be on the volume the Stores use.
func NewSharedSyncFS(barrierPath string) (*SharedSyncFS, error) {
	f, err := os.Create(barrierPath)
	if err != nil {
		return nil, err
	}
	fs := &SharedSyncFS{barrier: f}
	fs.cond = sync.NewCond(&fs.mu)
	go fs.flusher()
	return fs, nil
}

// flusher runs a drive-wide flush whenever someone wants one, one at a time.
func (fs *SharedSyncFS) flusher() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for {
		for !fs.wanted {
			fs.cond.Wait()
		}
		fs.wanted = false
		fs.started++
		mine := fs.started
		fs.mu.Unlock()
		err := fs.barrier.Sync()
		fs.mu.Lock()
		fs.finished = mine
		if err != nil {
			fs.err = err
		}
		fs.cond.Broadcast()
	}
}

// flush returns once a drive-wide flush that began after the call has
// finished.
func (fs *SharedSyncFS) flush() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	need := fs.started + 1
	fs.wanted = true
	fs.cond.Broadcast()
	for fs.finished < need && fs.err == nil {
		if fs.started < need {
			fs.wanted = true
		}
		fs.cond.Wait()
	}
	return fs.err
}

type sharedFile struct {
	*os.File
	fs *SharedSyncFS
}

func (f sharedFile) Sync() error {
	if err := syscall.Fsync(int(f.Fd())); err != nil {
		return err
	}
	return f.fs.flush()
}

func (fs *SharedSyncFS) Create(path string) (File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return sharedFile{f, fs}, nil
}

func (fs *SharedSyncFS) Append(path string, size int64) (File, error) {
	f, err := fs.OSFS.Append(path, size)
	if err != nil {
		return nil, err
	}
	return sharedFile{f.(*os.File), fs}, nil
}

func (fs *SharedSyncFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := syscall.Fsync(int(d.Fd())); err != nil {
		return errors.Join(err, d.Close())
	}
	return errors.Join(fs.flush(), d.Close())
}
