// Package runfile keeps the KV index's runs in memory-mapped files
// (design 21). It implements index.RunFiles; the index package itself does
// no I/O.
package runfile

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

// Dir creates run files in a directory.
type Dir struct {
	path string
	// Fault, if set (tests), is called before each file operation
	// ("create", "write", "map", "seal") and fails it by returning an
	// error.
	Fault func(op string) error
}

// ErrUnsupported is returned where files cannot be mapped.
var ErrUnsupported = errors.New("runfile: memory-mapped run files need a Unix system")

// Supported reports whether this platform can map run files.
const Supported = supported

// New returns a Dir for path. It creates and removes one file to check the
// directory is usable.
func New(path string) (*Dir, error) {
	if !Supported {
		return nil, ErrUnsupported
	}
	d := &Dir{path: path}
	f, err := d.Create()
	if err != nil {
		return nil, err
	}
	f.Release()
	return d, nil
}

// Path returns the directory.
func (d *Dir) Path() string { return d.path }

func (d *Dir) fault(op string) error {
	if d.Fault != nil {
		return d.Fault(op)
	}
	return nil
}

// File is one run's file. It is unlinked at creation: only the descriptor
// and later the mapping keep it, nothing is left behind by a crash, and no
// other process can open it.
type File struct {
	mu   sync.Mutex
	refs int // references beyond the owner's (Ref); Release drops one
	d    *Dir
	f    *os.File // nil once sealed or released
	m    []byte   // the mapping, nil until Map and after Release
	size int64
}

// Create returns a new, empty run file.
func (d *Dir) Create() (*File, error) {
	if err := d.fault("create"); err != nil {
		return nil, err
	}
	f, err := createAnon(d.path)
	if err != nil {
		return nil, err
	}
	rf := &File{d: d, f: f}
	// Backstop: a KV dropped whole (a branch reloaded, a view replaced)
	// releases its files when the collector finds its runs unreachable.
	runtime.SetFinalizer(rf, (*File).Release)
	return rf, nil
}

// Write appends p.
func (f *File) Write(p []byte) error {
	if err := f.d.fault("write"); err != nil {
		return err
	}
	n, err := f.f.Write(p)
	f.size += int64(n)
	return err
}

// Map sizes the file for slots uint64s after its bytes (8-aligned), maps it
// writable and returns the bytes written so far and the slots (zero).
func (f *File) Map(slots int) ([]byte, []uint64, error) {
	if err := f.d.fault("map"); err != nil {
		return nil, nil, err
	}
	so := (f.size + 7) &^ 7
	total := so + 8*int64(slots)
	if err := f.f.Truncate(total); err != nil {
		return nil, nil, err
	}
	m, err := mmap(f.f, int(total))
	if err != nil {
		return nil, nil, err
	}
	f.m = m
	var s []uint64
	if slots > 0 {
		s = unsafe.Slice((*uint64)(unsafe.Pointer(&m[so])), slots)
	}
	return m[:f.size:f.size], s, nil
}

// Seal makes the mapping read-only (a stray write faults instead of
// corrupting the index), advises random access (a fault on an evicted page
// reads one page, not a readahead window) and closes the descriptor.
func (f *File) Seal() error {
	if err := f.d.fault("seal"); err != nil {
		return err
	}
	if err := seal(f.m); err != nil {
		return err
	}
	err := f.f.Close()
	f.f = nil
	return err
}

// Ref adds a reference: the file stays mapped until Release has been
// called once more than Ref (a checkpoint reading a run the KV replaces
// meanwhile, design 24).
func (f *File) Ref() {
	f.mu.Lock()
	f.refs++
	f.mu.Unlock()
}

// Release drops a reference; the last one unmaps and closes the file.
// Idempotent after that. The caller guarantees no reader still uses the
// mapping; the finalizer calls it only once the file is unreachable, so
// never concurrently with the owner.
func (f *File) Release() {
	f.mu.Lock()
	if f.refs > 0 {
		f.refs--
		f.mu.Unlock()
		return
	}
	defer f.mu.Unlock()
	if f.m != nil {
		unmap(f.m)
		f.m = nil
	}
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	runtime.SetFinalizer(f, nil)
}

// createUnlinked creates a file in dir and removes its name. A process
// killed between the two leaves the file behind; createAnon avoids that
// window where the OS can (Linux O_TMPFILE).
func createUnlinked(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, "loget-kv-*.run")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
