package loget

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/internal/runfile"
	"github.com/raman20/loget/store"
)

// countFiles wraps run files and counts the ones created.
type countFiles struct {
	runFiles
	n *atomic.Int64
}

func (c countFiles) Create() (index.RunFile, error) {
	f, err := c.runFiles.Create()
	if err == nil {
		c.n.Add(1)
	}
	return f, err
}

// testIndexFiles returns run files in dir (every run in a file, fault may
// fail any operation) and the count of files created.
func testIndexFiles(t testing.TB, dir string, fault func(string) error) (index.RunFiles, *atomic.Int64) {
	if !runfile.Supported {
		t.Skip("run files need mmap")
	}
	d, err := runfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Fault = fault
	c := countFiles{runFiles{d}, new(atomic.Int64)}
	return c, c.n
}

func noFiles(t testing.TB, dir string) {
	t.Helper()
	if ents, err := os.ReadDir(dir); err != nil || len(ents) != 0 {
		t.Fatalf("index dir: %d entries, %v", len(ents), err)
	}
}

// fillIndex commits n keys in batches, compacting halfway.
func fillIndex(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i += 250 {
		if err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < i+250 && j < n; j++ {
				tx.Put(fmt.Sprintf("k%06d", j), []byte(fmt.Sprint("v", j)))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if i == n/2 {
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// checkIndex checks every key, a scan and index == fold(log).
func checkIndex(t *testing.T, db *DB, n int) {
	t.Helper()
	if err := db.View(ctx, func(tx *Tx) error {
		for j := 0; j < n; j++ {
			v, err := tx.Get(fmt.Sprintf("k%06d", j))
			if err != nil || string(v) != fmt.Sprint("v", j) {
				t.Fatalf("k%06d = %q, %v", j, v, err)
			}
		}
		kvs, err := tx.Scan("k")
		if err != nil || len(kvs) != n {
			t.Fatalf("scan: %d keys, %v", len(kvs), err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	d1, _, _ := db.IndexDigests("main")
	d2, _ := db.RebuildDigests("main")
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatal("index != fold(log)")
	}
}

// TestIndexFiles: with every run in a run file, a database written,
// compacted, snapshotted and reopened reads the same; nothing is left in
// the directory.
func TestIndexFiles(t *testing.T) {
	dir := t.TempDir()
	fs, created := testIndexFiles(t, dir, nil)
	st := store.NewMem()
	const n = 6000
	db := mustOpen(t, st, WithIndexFiles(fs), WithSnapshotEvery(7))
	fillIndex(t, db, n)
	checkIndex(t, db, n)
	db.Close()
	db = mustOpen(t, st, WithIndexFiles(fs))
	defer db.Close()
	checkIndex(t, db, n)
	if created.Load() == 0 {
		t.Fatal("no run file was created")
	}
	if s := db.Stats(); s.KVIndexFileErrors != 0 {
		t.Fatalf("%d run-file errors", s.KVIndexFileErrors)
	}
	noFiles(t, dir)
}

// TestIndexFileFaults: when run files fail the index moves to the heap,
// counts the errors and still reads the same.
func TestIndexFileFaults(t *testing.T) {
	for _, op := range []string{"create", "write", "map", "seal"} {
		t.Run(op, func(t *testing.T) {
			fs, _ := testIndexFiles(t, t.TempDir(), func(o string) error {
				if o == op {
					return errors.New("injected")
				}
				return nil
			})
			st := store.NewMem()
			const n = 3000
			db := mustOpen(t, st, WithSnapshotEvery(5))
			fillIndex(t, db, n)
			db.Close()
			db = mustOpen(t, st, WithIndexFiles(fs))
			defer db.Close()
			checkIndex(t, db, n)
			if db.Stats().KVIndexFileErrors == 0 {
				t.Fatal("no run-file error counted")
			}
		})
	}
}

// TestIndexDirOption: an explicit unusable directory fails Open; a usable
// one is used and stays empty; IndexInMemory needs none.
func TestIndexDirOption(t *testing.T) {
	if !runfile.Supported {
		t.Skip("run files need mmap")
	}
	if _, err := OpenStore(ctx, store.NewMem(), quiet(), WithIndexDir(filepath.Join(t.TempDir(), "missing"))); err == nil {
		t.Fatal("Open with a missing index dir succeeded")
	}
	dir := t.TempDir()
	db := mustOpen(t, store.NewMem(), WithIndexDir(dir))
	fillIndex(t, db, 1000)
	checkIndex(t, db, 1000)
	db.Close()
	noFiles(t, dir)
	db = mustOpen(t, store.NewMem(), WithIndexInMemory(true), WithIndexDir(filepath.Join(dir, "missing")))
	db.Close()
}

// TestIndexResidentBytes: with a resident bound (design 25) the run pages
// are dropped from the process while readers and writers run, and every
// read still returns the same; Close stops the check.
func TestIndexResidentBytes(t *testing.T) {
	if !runfile.Supported {
		t.Skip("run files need mmap")
	}
	d, err := runfile.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	const n = 6000
	db := mustOpen(t, st, WithIndexFiles(runFiles{d}), WithIndexResidentBytes(1))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // drop far more often than the 100 ms check would
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				d.DropAll()
			}
		}
	}()
	fillIndex(t, db, n)
	checkIndex(t, db, n)
	close(stop)
	<-done
	if d.DropAll() == 0 {
		t.Fatal("no mapped run file to drop")
	}
	db.Close()
	db = mustOpen(t, st, WithIndexFiles(runFiles{d}), WithIndexResidentBytes(1))
	checkIndex(t, db, n)
	db.Close()
	db.Close() // stop is idempotent
}
