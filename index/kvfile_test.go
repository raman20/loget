package index

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/internal/runfile"
	"pgregory.net/rapid"
)

// files adapts runfile.Dir to RunFiles and counts what is mapped.
type files struct{ d *runfile.Dir }

func (f files) Create() (RunFile, error) {
	rf, err := f.d.Create()
	if err != nil {
		return nil, err
	}
	return rf, nil
}

// testFiles returns run files in a fresh directory, with every run (not
// only those of 1 MiB and more) in a file until the test ends. fault, if
// set, may fail any file operation.
func testFiles(t testing.TB, fault func(op string) error) (RunFiles, string) {
	if !runfile.Supported {
		t.Skip("run files need mmap")
	}
	dir := t.TempDir()
	d, err := runfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Fault = fault
	old := minFileRun
	minFileRun = 0
	t.Cleanup(func() { minFileRun = old })
	return files{d}, dir
}

// emptyDir fails unless dir holds no files: run files are unlinked when
// created, so none is ever visible.
func emptyDir(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("%d files left in %s", len(ents), dir)
	}
}

// TestKVFilesRefineMap: TestKVRunsRefineMap and TestKVSetLatestRefineMap
// with every run in a mapped file.
func TestKVFilesRefineMap(t *testing.T) {
	fs, dir := testFiles(t, nil)
	rapid.Check(t, func(rt *rapid.T) {
		refineMap(rt, KVOptions{Inline: 3, Files: fs, OnFileError: func(err error) { rt.Fatal(err) }})
	})
	rapid.Check(t, func(rt *rapid.T) {
		setLatestRefineMap(rt, KVOptions{Inline: 3, Files: fs, OnFileError: func(err error) { rt.Fatal(err) }})
	})
	// The models above do not say whether runs were mapped; this does.
	l := NewKVOptions(KVOptions{Inline: 3, Files: fs})
	l.hotMax = 4
	for i := 1; i <= 100; i++ {
		l.Apply(&Entry{LSN: uint64(i), Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: fmt.Sprint("k", i%37), Value: []byte("v")}}}, testEnv{})
	}
	mapped, _ := l.FileStats()
	if mapped == 0 || mapped != len(l.runs) {
		t.Fatalf("%d of %d runs mapped", mapped, len(l.runs))
	}
	emptyDir(t, dir)
}

// TestKVFilesFaults: any run-file operation may fail; the KV reports it,
// falls back to the heap and still matches the model.
func TestKVFilesFaults(t *testing.T) {
	var mu sync.Mutex
	var p float64
	rng := rand.New(rand.NewSource(1))
	errFault := errors.New("injected")
	fs, dir := testFiles(t, func(op string) error {
		mu.Lock()
		defer mu.Unlock()
		if rng.Float64() < p {
			return fmt.Errorf("%s: %w", op, errFault)
		}
		return nil
	})
	failed := 0
	check := func(rt *rapid.T, fn func(*rapid.T, KVOptions)) {
		mu.Lock()
		p = rapid.SampledFrom([]float64{0, 0.05, 0.3, 1}).Draw(rt, "p")
		mu.Unlock()
		fn(rt, KVOptions{Inline: 3, Files: fs, OnFileError: func(err error) {
			if !errors.Is(err, errFault) {
				rt.Fatalf("unexpected run-file error %v", err)
			}
			failed++
		}})
	}
	rapid.Check(t, func(rt *rapid.T) { check(rt, refineMap) })
	rapid.Check(t, func(rt *rapid.T) { check(rt, setLatestRefineMap) })
	if failed == 0 {
		t.Fatal("no fault was injected")
	}
	emptyDir(t, dir)
}

// TestKVFilesLoadFault: a snapshot whose runs cannot be written to files
// loads on the heap, identically.
func TestKVFilesLoadFault(t *testing.T) {
	for _, op := range []string{"create", "write", "map", "seal"} {
		t.Run(op, func(t *testing.T) {
			fs, _ := testFiles(t, func(o string) error {
				if o == op {
					return errors.New("injected")
				}
				return nil
			})
			src := kvSeed(true)
			snap := src.Snapshot()
			n := 0
			l, err := LoadKVOptions(snap, KVOptions{Inline: 4, Files: fs, OnFileError: func(error) { n++ }})
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatalf("%d file errors reported, want 1", n)
			}
			if r, _ := l.FileStats(); r != 0 {
				t.Fatalf("%d runs mapped after a failure", r)
			}
			if !reflect.DeepEqual(l.Snapshot(), snap) {
				t.Fatal("snapshot differs after a heap fallback")
			}
		})
	}
}

// TestKVFilesValuesOwned: values read from a mapped run stay valid after
// the run is replaced and unmapped (they are copies).
func TestKVFilesValuesOwned(t *testing.T) {
	fs, _ := testFiles(t, nil)
	l := NewKVOptions(KVOptions{Inline: 64, Files: fs})
	l.hotMax = 2
	put := func(lsn uint64, k, v string) {
		l.Apply(&Entry{LSN: lsn, Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: k, Value: []byte(v)}}}, testEnv{})
	}
	put(1, "a", "alpha")
	put(2, "b", "beta")
	got, ok := l.Get("kv", "a", 2)
	if !ok || l.runs[0].mp == nil {
		t.Fatal("a is not in a mapped run")
	}
	sc := l.Scan("kv", "", 2)
	var lv []byte
	_ = l.Latest(func(_, k string, v Version) error {
		if k == "b" {
			lv = v.Val
		}
		return nil
	})
	for i := uint64(3); i < 200; i++ { // freeze and merge: the first run is unmapped
		put(i, fmt.Sprint("k", i), "x")
	}
	l.work(-1)
	if string(got.Val) != "alpha" || string(sc[1].Val) != "beta" || string(lv) != "beta" {
		t.Fatalf("values changed: %q %q %q", got.Val, sc[1].Val, lv)
	}
}

// TestKVFilesConcurrent: readers under a read lock while a writer freezes,
// merges and shreds under the write lock (run with -race; a read of an
// unmapped run would crash).
func TestKVFilesConcurrent(t *testing.T) {
	fs, dir := testFiles(t, nil)
	l := NewKVOptions(KVOptions{Inline: 8, Files: fs})
	l.hotMax = 64
	var mu sync.RWMutex
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				mu.RLock()
				v, ok := l.Get("kv", fmt.Sprint("k", (i*7+g)%500), ^uint64(0))
				ps := l.Scan("kv", fmt.Sprint("k", i%10), ^uint64(0))
				mu.RUnlock()
				if ok && v.Subject == "" && len(v.Val) != 6 {
					t.Errorf("bad value %q", v.Val)
					return
				}
				for _, p := range ps {
					if p.Subject == "" && len(p.Val) != 6 {
						t.Errorf("bad scan value %q", p.Val)
						return
					}
				}
			}
		}(g)
	}
	for i := 1; i <= 20000; i++ {
		op := format.Op{Kind: format.OpPut, Space: "kv", Key: fmt.Sprint("k", i%500), Value: []byte(fmt.Sprintf("%06d", i))}
		if i%3 == 0 {
			op.Subject = fmt.Sprint("s", i%5)
		}
		e := &Entry{LSN: uint64(i), Ops: []format.Op{op}}
		if i%2500 == 0 {
			e.Ops = append(e.Ops, format.Op{Kind: format.OpShred, Key: fmt.Sprint("s", i%5)})
		}
		mu.Lock()
		l.Apply(e, testEnv{})
		mu.Unlock()
	}
	close(stop)
	wg.Wait()
	if r, _ := l.FileStats(); r == 0 {
		t.Fatal("no run was mapped")
	}
	emptyDir(t, dir)
}
