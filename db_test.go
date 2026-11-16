package loget

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/raman20/loget/store"
)

var ctx = context.Background()

func quiet() Option { return WithLogf(func(string, ...any) {}) }

func mustOpen(t testing.TB, st store.Store, opts ...Option) *DB {
	t.Helper()
	db, err := OpenStore(ctx, st, append([]Option{quiet(), WithIndex(tallyKind())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func backends(t *testing.T) map[string]func() store.Store {
	return map[string]func() store.Store{
		"mem": func() store.Store { return store.NewMem() },
		"fs": func() store.Store {
			s, err := store.NewFS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		"journal": func() store.Store {
			s, err := store.NewJournal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
}

func TestKVRoundTripAndRestart(t *testing.T) {
	for name, mk := range backends(t) {
		t.Run(name, func(t *testing.T) {
			st := mk()
			db := mustOpen(t, st)
			for i := 0; i < 50; i++ {
				if err := db.Put(ctx, fmt.Sprintf("k%02d", i), []byte(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
			}
			db.Update(ctx, func(tx *Tx) error { return tx.Delete("k07") })
			db.Close()
			db = mustOpen(t, st)
			defer db.Close()
			v, err := db.Get(ctx, "k42")
			if err != nil || string(v) != "42" {
				t.Fatalf("got %q %v", v, err)
			}
			if _, err := db.Get(ctx, "k07"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted key visible: %v", err)
			}
			db.View(ctx, func(tx *Tx) error {
				kvs, _ := tx.Scan("k0")
				if len(kvs) != 9 {
					t.Fatalf("scan %d", len(kvs))
				}
				return nil
			})
		})
	}
}

// Regression for previous-prototype bug #1: stale reads after restart (random WAL
// replay order lost latest writes, 30/50 runs). Overwrite the same keys
// many times across many restarts; the latest value must always win.
func TestRegressionStaleReadsAfterRestart(t *testing.T) {
	st := store.NewMem()
	for round := 0; round < 30; round++ {
		db := mustOpen(t, st, WithSnapshotEvery(7))
		for i := 0; i < 20; i++ {
			db.Put(ctx, fmt.Sprintf("key%d", i%5), []byte(fmt.Sprintf("r%d-i%d", round, i)))
		}
		db.Close()
		db = mustOpen(t, st, WithSnapshotEvery(7))
		for k := 0; k < 5; k++ {
			v, err := db.Get(ctx, fmt.Sprintf("key%d", k))
			want := fmt.Sprintf("r%d-i%d", round, 15+k)
			if err != nil || string(v) != want {
				t.Fatalf("round %d key%d: got %q want %q (%v)", round, k, v, want, err)
			}
		}
		db.Close()
	}
}

// Regression for previous-prototype bug #4: one partial/torn index file bricked open.
// Here: a damaged tail segment refuses open unless WithRepairTail, which
// repairs it; a corrupt snapshot is ignored.
func TestRegressionTornFilesDoNotBlockOpen(t *testing.T) {
	st := store.NewMem()
	// no Close checkpoint: it would cover the segment torn below, and a
	// checkpoint past the head is corruption (newest segments missing)
	db := mustOpen(t, st, WithSnapshotEvery(3), WithCheckpointBytes(-1))
	for i := 0; i < 10; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("v"))
	}
	m := db.Manifest()
	db.Close()
	// corrupt the checkpoint and tear the last segment in half
	var lastSeg string
	st.Raw(func(o map[string][]byte) {
		for k := range o {
			if len(k) > 7 && k[:7] == "log/L1/" && k > lastSeg {
				lastSeg = k
			}
		}
		o[ckptDescObj(m.Logs["L1"].Ckpt.Key)] = []byte("garbage")
		o[lastSeg] = o[lastSeg][:len(o[lastSeg])/2]
	})
	// by default a damaged tail is corruption: refuse, change nothing
	dump := func() string {
		var b strings.Builder
		st.Raw(func(o map[string][]byte) {
			ks := make([]string, 0, len(o))
			for k := range o {
				if !strings.HasPrefix(k, "manifest/") { // open takes a new epoch first
					ks = append(ks, k)
				}
			}
			sort.Strings(ks)
			for _, k := range ks {
				fmt.Fprintf(&b, "%s=%x;", k, o[k])
			}
		})
		return b.String()
	}
	before := dump()
	if _, err := OpenStore(ctx, st); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open with damaged tail: %v, want ErrCorrupt", err)
	}
	if dump() != before {
		t.Fatal("refused open changed log data")
	}
	db = mustOpen(t, st, WithRepairTail(true))
	defer db.Close()
	if _, err := db.Get(ctx, "k0"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, "after", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

func TestCorruptMiddleSegmentRefusesOpen(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithCheckpointBytes(-1))
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("v"))
	}
	db.Close()
	st.Raw(func(o map[string][]byte) {
		k := segKey("L1", 3)
		o[k][len(o[k])-1] ^= 1
	})
	if _, err := OpenStore(ctx, st, quiet()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestSerializableNoLostUpdate(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				err := db.Update(ctx, func(tx *Tx) error {
					v, _ := tx.Get("counter")
					n := 0
					fmt.Sscan(string(v), &n)
					return tx.Put("counter", []byte(fmt.Sprint(n+1)))
				})
				if err != nil && !errors.Is(err, ErrConflict) {
					t.Error(err)
				}
				if err == nil {
					mu.Lock()
					ok++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	// every acknowledged increment must be counted exactly once
	v, _ := db.Get(ctx, "counter")
	if string(v) != fmt.Sprint(ok) || ok < 300 {
		t.Fatalf("counter=%s acked=%d", v, ok)
	}
}

func TestBranchRewindPromoteTimeTravel(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	db.Put(ctx, "plan", []byte("A0"))
	lsn1, _ := db.Branch("main").Head()
	db.Put(ctx, "plan", []byte("A1"))
	if err := db.CreateBranch(ctx, "try-b", "main", lsn1); err != nil {
		t.Fatal(err)
	}
	b := db.Branch("try-b")
	b.Update(ctx, func(tx *Tx) error { return tx.Put("plan", []byte("B1")) })
	get := func(br string) string {
		var s string
		db.Branch(br).View(ctx, func(tx *Tx) error { v, _ := tx.Get("plan"); s = string(v); return nil })
		return s
	}
	if get("main") != "A1" || get("try-b") != "B1" {
		t.Fatalf("isolation: main=%s b=%s", get("main"), get("try-b"))
	}
	// time travel
	db.Branch("main").ViewAt(ctx, lsn1, func(tx *Tx) error {
		if v, _ := tx.Get("plan"); string(v) != "A0" {
			t.Fatalf("at %d: %s", lsn1, v)
		}
		return nil
	})
	// rewind main to lsn1, then write
	if err := db.Rewind(ctx, "main", lsn1); err != nil {
		t.Fatal(err)
	}
	if get("main") != "A0" {
		t.Fatal("rewind failed")
	}
	db.Put(ctx, "plan", []byte("A2"))
	// promote is not fast-forward now (main diverged from try-b)
	if err := db.Promote(ctx, "try-b", "main"); !errors.Is(err, ErrNotFastForward) {
		t.Fatalf("expected not-ff, got %v", err)
	}
	db.CreateBranch(ctx, "next", "main", 0)
	db.Branch("next").Update(ctx, func(tx *Tx) error { return tx.Put("plan", []byte("N1")) })
	if err := db.Promote(ctx, "next", "main"); err != nil {
		t.Fatal(err)
	}
	if get("main") != "N1" {
		t.Fatal("promote")
	}
	// old head still reachable via reflog
	m := db.Manifest()
	var old string
	for _, r := range m.Reflog {
		if r.Why == "rewind" {
			old = r.From
		}
	}
	db.ViewLog(ctx, old, 0, func(tx *Tx) error {
		if v, _ := tx.Get("plan"); string(v) != "A1" {
			t.Fatalf("old head %s", v)
		}
		return nil
	})
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	if get("main") != "N1" || get("try-b") != "B1" {
		t.Fatal("branches after restart")
	}
	r, _ := db.Verify(ctx)
	if !r.OK {
		t.Fatalf("%+v", r)
	}
}

func TestShred(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	db.Update(ctx, func(tx *Tx) error {
		tx.Put("pref/u7", []byte("likes tea"), Subject("u-7"))
		tx.Put("pref/u8", []byte("likes coffee"), Subject("u-8"))
		tx.PutRecord("t:mem", "m1", []byte("tea"), Subject("u-7"))
		tx.PutRecord("t:mem", "m2", []byte("tea"), Subject("u-8"))
		return tx.PutRecord("t:mem", "m3", []byte("tea"))
	})
	before, _ := db.Branch("main").Head()
	db.CreateBranch(ctx, "old", "main", 0)
	cert, err := db.Shred(ctx, "u-7")
	if err != nil || !VerifyCertificate(cert) {
		t.Fatalf("cert %v %v", cert, err)
	}
	check := func(db *DB, br string, at uint64) {
		db.Branch(br).ViewAt(ctx, at, func(tx *Tx) error {
			if _, err := tx.Get("pref/u7"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s@%d: shredded KV readable", br, at)
			}
			if v, _ := tx.Get("pref/u8"); string(v) != "likes coffee" {
				t.Fatalf("other subject damaged")
			}
			if _, err := tx.GetRecord("t:mem", "m1"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s@%d: shredded record readable: %v", br, at, err)
			}
			if n, _ := tallyCount(tx, "t:mem", "tea"); n != 2 {
				t.Fatalf("%s@%d: derived index still counts the shredded record: %d", br, at, n)
			}
			return nil
		})
	}
	check(db, "main", 0)
	check(db, "main", before) // time travel to before the shred
	check(db, "old", 0)       // a branch created before the shred
	if err := db.Put(ctx, "x", []byte("y"), Subject("u-7")); !errors.Is(err, ErrShredded) {
		t.Fatalf("write to shredded subject: %v", err)
	}
	r, _ := db.Verify(ctx)
	if !r.OK || r.Unreadable == 0 {
		t.Fatalf("verify after shred: %+v", r)
	}
	// index == fold(redact(log)) after shred
	d1, _, _ := db.IndexDigests("main")
	d2, _ := db.RebuildDigests("main")
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatal("index != fold(redacted log) after shred")
	}
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	check(db, "main", 0)
	check(db, "old", 0)
	// no plaintext of u-7 anywhere in the bucket
	st.Raw(func(o map[string][]byte) {
		for k, v := range o {
			for _, s := range []string{"likes tea", "secret", "u7 did x"} {
				if contains(v, s) {
					t.Fatalf("plaintext %q found in %s", s, k)
				}
			}
		}
	})
}

func contains(b []byte, s string) bool {
	return len(s) > 0 && len(b) >= len(s) && (string(b) == s || indexOf(b, s) >= 0)
}

func indexOf(b []byte, s string) int {
	for i := 0; i+len(s) <= len(b); i++ {
		if string(b[i:i+len(s)]) == s {
			return i
		}
	}
	return -1
}

func TestFencingZombieWriter(t *testing.T) {
	st := store.NewMem()
	a := mustOpen(t, st)
	a.Put(ctx, "k", []byte("a1"))
	b := mustOpen(t, st) // takes over: epoch+1, fences every log
	if b.Epoch() <= a.Epoch() {
		t.Fatal("epoch")
	}
	if err := a.Put(ctx, "k", []byte("zombie")); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie write: %v", err)
	}
	if err := a.CreateBranch(ctx, "z", "main", 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie manifest write: %v", err)
	}
	if err := b.Put(ctx, "k", []byte("b1")); err != nil {
		t.Fatal(err)
	}
	if v, _ := b.Get(ctx, "k"); string(v) != "b1" {
		t.Fatal(string(v))
	}
	a.Close()
	b.Close()
}

func TestSnapshotEqualsFold(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(5))
	for i := 0; i < 23; i++ {
		db.Update(ctx, func(tx *Tx) error {
			tx.Put(fmt.Sprint("k", i%4), []byte(fmt.Sprint(i)))
			return tx.PutRecord("t:x", fmt.Sprint(i%7), []byte{byte(i % 3)})
		})
	}
	db.Close()
	db = mustOpen(t, st, WithSnapshotEvery(5))
	defer db.Close()
	if db.Manifest().Logs["L1"].Ckpt == nil {
		t.Fatal("no checkpoint written")
	}
	d1, _, _ := db.IndexDigests("main")
	d2, _ := db.RebuildDigests("main")
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatal("snapshot+tail != full fold")
	}
}

// Regression for the race found by TLC (spec/LogEtLog.tla): concurrent
// openers must never leave the log with epochs going backwards.
func TestConcurrentOpenersNeverBrickLog(t *testing.T) {
	for round := 0; round < 30; round++ {
		st := store.NewMem()
		db := mustOpen(t, st)
		db.Put(ctx, "k", []byte("v"))
		db.Close()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				d, err := OpenStore(ctx, st, quiet())
				if err != nil {
					return // losing an open race is fine (ErrFenced / retry)
				}
				d.Put(ctx, fmt.Sprint("w", i), []byte("x"))
				d.Crash()
			}(i)
		}
		wg.Wait()
		d, err := OpenStore(ctx, st, quiet())
		if err != nil {
			t.Fatalf("round %d: log bricked by racing openers: %v", round, err)
		}
		if r, _ := d.Verify(ctx); !r.OK {
			t.Fatalf("round %d: %+v", round, r)
		}
		d.Close()
	}
}
