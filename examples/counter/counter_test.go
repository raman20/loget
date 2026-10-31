package counter

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/raman20/loget"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

var ctx = context.Background()

func open(t *testing.T, st store.Store, k index.Kind, opts ...loget.Option) *loget.DB {
	t.Helper()
	opts = append([]loget.Option{loget.WithLogf(t.Logf), loget.WithIndex(k)}, opts...)
	db, err := loget.OpenStore(ctx, st, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func count(t *testing.T, db *loget.DB, br string, at uint64, name, val string) int {
	t.Helper()
	var n int
	err := db.Branch(br).ViewAt(ctx, at, func(tx *loget.Tx) error {
		var err error
		n, err = Count(tx, name, val)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// indexEqualsFold: the live index equals a fold of the log from scratch.
func indexEqualsFold(t *testing.T, db *loget.DB, br string) {
	t.Helper()
	d1, _, err := db.IndexDigests(br)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := db.RebuildDigests(br)
	if err != nil {
		t.Fatal(err)
	}
	if d1[Name] != d2[Name] || d1[Name] == ([32]byte{}) {
		t.Fatalf("%s: counter index != fold(log)", br)
	}
}

func TestCounts(t *testing.T) {
	db := open(t, store.NewMem(), Kind())
	defer db.Close()
	must := func(fn func(tx *loget.Tx) error) {
		t.Helper()
		if err := db.Update(ctx, fn); err != nil {
			t.Fatal(err)
		}
	}
	must(func(tx *loget.Tx) error {
		Set(tx, "lunch", "ana", "tacos")
		Set(tx, "lunch", "bo", "tacos")
		Set(tx, "lunch", "cy", "soup")
		return Set(tx, "dinner", "ana", "tacos") // another counter
	})
	if n := count(t, db, "main", 0, "lunch", "tacos"); n != 2 {
		t.Fatalf("tacos %d", n)
	}
	must(func(tx *loget.Tx) error { return Set(tx, "lunch", "bo", "soup") }) // change: moves the count
	must(func(tx *loget.Tx) error { return Remove(tx, "lunch", "cy") })
	must(func(tx *loget.Tx) error { return Remove(tx, "lunch", "nobody") }) // absent: no-op
	db.View(ctx, func(tx *loget.Tx) error {
		c, err := Counts(tx, "lunch")
		if err != nil || fmt.Sprint(c) != "map[soup:1 tacos:1]" {
			t.Fatalf("lunch %v %v", c, err)
		}
		c, _ = Counts(tx, "dinner")
		if fmt.Sprint(c) != "map[tacos:1]" {
			t.Fatalf("dinner %v", c)
		}
		c, _ = Counts(tx, "none")
		if len(c) != 0 {
			t.Fatalf("none %v", c)
		}
		// the items are ordinary records
		v, err := tx.GetRecord(Prefix+"lunch", "bo")
		if err != nil || string(v) != "soup" {
			t.Fatalf("record %q %v", v, err)
		}
		return nil
	})
	// the index folds committed records: a transaction's own writes are
	// in its record reads, not yet in the counts
	must(func(tx *loget.Tx) error {
		Set(tx, "lunch", "dee", "tacos")
		if n, _ := Count(tx, "lunch", "tacos"); n != 1 {
			t.Errorf("own write counted: %d", n)
		}
		if v, _ := tx.GetRecord(Prefix+"lunch", "dee"); string(v) != "tacos" {
			t.Errorf("own write not readable as a record")
		}
		return nil
	})
	indexEqualsFold(t, db, "main")
}

func TestNotRegistered(t *testing.T) {
	db, err := loget.OpenStore(ctx, store.NewMem(), loget.WithLogf(t.Logf))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(ctx, func(tx *loget.Tx) error { return Set(tx, "c", "i", "v") })
	if !errors.Is(err, loget.ErrBadValue) {
		t.Fatalf("write to an unowned space: %v", err)
	}
	err = db.View(ctx, func(tx *loget.Tx) error { _, err := Count(tx, "c", "v"); return err })
	if !errors.Is(err, loget.ErrNotFound) {
		t.Fatalf("read of an unregistered index: %v", err)
	}
}

// TestDeclaredReadsKeepInvariant: "at most one seat" holds under a
// concurrent writer because Count declares the counter read (OCC): the
// first attempt conflicts, the retry sees the seat taken.
func TestDeclaredReadsKeepInvariant(t *testing.T) {
	db := open(t, store.NewMem(), Kind())
	defer db.Close()
	errFull := errors.New("full")
	var attempts atomic.Int32
	err := db.Update(ctx, func(tx *loget.Tx) error {
		n, err := Count(tx, "seats", "taken")
		if err != nil {
			return err
		}
		if attempts.Add(1) == 1 { // a rival takes the last seat meanwhile
			done := make(chan error)
			go func() {
				done <- db.Update(ctx, func(tx *loget.Tx) error { return Set(tx, "seats", "rival", "taken") })
			}()
			if err := <-done; err != nil {
				return err
			}
		}
		if n >= 1 {
			return errFull
		}
		return Set(tx, "seats", "me", "taken")
	})
	if !errors.Is(err, errFull) || attempts.Load() != 2 {
		t.Fatalf("got %v after %d attempts, want full after 2", err, attempts.Load())
	}
	if n := count(t, db, "main", 0, "seats", "taken"); n != 1 {
		t.Fatalf("%d seats taken", n)
	}
}

func TestTimeTravelAndBranches(t *testing.T) {
	db := open(t, store.NewMem(), Kind())
	defer db.Close()
	var lsns []uint64
	for i := 0; i < 6; i++ {
		lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *loget.Tx) error {
			return Set(tx, "c", fmt.Sprint("i", i%4), fmt.Sprint(i%2))
		})
		if err != nil {
			t.Fatal(err)
		}
		lsns = append(lsns, lsn)
	}
	// after i: items i0..i3 = 0,1,0,1 then i0=0 (i=4), i1=1 (i=5)
	for i, want := range []int{1, 1, 2, 2, 2, 2} {
		if n := count(t, db, "main", lsns[i], "c", "0"); n != want {
			t.Fatalf("at %d: %d, want %d", lsns[i], n, want)
		}
	}
	if err := db.CreateBranch(ctx, "exp", "main", lsns[1]); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("exp").Update(ctx, func(tx *loget.Tx) error { return Set(tx, "c", "x", "0") }); err != nil {
		t.Fatal(err)
	}
	if a, b := count(t, db, "exp", 0, "c", "0"), count(t, db, "main", 0, "c", "0"); a != 2 || b != 2 {
		t.Fatalf("exp %d main %d", a, b)
	}
	if n := count(t, db, "exp", 0, "c", "1"); n != 1 {
		t.Fatalf("exp c=1: %d", n)
	}
	if err := db.Rewind(ctx, "main", lsns[0]); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "main", 0, "c", "0"); n != 1 {
		t.Fatalf("after rewind: %d", n)
	}
	indexEqualsFold(t, db, "main")
	indexEqualsFold(t, db, "exp")
}

func TestShred(t *testing.T) {
	db := open(t, store.NewMem(), Kind(), loget.WithSnapshotEvery(2))
	defer db.Close()
	for i, voter := range []string{"ana", "bo", "cy", "ana"} {
		err := db.Update(ctx, func(tx *loget.Tx) error {
			return Set(tx, "poll", fmt.Sprint(voter, i), "yes", loget.Subject(voter))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	before, _ := db.Branch("main").Head()
	if _, err := db.Shred(ctx, "ana"); err != nil {
		t.Fatal(err)
	}
	for _, at := range []uint64{0, before, before - 1} {
		if n := count(t, db, "main", at, "poll", "yes"); n > 2 {
			t.Fatalf("at %d: shredded votes still counted (%d)", at, n)
		}
	}
	if n := count(t, db, "main", 0, "poll", "yes"); n != 2 {
		t.Fatalf("%d", n)
	}
	indexEqualsFold(t, db, "main")
}

// workload writes n random transactions to db.
func workload(t *testing.T, db *loget.DB, r *rand.Rand, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := db.Update(ctx, func(tx *loget.Tx) error {
			for j := r.Intn(3); j >= 0; j-- {
				name, item := fmt.Sprint("c", r.Intn(3)), fmt.Sprint("i", r.Intn(12))
				var opts []loget.WriteOpt
				if r.Intn(4) == 0 {
					opts = append(opts, loget.Subject(fmt.Sprint("s", r.Intn(3))))
				}
				var err error
				switch r.Intn(5) {
				case 0:
					err = Remove(tx, name, item)
				case 1:
					err = tx.Put(item, []byte("kv")) // other indexes' ops are skipped
				default:
					err = Set(tx, name, item, fmt.Sprint(r.Intn(4)), opts...)
				}
				if err != nil && !errors.Is(err, loget.ErrShredded) {
					return err
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, loget.ErrShredded) {
			t.Fatal(err)
		}
		if r.Intn(40) == 0 {
			db.Shred(ctx, fmt.Sprint("s", r.Intn(3)))
		}
	}
}

func allCounts(t *testing.T, db *loget.DB) string {
	t.Helper()
	var b strings.Builder
	db.View(ctx, func(tx *loget.Tx) error {
		for i := 0; i < 3; i++ {
			c, err := Counts(tx, fmt.Sprint("c", i))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(&b, c)
		}
		return nil
	})
	return b.String()
}

// TestIndexContract: the counter's state depends only on live records,
// so folding the full log and folding the compacted form (base records
// + tail) give the same state. Every derived index must pass this test
// (docs/index-authoring.md).
func TestIndexContract(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		st := store.NewMem()
		db := open(t, st, Kind(), loget.WithSnapshotEvery(1+r.Intn(8)))
		workload(t, db, r, 30+r.Intn(30))
		full, _, _ := db.IndexDigests("main")
		want := allCounts(t, db)
		if _, err := db.Compact(ctx, loget.CompactOptions{}); err != nil {
			t.Fatal(err)
		}
		folded, err := db.RebuildDigests("main") // base records + tail
		if err != nil {
			t.Fatal(err)
		}
		if folded[Name] != full[Name] {
			t.Fatalf("seed %d: fold(compacted) != fold(full log)", seed)
		}
		if got := allCounts(t, db); got != want {
			t.Fatalf("seed %d: counts changed by compaction:\n%s\nwant\n%s", seed, got, want)
		}
		workload(t, db, r, 20)
		want = allCounts(t, db)
		indexEqualsFold(t, db, "main")
		db.Close()
		db = open(t, st, Kind()) // snapshot restore + tail
		if got := allCounts(t, db); got != want {
			t.Fatalf("seed %d: after reopen\n%s\nwant\n%s", seed, got, want)
		}
		indexEqualsFold(t, db, "main")
		db.Close()
	}
}

// TestSnapshotRestoreAndVersionBump: a reopen restores the index from the
// snapshot section; after a version bump the old section is ignored and
// the index is rebuilt by replay, with the same state.
func TestSnapshotRestoreAndVersionBump(t *testing.T) {
	st := store.NewMem()
	db := open(t, st, Kind(), loget.WithSnapshotEvery(5))
	workload(t, db, rand.New(rand.NewSource(7)), 40)
	want := allCounts(t, db)
	db.Close()

	var restores atomic.Int32
	counting := func() index.Kind {
		k := Kind()
		k.Restore = func(b []byte) (index.Index, error) { restores.Add(1); return restore(b) }
		return k
	}
	db = open(t, st, counting())
	if restores.Load() != 1 || allCounts(t, db) != want {
		t.Fatalf("v1 reopen: %d restores, counts\n%s", restores.Load(), allCounts(t, db))
	}
	db.Close()

	old := Version
	Version = 2
	defer func() { Version = old }()
	restores.Store(0)
	db = open(t, st, counting())
	defer db.Close()
	if restores.Load() != 0 || allCounts(t, db) != want {
		t.Fatalf("v2 reopen: %d restores of a v1 section, counts\n%s", restores.Load(), allCounts(t, db))
	}
	indexEqualsFold(t, db, "main")
}

func TestSnapshotEncoding(t *testing.T) {
	s := newIndex().(*state)
	s.set(Prefix+"b", "k2", item{"x", ""})
	s.set(Prefix+"a", "k1", item{"y", "s1"})
	s.set(Prefix+"a", "k0", item{"y", ""})
	b := s.Snapshot()
	r, err := restore(b)
	if err != nil || string(r.(*state).Snapshot()) != string(b) || r.Digest() != s.Digest() {
		t.Fatalf("round trip: %v", err)
	}
	if fmt.Sprint(r.(*state).counts) != fmt.Sprint(s.counts) {
		t.Fatalf("counts %v vs %v", r.(*state).counts, s.counts)
	}
	for n := 1; n < len(b); n++ {
		if _, err := restore(b[:n]); err == nil {
			if _, err2 := restore(b[n:]); err2 == nil {
				continue // a cut between two whole items is a valid shorter snapshot
			}
			t.Fatalf("truncation to %d accepted", n)
		}
	}
	s.DropSubject("s1")
	if s.counts[Prefix+"a"]["y"] != 1 {
		t.Fatalf("drop: %v", s.counts)
	}
}

func FuzzRestore(f *testing.F) {
	s := newIndex().(*state)
	s.set(Prefix+"a", "k", item{"v", "s"})
	f.Add(s.Snapshot())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		l, err := restore(b)
		if err != nil {
			return
		}
		if again := l.(*state).Snapshot(); string(again) != string(b) {
			t.Fatalf("accepted bytes that do not re-encode identically")
		}
	})
}
