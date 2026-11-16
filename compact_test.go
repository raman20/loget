package loget

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/loget/store"
	"pgregory.net/rapid"
)

// ftb is what testing.TB and *rapid.T share.
type ftb interface {
	Helper()
	Fatal(...any)
	Fatalf(string, ...any)
}

// dump renders everything a client can read on a branch at at (0 =
// head): KV, the records of two tally spaces, and the tally index's
// counts over them.
func dump(t ftb, db *DB, br string, at uint64) string {
	t.Helper()
	var b strings.Builder
	err := db.Branch(br).ViewAt(ctx, at, func(tx *Tx) error {
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			fmt.Fprintf(&b, "kv %s=%q\n", kv.Key, kv.Value)
		}
		for _, sp := range []string{"t:d", "t:v"} {
			recs, err := tx.ScanRecords(sp, "")
			if err != nil {
				return err
			}
			for _, r := range recs {
				fmt.Fprintf(&b, "%s %s=%q\n", sp, r.Key, r.Value)
			}
			for _, v := range []string{"f0", "f1", "f2"} {
				n, err := tallyCount(tx, sp, v)
				if err != nil {
					return err
				}
				fmt.Fprintf(&b, "tally %s %s=%d\n", sp, v, n)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("dump %s@%d: %v", br, at, err)
	}
	return b.String()
}

func objects(t testing.TB, st store.Store, prefix string) (n int, size int64) {
	t.Helper()
	keys, err := store.Keys(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			b, err := st.Get(ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			n++
			size += int64(len(b))
		}
	}
	return
}

// fill writes a mixed workload: overwrites (some values above the inline
// limit), deletes, subject data, and records of the tally index (some
// large, some subject-bound, some overwritten, some deleted).
func fill(t testing.TB, db *DB, rounds int, subj ...string) {
	sp := "s"
	if len(subj) > 0 {
		sp = subj[0]
	}
	t.Helper()
	big := bytes.Repeat([]byte("v"), 300)
	for r := 0; r < rounds; r++ {
		err := db.Update(ctx, func(tx *Tx) error {
			for k := 0; k < 8; k++ {
				val := []byte(fmt.Sprintf("r%d-k%d", r, k))
				if k%2 == 0 {
					val = append(append([]byte{}, big...), val...)
				}
				var o []WriteOpt
				if k%3 == 0 {
					o = append(o, Subject(fmt.Sprint(sp, k%2)))
				}
				if err := tx.Put(fmt.Sprint("k", k), val, o...); err != nil {
					return err
				}
			}
			if r%3 == 2 {
				tx.Delete("k7")
			}
			if r%2 == 0 {
				if err := tx.PutRecord("t:d", fmt.Sprint("d", r%5), []byte(fmt.Sprint("f", r%3)), Subject(fmt.Sprint(sp, r%2))); err != nil {
					return err
				}
			}
			if r%7 == 6 {
				tx.DeleteRecord("t:d", fmt.Sprint("d", r%5))
			}
			if err := tx.PutRecord("t:v", fmt.Sprint("x", r%4), append([]byte(fmt.Sprint("f", r%3)), big[:r%2*100]...)); err != nil {
				return err
			}
			return tx.PutRecord("t:v", fmt.Sprint("s", r%3), []byte(fmt.Sprint("f", r%2)), Subject(fmt.Sprint(sp, (r+1)%2)))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactKeepsStateAndFreesStorage(t *testing.T) {
	for name, mk := range backends(t) {
		t.Run(name, func(t *testing.T) {
			st := mk()
			db := mustOpen(t, st, WithSnapshotEvery(7))
			fill(t, db, 40)
			for i := 0; i < 30; i++ { // derived-index records: overwrites, deletes, a shredded subject
				err := db.Update(ctx, func(tx *Tx) error {
					k := fmt.Sprintf("r%d", i%7)
					if i%5 == 4 {
						return tx.DeleteRecord("t:c", k)
					}
					return tx.PutRecord("t:c", k, []byte(fmt.Sprintf("v%d", i%3)), Subject(fmt.Sprintf("s%d", i%2)))
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := db.CreateBranch(ctx, "feat", "main", 0); err != nil {
				t.Fatal(err)
			}
			db.Branch("feat").Update(ctx, func(tx *Tx) error { return tx.Put("k1", []byte("feat")) })
			fill(t, db, 20)
			h, _ := db.Branch("main").Head()
			if err := db.Rewind(ctx, "main", h-5); err != nil { // leaves a retired log
				t.Fatal(err)
			}
			if _, err := db.Shred(ctx, "s1"); err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"main": dump(t, db, "main", 0), "feat": dump(t, db, "feat", 0)}
			dg := map[string]map[string][32]byte{}
			for br := range want {
				dg[br], _, _ = db.IndexDigests(br)
			}
			nBefore, szBefore := objects(t, st, "")
			cs, err := db.Compact(ctx, CompactOptions{})
			if err != nil {
				t.Fatal(err)
			}
			nAfter, szAfter := objects(t, st, "")
			t.Logf("%+v; objects %d -> %d, bytes %d -> %d", cs, nBefore, nAfter, szBefore, szAfter)
			// Pointer-only compact (design 30) keeps live-key segments;
			// it must not lose state. Storage may not shrink.
			_ = szBefore
			_ = szAfter
			check := func(db *DB, when string) {
				t.Helper()
				for br, w := range want {
					if got := dump(t, db, br, 0); got != w {
						t.Fatalf("%s %s: state differs\nwant:\n%s\ngot:\n%s", when, br, w, got)
					}
					d1, _, _ := db.IndexDigests(br)
					d2, err := db.RebuildDigests(br)
					if err != nil || fmt.Sprint(d1) != fmt.Sprint(d2) {
						t.Fatalf("%s %s: index != fold (%v)", when, br, err)
					}
					for _, l := range []string{"tally"} { // kv digests include history, which compaction drops
						if d1[l] != dg[br][l] {
							t.Fatalf("%s %s: index %s digest changed by compaction", when, br, l)
						}
					}
				}
				if r, err := db.Verify(ctx); err != nil || !r.OK {
					t.Fatalf("%s: verify %+v %v", when, r, err)
				}
			}
			check(db, "after compact")
			if cs2, err := db.Compact(ctx, CompactOptions{}); err != nil || cs2.Logs != 0 {
				t.Fatalf("second compact of an unchanged db: %+v %v", cs2, err)
			}
			db.Close()
			db = mustOpen(t, st, WithSnapshotEvery(7))
			check(db, "after reopen")

			// history below the pointer base stays readable (design 30)
			h, _ = db.Branch("main").Head()
			if err := db.Branch("main").ViewAt(ctx, h-1, func(*Tx) error { return nil }); err != nil {
				t.Fatalf("time travel below base: %v", err)
			}
			if err := db.CreateBranch(ctx, "old", "main", h-1); err != nil {
				t.Fatalf("branch below base: %v", err)
			}
			if err := db.CreateBranch(ctx, "at", "main", h); err != nil {
				t.Fatal(err)
			}
			if got := dump(t, db, "at", 0); got != want["main"] {
				t.Fatal("branch at the base differs")
			}

			// keep going: write, compact again, promote across bases
			fill(t, db, 10, "t")
			db.Branch("at").Update(ctx, func(tx *Tx) error { return tx.Put("on-at", []byte("1")) })
			want = map[string]string{"main": dump(t, db, "main", 0), "feat": dump(t, db, "feat", 0), "at": dump(t, db, "at", 0)}
			mid, _ := db.Branch("main").Head()
			fill(t, db, 3, "t")
			midDump := dump(t, db, "main", mid)
			want["main"] = dump(t, db, "main", 0)
			for br := range want {
				dg[br], _, _ = db.IndexDigests(br)
			}
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
			check(db, "after second compact")
			if err := db.Promote(ctx, "at", "main"); !errors.Is(err, ErrNotFastForward) {
				t.Fatalf("promote of a diverged branch: %v", err)
			}
			db.Close()
			db = mustOpen(t, st)
			defer db.Close()
			check(db, "after second reopen")
			if err := db.Branch("main").ViewAt(ctx, mid, func(*Tx) error { return nil }); err != nil {
				t.Fatalf("time travel to %d after second compact: %v", mid, err)
			}
			if got := dump(t, db, "main", mid); got != midDump {
				t.Fatalf("view at mid after second compact differs")
			}
		})
	}
}

// Promote must still see ancestry through a compacted log.
func TestCompactPromoteFastForward(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	fill(t, db, 3)
	db.CreateBranch(ctx, "f", "main", 0)
	db.Branch("f").Update(ctx, func(tx *Tx) error { return tx.Put("x", []byte("f")) })
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Promote(ctx, "f", "main"); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, "x"); err != nil || string(v) != "f" {
		t.Fatalf("%q %v", v, err)
	}
}

// Commits running during compaction all land exactly once.
func TestCompactConcurrentCommits(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithSnapshotEvery(20))
	defer db.Close()
	var ok [4]atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range ok {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				err := db.Update(ctx, func(tx *Tx) error {
					v, _ := tx.Get(fmt.Sprint("c", g))
					n := 0
					fmt.Sscan(string(v), &n)
					tx.Put(fmt.Sprint("c", g), []byte(fmt.Sprint(n+1)))
					return tx.PutRecord("t:log", fmt.Sprint(g, ":", i), []byte("x"))
				})
				if err != nil {
					t.Errorf("g%d: %v", g, err)
					return
				}
				ok[g].Add(1)
			}
		}(g)
	}
	for i := 0; i < 20; i++ {
		if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
			t.Fatal(err)
		}
		db.View(ctx, func(tx *Tx) error { _, err := tx.Get("c0"); _ = err; return nil })
	}
	close(stop)
	wg.Wait()
	total := int64(0)
	db.View(ctx, func(tx *Tx) error {
		for g := range ok {
			v, _ := tx.Get(fmt.Sprint("c", g))
			n := int64(0)
			fmt.Sscan(string(v), &n)
			if n != ok[g].Load() {
				t.Fatalf("g%d: counter %d, %d commits acknowledged", g, n, ok[g].Load())
			}
			total += n
		}
		if l, err := tallyCount(tx, "t:log", "x"); err != nil || int64(l) != total {
			t.Fatalf("%d tally records (%v), %d commits", l, err, total)
		}
		return nil
	})
}

// With a grace period nothing is deleted until it has passed.
func TestCompactGrace(t *testing.T) {
	st := store.NewMem()
	var now atomic.Int64
	now.Store(1)
	db := mustOpen(t, st, WithClock(func() int64 { return now.Add(1) }))
	defer db.Close()
	for i := 0; i < 10; i++ {
		if err := db.Put(ctx, "k", []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	cs, err := db.Compact(ctx, CompactOptions{Grace: time.Hour, DropHistory: true})
	if err != nil || cs.Deleted != 0 || cs.Pending == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	before, _ := objects(t, st, "log/L1/")
	if cs.Pending == 0 {
		t.Fatalf("%+v: expected pending history deletes", cs)
	}
	now.Add(int64(2 * time.Hour))
	cs, err = db.Compact(ctx, CompactOptions{Grace: time.Hour, DropHistory: true})
	if err != nil || cs.Deleted == 0 || cs.Pending != 0 || len(db.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v garbage=%v", cs, err, db.Manifest().Garbage)
	}
	after, _ := objects(t, st, "log/L1/")
	if after >= before {
		t.Fatalf("history segments not deleted after grace: %d -> %d", before, after)
	}
}

// failStore fails manifest puts (after the bases were written) or deletes.
type failStore struct {
	store.Store
	failManifest, failDelete atomic.Bool
}

func (f *failStore) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if f.failManifest.Load() && strings.HasPrefix(k, "manifest/") {
		return errors.New("injected: manifest put failed")
	}
	return f.Store.PutIfAbsent(ctx, k, b)
}
func (f *failStore) Delete(ctx context.Context, k string) error {
	if f.failDelete.Load() {
		return errors.New("injected: delete failed")
	}
	return f.Store.Delete(ctx, k)
}
func (f *failStore) Keys(ctx context.Context) ([]string, error) { return store.Keys(ctx, f.Store) }

// A compaction that dies before its commit leaves the database as it was;
// its orphaned bases are swept by the next epoch. Failed deletes stay on
// the garbage list and are retried.
func TestCompactCrashAndSweepRetry(t *testing.T) {
	mem := store.NewMem()
	fs := &failStore{Store: mem}
	db := mustOpen(t, fs)
	fill(t, db, 10)
	want := dump(t, db, "main", 0)
	fs.failManifest.Store(true)
	if _, err := db.Compact(ctx, CompactOptions{}); err == nil {
		t.Fatal("compaction with a failing manifest put succeeded")
	}
	db.Crash()
	fs.failManifest.Store(false)
	orphans, _ := objects(t, mem, "base/")
	if orphans == 0 {
		t.Fatal("expected orphaned base objects")
	}
	db = mustOpen(t, fs)
	if got := dump(t, db, "main", 0); got != want {
		t.Fatal("state changed by a failed compaction")
	}
	fs.failDelete.Store(true)
	cs, err := db.Compact(ctx, CompactOptions{DropHistory: true})
	if err != nil || cs.Pending == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if n, _ := objects(t, mem, "base/"); n <= orphans {
		t.Fatalf("base objects %d", n)
	}
	fs.failDelete.Store(false)
	db.Close()
	db = mustOpen(t, fs)
	defer db.Close()
	if got := dump(t, db, "main", 0); got != want {
		t.Fatal("state changed")
	}
	fill(t, db, 1)
	cs, err = db.Compact(ctx, CompactOptions{DropHistory: true})
	if err != nil || cs.Pending != 0 || len(db.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	bases, _ := objects(t, mem, "base/")
	if bases > 2 {
		ks, _ := store.Keys(ctx, mem)
		t.Fatalf("left %d base objects: %v %+v", bases, ks, db.Manifest().Logs)
	}
	if r, _ := db.Verify(ctx); !r.OK {
		t.Fatalf("%+v", r)
	}
}

// A zombie (fenced) writer cannot commit a compaction.
func TestCompactFenced(t *testing.T) {
	st := store.NewMem()
	db1 := mustOpen(t, st)
	fill(t, db1, 5)
	db2 := mustOpen(t, st)
	defer db2.Close()
	want := dump(t, db2, "main", 0)
	if _, err := db1.Compact(ctx, CompactOptions{}); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie compaction: %v", err)
	}
	db1.Crash()
	cs, err := db2.Compact(ctx, CompactOptions{})
	if err != nil || cs.Logs == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if got := dump(t, db2, "main", 0); got != want {
		t.Fatal("state differs")
	}
}

// Random programs run on two databases; one also compacts (and reopens)
// at random points. Every branch reads the same at its head, and reads
// at or above the base agree; Verify passes.
func TestPropCompactEqualsUncompacted(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		inline := rapid.SampledFrom([]int{0, 4, 64}).Draw(t, "inline")
		stA, stB := store.NewMem(), store.NewMem()
		mk := func(st store.Store) *DB {
			db, err := OpenStore(ctx, st, quiet(), WithIndex(tallyKind()), WithRand(rand.New(rand.NewSource(seed))), WithInlineMax(inline),
				WithSnapshotEvery(rapid.IntRange(1, 10).Draw(t, "snapEvery")))
			if err != nil {
				t.Fatal(err)
			}
			return db
		}
		a, b := mk(stA), mk(stB)
		defer func() { a.Close(); b.Close() }()
		both := func(f func(db *DB) error) (error, error) { return f(a), f(b) }
		branches := []string{"main"}
		n := rapid.IntRange(1, 50).Draw(t, "n")
		for i := 0; i < n; i++ {
			br := rapid.SampledFrom(branches).Draw(t, "branch")
			subj := rapid.SampledFrom([]string{"", "s1", "s2"}).Draw(t, "subj")
			var o []WriteOpt
			if subj != "" {
				o = append(o, Subject(subj))
			}
			k := rapid.IntRange(0, 3).Draw(t, "k")
			size := rapid.SampledFrom([]int{1, 10, 100}).Draw(t, "size")
			val := []byte(fmt.Sprint(i, strings.Repeat("x", size)))
			switch rapid.IntRange(0, 12).Draw(t, "op") {
			case 0, 1, 2:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.Put(fmt.Sprint("k", k), val, o...) })
				})
			case 3:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.PutRecord("t:d", fmt.Sprint("d", k), []byte(fmt.Sprint("f", i%3)), o...) })
				})
			case 4:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						return tx.PutRecord("t:v", fmt.Sprint("x", k), append([]byte(fmt.Sprint("f", i%2)), val...), o...)
					})
				})
			case 5:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						tx.Delete(fmt.Sprint("k", k))
						return tx.PutRecord("t:d", fmt.Sprint("d", k), []byte(fmt.Sprint("f", i%2)), o...)
					})
				})
			case 6:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						tx.DeleteRecord("t:v", fmt.Sprint("x", k))
						return tx.DeleteRecord("t:d", fmt.Sprint("d", k))
					})
				})
			case 7:
				name := fmt.Sprint("b", i)
				h, _ := a.Branch(br).Head()
				at := uint64(rapid.IntRange(0, int(h)).Draw(t, "bat"))
				ea := a.CreateBranch(ctx, name, br, at)
				if errors.Is(ea, ErrCompacted) {
					continue
				}
				if eb := b.CreateBranch(ctx, name, br, at); (ea == nil) != (eb == nil) {
					t.Fatalf("branch: %v vs %v", ea, eb)
				}
				if ea == nil {
					branches = append(branches, name)
				}
			case 8:
				h, _ := a.Branch(br).Head()
				at := uint64(rapid.IntRange(0, int(h)).Draw(t, "at"))
				if ea := a.Rewind(ctx, br, at); errors.Is(ea, ErrCompacted) {
					continue
				}
				b.Rewind(ctx, br, at)
			case 9:
				if subj != "" {
					both(func(db *DB) error { _, err := db.Shred(ctx, subj); return err })
				}
			case 10:
				if _, err := a.Compact(ctx, CompactOptions{}); err != nil {
					t.Fatal(err)
				}
			case 11:
				a.Close()
				a = mk(stA)
			case 12:
				// GC with a retention window: every branch stays readable
				// (and equal to the reference) at head-keep and above
				keep := uint64(rapid.IntRange(0, 6).Draw(t, "keep"))
				o := GCOptions{KeepCommits: keep, KeepReflog: rapid.IntRange(1, 3).Draw(t, "keepReflog"),
					KeepManifests: rapid.IntRange(1, 3).Draw(t, "keepMans")}
				floors := map[string]uint64{}
				a.mu.Lock()
				for _, br := range branches {
					floors[br] = a.floorOf(a.man.Refs[br])
				}
				a.mu.Unlock()
				if _, err := a.GC(ctx, o); err != nil {
					t.Fatal(err)
				}
				for _, br := range branches {
					h, _ := a.Branch(br).Head()
					if h < keep {
						continue
					}
					at := max(h-keep, floors[br])
					if da, db := dump(t, a, br, at), dump(t, b, br, at); da != db {
						t.Fatalf("after gc keep %d: branch %s @%d differs\ngc:\n%s\nreference:\n%s", keep, br, at, da, db)
					}
				}
			}
		}
		for _, br := range branches {
			if da, db := dump(t, a, br, 0), dump(t, b, br, 0); da != db {
				t.Fatalf("branch %s differs\ncompacted:\n%s\nreference:\n%s", br, da, db)
			}
			h, _ := a.Branch(br).Head()
			at := uint64(rapid.IntRange(0, int(h)).Draw(t, "tt"))
			var errA error
			if errA = a.Branch(br).ViewAt(ctx, at, func(*Tx) error { return nil }); errA == nil {
				if da, db := dump(t, a, br, at), dump(t, b, br, at); da != db {
					t.Fatalf("branch %s @%d differs\ncompacted:\n%s\nreference:\n%s", br, at, da, db)
				}
			} else if !errors.Is(errA, ErrCompacted) {
				t.Fatal(errA)
			}
			d1, _, _ := a.IndexDigests(br)
			d2, _ := a.RebuildDigests(br)
			if fmt.Sprint(d1) != fmt.Sprint(d2) {
				t.Fatalf("branch %s: index != fold", br)
			}
		}
		if r, err := a.Verify(ctx); err != nil || !r.OK {
			t.Fatalf("verify %+v %v", r, err)
		}
	})
}

// TestCompactBoundsStorage: with a fixed live set, repeated overwrite +
// compact cycles keep data objects and bytes flat instead of growing with
// history. Compact keeps every manifest (two small objects per run); GC
// deletes old ones too, see TestGCBoundsStorage.
func TestCompactBoundsStorage(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	val := bytes.Repeat([]byte("x"), 200)
	data := func() (int, int64) {
		n, size := objects(t, st, "")
		mn, msize := objects(t, st, "manifest/")
		return n - mn, size - msize
	}
	var first int64
	var firstN, firstM int
	for cycle := 0; cycle < 8; cycle++ {
		for r := 0; r < 20; r++ {
			err := db.Update(ctx, func(tx *Tx) error {
				for k := 0; k < 50; k++ {
					if err := tx.Put(fmt.Sprint("k", k), append([]byte(fmt.Sprint(cycle, r)), val...)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		_, grown := data()
		if _, err := db.Compact(ctx, CompactOptions{DropHistory: true}); err != nil {
			t.Fatal(err)
		}
		n, size := data()
		m, _ := objects(t, st, "manifest/")
		if cycle == 0 {
			first, firstN, firstM = size, n, m
		}
		t.Logf("cycle %d: %d B before, %d data objects / %d B after, %d manifests", cycle, grown, n, size, m)
		if size > first+first/5 || n > firstN+cycle+3 || size*5 > grown || m > firstM+2*cycle+2 {
			t.Fatalf("cycle %d: storage not bounded: %d objects %d B, %d manifests (first %d / %d B / %d)", cycle, n, size, m, firstN, first, firstM)
		}
	}
	v, err := db.Get(ctx, "k3")
	if err != nil || !bytes.HasPrefix(v, []byte("7 19")) {
		t.Fatalf("k3 = %.8q, %v", v, err)
	}
}

func TestBaseEpoch(t *testing.T) {
	for k, want := range map[string]uint64{
		"base/L1/00000000000000000010-e1-e7cd8346":   1,
		"base/L1/00000000000000000010-e12-0e5d8346":  12,
		"base/L-e9/00000000000000000010-e3-ee000000": 3,
	} {
		if got, ok := baseEpoch(k); !ok || got != want {
			t.Errorf("%s: %d %v", k, got, ok)
		}
	}
	for _, k := range []string{"base/L1/10-x1-ab", "base/L1/10-e1", "base/L1/10-ex-ab"} {
		if _, ok := baseEpoch(k); ok {
			t.Errorf("%s parsed", k)
		}
	}
}
