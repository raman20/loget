package loget

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/raman20/loget/store"
	"pgregory.net/rapid"
)

// refRange is the reference for a range read: filter a plain map by the
// options, sort, reverse, cut at the limit.
func refRange(m map[string]string, o IterOptions) []KV {
	var out []KV
	for k, v := range m {
		if strings.HasPrefix(k, o.Prefix) && k >= o.Start && (o.End == "" || k < o.End) {
			out = append(out, KV{k, []byte(v)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return (out[i].Key < out[j].Key) != o.Reverse })
	if o.Limit > 0 && len(out) > o.Limit {
		out = out[:o.Limit]
	}
	return out
}

func eqKVs(a, b []KV) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
			return false
		}
	}
	return true
}

var iterKeys = []string{"", "a", "a\x00", "a\xff", "ab", "abc", "ac", "b", "b1", "b10", "b2", "ba", "c", "\xff", "\xff\xff"}

func drawOpts(t *rapid.T) IterOptions {
	b := append([]string{""}, iterKeys...)
	return IterOptions{
		Prefix:  rapid.SampledFrom(append(b, "a", "b1", "\xff")).Draw(t, "prefix"),
		Start:   rapid.SampledFrom(b).Draw(t, "start"),
		End:     rapid.SampledFrom(b).Draw(t, "end"),
		Reverse: rapid.Bool().Draw(t, "reverse"),
		Limit:   rapid.IntRange(0, 6).Draw(t, "limit"),
	}
}

// TestIterRefinesOrderedMap: committed history, a snapshot at a random
// LSN, uncommitted writes of the reading transaction and random bounds;
// Range, the Iterator and a paginated walk all equal the reference, and
// keep doing so while other commits land between pages.
func TestIterRefinesOrderedMap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		db, err := OpenStore(ctx, store.NewMem(), quiet())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var hist []map[string]string // state after each commit
		cur := map[string]string{}
		var lsns []uint64
		for c := rapid.IntRange(0, 8).Draw(t, "commits"); c > 0; c-- {
			next := map[string]string{}
			for k, v := range cur {
				next[k] = v
			}
			lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error {
				for n := rapid.IntRange(1, 6).Draw(t, "ops"); n > 0; n-- {
					k := rapid.SampledFrom(iterKeys).Draw(t, "key")
					if rapid.IntRange(0, 3).Draw(t, "del") == 0 {
						delete(next, k)
						if err := tx.Delete(k); err != nil {
							return err
						}
					} else {
						v := fmt.Sprint(len(hist), "-", n)
						next[k] = v
						if err := tx.Put(k, []byte(v)); err != nil {
							return err
						}
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			cur = next
			hist, lsns = append(hist, cur), append(lsns, lsn)
		}
		snap, at := map[string]string{}, uint64(0)
		if len(hist) > 0 {
			i := rapid.IntRange(0, len(hist)-1).Draw(t, "snap")
			snap, at = hist[i], lsns[i]
		}
		check := func(tx *Tx, want map[string]string) {
			o := drawOpts(t)
			got, err := tx.Range(o)
			if err != nil {
				t.Fatal(err)
			}
			if w := refRange(want, o); !eqKVs(got, w) {
				t.Fatalf("Range(%+v) = %q, want %q", o, got, w)
			}
			// pages of size limit (at least 1) walk the whole range
			po := o
			po.Limit = max(o.Limit, 1)
			var all []KV
			for range 100 {
				page, err := tx.Range(po)
				if err != nil {
					t.Fatal(err)
				}
				all = append(all, page...)
				if len(page) < po.Limit {
					break
				}
				po = po.After(page[len(page)-1].Key)
			}
			wo := o
			wo.Limit = 0
			if w := refRange(want, wo); !eqKVs(all, w) {
				t.Fatalf("pages of %+v = %q, want %q", o, all, w)
			}
		}
		if err := db.Branch("main").ViewAt(ctx, at, func(tx *Tx) error {
			check(tx, snap)
			// commits between pages are invisible to a running iterator
			it := tx.Iter(IterOptions{})
			var got []KV
			for i := 0; it.Next(); i++ {
				got = append(got, KV{it.Key(), it.Value()})
				if i == 0 {
					if err := db.Put(ctx, "a", []byte("later")); err != nil {
						return err
					}
					if err := db.Update(ctx, func(tx *Tx) error { return tx.Delete("b") }); err != nil {
						return err
					}
				}
			}
			if w := refRange(snap, IterOptions{}); it.Err() != nil || !eqKVs(got, w) {
				t.Fatalf("iterator under concurrent commits = %q %v, want %q", got, it.Err(), w)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		head := map[string]string{}
		_ = db.View(ctx, func(tx *Tx) error {
			kvs, err := tx.Scan("")
			for _, kv := range kvs {
				head[kv.Key] = string(kv.Value)
			}
			return err
		})
		// own writes overlay the snapshot
		err = db.Update(ctx, func(tx *Tx) error {
			want := map[string]string{}
			for k, v := range head {
				want[k] = v
			}
			for n := rapid.IntRange(0, 5).Draw(t, "local"); n > 0; n-- {
				k := rapid.SampledFrom(iterKeys).Draw(t, "lkey")
				if rapid.Bool().Draw(t, "ldel") {
					delete(want, k)
					if err := tx.Delete(k); err != nil {
						return err
					}
				} else {
					want[k] = "local"
					if err := tx.Put(k, []byte("local")); err != nil {
						return err
					}
				}
			}
			check(tx, want)
			return errors.New("rollback")
		})
		if err == nil || err.Error() != "rollback" {
			t.Fatal(err)
		}
	})
}

// TestIterManyPages walks 5000 keys forwards and backwards in pages that
// grow to the maximum, with a write between every step.
func TestIterManyPages(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	const n = 5000
	for i := 0; i < n; i += 500 {
		if err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < i+500; j++ {
				if err := tx.Put(fmt.Sprintf("k%05d", j), []byte(fmt.Sprint(j))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for pass, rev := range []bool{false, true} {
		mine := fmt.Sprint("new", pass)
		_ = db.View(ctx, func(tx *Tx) error {
			i := 0
			for k, v := range tx.Iter(IterOptions{Prefix: "k", Reverse: rev}).All() {
				j := i
				if rev {
					j = n - 1 - i
				}
				if k != fmt.Sprintf("k%05d", j) || string(v) == mine || (string(v) != fmt.Sprint(j) && !strings.HasPrefix(string(v), "new")) {
					t.Fatalf("rev=%v step %d: %q=%q", rev, i, k, v)
				}
				if i%7 == 0 {
					if err := db.Put(ctx, fmt.Sprintf("k%05d", (i*31)%n), []byte(mine)); err != nil {
						t.Fatal(err)
					}
				}
				i++
			}
			if i != n {
				t.Fatalf("rev=%v: %d keys, want %d", rev, i, n)
			}
			return nil
		})
	}
}

// attempts runs fn in an Update and returns how many times it ran; mid
// runs once, after the first attempt's reads, as a concurrent commit.
func attempts(t *testing.T, db *DB, read func(tx *Tx), mid func() error) int {
	t.Helper()
	runs := 0
	err := db.Update(ctx, func(tx *Tx) error {
		runs++
		read(tx)
		if runs == 1 {
			if err := mid(); err != nil {
				return err
			}
		}
		return tx.Put("out", []byte("x"))
	})
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

// TestIterConflictRanges: an Update conflicts with a commit into the part
// of a range it read (phantoms included) and not with one past where it
// stopped, in either direction.
func TestIterConflictRanges(t *testing.T) {
	var db *DB
	fresh := func() {
		if db != nil {
			db.Close()
		}
		db = mustOpen(t, store.NewMem())
		for _, k := range []string{"a", "c", "e", "g"} {
			if err := db.Put(ctx, k, []byte(k)); err != nil {
				t.Fatal(err)
			}
		}
	}
	fresh()
	defer func() { db.Close() }()
	walk := func(o IterOptions) func(tx *Tx) {
		return func(tx *Tx) {
			if _, err := tx.Range(o); err != nil {
				t.Fatal(err)
			}
		}
	}
	put := func(k string) func() error { return func() error { return db.Put(ctx, k, []byte("n")) } }
	cases := []struct {
		name string
		o    IterOptions
		w    string
		runs int
	}{
		{"phantom in gap", IterOptions{Start: "b", End: "f"}, "d", 2},
		{"insert at start bound", IterOptions{Start: "b", End: "f"}, "b", 2},
		{"insert at end bound", IterOptions{Start: "b", End: "f"}, "f", 1},
		{"below start", IterOptions{Start: "b", End: "f"}, "a", 1},
		{"after limit stop", IterOptions{Start: "b", Limit: 1}, "d", 1},
		{"before limit stop", IterOptions{Start: "b", Limit: 2}, "d", 2},
		{"limit stop key itself", IterOptions{Start: "b", Limit: 1}, "c", 2},
		{"unbounded tail", IterOptions{Start: "f"}, "zzz", 2},
		{"reverse after stop", IterOptions{Reverse: true, Limit: 1}, "f", 1},
		{"reverse before stop", IterOptions{Reverse: true, Limit: 1}, "h", 2},
		{"reverse all", IterOptions{Reverse: true, End: "d"}, "", 2},
		{"prefix", IterOptions{Prefix: "c"}, "cz", 2},
		{"prefix other", IterOptions{Prefix: "c"}, "d", 1},
		{"empty range", IterOptions{Start: "d", End: "d"}, "d", 1},
	}
	for _, c := range cases {
		fresh()
		if got := attempts(t, db, walk(c.o), put(c.w)); got != c.runs {
			t.Errorf("%s: %+v then write %q: %d attempts, want %d", c.name, c.o, c.w, got, c.runs)
		}
	}
	// stopping early: only what was returned is read
	fresh()
	if got := attempts(t, db, func(tx *Tx) {
		it := tx.Iter(IterOptions{Start: "b"})
		it.Next() // "c"
	}, put("d2")); got != 1 {
		t.Errorf("abandoned iterator: write past it conflicted (%d attempts)", got)
	}
	// read-only transactions track nothing
	_ = db.View(ctx, func(tx *Tx) error {
		tx.Range(IterOptions{})
		if len(tx.ranges) != 0 {
			t.Errorf("View tracked %d ranges", len(tx.ranges))
		}
		return nil
	})
}

func TestIterOptionsAfter(t *testing.T) {
	o := IterOptions{Reverse: true}.After("")
	if lo, hi, inf := o.bounds(); inf || lo < hi {
		t.Fatalf("After(\"\") in reverse = %+v, want an empty range", o)
	}
	if got := (IterOptions{Prefix: "p"}).After("p1"); got.Start != "p1\x00" || got.Prefix != "p" {
		t.Fatalf("After = %+v", got)
	}
	if !reflect.DeepEqual(IterOptions{End: "x", Reverse: true}.After("m"), IterOptions{End: "m", Reverse: true}) {
		t.Fatal("reverse After")
	}
}

// TestIterConflictModel: T1 reads a random range (stopping at a random
// limit), T2 commits random writes, T1 commits. T1 must conflict exactly
// when T2 wrote a key the model says T1 read: in the bounds, and through
// the last key returned unless the range was exhausted.
func TestIterConflictModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		db, err := OpenStore(ctx, store.NewMem(), quiet())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, k := range rapid.SliceOfDistinct(rapid.SampledFrom(iterKeys), func(s string) string { return s }).Draw(t, "initial") {
			if err := db.Put(ctx, k, []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		o := drawOpts(t)
		w2 := rapid.SliceOfN(rapid.SampledFrom(append(iterKeys, "a0", "b15", "bb", "d")), 1, 3).Draw(t, "w2")
		runs := 0
		var got []KV
		err = db.Update(ctx, func(tx *Tx) error {
			runs++
			if runs > 1 {
				return nil
			}
			var err error
			if got, err = tx.Range(o); err != nil {
				return err
			}
			if err := db.Update(ctx, func(t2 *Tx) error {
				for _, k := range w2 {
					if err := t2.Put(k, []byte("w2")); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			return tx.Put("~out", []byte("x"))
		})
		if err != nil {
			t.Fatal(err)
		}
		exhausted := o.Limit == 0 || len(got) < o.Limit
		lo, hi, inf := o.bounds()
		want := false
		for _, k := range w2 {
			if k < lo || (!inf && k >= hi) {
				continue
			}
			if exhausted || (!o.Reverse && k <= got[len(got)-1].Key) || (o.Reverse && k >= got[len(got)-1].Key) {
				want = true
			}
		}
		if (runs == 2) != want {
			t.Fatalf("%+v read %q, then T2 wrote %q: conflict %v, want %v", o, got, w2, runs == 2, want)
		}
	})
}
