package loget

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/loget/store"
)

// ttlRef is the reference model: per key, the history of (lsn, val, del, exp).
type ttlRef struct {
	hist map[string][]ttlVer
	ts   map[uint64]int64 // commit TS of the frames this test wrote
	lsns []uint64
	// reads that hit an expired (live, unexpired) version: the model must
	// exercise both
	expired, unexpired int
}

type ttlVer struct {
	lsn uint64
	val string
	del bool
	exp int64
}

func (r *ttlRef) get(k string, at uint64, now int64) (string, bool) {
	var cur *ttlVer
	for i := range r.hist[k] {
		if r.hist[k][i].lsn <= at {
			cur = &r.hist[k][i]
		}
	}
	if cur != nil && !cur.del && cur.exp != 0 {
		if cur.exp <= now {
			r.expired++
		} else {
			r.unexpired++
		}
	}
	if cur == nil || cur.del || (cur.exp != 0 && cur.exp <= now) {
		return "", false
	}
	return cur.val, true
}

func (r *ttlRef) keys() []string {
	var ks []string
	for k := range r.hist {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// check compares every key (Get and Scan) at LSN at with the model.
func (r *ttlRef) check(t *testing.T, db *DB, at uint64, label string) {
	t.Helper()
	err := db.Branch("main").ViewAt(ctx, at, func(tx *Tx) error {
		now, err := tx.Now()
		if err != nil {
			return err
		}
		if want, ok := r.ts[at]; ok && now != want {
			return fmt.Errorf("Now at %d = %d, want %d", at, now, want)
		}
		var want []string
		for _, k := range r.keys() {
			v, ok := r.get(k, at, now)
			got, err := tx.Get(k)
			if ok != (err == nil) || (ok && string(got) != v) {
				return fmt.Errorf("get %s at %d (now %d): %q %v, want %q %v", k, at, now, got, err, v, ok)
			}
			if ok {
				want = append(want, k+"="+v)
			}
		}
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		var got []string
		for _, kv := range kvs {
			got = append(got, kv.Key+"="+string(kv.Value))
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			return fmt.Errorf("scan at %d: %v, want %v", at, got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

// TestTTLModel runs random puts (with and without TTL), deletes and clock
// steps, and checks reads at many LSNs against the model live, after
// reopen (replay or checkpoint), and after compaction.
func TestTTLModel(t *testing.T) {
	for _, every := range []int{0, 7} {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("ckpt%d/seed%d", every, seed), func(t *testing.T) {
				ttlModel(t, seed, every)
			})
		}
	}
}

func ttlModel(t *testing.T, seed uint64, every int) {
	rng := rand.New(rand.NewPCG(seed, 27))
	var clk atomic.Int64
	clk.Store(1000)
	st := store.NewMem()
	opts := []Option{WithClock(clk.Load), WithSnapshotEvery(every)}
	db := mustOpen(t, st, opts...)
	defer func() { db.Close() }()
	r := &ttlRef{hist: map[string][]ttlVer{}, ts: map[uint64]int64{}}
	var lastTS int64
	for i := 0; i < 150; i++ {
		clk.Add(int64(rng.IntN(8)))
		k := fmt.Sprintf("k%d", rng.IntN(8))
		v := fmt.Sprintf("v%d", i)
		var ver ttlVer
		var lsn uint64
		var err error
		switch c := rng.IntN(10); {
		case c < 3:
			lsn, err = db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v)) })
			ver = ttlVer{val: v}
		case c < 8:
			d := int64(1 + rng.IntN(30))
			lsn, err = db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v), TTL(time.Duration(d))) })
			ver = ttlVer{val: v, exp: -d}
		case c < 9:
			// absolute expiry, sometimes already past
			e := clk.Load() + int64(rng.IntN(20)) - 5
			lsn, err = db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v), ExpiresAt(time.Unix(0, e))) })
			ver = ttlVer{val: v, exp: e}
		default:
			lsn, err = db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { return tx.Delete(k) })
			ver = ttlVer{del: true}
		}
		if err != nil {
			t.Fatal(err)
		}
		ts := max(lastTS, clk.Load())
		lastTS = ts
		if ver.exp < 0 {
			ver.exp = ts - ver.exp
		}
		ver.lsn = lsn
		r.hist[k] = append(r.hist[k], ver)
		r.ts[lsn] = ts
		r.lsns = append(r.lsns, lsn)
	}
	all := func(label string, from uint64) {
		for _, at := range r.lsns {
			if at >= from {
				r.check(t, db, at, label)
			}
		}
	}
	all("live", 0)
	if r.expired < 50 || r.unexpired < 50 {
		t.Fatalf("model reads: %d expired, %d unexpired", r.expired, r.unexpired)
	}
	db.Close()
	db = mustOpen(t, st, opts...)
	all("reopen", 0)
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	head := r.lsns[len(r.lsns)-1]
	r.check(t, db, head, "compacted")
	db.Close()
	db = mustOpen(t, st, opts...)
	r.check(t, db, head, "compacted+reopen")
	// later writes on top of the base
	clk.Add(100)
	if err := db.Put(ctx, "late", []byte("x"), TTL(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, "late"); err != nil {
		t.Fatalf("late: %v", err)
	}
}

func TestTTLBasic(t *testing.T) {
	var clk atomic.Int64
	clk.Store(100)
	db := mustOpen(t, store.NewMem(), WithClock(clk.Load))
	defer db.Close()
	b := db.Branch("main")
	put, err := b.UpdateLSN(ctx, func(tx *Tx) error { return tx.Put("a", []byte("1"), TTL(10)) })
	if err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, "a"); err != nil || string(v) != "1" {
		t.Fatalf("fresh: %q %v", v, err)
	}
	// the wall clock passing does not expire it: reads use commit time
	clk.Store(500)
	if _, err := db.Get(ctx, "a"); err != nil {
		t.Fatalf("before any later commit: %v", err)
	}
	if err := db.Put(ctx, "b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after a commit at 500: %v", err)
	}
	// time travel sees it at its own commit time
	b.ViewAt(ctx, put, func(tx *Tx) error {
		if _, err := tx.Get("a"); err != nil {
			t.Errorf("ViewAt: %v", err)
		}
		return nil
	})
	// a snapshot with an explicit read time
	s, err := db.Snapshot(ctx, SnapshotOptions{Now: 105})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	s.View(ctx, func(tx *Tx) error {
		if _, err := tx.Get("a"); err != nil {
			t.Errorf("snapshot Now=105: %v", err)
		}
		return nil
	})
	// invalid uses
	for name, err := range map[string]error{
		"ttl0":   db.Put(ctx, "x", nil, TTL(0)),
		"neg":    db.Put(ctx, "x", nil, TTL(-time.Second)),
		"epoch":  db.Put(ctx, "x", nil, ExpiresAt(time.Unix(0, 0))),
		"record": db.Update(ctx, func(tx *Tx) error { return tx.PutRecord("tally", "x", nil, TTL(time.Second)) }),
	} {
		if !errors.Is(err, ErrBadValue) {
			t.Errorf("%s: %v, want ErrBadValue", name, err)
		}
	}
	var bt Batch
	if err := bt.Put("x", nil, TTL(0)); !errors.Is(err, ErrBadValue) {
		t.Errorf("batch ttl0: %v", err)
	}
	if err := bt.Put("c", []byte("3"), TTL(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Apply(ctx, &bt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, "c"); err != nil {
		t.Fatalf("batch ttl: %v", err)
	}
}

// TestTTLConflict: a transaction that read a key as live conflicts when a
// commit before it reaches the key's expiry, and its retry sees it gone.
func TestTTLConflict(t *testing.T) {
	var clk atomic.Int64
	clk.Store(100)
	db := mustOpen(t, store.NewMem(), WithClock(clk.Load))
	defer db.Close()
	if err := db.Put(ctx, "a", []byte("1"), TTL(10)); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	var seen []bool
	err := db.Update(ctx, func(tx *Tx) error {
		attempts++
		_, err := tx.Get("a")
		seen = append(seen, err == nil)
		if attempts == 1 {
			clk.Store(200)
			if err := db.Put(ctx, "other", []byte("x")); err != nil {
				return err
			}
		}
		return tx.Put("b", []byte(fmt.Sprint(err == nil)))
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || !seen[0] || seen[1] {
		t.Fatalf("attempts %d seen %v, want a conflict then a retry that sees a expired", attempts, seen)
	}
	// an iterator read counts too
	if err := db.Put(ctx, "p/1", []byte("1"), TTL(10)); err != nil {
		t.Fatal(err)
	}
	attempts = 0
	err = db.Update(ctx, func(tx *Tx) error {
		attempts++
		kvs, err := tx.Scan("p/")
		if err != nil {
			return err
		}
		if attempts == 1 {
			if len(kvs) != 1 {
				return fmt.Errorf("scan: %v", kvs)
			}
			clk.Store(300)
			if err := db.Put(ctx, "other", []byte("y")); err != nil {
				return err
			}
		} else if len(kvs) != 0 {
			return fmt.Errorf("retry scan: %v", kvs)
		}
		return tx.Put("q", []byte("1"))
	})
	if err != nil || attempts != 2 {
		t.Fatalf("scan conflict: %d %v", attempts, err)
	}
}
