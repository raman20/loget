package loget

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/raman20/loget/store"
)

func TestBatchBasic(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	db.Put(ctx, "gone", []byte("x"))
	var b Batch
	if lsn, err := db.Apply(ctx, &b); err != nil || lsn == 0 {
		t.Fatalf("empty batch: %d %v", lsn, err)
	}
	buf := []byte("v1")
	b.Put("a", buf)
	buf[0] = 'X' // the batch copied it
	b.Put("b", []byte("v2"))
	b.Put("a", []byte("v3")) // later write of the same key wins
	b.Delete("gone")
	before, _ := db.Branch("main").Head()
	lsn, err := db.Apply(ctx, &b)
	if err != nil || lsn != before+1 {
		t.Fatalf("apply: lsn %d (head was %d) %v", lsn, before, err)
	}
	want := map[string]string{"a": "v3", "b": "v2"}
	for k, v := range want {
		if got, err := db.Get(ctx, k); err != nil || string(got) != v {
			t.Fatalf("%s = %q %v, want %q", k, got, err, v)
		}
	}
	if _, err := db.Get(ctx, "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("gone: %v", err)
	}
	// a snapshot before the batch sees none of it
	db.Branch("main").ViewAt(ctx, before, func(tx *Tx) error {
		if _, err := tx.Get("a"); !errors.Is(err, ErrNotFound) {
			t.Errorf("a visible before the batch: %v", err)
		}
		return nil
	})
	b.Reset()
	if b.Len() != 0 || b.Size() != frameOverhead {
		t.Fatalf("reset: %d %d", b.Len(), b.Size())
	}
}

func TestBatchLimits(t *testing.T) {
	var b Batch
	if err := b.Put(strings.Repeat("k", MaxKeyBytes+1), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("long key: %v", err)
	}
	big := make([]byte, MaxTxBytes/2)
	if err := b.Put("a", big); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("b", big); !errors.Is(err, ErrTooLarge) || b.Len() != 1 {
		t.Fatalf("over MaxTxBytes: %v, len %d", err, b.Len())
	}
	b.Reset()
	for i := 0; i < MaxTxOps; i++ {
		if err := b.Delete("k"); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
	}
	if err := b.Delete("k"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over MaxTxOps: %v", err)
	}
	// a batch at the limit commits (one frame of MaxTxOps ops)
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	if _, err := db.Apply(ctx, &b); err != nil {
		t.Fatalf("batch of MaxTxOps deletes: %v", err)
	}
}

func TestBatchShreddedSubject(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	db.Put(ctx, "s", []byte("x"), Subject("alice"))
	if _, err := db.Shred(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	var b Batch
	b.Put("ok", []byte("1"))
	b.Put("s2", []byte("2"), Subject("alice"))
	if _, err := db.Apply(ctx, &b); !errors.Is(err, ErrShredded) {
		t.Fatalf("apply with shredded subject: %v", err)
	}
	if _, err := db.Get(ctx, "ok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial batch committed: %v", err)
	}
}

// Readers (point reads, scans and iterators) never see part of a batch,
// and a read-write transaction that read a key a batch wrote conflicts.
func TestBatchAtomicUnderConcurrency(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	const keys = 16
	var stop atomic.Bool
	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				db.View(ctx, func(tx *Tx) error {
					seen := map[string]bool{}
					it := tx.Iter(IterOptions{Prefix: "k/"})
					for it.Next() {
						seen[string(it.Value())] = true
					}
					if len(seen) > 1 {
						errc <- fmt.Errorf("iterator saw %d generations", len(seen))
					}
					kvs, _ := tx.Scan("k/")
					if len(kvs) != 0 && len(kvs) != keys {
						errc <- fmt.Errorf("scan saw %d of %d keys", len(kvs), keys)
					}
					return nil
				})
			}
		}()
	}
	for gen := 0; gen < 200; gen++ {
		var b Batch
		for i := 0; i < keys; i++ {
			b.Put(fmt.Sprintf("k/%02d", i), []byte(fmt.Sprint(gen)))
		}
		if _, err := db.Apply(ctx, &b); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	// OCC: a transaction that read k/00 before a batch wrote it conflicts
	tries := 0
	err := db.Update(ctx, func(tx *Tx) error {
		tries++
		tx.Get("k/00")
		if tries == 1 {
			var b Batch
			b.Put("k/00", []byte("batch"))
			if _, err := db.Apply(ctx, &b); err != nil {
				return err
			}
		}
		return tx.Put("k/00", []byte("tx"))
	})
	if err != nil || tries < 2 {
		t.Fatalf("tx over a batch: %v after %d tries (want a conflict and retry)", err, tries)
	}
}

// Property: random batches (puts, deletes, repeated keys) against a model map.
func TestBatchPropertyVsModel(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, 26))
		db := mustOpen(t, store.NewMem())
		model := map[string]string{}
		for n := 0; n < 50; n++ {
			var b Batch
			next := map[string]*string{}
			for i := rng.IntN(20); i >= 0; i-- {
				k := fmt.Sprintf("k%02d", rng.IntN(30))
				if rng.IntN(3) == 0 {
					b.Delete(k)
					next[k] = nil
				} else {
					v := fmt.Sprint(rng.Uint32())
					b.Put(k, []byte(v))
					next[k] = &v
				}
			}
			if _, err := db.Apply(ctx, &b); err != nil {
				t.Fatal(err)
			}
			for k, v := range next {
				if v == nil {
					delete(model, k)
				} else {
					model[k] = *v
				}
			}
		}
		kvs, _ := scanAll(db)
		if len(kvs) != len(model) {
			t.Fatalf("seed %d: %d keys, model %d", seed, len(kvs), len(model))
		}
		for _, kv := range kvs {
			if model[kv.Key] != string(kv.Value) {
				t.Fatalf("seed %d: %s = %q, model %q", seed, kv.Key, kv.Value, model[kv.Key])
			}
		}
		db.Close()
	}
}

func scanAll(db *DB) (kvs []KV, err error) {
	err = db.View(ctx, func(tx *Tx) error { kvs, err = tx.Scan(""); return err })
	return
}
