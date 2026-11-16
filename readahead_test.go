package loget

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/raman20/loget/store"
)

// raValue is the value of key i: above the inline limit, so scans read it
// from the log (and read-ahead has work to do).
func raValue(i int) []byte {
	return bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 100)
}

func loadRA(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i += 1000 {
		if err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < min(i+1000, n); j++ {
				if err := tx.Put(fmt.Sprintf("k%06d", j), raValue(j)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// A long scan reads ahead, returns exactly the keys and values in order
// both ways, and with writes landing between steps still sees only its
// snapshot.
func TestIterReadAhead(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	const n = 12000
	loadRA(t, db, n)
	for _, rev := range []bool{false, true} {
		aheads := 0
		err := db.View(ctx, func(tx *Tx) error {
			it := tx.Iter(IterOptions{Prefix: "k", Reverse: rev})
			i := 0
			for it.Next() {
				j := i
				if rev {
					j = n - 1 - i
				}
				if it.Key() != fmt.Sprintf("k%06d", j) || !bytes.Equal(it.Value(), raValue(j)) {
					t.Fatalf("rev=%v step %d: key %q", rev, i, it.Key())
				}
				if it.ahead != nil {
					aheads++
				}
				if i%997 == 0 { // a newer commit is invisible to the snapshot
					if err := db.Put(ctx, fmt.Sprintf("k%06d", (i*31)%n), []byte("new")); err != nil {
						t.Fatal(err)
					}
				}
				i++
			}
			if i != n {
				t.Fatalf("rev=%v: %d keys, want %d", rev, i, n)
			}
			return it.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		if aheads == 0 {
			t.Fatalf("rev=%v: never read ahead", rev)
		}
		// restore the values the pass overwrote
		loadRA(t, db, n)
	}
}

// An iterator abandoned mid-scan leaves no goroutine behind once Close
// returns, and Close waits for its read-ahead.
func TestIterReadAheadAbandoned(t *testing.T) {
	before := runtime.NumGoroutine()
	db := mustOpen(t, store.NewMem())
	loadRA(t, db, 6000)
	for range 20 {
		_ = db.View(ctx, func(tx *Tx) error {
			it := tx.Iter(IterOptions{})
			for i := 0; i < 2100 && it.Next(); i++ {
			}
			return nil // abandoned with a page in flight
		})
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > before {
		t.Fatalf("%d goroutines after Close, %d before", g, before)
	}
	if db.bg.add() {
		t.Fatal("read-ahead registered after Close")
	}
}

// With reads failing at random, a scan never returns a wrong value: each
// key either carries its value or the scan stops with an error.
func TestIterReadAheadFaulty(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	const n = 8000
	loadRA(t, db, n)
	db.Close()
	opened := 0
	for seed := range int64(8) {
		f := store.NewFaulty(mem, seed, store.FaultProbs{ReadFail: 0.02})
		db, err := OpenStore(ctx, f, quiet())
		if err != nil {
			continue // a failed recovery read; another seed opens
		}
		opened++
		err = db.View(ctx, func(tx *Tx) error {
			it := tx.Iter(IterOptions{})
			i := 0
			for it.Next() {
				if it.Key() != fmt.Sprintf("k%06d", i) || !bytes.Equal(it.Value(), raValue(i)) {
					t.Fatalf("seed %d step %d: key %q wrong value", seed, i, it.Key())
				}
				i++
			}
			if it.Err() == nil && i != n {
				t.Fatalf("seed %d: %d keys and no error, want %d", seed, i, n)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	if opened == 0 {
		t.Fatal("no seed opened")
	}
}
