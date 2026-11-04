package loget

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/raman20/loget/store"
)

// TestIterPrefetch: iterators that read page values in batches return the
// same values as Get, forward and reverse, across segments, a compaction
// base's chunks, deletes and subject (encrypted) values.
func TestIterPrefetch(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	val := func(i, r int) []byte { return []byte(fmt.Sprintf("v%06d-r%d-%s", i, r, strings.Repeat("x", 90+i%50))) }
	write := func(r, lo, hi int) {
		for i := lo; i < hi; i += 100 {
			err := db.Update(ctx, func(tx *Tx) error {
				for j := i; j < i+100 && j < hi; j++ {
					var err error
					switch {
					case j%17 == 0 && r > 0:
						err = tx.Delete(fmt.Sprintf("k%06d", j))
					case j%13 == 0:
						err = tx.Put(fmt.Sprintf("k%06d", j), val(j, r), Subject("s"))
					default:
						err = tx.Put(fmt.Sprintf("k%06d", j), val(j, r))
					}
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	check := func(stage string) {
		t.Helper()
		err := db.View(ctx, func(tx *Tx) error {
			for _, rev := range []bool{false, true} {
				it := tx.Iter(IterOptions{Reverse: rev})
				n := 0
				for it.Next() {
					want, err := tx.Get(it.Key())
					if err != nil {
						return err
					}
					if !bytes.Equal(it.Value(), want) {
						return fmt.Errorf("%s rev=%v %s: %q != %q", stage, rev, it.Key(), it.Value(), want)
					}
					n++
				}
				if err := it.Err(); err != nil {
					return err
				}
				if n < 2000 {
					return fmt.Errorf("%s: only %d keys", stage, n)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	write(0, 0, 3000)
	check("log")
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	check("base")
	write(1, 1000, 2000) // half the keys move back to segments
	check("mixed")
}

// TestIterPrefetchCorrupt: a value whose stored bytes fail their checksum
// is an error from the iterator, never wrong data.
func TestIterPrefetchCorrupt(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	defer db.Close()
	err := db.Update(ctx, func(tx *Tx) error {
		for i := 0; i < 200; i++ {
			if err := tx.Put(fmt.Sprintf("k%04d", i), []byte(fmt.Sprintf("value-%04d-%s", i, strings.Repeat("y", 100)))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	hit := false
	st.Raw(func(m map[string][]byte) {
		for k, b := range m {
			if i := bytes.Index(b, []byte("value-0100-")); i >= 0 && strings.HasPrefix(k, "log/") {
				b[i+3] ^= 1
				hit = true
			}
		}
	})
	if !hit {
		t.Fatal("value not found in any segment")
	}
	err = db.View(ctx, func(tx *Tx) error {
		it := tx.Iter(IterOptions{})
		for it.Next() {
			if it.Key() == "k0100" {
				return fmt.Errorf("corrupt value returned: %q", it.Value())
			}
		}
		return it.Err()
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}
