package loget

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/raman20/loget/store"
)

// rangeCounter counts value reads (GetRange) on the wrapped store.
type rangeCounter struct {
	store.Store
	ranges atomic.Int64
}

func (c *rangeCounter) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	if strings.HasPrefix(k, "log/") || strings.HasPrefix(k, "base/") {
		c.ranges.Add(1)
	}
	return store.GetRange(ctx, c.Store, k, off, n)
}

// Compaction reads values in batches: on S3 one GET per live key made a
// compaction cost one request and one round trip per key (found with the
// s3sim proxy: 48k GETs to compact 50k keys). Values written together are
// read together; a log of many small commits costs about one read per
// segment, not per key. The bases must still hold every value exactly.
func TestCompactBatchesValueReads(t *testing.T) {
	val := func(i, ver int) []byte { return bytes.Repeat([]byte(fmt.Sprintf("%d.%d|", i, ver)), 20) }
	for _, c := range []struct {
		name     string
		keys     int
		perTxn   int
		maxReads func(segs int) int64
	}{
		{"bulk", 20000, 20000, func(int) int64 { return 10 }},
		{"small-commits", 3000, 3, func(segs int) int64 { return int64(segs) + 10 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := &rangeCounter{Store: store.NewMem()}
			db, err := OpenStore(ctx, st, quiet())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for s := 0; s < c.keys; s += c.perTxn {
				b := &Batch{}
				for i := s; i < min(s+c.perTxn, c.keys); i++ {
					b.Put(fmt.Sprintf("k%06d", (i*7919)%c.keys), val((i*7919)%c.keys, 0))
				}
				if _, err := db.Apply(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			segs := int(db.Stats().Segments)
			st.ranges.Store(0)
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
			reads := st.ranges.Load()
			t.Logf("%d keys in %d segments: %d value reads during compaction", c.keys, segs, reads)
			if reads > c.maxReads(segs) {
				t.Fatalf("%d value reads to compact %d keys in %d segments (want <= %d)", reads, c.keys, segs, c.maxReads(segs))
			}
			// a second compaction reads the base's chunks the same way
			if err := db.Put(ctx, "k000000", val(0, 1)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
			db.Close()
			db, err = OpenStore(ctx, st, quiet())
			if err != nil {
				t.Fatal(err)
			}
			db.View(ctx, func(tx *Tx) error {
				kvs, err := tx.Scan("k")
				if err != nil || len(kvs) != c.keys {
					t.Fatalf("scan: %d keys, %v", len(kvs), err)
				}
				for _, kv := range kvs {
					var i int
					fmt.Sscanf(kv.Key, "k%d", &i)
					want := val(i, 0)
					if i == 0 {
						want = val(0, 1)
					}
					if !bytes.Equal(kv.Value, want) {
						t.Fatalf("%s = %q, want %q", kv.Key, kv.Value, want)
					}
				}
				return nil
			})
		})
	}
}

// ctxStore honours the context on value reads (as S3 does) and cancels
// cancel after the first one.
type ctxStore struct {
	store.Store
	cancel context.CancelFunc
	ranges atomic.Int64
}

func (c *ctxStore) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ranges.Add(1) == 1 && c.cancel != nil {
		c.cancel()
	}
	return store.GetRange(ctx, c.Store, k, off, n)
}

// A cancelled (or timed-out) compaction stops: it used to fall back to one
// read per value under a background context, so with the caller long gone
// it went on for many minutes on S3 while commits waited (found with the
// s3sim hostile profile: a 2-minute compaction deadline, 20+ minutes of
// GETs). After the cancel the database still works.
func TestCompactCancelStops(t *testing.T) {
	st := &ctxStore{Store: store.NewMem()}
	db, err := OpenStore(ctx, st, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for s := 0; s < 3000; s += 3 { // many segments: many value reads
		b := &Batch{}
		for i := s; i < s+3; i++ {
			b.Put(fmt.Sprintf("k%06d", (i*7919)%3000), bytes.Repeat([]byte{'v'}, 100))
		}
		if _, err := db.Apply(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	st.ranges.Store(0)
	st.cancel = cancel
	_, err = db.Compact(cctx, CompactOptions{})
	st.cancel = nil
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("compaction under a cancelled context: %v (%d value reads)", err, st.ranges.Load())
	}
	if n := st.ranges.Load(); n > 2*compactPar {
		t.Fatalf("%d value reads after the cancel", n)
	}
	if err := db.Put(ctx, "after", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, "k000001"); err != nil || len(v) != 100 {
		t.Fatalf("after: %q %v", v, err)
	}
}
