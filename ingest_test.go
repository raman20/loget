package loget

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/raman20/loget/store"
)

func ingestN(n int, tag string) IngestSource {
	return func(add func(string, []byte) error) error {
		for i := 0; i < n; i++ {
			if err := add(fmt.Sprintf("k%08d", i), []byte(tag+fmt.Sprint(i))); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestIngestBasic(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	if err := db.Put(ctx, "k00000005", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, "z", []byte("z")); err != nil {
		t.Fatal(err)
	}
	s, err := db.Ingest(ctx, ingestN(1000, "v"))
	if err != nil || s.Keys != 1000 || s.LSN != 3 {
		t.Fatalf("%+v %v", s, err)
	}
	check := func(db *DB) {
		t.Helper()
		got := scanMap(t, db)
		if len(got) != 1001 || got["k00000005"] != "v5" || got["z"] != "z" || got["k00000999"] != "v999" {
			t.Fatalf("%d keys, k5=%q", len(got), got["k00000005"])
		}
	}
	check(db)
	db.Put(ctx, "k00000007", []byte("new"))
	if got := scanMap(t, db)["k00000007"]; got != "new" {
		t.Fatal(got)
	}
	db.Close()
	db = mustOpen(t, st)
	if got := scanMap(t, db)["k00000007"]; got != "new" {
		t.Fatal(got)
	}
	db.Close()
}

func TestIngestUnsortedCommitsNothing(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	defer db.Close()
	_, err := db.Ingest(ctx, func(add func(string, []byte) error) error {
		if err := add("b", nil); err != nil {
			return err
		}
		return add("a", nil)
	})
	if !errors.Is(err, ErrUnsorted) {
		t.Fatal(err)
	}
	if n := len(scanMap(t, db)); n != 0 {
		t.Fatal(n)
	}
	if err := db.Put(ctx, "x", []byte("1")); err != nil {
		t.Fatal(err)
	}
}

func scanMap(t testing.TB, db *DB) map[string]string {
	t.Helper()
	kvs, err := scanAll(db)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]string{}
	for _, kv := range kvs {
		m[kv.Key] = string(kv.Value)
	}
	return m
}

// TestIngestProperty mixes ingests with puts, deletes, checkpoints,
// compaction and reopen, against a model; live indexes must equal a full
// replay of the log, and a reopen (checkpoint + tail) must equal both.
func TestIngestProperty(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		st := store.NewMem()
		opts := []Option{WithSnapshotEvery(3), WithIndexDir(t.TempDir()), WithIndexResidentBytes(1 << 12)}
		if seed%2 == 0 {
			opts = []Option{WithSnapshotEvery(2)}
		}
		db := mustOpen(t, st, opts...)
		model := map[string]string{}
		key := func() string { return fmt.Sprintf("k%03d", rng.Intn(400)) }
		for step := 0; step < 60; step++ {
			switch r := rng.Intn(10); {
			case r < 3:
				n := rng.Intn(150)
				var ks []string
				seen := map[string]bool{}
				for i := 0; i < n; i++ {
					if k := key(); !seen[k] {
						seen[k] = true
						ks = append(ks, k)
					}
				}
				sort.Strings(ks)
				tag := fmt.Sprintf("i%d.%d.", seed, step)
				_, err := db.Ingest(ctx, func(add func(string, []byte) error) error {
					for _, k := range ks {
						if err := add(k, []byte(tag+k)); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, k := range ks {
					model[k] = tag + k
				}
			case r < 6:
				k, v := key(), fmt.Sprint(step)
				if err := db.Put(ctx, k, []byte(v)); err != nil {
					t.Fatal(err)
				}
				model[k] = v
			case r < 8:
				k := key()
				if err := db.Update(ctx, func(tx *Tx) error { return tx.Delete(k) }); err != nil {
					t.Fatal(err)
				}
				delete(model, k)
			case r < 9:
				if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
					t.Fatal(err)
				}
			default:
				db.Close()
				db = mustOpen(t, st, opts...)
			}
			if step%10 == 9 {
				live, _, err := db.IndexDigests("main")
				if err != nil {
					t.Fatal(err)
				}
				full, err := db.RebuildDigests("main")
				if err != nil {
					t.Fatal(err)
				}
				if fmt.Sprint(live) != fmt.Sprint(full) {
					t.Fatalf("seed %d step %d: live %x != replay %x", seed, step, live, full)
				}
			}
		}
		got := scanMap(t, db)
		if fmt.Sprint(got) != fmt.Sprint(model) {
			t.Fatalf("seed %d: %d keys, model %d", seed, len(got), len(model))
		}
		live, _, _ := db.IndexDigests("main")
		db.Close()
		db = mustOpen(t, st, opts...)
		again, _, _ := db.IndexDigests("main")
		if fmt.Sprint(live) != fmt.Sprint(again) {
			t.Fatalf("seed %d: reopen digest differs", seed)
		}
		db.Close()
	}
}

// A read-write transaction whose snapshot predates an ingest conflicts.
func TestIngestConflicts(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	db.Put(ctx, "a", []byte("0"))
	first, tries := true, 0
	err := db.Update(ctx, func(tx *Tx) error {
		tries++
		if _, err := tx.Get("k00000001"); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if first {
			first = false
			if _, err := db.Ingest(ctx, ingestN(10, "v")); err != nil {
				t.Fatal(err)
			}
		}
		return tx.Put("b", []byte("1"))
	})
	if err != nil || tries != 2 {
		t.Fatal(err, tries)
	}
}

// Ingest pieces stored by an ingest that never committed are swept once
// their epoch is over; committed ones are kept until GC drops the segment.
func TestIngestOrphanSweep(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	db.Put(ctx, "a", []byte("0"))
	if _, err := db.Ingest(ctx, ingestN(50, "v")); err != nil {
		t.Fatal(err)
	}
	id := db.Manifest().Refs["main"]
	orphan := ingestPieceKey(ingestKey(id, 1, db.Epoch(), []byte{1, 2}), 0)
	st.PutIfAbsent(ctx, orphan, []byte("junk"))
	db.Put(ctx, "b", []byte("1"))
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, orphan); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan kept: %v", err)
	}
	if got := scanMap(t, db); len(got) != 52 || got["k00000049"] != "v49" {
		t.Fatal(len(got))
	}
}

// A damaged or missing ingest piece fails replay with ErrCorrupt.
func TestIngestDamagedPiece(t *testing.T) {
	for _, damage := range []string{"flip", "delete"} {
		st := store.NewMem()
		opts := []Option{WithCheckpointBytes(-1), WithSnapshotEvery(1 << 20)}
		db := mustOpen(t, st, opts...)
		if _, err := db.Ingest(ctx, ingestN(100, "v")); err != nil {
			t.Fatal(err)
		}
		id := db.Manifest().Refs["main"]
		db.Close()
		keys, err := st.List(ctx, "log/"+id+"/")
		if err != nil {
			t.Fatal(err)
		}
		var piece string
		for _, k := range keys {
			if strings.Contains(k, ".ingest.") {
				piece = k
			}
		}
		if piece == "" {
			t.Fatal("no piece stored")
		}
		v, err := OpenStore(ctx, st, append([]Option{quiet()}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		if r, err := v.Verify(ctx); err != nil || !r.OK {
			t.Fatalf("verify before damage: %+v %v", r, err)
		}
		v.Close()
		b, _ := st.Get(ctx, piece)
		st.Delete(ctx, piece)
		if damage == "flip" {
			b[len(b)/2] ^= 1
			st.PutIfAbsent(ctx, piece, b)
		}
		if _, err := OpenStore(ctx, st, append([]Option{quiet()}, opts...)...); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: open: %v", damage, err)
		}
	}
}
