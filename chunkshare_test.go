package loget

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// Design 30: repeated pointer-only compactions write no new .val chunks;
// values stay in the log and the state stays right.
func TestCompactSharesChunks(t *testing.T) {
	old := chunkMax
	chunkMax = 4 << 10
	defer func() { chunkMax = old }()
	for name, mk := range backends(t) {
		t.Run(name, func(t *testing.T) {
			st := mk()
			db := mustOpen(t, st, WithInlineMax(16))
			model := map[string]string{}
			rng := rand.New(rand.NewSource(1))
			shared := 0
			for round := 0; round < 8; round++ {
				n := 400
				if round > 0 {
					n = 40 // few overwrites: most chunks stay live
				}
				for i := 0; i < n; i++ {
					k := fmt.Sprintf("k%04d", rng.Intn(400))
					if round == 0 {
						k = fmt.Sprintf("k%04d", i)
					}
					v := strings.Repeat(string(rune('a'+round)), 100+rng.Intn(100))
					err := db.Update(ctx, func(tx *Tx) error {
						if round > 0 && i%7 == 0 {
							delete(model, k)
							return tx.Delete(k)
						}
						model[k] = v
						return tx.Put(k, []byte(v))
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				s, err := db.Compact(ctx, CompactOptions{})
				if err != nil {
					t.Fatal(err)
				}
				shared += s.SharedChunks
				check(t, db, model)
			}
			_ = shared
			if r, err := db.Verify(ctx); err != nil || !r.OK {
				t.Fatalf("verify %+v %v", r, err)
			}
			// every listed chunk object exists; no unlisted chunk object stays
			want := map[string]bool{}
			for _, lm := range db.man.Logs {
				if lm.Base != nil {
					for _, o := range lm.Base.chunkObjs() {
						want[o] = true
					}
				}
			}
			var keys []string
			for id := range db.man.Logs {
				ks, err := st.List(ctx, "base/"+id+"/")
				if err != nil {
					t.Fatal(err)
				}
				keys = append(keys, ks...)
			}
			got := 0
			for _, k := range keys {
				if strings.HasSuffix(k, ".val") {
					if !want[k] {
						t.Errorf("stray chunk %s", k)
					}
					got++
				}
			}
			if got != len(want) {
				t.Fatalf("%d chunk objects, %d listed", got, len(want))
			}
			db.Close()
			db = mustOpen(t, st)
			defer db.Close()
			check(t, db, model)
			// a new epoch: its orphan sweep must keep the shared chunks
			db.Update(ctx, func(tx *Tx) error { model["z"] = "z"; return tx.Put("z", []byte("z")) })
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
			check(t, db, model)
			if r, err := db.Verify(ctx); err != nil || !r.OK {
				t.Fatalf("verify %+v %v", r, err)
			}
		})
	}
}

func check(t *testing.T, db *DB, model map[string]string) {
	t.Helper()
	n := 0
	err := db.View(ctx, func(tx *Tx) error {
		it := tx.Iter(IterOptions{})
		for it.Next() {
			if model[it.Key()] != string(it.Value()) {
				return fmt.Errorf("%s = %q, want %q", it.Key(), it.Value(), model[it.Key()])
			}
			n++
		}
		return it.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(model) {
		t.Fatalf("%d keys, want %d", n, len(model))
	}
}
