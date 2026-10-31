package loget

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/raman20/loget/store"
)

// batchCount wraps a store, counting batch and single deletes.
type batchCount struct {
	store.Store
	mu      sync.Mutex
	batches int
	batched int
	singles []string
}

func (b *batchCount) DeleteBatch(ctx context.Context, keys []string) error {
	b.mu.Lock()
	b.batches++
	b.batched += len(keys)
	b.mu.Unlock()
	return store.DeleteBatch(ctx, b.Store, keys)
}

func (b *batchCount) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	b.singles = append(b.singles, key)
	b.mu.Unlock()
	return b.Store.Delete(ctx, key)
}

func (b *batchCount) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return store.GetRange(ctx, b.Store, key, off, n)
}

// Compaction's garbage reaches the store's batch delete through the DB's
// wrapper (holdStore), not a loop of single deletes; while a backup holds
// deletes the batch is queued and goes out as one batch on release.
func TestCompactDeletesInBatches(t *testing.T) {
	j, err := store.NewJournal(t.TempDir())
	if err != nil {
		t.Skip(err)
	}
	bc := &batchCount{Store: j}
	db := mustOpen(t, bc, WithSnapshotEvery(1<<30))
	defer db.Close()
	fill(t, db, 300) // one segment per commit
	st, err := db.Compact(ctx, CompactOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range bc.singles {
		if strings.HasPrefix(k, "log/") || strings.HasPrefix(k, "base/") || strings.HasPrefix(k, "snap/") {
			t.Fatalf("garbage %s deleted on its own (singles %d, batches %d)", k, len(bc.singles), bc.batches)
		}
	}
	if bc.batches == 0 || bc.batched < 300 || st.Deleted < 300 {
		t.Fatalf("%d batches of %d keys, %d deleted", bc.batches, bc.batched, st.Deleted)
	}
	if keys, _ := j.List(ctx, "log/"); len(keys) > 2 {
		t.Fatalf("%d log dirs", len(keys))
	}

	h := db.st.(*holdStore)
	release := h.hold()
	fill(t, db, 50)
	before := bc.batched
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if bc.batched != before {
		t.Fatalf("%d deletes passed a backup hold", bc.batched-before)
	}
	release(ctx, t.Logf)
	if bc.batched < before+50 {
		t.Fatalf("held deletes not released as a batch: %d", bc.batched-before)
	}
}
