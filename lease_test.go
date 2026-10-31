package loget_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/raman20/loget"
	"github.com/raman20/loget/store"
)

// The opener names whom it fenced; the fenced writer names who fenced it.
func TestLeaseMessages(t *testing.T) {
	ctx := context.Background()
	st := store.NewMem()
	var mu sync.Mutex
	var logs []string
	logf := func(f string, a ...any) { mu.Lock(); logs = append(logs, fmt.Sprintf(f, a...)); mu.Unlock() }
	a, err := loget.OpenStore(ctx, st, loget.WithHolder("proc-A"), loget.WithLogf(logf))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	b, err := loget.OpenStore(ctx, st, loget.WithHolder("proc-B"), loget.WithLogf(logf))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	mu.Lock()
	joined := strings.Join(logs, "\n")
	mu.Unlock()
	if !strings.Contains(joined, "from epoch 1 (proc-A") {
		t.Errorf("opener log does not name the fenced holder:\n%s", joined)
	}
	err = a.Put(ctx, "k", []byte("v2"))
	if !errors.Is(err, loget.ErrFenced) || !strings.Contains(err.Error(), "epoch 2 (proc-B") {
		t.Errorf("fenced writer: %v", err)
	}
	a.Close()
	// GC keeps only recent lease records.
	for i := 0; i < 20; i++ {
		b.Close()
		if b, err = loget.OpenStore(ctx, st, loget.WithHolder("proc-C")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.GC(ctx, loget.GCOptions{}); err != nil {
		t.Fatal(err)
	}
	ks, _ := st.List(ctx, "lease/")
	if len(ks) > 16 {
		t.Errorf("%d lease records after GC, want ≤ 16", len(ks))
	}
}

func TestMaxKeyBytes(t *testing.T) {
	ctx := context.Background()
	db, err := loget.OpenStore(ctx, store.NewMem(), loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put(ctx, strings.Repeat("k", loget.MaxKeyBytes), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, strings.Repeat("k", loget.MaxKeyBytes+1), []byte("v")); !errors.Is(err, loget.ErrTooLarge) {
		t.Fatalf("oversized key: %v", err)
	}
	err = db.Update(ctx, func(tx *loget.Tx) error {
		return tx.PutRecord("kv", strings.Repeat("r", loget.MaxKeyBytes+1), []byte("v"))
	})
	if !errors.Is(err, loget.ErrTooLarge) {
		t.Fatalf("oversized record key: %v", err)
	}
}
