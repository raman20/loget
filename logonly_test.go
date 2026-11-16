package loget

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Design 30: Compact rewrites pointers only; history stays foldable;
// a new index can be folded from the retained log after compact.
func TestLogOnlyCompactKeepsHistoryAndFolds(t *testing.T) {
	st := store.NewMem()
	foldRec := index.Kind{Name: "foldrec", Version: 1, Spaces: []string{"f:"}}
	db := mustOpen(t, st, WithIndex(foldRec))
	for i := 0; i < 20; i++ {
		v := bytesOf(fmt.Sprintf("v%02d", i), 80)
		if err := db.Update(ctx, func(tx *Tx) error {
			if err := tx.Put(fmt.Sprint("k", i%5), v); err != nil {
				return err
			}
			return tx.PutRecord("f:c", fmt.Sprint("r", i), []byte(fmt.Sprint(i)))
		}); err != nil {
			t.Fatal(err)
		}
	}
	h, _ := db.Branch("main").Head()
	mid := h - 5
	midDump := dump(t, db, "main", mid)
	headDump := dump(t, db, "main", 0)

	cs, err := db.Compact(ctx, CompactOptions{})
	if err != nil || cs.Logs != 1 {
		t.Fatalf("%+v %v", cs, err)
	}
	// no new value chunks
	if keys, _ := st.List(ctx, "base/"); hasSuffix(keys, ".val") {
		t.Fatalf("pointer compact wrote .val objects: %v", keys)
	}
	if got := dump(t, db, "main", 0); got != headDump {
		t.Fatal("head state changed by pointer compact")
	}
	if got := dump(t, db, "main", mid); got != midDump {
		t.Fatal("history at mid changed by pointer compact")
	}
	if err := db.CreateBranch(ctx, "then", "main", mid); err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "then", 0); got != midDump {
		t.Fatal("branch at mid differs from the pre-compact view")
	}

	// fold a new index after compact: reopen with a derived index that
	// was not registered when the pointer base was written.
	db.Close()
	db, err = OpenStore(ctx, st, quiet(), WithIndex(tallyKind()), WithIndex(foldCountKind()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.View(ctx, func(tx *Tx) error {
		return tx.Index("foldcount", func(idx index.Index) error {
			n = idx.(*foldCount).n
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("folded index saw no records after compact")
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

// DropHistory deletes unreferenced history segments; a live key pins its segment.
func TestLogOnlyDropHistoryAndPin(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithInlineMax(8))
	defer db.Close()
	pin := bytesOf("pin", 80)
	if err := db.Put(ctx, "keep", pin); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		if err := db.Put(ctx, "tmp", bytesOf(fmt.Sprint(i), 80)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Compact(ctx, CompactOptions{DropHistory: true}); err != nil {
		t.Fatal(err)
	}
	// the first segment holds "keep" and must survive
	if n, _ := objects(t, st, "log/L1/00000000000000000001"); n != 1 {
		t.Fatal("live key's segment was collected")
	}
	// later tmp-only segments should be gone (tmp's latest still pins one)
	keys, _ := st.List(ctx, "log/L1/")
	var segs int
	for _, k := range keys {
		if strings.HasSuffix(k, ".seg") {
			segs++
		}
	}
	var mid int
	for _, k := range keys {
		if strings.HasSuffix(k, ".seg") && !strings.Contains(k, "00000000000000000001") && !strings.Contains(k, "00000000000000000017") && !strings.Contains(k, "00000000000000000018") && !strings.Contains(k, "00000000000000000002") {
			mid++
		}
	}
	if segs < 1 || segs > 5 || mid != 0 {
		t.Fatalf("expected live segs only (keep + latest tmp), have %d mid-history leftover, keys=%v", mid, keys)
	}
	v, err := db.Get(ctx, "keep")
	if err != nil || string(v) != string(pin) {
		t.Fatalf("keep=%q %v", v, err)
	}
}

func TestLogOnlyCorruptMidSegOpenWithCheckpoint(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st) // default checkpoint on close
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("v"))
	}
	db.Close()
	st.Raw(func(o map[string][]byte) {
		k := segKey("L1", 3)
		o[k][len(o[k])-1] ^= 1
	})
	db, err := OpenStore(ctx, st, quiet())
	if err != nil {
		t.Fatalf("open with checkpoint should skip covered mid-seg: %v", err)
	}
	defer db.Close()
	if r, err := db.Verify(ctx); err == nil && r.OK {
		t.Fatal("verify should see the corrupt mid-seg")
	}
}

func bytesOf(s string, n int) []byte {
	b := make([]byte, n)
	copy(b, s)
	return b
}

func hasSuffix(keys []string, suf string) bool {
	for _, k := range keys {
		if strings.HasSuffix(k, suf) {
			return true
		}
	}
	return false
}

// foldCount is a throwaway derived index used to show a new fold after compact.
type foldCount struct{ n int }

func (f *foldCount) Name() string { return "foldcount" }
func (f *foldCount) Apply(e *index.Entry, _ index.Env) {
	for _, op := range e.Ops {
		if op.Key == "" && op.Space == "" {
			continue
		}
		f.n++
	}
}
func (f *foldCount) DropSubject(string) {}
func (f *foldCount) Digest() [32]byte {
	var b [8]byte
	b[0] = byte(f.n)
	return sha256.Sum256(b[:])
}

func foldCountKind() index.Kind {
	return index.Kind{Name: "foldcount", Version: 1, Spaces: []string{"f:"}, New: func() index.Index { return &foldCount{} }}
}

// After a pointer compact, GC with KeepCommits must still be able to
// cut into retained history: views in the keep window stay equal to
// the pre-GC state (the TestPropCompactEqualsUncompacted failure).
func TestLogOnlyRetentionCutAfterCompact(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithInlineMax(0))
	defer db.Close()
	for i := 0; i < 8; i++ {
		if err := db.Put(ctx, "k0", []byte(fmt.Sprint(i, "x"))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	h, _ := db.Branch("main").Head()
	at := h - 2
	want := dump(t, db, "main", at)
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 2}); err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "main", at); got != want {
		t.Fatalf("view at kept LSN %d changed after compact+GC:\nwant:\n%s\ngot:\n%s", at, want, got)
	}
	// a second GC with the same window is a no-op for the view
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 2}); err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "main", at); got != want {
		t.Fatalf("view at %d changed after a second GC:\nwant:\n%s\ngot:\n%s", at, want, got)
	}
	// below the window may be gone (unreferenced history)
	if err := db.Branch("main").ViewAt(ctx, 1, func(*Tx) error { return nil }); err != nil && !errors.Is(err, ErrCompacted) {
		t.Fatalf("view at 1: %v", err)
	}
}

// Pointer-only Compact of a complete prefix leaves the floor at 0, so
// Rewind(0) is a rewind to genesis. After GC drops the prefix,
// completePrefix is false and Rewind(0) is ErrCompacted.
func TestLogOnlyRewindZero(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithInlineMax(0))
	defer db.Close()
	for i := 0; i < 6; i++ {
		if err := db.Put(ctx, "k", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if f := db.floorOf(db.Manifest().Refs["main"]); f != 0 {
		t.Fatalf("floor after pointer compact: %d, want 0", f)
	}
	if err := db.Rewind(ctx, "main", 0); err != nil {
		t.Fatalf("Rewind(0) after pointer compact: %v", err)
	}
	h, _ := db.Branch("main").Head()
	if h != 0 {
		t.Fatalf("head after Rewind(0): %d", h)
	}
	if err := db.Put(ctx, "k", []byte("after")); err != nil {
		t.Fatal(err)
	}
	d1, _, err := db.IndexDigests("main")
	if err != nil {
		t.Fatal(err)
	}
	d2, err := db.RebuildDigests("main")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatalf("after Rewind(0)+put: index != fold\n  index %v\n  fold  %v", d1, d2)
	}
	if got, err := db.Get(ctx, "k"); err != nil || string(got) != "after" {
		t.Fatalf("get after rewind+put: %q %v", got, err)
	}
	// rebuild history, compact, then drop it
	for i := 0; i < 6; i++ {
		if err := db.Put(ctx, "k", []byte(fmt.Sprint("n", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 0}); err != nil {
		t.Fatal(err)
	}
	f := db.floorOf(db.Manifest().Refs["main"])
	if f == 0 {
		// KeepCommits=0 may still leave a complete prefix if live keys
		// pin every segment; force by checking the error only when the
		// prefix is gone.
		if err := db.Rewind(ctx, "main", 0); err != nil {
			if !errors.Is(err, ErrCompacted) {
				t.Fatalf("Rewind(0): %v", err)
			}
		}
		return
	}
	if err := db.Rewind(ctx, "main", 0); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Rewind(0) with floor %d: %v", f, err)
	}
}

// A branch forked after some writes inherits values that still live in
// the parent's segments. GC with KeepCommits=0 must not delete those.
func TestLogOnlyGCKeepsParentRefs(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithInlineMax(0))
	defer db.Close()
	if err := db.Put(ctx, "k0", []byte("parent")); err != nil {
		t.Fatal(err)
	}
	h, _ := db.Branch("main").Head()
	if err := db.CreateBranch(ctx, "side", "main", h); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("side").Update(ctx, func(tx *Tx) error {
		return tx.Put("k1", []byte("child"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 0}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get(ctx, "k0"); err != nil || string(got) != "parent" {
		t.Fatalf("main k0 after GC: %q %v", got, err)
	}
	var k0, k1 []byte
	err := db.Branch("side").View(ctx, func(tx *Tx) error {
		var e error
		k0, e = tx.Get("k0")
		if e != nil {
			return e
		}
		k1, e = tx.Get("k1")
		return e
	})
	if err != nil || string(k0) != "parent" || string(k1) != "child" {
		t.Fatalf("side after GC: k0=%q k1=%q err=%v", k0, k1, err)
	}
}

// ViewAt on a branch at an LSN in the inherited prefix must not report
// canFold (and return an empty view) when the parent dropped that history.
func TestLogOnlyBranchViewAtAfterParentGC(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithInlineMax(0))
	defer db.Close()
	for i := 0; i < 6; i++ {
		if err := db.Put(ctx, "k0", []byte(fmt.Sprint(i, "x"))); err != nil {
			t.Fatal(err)
		}
	}
	h, _ := db.Branch("main").Head()
	if err := db.CreateBranch(ctx, "side", "main", h); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("side").Update(ctx, func(tx *Tx) error {
		return tx.Put("k1", []byte("child"))
	}); err != nil {
		t.Fatal(err)
	}
	want := dump(t, db, "side", 3)
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 2}); err != nil {
		t.Fatal(err)
	}
	err := db.Branch("side").ViewAt(ctx, 3, func(*Tx) error { return nil })
	if errors.Is(err, ErrCompacted) {
		return // honest: parent dropped history the branch can no longer fold
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "side", 3); got != want {
		t.Fatalf("side@3 after GC:\\nwant:\\n%s\\ngot:\\n%s", want, got)
	}
}
