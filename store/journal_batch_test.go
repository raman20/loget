//go:build unix

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// putKeys puts n objects whose data contains "SECRET" and returns the keys.
func putKeys(t *testing.T, j *Journal, n int) []string {
	t.Helper()
	var keys []string
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("log/L1/%020d.seg", i)
		if err := j.PutIfAbsent(context.Background(), k, []byte(fmt.Sprint("SECRET-", i, "-", bytes.Repeat([]byte{'x'}, i)))); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}

func TestJournalDeleteBatch(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	keys := putKeys(t, j, 300)
	defer func(m int) { jrnMaxBatch = m }(jrnMaxBatch)
	jrnMaxBatch = 1000 // ~33 keys per record: several batches
	syncs := 0
	jtrace = func(ev jev) {
		if ev.kind == 's' {
			syncs++
		}
	}
	del := append([]string{"missing/1"}, keys[:200]...)
	del = append(del, keys[5], keys[7]) // duplicates
	err := j.DeleteBatch(ctx, del)
	jtrace = nil
	if err != nil {
		t.Fatal(err)
	}
	batches := (200*(2+len(keys[0])) + jrnMaxBatch - 1) / jrnMaxBatch
	if syncs > 2*(batches+1) {
		t.Fatalf("%d syncs for %d batches (want 2 per batch, not 2 per key)", syncs, batches)
	}
	check := func(j *Journal) {
		t.Helper()
		for i, k := range keys {
			_, err := j.Get(ctx, k)
			if i < 200 && !errors.Is(err, ErrNotFound) || i >= 200 && err != nil {
				t.Fatalf("%s: %v", k, err)
			}
		}
	}
	check(j)
	j.Close()
	raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
	for i := 0; i < 200; i++ {
		if bytes.Contains(raw, []byte(fmt.Sprint("SECRET-", i, "-"))) {
			t.Fatalf("deleted bytes of key %d still in the file", i)
		}
	}
	j2 := mustJournal(t, d)
	defer j2.Close()
	check(j2)
	if err := j2.DeleteBatch(ctx, []string{""}); err == nil {
		t.Fatal("empty key accepted")
	}
}

// A version 1 file (an older build's) keeps one record per delete, so
// that build can still open it after a batch delete by this one.
func TestJournalDeleteBatchV1(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	keys := putKeys(t, j, 20)
	j.Close()
	p := filepath.Join(d, jrnFile)
	f, _ := os.OpenFile(p, os.O_RDWR, 0)
	h := make([]byte, jrnHead)
	f.ReadAt(h, 0)
	copy(h, jrnMagicV1)
	binary.LittleEndian.PutUint32(h[24:28], crc32.Checksum(h[:24], jcrc))
	f.WriteAt(h, 0)
	f.Close()
	j = mustJournal(t, d)
	if j.ver != 1 {
		t.Fatalf("version %d", j.ver)
	}
	if err := j.DeleteBatch(ctx, keys[:10]); err != nil {
		t.Fatal(err)
	}
	j.Close()
	j = mustJournal(t, d)
	defer j.Close()
	n := 0
	j.end = jrnHead
	j.idx = map[string]jent{}
	j.scan(func(kind byte, _ string, _ jent) {
		if kind == recDels {
			t.Fatal("batch record in a version 1 file")
		}
		n++
	})
	if raw, _ := os.ReadFile(p); string(raw[:8]) != jrnMagicV1 || n != 30 {
		t.Fatalf("magic %q, %d records", raw[:8], n)
	}
}

// batchImage returns a journal file with n puts and the batch record that
// deletes the first del of them (not yet in the file), at offset at.
func batchImage(t *testing.T, n, del int) (base, rec []byte, at int64, keys []string) {
	t.Helper()
	d := t.TempDir()
	j := mustJournal(t, d)
	keys = putKeys(t, j, n)
	var data []byte
	for _, k := range keys[:del] {
		data = binary.LittleEndian.AppendUint16(data, uint16(len(k)))
		data = append(data, k...)
	}
	at = j.end
	rec = j.record(at, recDels, delsKey, data)
	j.Close()
	base, _ = os.ReadFile(filepath.Join(d, jrnFile))
	return base, rec, at, keys
}

func openImage(t *testing.T, img []byte) (*Journal, error) {
	t.Helper()
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, jrnFile), img, 0o644); err != nil {
		t.Fatal(err)
	}
	return NewJournal(d)
}

// A torn batch (any subset of its 512-byte sectors on disk, in any order:
// the power-loss model) is a torn tail: open succeeds, nothing of the
// batch is applied, and the journal accepts appends. This is what one
// record per batch buys: N delete records torn the same way leave a
// damaged record followed by valid ones, which open must call corruption
// (TestJournalTornDeleteRecordsAreCorruption).
func TestJournalTornBatch(t *testing.T) {
	ctx := context.Background()
	base, rec, at, keys := batchImage(t, 80, 80)
	first, last := at/512, (at+int64(len(rec))-1)/512
	sectors := int(last - first + 1)
	if sectors < 3 || sectors > 12 {
		t.Fatalf("batch spans %d sectors", sectors)
	}
	for mask := 0; mask < 1<<sectors; mask++ {
		img := append([]byte(nil), base...)
		for s := 0; s < sectors; s++ {
			if mask&(1<<s) == 0 {
				continue
			}
			lo := max(at, (first+int64(s))*512)
			hi := min(at+int64(len(rec)), (first+int64(s)+1)*512)
			copy(img[lo:hi], rec[lo-at:hi-at])
		}
		j, err := openImage(t, img)
		if err != nil {
			t.Fatalf("sectors %b: torn batch read as %v", mask, err)
		}
		full := mask == 1<<sectors-1
		for _, k := range keys {
			_, err := j.Get(ctx, k)
			if full != errors.Is(err, ErrNotFound) || !full && err != nil {
				t.Fatalf("sectors %b: %s: %v (batch on disk: %v)", mask, k, err, full)
			}
		}
		if err := j.PutIfAbsent(ctx, "after/crash", []byte("x")); err != nil {
			t.Fatalf("sectors %b: append: %v", mask, err)
		}
		j.Close()
	}
}

// The batch record is durable but the crash cut the erasing short: some
// puts marked dead, one header rewrite torn, data partly punched. Open
// finishes every delete of the batch and leaves none of their bytes.
func TestJournalBatchInterruptedErase(t *testing.T) {
	ctx := context.Background()
	base, rec, at, keys := batchImage(t, 40, 30)
	for cut := 0; cut <= 30; cut += 3 {
		d := t.TempDir()
		p := filepath.Join(d, jrnFile)
		os.WriteFile(p, base, 0o644)
		j := mustJournal(t, d) // for the file's salt; the batch is not in it yet
		type w struct {
			off  int64
			data []byte
		}
		var ws []w
		for i, k := range keys[:cut] {
			e := j.idx[k]
			h := make([]byte, jrnHdr+e.klen)
			j.f.ReadAt(h, e.off)
			h[4] = recDead
			binary.LittleEndian.PutUint32(h[:4], j.sum(e.off, h[4:jrnHdr], h[jrnHdr:]))
			if i == cut-1 { // torn: only part of the dead header landed
				ws = append(ws, w{e.off + 4, h[4:6]})
				continue
			}
			ws = append(ws, w{e.off, h}, w{e.off + int64(len(h)), make([]byte, e.dlen)})
		}
		j.Close()
		f, _ := os.OpenFile(p, os.O_RDWR, 0)
		f.WriteAt(rec, at)
		for _, x := range ws {
			f.WriteAt(x.data, x.off)
		}
		f.Close()
		j = mustJournal(t, d)
		for i, k := range keys {
			_, err := j.Get(ctx, k)
			if i < 30 && !errors.Is(err, ErrNotFound) || i >= 30 && err != nil {
				t.Fatalf("cut %d: %s: %v", cut, k, err)
			}
		}
		j.Close()
		raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
		for i := 0; i < 30; i++ {
			if bytes.Contains(raw, []byte(fmt.Sprint("SECRET-", i, "-"))) {
				t.Fatalf("cut %d: bytes of deleted key %d left", cut, i)
			}
		}
	}
}

// Why a batch is one record: per-key delete records torn out of order
// (an earlier one lost, a later one on disk) are indistinguishable from
// corruption. Genuine corruption is still reported with batch records:
// a damaged batch followed by a valid record.
func TestJournalTornDeleteRecordsAreCorruption(t *testing.T) {
	base, _, at, keys := batchImage(t, 10, 0)
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, jrnFile), base, 0o644)
	j := mustJournal(t, d)
	r1 := j.record(at, recDel, keys[0], nil)
	r2 := j.record(at+int64(len(r1)), recDel, keys[1], nil)
	j.Close()
	img := append([]byte(nil), base...)
	copy(img[at:], r1)
	img[at+3] ^= 0xff // the first record's write did not land intact
	copy(img[at+int64(len(r1)):], r2)
	if _, err := openImage(t, img); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("per-key records torn out of order: %v", err)
	}

	base, rec, at, _ := batchImage(t, 10, 5)
	os.WriteFile(filepath.Join(d, jrnFile), base, 0o644)
	j = mustJournal(t, d)
	put := j.record(at+int64(len(rec)), recPut, "after", []byte("x"))
	j.Close()
	img = append([]byte(nil), base...)
	copy(img[at:], rec)
	img[at+int64(len(rec))-1] ^= 0xff
	copy(img[at+int64(len(rec)):], put)
	if _, err := openImage(t, img); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged batch followed by a valid record: %v", err)
	}
	// a CRC-valid batch with a malformed key list is corruption too
	d2 := t.TempDir()
	os.WriteFile(filepath.Join(d2, jrnFile), base, 0o644)
	j = mustJournal(t, d2)
	bad := j.record(at, recDels, delsKey, []byte{5, 0, 'a'})
	j.Close()
	img = append([]byte(nil), base...)
	copy(img[at:], bad)
	if _, err := openImage(t, img); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed batch: %v", err)
	}
}

// Faulty forwards batches; an injected failure leaves a prefix deleted.
func TestFaultyDeleteBatch(t *testing.T) {
	ctx := context.Background()
	j := mustJournal(t, t.TempDir())
	defer j.Close()
	keys := putKeys(t, j, 50)
	f := NewFaulty(j, 1, FaultProbs{FailBefore: 1})
	if err := f.DeleteBatch(ctx, keys); !errors.Is(err, ErrInjected) {
		t.Fatalf("injected: %v", err)
	}
	gone := 0
	for _, k := range keys {
		if _, err := j.Get(ctx, k); errors.Is(err, ErrNotFound) {
			gone++
		} else if gone > 0 && err == nil {
			// fine only if the deleted ones are a prefix
			for _, k2 := range keys[gone:] {
				if _, err := j.Get(ctx, k2); err != nil {
					t.Fatalf("not a prefix: %s %v", k2, err)
				}
			}
			break
		}
	}
	f.P.FailBefore = 0
	if err := f.DeleteBatch(ctx, keys); err != nil {
		t.Fatal(err)
	}
	if ks, _ := j.Keys(ctx); len(ks) != 0 {
		t.Fatalf("%d keys left", len(ks))
	}
}
