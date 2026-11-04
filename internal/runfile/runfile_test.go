package runfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileRoundTrip(t *testing.T) {
	if !Supported {
		t.Skip("no mmap")
	}
	dir := t.TempDir()
	d, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create()
	if err != nil {
		t.Fatal(err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("file visible in %s: unlinking failed", dir)
	}
	want := bytes.Repeat([]byte("abc"), 5000) // not a multiple of 8
	if err := f.Write(want[:7]); err != nil {
		t.Fatal(err)
	}
	if err := f.Write(want[7:]); err != nil {
		t.Fatal(err)
	}
	b, s, err := f.Map(100)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, want) || cap(b) != len(want) {
		t.Fatalf("mapped %d bytes (cap %d), want %d", len(b), cap(b), len(want))
	}
	for i := range s {
		if s[i] != 0 {
			t.Fatalf("slot %d = %d, want 0", i, s[i])
		}
		s[i] = uint64(i) * 3
	}
	if err := f.Seal(); err != nil {
		t.Fatal(err)
	}
	if s[99] != 297 || !bytes.Equal(b, want) {
		t.Fatal("contents changed by Seal")
	}
	f.Release()
	f.Release() // idempotent
}

func TestNewFails(t *testing.T) {
	if !Supported {
		if _, err := New(t.TempDir()); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("New = %v, want ErrUnsupported", err)
		}
		return
	}
	if _, err := New(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("New on a missing directory succeeded")
	}
}

func TestFault(t *testing.T) {
	if !Supported {
		t.Skip("no mmap")
	}
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"create", "write", "map", "seal"} {
		injected := errors.New(op)
		d.Fault = func(o string) error {
			if o == op {
				return injected
			}
			return nil
		}
		f, err := d.Create()
		if err == nil {
			if err = f.Write([]byte("x")); err == nil {
				if _, _, err = f.Map(1); err == nil {
					err = f.Seal()
				}
			}
			f.Release()
		}
		if err != injected {
			t.Fatalf("op %s: err %v", op, err)
		}
	}
}

// A run file never has a directory entry once Create returns, and on
// Linux never at all (O_TMPFILE), so a SIGKILL leaves nothing behind.
func TestCreateLeavesNoEntry(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if ents, err := os.ReadDir(dir); err != nil || len(ents) != 0 {
		t.Fatalf("%d entries, %v", len(ents), err)
	}
	f.Release()
}

// TestFileRef: the file stays mapped until Release has been called once
// more than Ref, from any goroutine.
func TestFileRef(t *testing.T) {
	if !Supported {
		t.Skip("no mmap")
	}
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create()
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("xyz"), 3000)
	if err := f.Write(want); err != nil {
		t.Fatal(err)
	}
	b, _, err := f.Map(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Seal(); err != nil {
		t.Fatal(err)
	}
	f.Ref()
	f.Ref()
	done := make(chan bool)
	go func() {
		f.Release() // a reader's
		done <- true
	}()
	f.Release() // the owner's
	<-done
	if !bytes.Equal(b, want) || f.m == nil {
		t.Fatal("unmapped while referenced")
	}
	f.Release() // the last
	if f.m != nil {
		t.Fatal("still mapped after the last Release")
	}
	f.Release() // idempotent
}

// TestDropKeepsData: dropping pages (design 25) never changes what a
// later read of the mapping returns, for any range including unaligned
// and out-of-bounds ones.
func TestDropKeepsData(t *testing.T) {
	if !Supported {
		t.Skip("no mmap")
	}
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	want := make([]byte, 5<<20+123)
	for i := range want {
		want[i] = byte(i * 7)
	}
	if err := f.Write(want); err != nil {
		t.Fatal(err)
	}
	b, _, err := f.Map(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Seal(); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int{{0, 1 << 20}, {17, 3<<20 + 5}, {0, len(want)}, {len(want) - 10, 1 << 30}, {5, 0}} {
		f.Drop(r[0], r[1])
		if !bytes.Equal(b, want) {
			t.Fatalf("data changed after Drop(%d, %d)", r[0], r[1])
		}
	}
}
