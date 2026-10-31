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
