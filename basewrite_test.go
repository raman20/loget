package loget

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"pgregory.net/rapid"
)

// encodeBase is the in-memory base image writeBase built before design 23.
func encodeBase(lsn uint64, kv []byte) []byte {
	b := append([]byte(baseMagic), make([]byte, 16)...)
	binary.LittleEndian.PutUint64(b[8:], lsn)
	binary.LittleEndian.PutUint64(b[16:], uint64(len(kv)))
	b = append(b, kv...)
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

// The streamed base image (heap or run file, KV2 or KV3) is byte-identical
// to the old SetLatest + Snapshot + encodeBase path.
func TestBaseStreamMatchesOldImage(t *testing.T) {
	dir := t.TempDir()
	files, created := testIndexFiles(t, dir, nil)
	rapid.Check(t, func(t *rapid.T) {
		db := &DB{opts: Options{Logf: func(string, ...any) {}}}
		if rapid.Bool().Draw(t, "file") {
			db.runFiles = files
		}
		inline := rapid.SampledFrom([]int{0, 64}).Draw(t, "inline")
		chunks := rapid.Bool().Draw(t, "chunks")
		n := rapid.IntRange(0, 2000).Draw(t, "n")
		old := index.NewKVInline(inline)
		bs := db.newBaseStream(old.Inline())
		for i := 0; i < n; i++ {
			v := index.Version{LSN: uint64(i + 1), Op: uint32(i % 3)}
			if i%7 == 0 {
				v.Subject = "s"
			}
			if l := rapid.IntRange(0, 2*baseFlushAt/n+100).Draw(t, "len"); l > inline && chunks {
				v.Chunk, v.Off, v.Len, v.Sum = uint32(i%4+1), uint32(i), uint32(l), 7
			} else if l > 0 || i%2 == 0 {
				v.Val = bytes.Repeat([]byte{byte(i)}, l)
			}
			k := fmt.Sprintf("k%06d", i)
			old.SetLatest("sp", k, v)
			if err := bs.add("sp", k, v); err != nil {
				t.Fatal(err)
			}
		}
		lsn := rapid.Uint64().Draw(t, "lsn")
		img, release, err := bs.finish(lsn)
		if err != nil {
			t.Fatal(err)
		}
		want := encodeBase(lsn, old.Snapshot())
		if !bytes.Equal(img, want) {
			t.Fatalf("image differs: %d vs %d bytes", len(img), len(want))
		}
		release()
	})
	if created.Load() == 0 {
		t.Fatal("no base went through a run file")
	}
	noFiles(t, dir)
}

// A run file that cannot be created puts the base on the heap; one that
// fails mid-write fails the base (nothing is left behind either way).
func TestBaseStreamFileFaults(t *testing.T) {
	dir := t.TempDir()
	for _, op := range []string{"create", "write", "map"} {
		boom := errors.New("boom " + op)
		files, _ := testIndexFiles(t, dir, func(o string) error {
			if o == op {
				return boom
			}
			return nil
		})
		db := &DB{runFiles: files, opts: Options{Logf: func(string, ...any) {}}}
		bs := db.newBaseStream(64)
		var err error
		for i := 0; i < 50000 && err == nil; i++ {
			err = bs.add("", fmt.Sprintf("k%06d", i), index.Version{LSN: 1, Val: []byte("0123456789012345678901234567890")})
		}
		var img []byte
		release := func() {}
		if err == nil {
			img, release, err = bs.finish(9)
		}
		if op == "create" {
			if err != nil || len(img) == 0 || db.met.kvFileErrors.Load() != 1 {
				t.Fatalf("%s: %v, %d bytes, %d file errors", op, err, len(img), db.met.kvFileErrors.Load())
			}
			release()
		} else if !errors.Is(err, boom) {
			t.Fatalf("%s: %v", op, err)
		} else {
			bs.abandon()
		}
		noFiles(t, dir)
	}
}
