package index

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/raman20/loget/format"
	"pgregory.net/rapid"
)

// sources turns a cut into stored runs (the bytes copied, as a store
// would) with small pieces.
func sources(c *Cut) []RunSource {
	var out []RunSource
	for _, cr := range c.Runs {
		b := bytes.Clone(cr.Bytes())
		m := cr.Meta()
		out = append(out, RunSource{Key: cr.Key(), Meta: m, Piece: func(i int) ([]byte, error) {
			return b[i*PieceBytes : min((i+1)*PieceBytes, len(b))], nil
		}})
	}
	return out
}

func setPieces(t testing.TB, n int) {
	old := PieceBytes
	PieceBytes = n
	t.Cleanup(func() { PieceBytes = old })
}

// TestKVCutRefinesSnapshot: a KV loaded from the stored runs of a cut is
// the KV at the cut (same encoding, same reads), whatever the KV did
// after the cut (merges and shreds replace the runs the cut pinned), with
// runs on the heap or in files; runs stored once keep their key.
func TestKVCutRefinesSnapshot(t *testing.T) {
	setPieces(t, 7)
	fs, dir := testFiles(t, nil)
	for _, files := range []RunFiles{nil, fs} {
		rapid.Check(t, func(rt *rapid.T) {
			o := KVOptions{Inline: 3, Files: files, OnFileError: func(err error) { rt.Fatal(err) }}
			log := genLog(rt)
			cutAt := rapid.IntRange(0, len(log)).Draw(rt, "cut")
			l := NewKVOptions(o)
			l.hotMax = rapid.IntRange(1, 8).Draw(rt, "hotMax")
			env := testEnv{}
			for _, e := range log[:cutAt] {
				l.Apply(e, env)
			}
			want := l.Snapshot()
			c := l.Cut()
			defer c.Release()
			for i, cr := range c.Runs {
				if !cr.Hot && i%2 == 0 {
					cr.SetKey(fmt.Sprint("r", i))
				}
			}
			for _, e := range log[cutAt:] { // replaces pinned runs
				l.Apply(e, env)
			}
			l.DropSubject("a")
			got, err := LoadKVRuns(o, sources(c))
			if err != nil {
				rt.Fatal(err)
			}
			if !bytes.Equal(got.Snapshot(), want) {
				rt.Fatalf("loaded KV differs from the KV at the cut")
			}
			ref := NewKV()
			ref.inline = 3
			for _, e := range log[:cutAt] {
				ref.Apply(e, env)
			}
			for _, sp := range spaces {
				for k := 0; k < 5; k++ {
					key := fmt.Sprint("k", k)
					for at := uint64(0); at <= uint64(cutAt)+1; at++ {
						a, aok := got.Get(sp, key, at)
						b, bok := ref.Get(sp, key, at)
						if aok != bok || aok && (a.LSN != b.LSN || !bytes.Equal(a.Val, b.Val)) {
							rt.Fatalf("Get(%s,%s,%d) = %v %v, want %v %v", sp, key, at, a, aok, b, bok)
						}
					}
				}
			}
			// keys survive in the loaded KV: its next cut names them
			c2 := got.Cut()
			for i, cr := range c2.Runs {
				if i < len(c.Runs) && cr.Key() != c.Runs[i].Key() && !c.Runs[i].Hot {
					// merges planned at load may have replaced runs: only
					// runs present in both must agree
					if cr.Key() != "" {
						rt.Fatalf("run %d key %q, stored as %q", i, cr.Key(), c.Runs[i].Key())
					}
				}
			}
			c2.Release()
			got.dropRuns()
		})
	}
	emptyDir(t, dir)
}

// TestKVCutPinsRuns: a run replaced while a cut holds it stays mapped
// until the cut is released.
func TestKVCutPinsRuns(t *testing.T) {
	fs, _ := testFiles(t, nil)
	l := NewKVOptions(KVOptions{Inline: 3, Files: fs})
	l.hotMax = 4
	put := func(i int) {
		l.Apply(&Entry{LSN: uint64(i), Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: fmt.Sprint("k", i%13), Value: []byte("v")}}}, testEnv{})
	}
	for i := 1; i <= 40; i++ {
		put(i)
	}
	c := l.Cut()
	var before [][]byte
	for _, cr := range c.Runs {
		before = append(before, bytes.Clone(cr.Bytes()))
	}
	for i := 41; i <= 400; i++ { // every run of the cut is merged away
		put(i)
	}
	l.work(-1)
	for i, cr := range c.Runs {
		if !bytes.Equal(cr.Bytes(), before[i]) {
			t.Fatalf("run %d changed under the cut", i)
		}
	}
	c.Release()
	c.Release() // idempotent
	for _, cr := range c.Runs {
		if cr.ref != nil {
			t.Fatal("reference kept after Release")
		}
	}
}

// TestLoadKVRunsRejects: damaged pieces and descriptions fail with
// ErrBadRun; nothing is loaded.
func TestLoadKVRunsRejects(t *testing.T) {
	setPieces(t, 16)
	l := kvSeed(true)
	l.freeze()
	c := l.Cut()
	defer c.Release()
	good := sources(c)
	if _, err := LoadKVRuns(KVOptions{Inline: 4}, good); err != nil {
		t.Fatal(err)
	}
	mut := func(f func(s *RunSource, b []byte)) []RunSource {
		s := good[0]
		b := bytes.Clone(c.Runs[0].Bytes())
		f(&s, b)
		s.Piece = func(i int) ([]byte, error) { return b[i*PieceBytes : min((i+1)*PieceBytes, len(b))], nil }
		return []RunSource{s}
	}
	cases := map[string][]RunSource{
		"flipped bit": mut(func(_ *RunSource, b []byte) { b[3] ^= 1 }),
		"bad count":   mut(func(s *RunSource, _ []byte) { s.Meta.N++ }),
		"bad len":     mut(func(s *RunSource, _ []byte) { s.Meta.Len-- }),
		"no sums":     mut(func(s *RunSource, _ []byte) { s.Meta.Sums = nil }),
		"no piece":    mut(func(s *RunSource, _ []byte) { s.Meta.Piece = 0 }),
		"big piece":   mut(func(s *RunSource, _ []byte) { s.Meta.Piece = maxPieceBytes + 1 }),
		// a damaged entry with matching checksums: the structural check
		"bad entry (sum ok)": mut(func(s *RunSource, b []byte) {
			b[0] = 5 // the first key shares a prefix with nothing
			s.Meta = RunMeta{Len: s.Meta.Len, N: s.Meta.N, Vers: s.Meta.Vers, Piece: s.Meta.Piece}
			for p := 0; p < len(b); p += PieceBytes {
				s.Meta.Sums = append(s.Meta.Sums, crc32.Checksum(b[p:min(p+PieceBytes, len(b))], Castagnoli))
			}
		}),
	}
	for name, src := range cases {
		if _, err := LoadKVRuns(KVOptions{Inline: 4}, src); !errors.Is(err, ErrBadRun) {
			t.Errorf("%s: err = %v, want ErrBadRun", name, err)
		}
	}
	short := []RunSource{good[0]}
	short[0].Piece = func(int) ([]byte, error) { return nil, errors.New("gone") }
	if _, err := LoadKVRuns(KVOptions{Inline: 4}, short); err == nil {
		t.Error("a missing piece loaded")
	}
}

// TestRunWriter: a run written key by key loads like the same KV.
func TestRunWriter(t *testing.T) {
	src := kvSeed(true)
	rw := NewRunWriter(KVOptions{Inline: 4}, 0)
	if err := src.each(func(k []byte, vs []Version) error {
		i := bytes.IndexByte(k, 0)
		return rw.Add(string(k[:i]), string(k[i+1:]), vs...)
	}); err != nil {
		t.Fatal(err)
	}
	c, err := rw.Finish()
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadKVRuns(KVOptions{Inline: 4}, sources(c))
	c.Release()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Snapshot(), src.Snapshot()) {
		t.Fatal("RunWriter run differs")
	}
	rw = NewRunWriter(KVOptions{}, 0)
	if rw.Add("kv", "b", Version{LSN: 1}) != nil || rw.Add("kv", "a", Version{LSN: 2}) == nil {
		t.Fatal("out-of-order key accepted")
	}
	rw.Abort()
}

// FuzzLoadKVRuns: any bytes with matching checksums either fail to load or
// load into a KV that serves every read without a panic.
func FuzzLoadKVRuns(f *testing.F) {
	l := kvSeed(true)
	l.freeze()
	c := l.Cut()
	f.Add(bytes.Clone(c.Runs[0].Bytes()), c.Runs[0].r.n, c.Runs[0].r.vers)
	c.Release()
	f.Fuzz(func(t *testing.T, b []byte, n, vers int) {
		if len(b) == 0 {
			return
		}
		m := RunMeta{Len: int64(len(b)), N: n, Vers: vers}
		for p := 0; p < len(b); p += PieceBytes {
			m.Sums = append(m.Sums, crc32.Checksum(b[p:min(p+PieceBytes, len(b))], Castagnoli))
		}
		kv, err := LoadKVRuns(KVOptions{Inline: 4}, []RunSource{{Meta: m, Piece: func(i int) ([]byte, error) {
			return b[i*PieceBytes : min((i+1)*PieceBytes, len(b))], nil
		}}})
		if err != nil {
			return
		}
		kv.Latest(func(space, key string, v Version) error {
			kv.Get(space, key, v.LSN)
			kv.LastWrite(space, key)
			kv.Scan(space, key[:len(key)/2], ^uint64(0))
			return nil
		})
		kv.DropSubject("s")
		if _, err := LoadKV(kv.Snapshot(), 4); err != nil {
			t.Fatalf("a loaded run does not encode as a valid snapshot: %v", err)
		}
	})
}
