package index

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"testing"

	"github.com/raman20/loget/format"
)

// TestIngestModel: a KV fed puts, deletes and ingests reads exactly like
// the map model fed the ingests as puts at the same LSN, at every LSN; a
// second KV that loads every ingest from its stored bytes (replay) ends
// with the same digest.
func TestIngestModel(t *testing.T) {
	setPieces(t, 64)
	for seed := int64(1); seed <= 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		o := KVOptions{Inline: 1 << 20}
		if seed%2 == 0 {
			fs, _ := testFiles(t, nil)
			o.Files = fs
		}
		kv, re, model := NewKVOptions(o), NewKVOptions(o), newMapKV(1<<20)
		lsn := uint64(0)
		for step := 0; step < 80; step++ {
			lsn++
			if rng.Intn(4) == 0 {
				var ks []string
				seen := map[string]bool{}
				for i := rng.Intn(60); i > 0; i-- {
					k := fmt.Sprintf("k%02d", rng.Intn(90))
					if !seen[k] {
						seen[k] = true
						ks = append(ks, k)
					}
				}
				sort.Strings(ks)
				g, err := NewIngest(o, "kv", lsn)
				if err != nil {
					t.Fatal(err)
				}
				var ops []format.Op
				for _, k := range ks {
					v := []byte(fmt.Sprint(lsn, k))
					if rng.Intn(10) == 0 {
						v = nil
					}
					if err := g.Add(k, v); err != nil {
						t.Fatal(err)
					}
					if v == nil {
						v = []byte{}
					}
					ops = append(ops, format.Op{Kind: format.OpPut, Space: "kv", Key: k, Value: v})
				}
				if len(ks) > 0 && !errors.Is(g.Add(ks[0], nil), ErrUnsorted) {
					t.Fatal("unsorted add accepted")
				}
				x, err := g.Finish()
				if err != nil {
					t.Fatal(err)
				}
				if x == nil {
					if len(ks) != 0 {
						t.Fatal("nil run")
					}
					lsn--
					continue
				}
				x.Key = fmt.Sprintf("log/L/%d.ingest", lsn)
				var stored []byte
				for i := range x.Meta.Sums {
					stored = append(stored, x.Piece(i)...)
				}
				y, err := re.LoadIngest(RunSource{Key: x.Key, Meta: x.Meta, Piece: func(i int) ([]byte, error) {
					return stored[i*PieceBytes : min((i+1)*PieceBytes, len(stored))], nil
				}}, lsn)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := re.LoadIngest(RunSource{Key: x.Key, Meta: x.Meta, Piece: func(i int) ([]byte, error) {
					return stored[i*PieceBytes : min((i+1)*PieceBytes, len(stored))], nil
				}}, lsn+1); !errors.Is(err, ErrBadRun) {
					t.Fatalf("wrong LSN accepted: %v", err)
				}
				iop := []format.Op{{Kind: format.OpIngest, Space: "kv", Key: x.Key}}
				kv.Apply(&Entry{LSN: lsn, Ops: iop, Ingest: x}, nil)
				re.Apply(&Entry{LSN: lsn, Ops: iop, Ingest: y}, nil)
				model.Apply(&Entry{LSN: lsn, Ops: ops}, nil)
				continue
			}
			k := fmt.Sprintf("k%02d", rng.Intn(90))
			op := format.Op{Kind: format.OpPut, Space: "kv", Key: k, Value: []byte(fmt.Sprint(lsn))}
			if rng.Intn(3) == 0 {
				op = format.Op{Kind: format.OpDel, Space: "kv", Key: k}
			}
			for _, l := range []interface{ Apply(*Entry, Env) }{kv, re, model} {
				l.Apply(&Entry{LSN: lsn, Ops: []format.Op{op}}, nil)
			}
		}
		for at := uint64(0); at <= lsn; at++ {
			for i := 0; i < 90; i++ {
				k := fmt.Sprintf("k%02d", i)
				a, aok := kv.Get("kv", k, at)
				b, bok := model.Get("kv", k, at)
				if aok != bok || a.LSN != b.LSN || !bytes.Equal(a.Val, b.Val) {
					t.Fatalf("seed %d %s@%d: %+v %v, model %+v %v", seed, k, at, a, aok, b, bok)
				}
			}
		}
		if kv.Digest() != re.Digest() {
			t.Fatalf("seed %d: loaded ingest differs from built", seed)
		}
	}
}

// FuzzLoadIngest: any bytes with matching checksums either fail to load
// as an ingest or install into a KV that serves reads without a panic.
func FuzzLoadIngest(f *testing.F) {
	g, _ := NewIngest(KVOptions{Inline: 4}, "kv", 7)
	g.Add("a", []byte("1"))
	g.Add("b", []byte("22222222"))
	x, _ := g.Finish()
	f.Add(bytes.Clone(x.Piece(0)), x.Meta.N, x.Meta.Vers)
	f.Fuzz(func(t *testing.T, b []byte, n, vers int) {
		if len(b) == 0 {
			return
		}
		m := RunMeta{Len: int64(len(b)), N: n, Vers: vers, Piece: PieceBytes}
		for p := 0; p < len(b); p += PieceBytes {
			m.Sums = append(m.Sums, crc32.Checksum(b[p:min(p+PieceBytes, len(b))], Castagnoli))
		}
		kv := NewKVOptions(KVOptions{Inline: 4})
		kv.Apply(&Entry{LSN: 3, Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: "a", Value: []byte("x")}}}, nil)
		y, err := kv.LoadIngest(RunSource{Key: "k", Meta: m, Piece: func(i int) ([]byte, error) {
			return b[i*PieceBytes : min((i+1)*PieceBytes, len(b))], nil
		}}, 7)
		if err != nil {
			return
		}
		kv.Apply(&Entry{LSN: 7, Ops: []format.Op{{Kind: format.OpIngest, Space: "kv", Key: "k"}}, Ingest: y}, nil)
		kv.Latest(func(space, key string, v Version) error {
			if v.LSN != 7 && key != "a" {
				t.Fatalf("%s at %d", key, v.LSN)
			}
			kv.Get(space, key, 5)
			kv.Scan(space, "", ^uint64(0))
			return nil
		})
		if _, err := LoadKV(kv.Snapshot(), 4); err != nil {
			t.Fatal(err)
		}
	})
}
