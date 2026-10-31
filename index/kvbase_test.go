package index

import (
	"bytes"
	"sort"
	"testing"

	"pgregory.net/rapid"
)

// The streamed base must be byte-identical to the in-memory path it
// replaces: SetLatest per key on a fresh KV, then Snapshot (design 23).
func TestBaseEncoderMatchesSnapshot(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		inline := rapid.SampledFrom([]int{0, 8, 64, 1 << 20}).Draw(t, "inline")
		n := rapid.IntRange(0, 300).Draw(t, "n")
		chunks := rapid.Bool().Draw(t, "chunks")
		type kv struct {
			space, key string
			v          Version
		}
		seen := map[string]bool{}
		var in []kv
		for i := 0; i < n; i++ {
			sp := rapid.SampledFrom([]string{"", "a", "b\x01"}).Draw(t, "space")
			k := rapid.StringN(0, 12, -1).Draw(t, "key")
			if seen[sp+"\x00"+k] {
				continue
			}
			seen[sp+"\x00"+k] = true
			v := Version{LSN: rapid.Uint64().Draw(t, "lsn"), Op: rapid.Uint32().Draw(t, "op")}
			v.Subject = rapid.SampledFrom([]string{"", "s", "subj\x00x"}).Draw(t, "subj")
			switch rapid.IntRange(0, 3).Draw(t, "val") {
			case 0: // value in a chunk or in the log
				v.Off, v.Len, v.Sum = rapid.Uint32().Draw(t, "off"), rapid.Uint32().Draw(t, "len"), rapid.Uint32().Draw(t, "sum")
				if chunks {
					v.Chunk = rapid.Uint32Range(0, 5).Draw(t, "chunk")
				}
			case 1:
				v.Val = []byte{}
			case 2:
				v.Val = []byte(rapid.String().Draw(t, "v"))
			}
			in = append(in, kv{sp, k, v})
		}
		sort.Slice(in, func(i, j int) bool { return ck(in[i].space, in[i].key) < ck(in[j].space, in[j].key) })

		old := NewKVInline(inline)
		be := NewBaseEncoder(old.Inline())
		var body []byte
		for _, e := range in {
			old.SetLatest(e.space, e.key, e.v)
			if err := be.Add(e.space, e.key, e.v); err != nil {
				t.Fatal(err)
			}
			if be.Buffered() > 100 {
				body = append(body, be.Take()...)
			}
		}
		body = append(body, be.Take()...)
		if !be.Chunked() {
			m, err := StripChunks(body)
			if err != nil {
				t.Fatal(err)
			}
			body = body[:m]
		}
		got := append(be.Header(), body...)
		want := old.Snapshot()
		if !bytes.Equal(got, want) {
			t.Fatalf("streamed base differs (%d vs %d bytes, chunked %v)", len(got), len(want), be.Chunked())
		}
		if _, err := LoadKVBase(got, inline); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBaseEncoderRejects(t *testing.T) {
	be := NewBaseEncoder(64)
	if err := be.Add("a", "b", Version{}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ s, k string }{{"a", "b"}, {"a", "a"}, {"", "z"}} {
		if be.Add(c.s, c.k, Version{}) == nil {
			t.Fatalf("out-of-order %q/%q accepted", c.s, c.k)
		}
	}
	if be.Add("b", "", Version{Del: true}) == nil {
		t.Fatal("delete accepted")
	}
	if _, err := StripChunks([]byte{1, 2, 3}); err == nil {
		t.Fatal("garbage accepted")
	}
	be = NewBaseEncoder(64)
	_ = be.Add("", "k", Version{Chunk: 1})
	if _, err := StripChunks(be.Take()); err == nil {
		t.Fatal("chunk stripped")
	}
}
