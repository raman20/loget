package index

import (
	"bytes"
	"testing"

	"github.com/raman20/loget/format"
)

// kvSeed is a small KV with every kind of version: inline, empty and nil
// values, positions, deletes, subjects, base chunks (KV3).
func kvSeed(chunk bool) *KV {
	l := NewKVInline(4)
	l.put(ck("kv", "a"), Version{LSN: 1, Val: []byte("x")})
	l.put(ck("kv", "a"), Version{LSN: 3, Off: 9, Len: 100, Sum: 7, Op: 1})
	l.put(ck("kv", "b"), Version{LSN: 2, Val: []byte{}, Subject: "s"})
	l.put(ck("kv", "b"), Version{LSN: 4, Del: true})
	l.put(ck("doc:c", "k"), Version{LSN: 5})
	if chunk {
		l.put(ck("kv", "z"), Version{LSN: 6, Off: 3, Len: 9, Chunk: 2})
	}
	return l
}

// kvSeedExp is kvSeed with expiring versions (KV4, design 27).
func kvSeedExp(chunk bool) *KV {
	l := kvSeed(chunk)
	l.put(ck("kv", "e"), Version{LSN: 7, Val: []byte("x"), Exp: 99})
	l.put(ck("kv", "f"), Version{LSN: 8, Off: 5, Len: 10, Sum: 1, Exp: 1 << 62})
	return l
}

// FuzzLoadKV: a snapshot or base either fails to load or loads into an index
// that re-encodes to the same bytes and serves every read without a panic.
func FuzzLoadKV(f *testing.F) {
	f.Add(kvSeed(false).Snapshot())
	f.Add(kvSeed(true).Snapshot())
	f.Add(kvSeedExp(false).Snapshot())
	f.Add(kvSeedExp(true).Snapshot())
	f.Add(NewKVInline(4).Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) { fuzzLoadKV(t, b, KVOptions{Inline: 4}) })
}

// FuzzLoadKVFiles: FuzzLoadKV with every run in a mapped file (design 21).
func FuzzLoadKVFiles(f *testing.F) {
	f.Add(kvSeed(false).Snapshot())
	f.Add(kvSeed(true).Snapshot())
	f.Add(kvSeedExp(false).Snapshot())
	f.Add(kvSeedExp(true).Snapshot())
	fs, _ := testFiles(f, nil)
	f.Fuzz(func(t *testing.T, b []byte) {
		fuzzLoadKV(t, b, KVOptions{Inline: 4, Files: fs, OnFileError: func(err error) { t.Fatal(err) }})
	})
}

func fuzzLoadKV(t *testing.T, b []byte, o KVOptions) {
	for _, strict := range []bool{true, false} {
		l, err := loadKV(b, o, strict)
		if err != nil {
			continue
		}
		want := b
		if !strict { // a base keeps its own inline limit field
			want = append([]byte{}, b...)
			copy(want[11:19], l.Snapshot()[11:19])
		}
		if got := l.Snapshot(); !bytes.Equal(got, want) {
			t.Fatalf("loaded bytes do not re-encode identically (strict=%v)", strict)
		}
		l.Latest(func(space, key string, v Version) error {
			l.Get(space, key, v.LSN)
			l.Get(space, key, v.LSN-1)
			l.LastWrite(space, key)
			l.Scan(space, key[:len(key)/2], ^uint64(0))
			return nil
		})
		l.DropSubject("s")
		l.Digest()
	}
}

// TestLoadKVRejectsMalformed: B-D1. Each case was accepted before the fix;
// the first one then panicked Latest ("slice bounds out of range").
func TestLoadKVRejectsMalformed(t *testing.T) {
	type kv struct {
		k string
		v []Version
	}
	build := func(f string, inline uint64, keys ...kv) []byte {
		e := &enc{}
		e.str(f)
		e.u64(inline)
		e.u64(uint64(len(keys)))
		for _, x := range keys {
			e.str(x.k)
			e.u64(uint64(len(x.v)))
			for _, v := range x.v {
				e.u64(v.LSN)
				e.u64(uint64(v.Off)<<32 | uint64(v.Len))
				e.u64(uint64(v.Op)<<32 | uint64(v.Sum))
				if f == "KV3" {
					e.u64(uint64(v.Chunk))
				}
				e.bytes(v.Val)
				e.str(v.Subject)
				e.bool(v.Del)
			}
		}
		return e.b
	}
	v1 := []Version{{LSN: 1, Val: []byte("x")}}
	good := build("KV2", 4, kv{"kv\x00a", v1}, kv{"kv\x00b", v1})
	if _, err := loadKV(build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2}, {LSN: 2, Op: 1}}}), KVOptions{Inline: 4}, true); err != nil {
		t.Fatalf("one entry writing a key twice rejected: %v", err)
	}
	if _, err := loadKV(good, KVOptions{Inline: 4}, true); err != nil {
		t.Fatalf("well-formed snapshot rejected: %v", err)
	}
	// raw: a one-key snapshot with the given u64 fields after the key,
	// then an empty subject and a false delete flag.
	raw := func(f, k string, u ...uint64) []byte {
		e := &enc{}
		e.str(f)
		e.u64(4)
		e.u64(1)
		e.str(k)
		for _, x := range u {
			e.u64(x)
		}
		e.str("")
		e.bool(false)
		return e.b
	}
	notCanon := func(b []byte, at int, c byte) []byte {
		b = append([]byte{}, b...)
		b[at] = c
		return b
	}
	for name, b := range map[string][]byte{
		"key without separator": build("KV2", 4, kv{"nokeysep", v1}),
		"duplicate key":         build("KV2", 4, kv{"kv\x00a", v1}, kv{"kv\x00a", v1}),
		"keys out of order":     build("KV2", 4, kv{"kv\x00b", v1}, kv{"kv\x00a", v1}),
		"versions out of order": build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2}, {LSN: 1}}}),
		"repeated version":      build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2, Op: 1}, {LSN: 2, Op: 1}}}),
		"ops out of order":      build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2, Op: 1}, {LSN: 2}}}),
		"no versions":           build("KV2", 4, kv{"kv\x00a", nil}),
		"KV3 without chunks":    build("KV3", 4, kv{"kv\x00a", v1}),
		"bool not 0 or 1":       notCanon(good, len(good)-1, 2),
		"inline mismatch":       build("KV2", 5, kv{"kv\x00a", v1}),
		"trailing bytes":        append(append([]byte{}, good...), 0),
		"truncated":             good[:len(good)-1],
		"bad format":            build("KV9", 4),
		"truncated in a u64":    good[:len(good)-5],
		"value past the end":    notCanon(good, len(good)-18, 0x7f),
		"count past the end":    raw("KV2", "kv\x00a", 1<<40),
		"chunk over 32 bits":    raw("KV3", "kv\x00a", 1, 1, 0, 0, 1<<32, ^uint64(0), 0),
	} {
		if _, err := loadKV(b, KVOptions{Inline: 4}, true); err != ErrBadSnapshot {
			t.Errorf("%s: err = %v, want ErrBadSnapshot", name, err)
		}
	}
	// A nil subject marker is not a string.
	e := &enc{}
	e.str("KV2")
	e.u64(4)
	e.u64(1)
	e.u64(^uint64(0))
	if _, err := loadKV(e.b, KVOptions{Inline: 4}, true); err != ErrBadSnapshot {
		t.Errorf("nil key string: err = %v", err)
	}
	// A base may carry another inline limit.
	if _, err := loadKV(build("KV2", 5, kv{"kv\x00a", v1}), KVOptions{Inline: 4}, false); err != nil {
		t.Errorf("base with another inline limit: %v", err)
	}
}

// TestNewKVInlineNegative: a negative inline limit means "never inline".
func TestNewKVInlineNegative(t *testing.T) {
	if got := NewKVInline(-1).Inline(); got != 0 {
		t.Fatalf("Inline() = %d, want 0", got)
	}
}

// FuzzKVRange: arbitrary keys (any bytes, of any length up to 255),
// written, deleted and frozen into runs in an order the input picks, then
// a range read with input bounds in both directions equals the model.
func FuzzKVRange(f *testing.F) {
	f.Add([]byte("\x03a\x01\x02b\x01\x00\x03a\x01\x01"), "a", "", uint8(3))
	f.Add([]byte("\x05\xff\xff\x01\x00\x00\x00\x00"), "", "\xff", uint8(9))
	f.Fuzz(func(t *testing.T, ops []byte, lo, hi string, hot uint8) {
		l, m := NewKVInline(3), newMapKV(3)
		l.hotMax, l.recentMax = 1+int(hot%8), 1+int(hot%3)
		head := uint64(0)
		for len(ops) >= 2 {
			n := int(ops[0])
			if n > len(ops)-2 {
				n = len(ops) - 2
			}
			key, kind := string(ops[1:1+n]), ops[1+n]
			ops = ops[2+n:]
			head++
			op := format.Op{Kind: format.OpPut, Space: "kv", Key: key, Value: []byte{kind}}
			if kind%4 == 0 {
				op = format.Op{Kind: format.OpDel, Space: "kv", Key: key}
			} else if kind%3 == 1 {
				op.Exp = int64(head) + 2 // design 27
			}
			e := &Entry{LSN: head, Ops: []format.Op{op}}
			l.Apply(e, testEnv{})
			m.Apply(e, testEnv{})
			if kind%5 == 0 {
				l.work(-1)
			}
		}
		for _, at := range []uint64{head / 2, head} {
			for _, rev := range []bool{false, true} {
				want := m.Range("kv", lo, hi, at, rev)
				var got []Pair
				l.Range("kv", lo, hi, at, rev, func(k string, v Version) bool { got = append(got, Pair{k, v}); return true })
				if len(got) != len(want) {
					t.Fatalf("Range(%q,%q,%d,%v): %d keys, want %d", lo, hi, at, rev, len(got), len(want))
				}
				for i := range got {
					if got[i].Key != want[i].Key || got[i].LSN != want[i].LSN || got[i].Exp != want[i].Exp {
						t.Fatalf("Range(%q,%q,%d,%v)[%d] = %q@%d, want %q@%d", lo, hi, at, rev, i, got[i].Key, got[i].LSN, want[i].Key, want[i].LSN)
					}
				}
			}
		}
	})
}
