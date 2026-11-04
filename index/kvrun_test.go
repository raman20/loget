package index

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/raman20/loget/format"
	"pgregory.net/rapid"
)

// checkKV compares every read of l with the map model.
func checkKV(t *rapid.T, l *KV, m *mapKV, head uint64, keys []string) {
	checkHot(t, l)
	if got, want := l.encode(), m.encode(); !bytes.Equal(got, want) {
		t.Fatalf("encoding differs:\n got %q\nwant %q", got, want)
	}
	for _, sp := range []string{"kv", "doc:c"} {
		for _, k := range keys {
			for at := uint64(0); at <= head+1; at++ {
				g, gok := l.Get(sp, k, at)
				w, wok := m.Get(sp, k, at)
				if gok != wok || !reflect.DeepEqual(g, w) {
					t.Fatalf("Get(%s,%s,%d) = %+v %v, want %+v %v", sp, k, at, g, gok, w, wok)
				}
			}
			if g, w := l.LastWrite(sp, k), m.LastWrite(sp, k); g != w {
				t.Fatalf("LastWrite(%s,%s) = %d, want %d", sp, k, g, w)
			}
		}
		for _, p := range []string{"", "k", "k1", "k10", "zz"} {
			for _, at := range []uint64{head / 2, head} {
				g, w := l.Scan(sp, p, at), m.Scan(sp, p, at)
				if len(g) != 0 || len(w) != 0 {
					if !reflect.DeepEqual(g, w) {
						t.Fatalf("Scan(%s,%q,%d) = %+v, want %+v", sp, p, at, g, w)
					}
				}
			}
		}
		checkRange(t, l, m, sp, head)
	}
	var got []string
	_ = l.Latest(func(sp, k string, v Version) error {
		got = append(got, fmt.Sprint(sp, "/", k, v.LSN, v.Op))
		return nil
	})
	var want []string
	for _, k := range sortedKeys(m.m) {
		vs := m.m[k]
		i := bytes.IndexByte([]byte(k), 0)
		want = append(want, fmt.Sprint(k[:i], "/", k[i+1:], vs[len(vs)-1].LSN, vs[len(vs)-1].Op))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Latest = %v, want %v", got, want)
	}
}

// rangeBounds are the bounds checkRange tries (the model keys are k0..k23).
var rangeBounds = []string{"", "k", "k1", "k1\x00", "k10", "k15", "k2", "k9", "zz"}

// checkRange compares KV.Range with the model for every pair of bounds,
// both directions and three snapshots, and stopped half way.
func checkRange(t *rapid.T, l *KV, m *mapKV, sp string, head uint64) {
	for _, lo := range rangeBounds {
		for _, hi := range rangeBounds {
			for _, at := range []uint64{0, head / 2, head} {
				for _, rev := range []bool{false, true} {
					want := m.Range(sp, lo, hi, at, rev)
					var got []Pair
					l.Range(sp, lo, hi, at, rev, func(k string, v Version) bool { got = append(got, Pair{k, v}); return true })
					if len(got) != 0 || len(want) != 0 {
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("Range(%s,%q,%q,%d,%v) = %+v, want %+v", sp, lo, hi, at, rev, got, want)
						}
					}
					if n := len(want) / 2; n > 0 {
						got = got[:0]
						l.Range(sp, lo, hi, at, rev, func(k string, v Version) bool { got = append(got, Pair{k, v}); return len(got) < n })
						if !reflect.DeepEqual(got, want[:n]) {
							t.Fatalf("Range(%s,%q,%q,%d,%v) stopped at %d = %+v, want %+v", sp, lo, hi, at, rev, n, got, want[:n])
						}
					}
				}
			}
		}
	}
}

// TestKVRunsRefineMap: with a tiny hot set (so runs are frozen, merged,
// rebuilt and reloaded constantly) the index matches the map model on
// every read and byte for byte in its encoding.
func TestKVRunsRefineMap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) { refineMap(t, KVOptions{Inline: 3}) })
}

// refineMap is TestKVRunsRefineMap for a KV configured by o (kvfile_test.go
// runs it with run files, and with run files that fail).
func refineMap(t *rapid.T, o KVOptions) {
	l, m := NewKVOptions(o), newMapKV(3)
	l.hotMax = rapid.IntRange(1, 12).Draw(t, "hotMax")
	l.recentMax = rapid.IntRange(1, 4).Draw(t, "recentMax")
	defer func(n int) { maxRunBytes = n }(maxRunBytes)
	maxRunBytes = rapid.IntRange(10, 400).Draw(t, "maxRunBytes")
	env := testEnv{}
	var keys []string
	for i := 0; i < 24; i++ {
		keys = append(keys, fmt.Sprint("k", i))
	}
	head := uint64(0)
	n := rapid.IntRange(0, 60).Draw(t, "steps")
	for s := 0; s < n; s++ {
		switch rapid.IntRange(0, 9).Draw(t, "what") {
		case 0: // shred
			sub := rapid.SampledFrom([]string{"a", "b"}).Draw(t, "shred")
			env[sub] = true
			head++
			e := &Entry{LSN: head, Ops: []format.Op{{Kind: format.OpShred, Key: sub}}}
			l.Apply(e, env)
			m.Apply(e, env)
		case 1: // snapshot round trip
			var err error
			if l, err = LoadKVOptions(l.Snapshot(), o); err != nil {
				t.Fatal(err)
			}
			l.hotMax = rapid.IntRange(1, 12).Draw(t, "hotMax")
			l.recentMax = rapid.IntRange(1, 4).Draw(t, "recentMax")
		case 2: // drain or advance the merge in progress
			l.work(rapid.IntRange(-1, 3).Draw(t, "work"))
		default:
			head++
			e := &Entry{LSN: head}
			for j := rapid.IntRange(1, 4).Draw(t, "nops"); j > 0; j-- {
				op := format.Op{Kind: format.OpPut, Space: rapid.SampledFrom([]string{"kv", "doc:c"}).Draw(t, "space"),
					Key: rapid.SampledFrom(keys).Draw(t, "key"), Subject: rapid.SampledFrom([]string{"", "", "a", "b"}).Draw(t, "subj")}
				switch rapid.IntRange(0, 4).Draw(t, "kind") {
				case 0:
					op.Kind, op.Subject = format.OpDel, ""
				case 1:
					op.Value = []byte{} // empty, not nil
				case 2:
					op.Value, op.ValOff = []byte("a long value"), 77 // kept as a position
				default:
					op.Value = []byte(fmt.Sprint(head))
				}
				e.Ops = append(e.Ops, op)
			}
			l.Apply(e, env)
			m.Apply(e, env)
		}
	}
	checkKV(t, l, m, head, keys)
}

// TestKVSetLatestRefineMap: building a base (SetLatest per key, including
// keys already frozen into runs) matches the model.
func TestKVSetLatestRefineMap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) { setLatestRefineMap(t, KVOptions{Inline: 3}) })
}

func setLatestRefineMap(t *rapid.T, o KVOptions) {
	l, m := NewKVOptions(o), newMapKV(3)
	l.hotMax = rapid.IntRange(1, 8).Draw(t, "hotMax")
	l.recentMax = rapid.IntRange(1, 3).Draw(t, "recentMax")
	var keys []string
	for i := 0; i < 12; i++ {
		keys = append(keys, fmt.Sprint("k", i))
	}
	n := rapid.IntRange(0, 40).Draw(t, "steps")
	for s := 0; s < n; s++ {
		k := rapid.SampledFrom(keys).Draw(t, "key")
		v := Version{LSN: uint64(rapid.IntRange(1, 50).Draw(t, "lsn")), Op: uint32(rapid.IntRange(0, 3).Draw(t, "op"))}
		switch rapid.IntRange(0, 2).Draw(t, "kind") {
		case 0:
			v.Chunk, v.Off, v.Len, v.Sum = 1, 9, 100, 5
		case 1:
			v.Val, v.Subject = []byte("x"), "a"
		}
		l.SetLatest("kv", k, v)
		m.SetLatest("kv", k, v)
	}
	checkKV(t, l, m, 50, keys)
	l.DropSubject("a")
	m.DropSubject("a")
	checkKV(t, l, m, 50, keys)
}

// TestKVRunsBounded: a long run of inserts keeps the run count small and
// merges complete.
func TestKVRunsBounded(t *testing.T) {
	l := NewKVInline(64)
	l.hotMax = 64
	e := &Entry{}
	maxRunsSeen := 0
	for i := 0; i < 200000; i++ {
		e.LSN++
		e.Ops = []format.Op{{Kind: format.OpPut, Space: "kv", Key: fmt.Sprintf("%08d", (i*7919)%200000), Value: []byte("v")}}
		l.Apply(e, noEnv{})
		if len(l.runs) > maxRunsSeen {
			maxRunsSeen = len(l.runs)
		}
	}
	if maxRunsSeen > 14 {
		t.Fatalf("%d runs at once", maxRunsSeen)
	}
	t.Logf("max runs %d, now %d", maxRunsSeen, len(l.runs))
	if _, ok := l.Get("kv", "00000000", e.LSN); !ok {
		t.Fatal("lost a key")
	}
}

// TestKVRunBigBlock: an entry more than 64 KiB into its block is found by
// scanning the block (its offset does not fit the slot).
func TestKVRunBigBlock(t *testing.T) {
	l := NewKV()
	l.hotMax = 1 << 30
	big := bytes.Repeat([]byte("x"), 40<<10)
	e := &Entry{LSN: 1}
	for _, k := range []string{"a", "b", "c", "d"} {
		e.Ops = append(e.Ops, format.Op{Kind: format.OpPut, Space: "kv", Key: k, Value: big})
	}
	l.Apply(e, noEnv{})
	l.freeze()
	for _, k := range []string{"a", "b", "c", "d"} {
		if v, ok := l.Get("kv", k, 1); !ok || len(v.Val) != len(big) {
			t.Fatalf("%s: %v %d", k, ok, len(v.Val))
		}
	}
	if _, ok := l.Get("kv", "e", 1); ok {
		t.Fatal("found an absent key")
	}
}

// TestKVRunsForcedMerge: when runs pile up faster than writes advance the
// merges (put alone does no merge work), freeze completes merges at once.
func TestKVRunsForcedMerge(t *testing.T) {
	l, m := NewKV(), newMapKV(math.MaxInt32)
	l.hotMax = 1
	for i := 0; i < 300; i++ {
		v := Version{LSN: uint64(i + 1), Val: []byte{byte(i)}}
		k := ck("kv", fmt.Sprintf("%03d", (i*37)%300))
		l.put(k, v)
		m.m[k] = append(m.m[k], v)
		if len(l.runs) > maxRuns+1 {
			t.Fatalf("%d runs", len(l.runs))
		}
	}
	if !bytes.Equal(l.encode(), m.encode()) {
		t.Fatal("encoding differs")
	}
}

// TestKVManyVersions: a key with hundreds of versions (multi-byte counts)
// survives freezing, merging and a snapshot.
func TestKVManyVersions(t *testing.T) {
	l, m := NewKV(), newMapKV(math.MaxInt32)
	l.hotMax = 50
	for i := 1; i <= 400; i++ {
		e := &Entry{LSN: uint64(i), Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: "k", Value: []byte{byte(i)}}}}
		l.Apply(e, noEnv{})
		m.Apply(e, noEnv{})
	}
	l.work(-1)
	l2, err := LoadKV(l.Snapshot(), math.MaxInt32)
	if err != nil || !bytes.Equal(l2.encode(), m.encode()) || !bytes.Equal(l.encode(), m.encode()) {
		t.Fatal("encoding differs", err)
	}
	if v, ok := l2.Get("kv", "k", 200); !ok || v.Val[0] != 200 {
		t.Fatal("version 200", v, ok)
	}
}

// TestKVLatestError: Latest stops at fn's first error.
func TestKVLatestError(t *testing.T) {
	l := kvSeed(true)
	l.freeze()
	stop, n := errors.New("stop"), 0
	if err := l.Latest(func(string, string, Version) error { n++; return stop }); err != stop || n != 1 {
		t.Fatal(err, n)
	}
}

func checkHot(t interface{ Fatalf(string, ...any) }, l *KV) {
	n := 0
	for _, vs := range l.m {
		n += vs.n()
	}
	if n != l.hot || len(l.keys)+len(l.recent) != len(l.m) {
		t.Fatalf("hot set: %d versions counted, %d held; %d+%d keys listed, %d held", l.hot, n, len(l.keys), len(l.recent), len(l.m))
	}
}

// TestKVRunOddVersions: versions only a decoded snapshot can hold (each
// field set alone, a delete that kept its value) survive a run exactly,
// and DropSubject still redacts them.
func TestKVRunOddVersions(t *testing.T) {
	odd := []Version{
		{LSN: 1, Sum: 7}, {LSN: 2, Len: 3}, {LSN: 3, Off: 4}, {LSN: 4, Chunk: 5},
		{LSN: 5, Del: true, Val: []byte("v"), Subject: "s"}, {LSN: 6, Del: true, Off: 1, Subject: "s"},
		{LSN: 7, Del: true, Subject: "s"}, {LSN: 8, Val: []byte{}}, {LSN: 9, Op: 300},
	}
	for i, v := range odd {
		l, m := NewKV(), newMapKV(math.MaxInt32)
		k := ck("kv", fmt.Sprint(i))
		l.put(k, v)
		m.m[k] = []Version{v}
		l.freeze()
		if !bytes.Equal(l.encode(), m.encode()) {
			t.Fatalf("%+v: encoding differs", v)
		}
		l.DropSubject("s")
		m.DropSubject("s")
		if !bytes.Equal(l.encode(), m.encode()) {
			t.Fatalf("%+v: encoding differs after DropSubject", v)
		}
	}
}

// TestKVRunsRefineMapParallelFill is TestKVRunsRefineMap with every run's
// table filled by the parallel path (design 25), tiny batches and tiny
// regions, so set-aside entries and wraparound are common.
func TestKVRunsRefineMapParallelFill(t *testing.T) {
	defer func(a, b int) { parFillMin, fillBatch = a, b }(parFillMin, fillBatch)
	parFillMin = 1
	rapid.Check(t, func(t *rapid.T) {
		fillBatch = rapid.IntRange(1, 7).Draw(t, "fillBatch")
		refineMap(t, KVOptions{Inline: 3})
	})
}

// TestParallelFillSameTable: the parallel fill of a large run gives the
// same table every time and every key is found.
func TestParallelFillSameTable(t *testing.T) {
	l := NewKVOptions(KVOptions{})
	env := testEnv{}
	const n = 300000
	for i := 0; i < n; i++ {
		l.Apply(&Entry{LSN: uint64(i + 1), Ops: []format.Op{{Kind: format.OpPut, Space: "kv", Key: fmt.Sprint("key", i), Value: []byte("v")}}}, env)
	}
	l.work(-1)
	var big *run
	for _, r := range l.runs {
		if big == nil || r.n > big.n {
			big = r
		}
	}
	if big == nil || big.n < parFillMin {
		t.Skipf("largest run too small for the parallel path")
	}
	want := append([]uint64(nil), big.slots...)
	conc := func(tasks []func()) {
		done := make(chan bool)
		for _, f := range tasks {
			go func() { f(); done <- true }()
		}
		for range tasks {
			<-done
		}
	}
	for i := 0; i < 3; i++ {
		for _, par := range []func([]func()){nil, conc} {
			clear(big.slots)
			big.fillSlots(nil, par)
			if !slices.Equal(big.slots, want) {
				t.Fatal("parallel fill is not deterministic")
			}
		}
	}
	for i := 0; i < n; i += 997 {
		if _, ok := l.Get("kv", fmt.Sprint("key", i), ^uint64(0)); !ok {
			t.Fatalf("key%d not found", i)
		}
	}
}
