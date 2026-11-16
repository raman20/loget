package loget

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/raman20/loget/index"
)

// The hand-built object keys are byte-identical to the fmt forms they
// replace (segments and chunks already in stores are named that way).
func TestObjectKeysMatchFmt(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	seqs := []uint64{0, 1, 9, 10, 99999, 1<<63 - 1, 1<<64 - 1}
	for range 1000 {
		seqs = append(seqs, r.Uint64()>>r.IntN(64))
	}
	for _, s := range seqs {
		if got, want := segKey("L7", s), fmt.Sprintf("log/%s/%020d.seg", "L7", s); got != want {
			t.Fatalf("segKey(%d) = %q, want %q", s, got, want)
		}
	}
	for _, n := range []int{-1, 0, 7, 999999, 1000000, 1234567890, r.IntN(1 << 30)} {
		if got, want := chunkKey("base/L1/5", n), fmt.Sprintf("%s.%06d.val", "base/L1/5", n); got != want {
			t.Fatalf("chunkKey(%d) = %q, want %q", n, got, want)
		}
	}
}

// locateSpan's span is exact: every LSN in it locates to the same log and
// segment, and the LSNs just outside it do not (or do not locate), over
// random lineages of logs forked at random points.
func TestLocateSpanExact(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for trial := range 200 {
		var l *logState
		next := uint64(1) // the next own LSN of the newest log
		for d := range 1 + r.IntN(4) {
			x := &logState{meta: LogMeta{ID: fmt.Sprintf("L%d", d)}, parent: l}
			if l != nil { // fork somewhere in the parent's frames
				x.meta.ParentLSN = l.meta.ParentLSN + uint64(r.IntN(int(next-l.meta.ParentLSN)))
				next = x.meta.ParentLSN + 1
			}
			for seq := uint64(1); seq <= uint64(1+r.IntN(6)); seq++ {
				n := uint64(1 + r.IntN(5))
				x.segs = append(x.segs, segIdx{seq: seq, first: next, n: n})
				next += n
			}
			l = x
		}
		for lsn := uint64(0); lsn <= next+1; lsn++ {
			x, s, ok := l.locate(lsn)
			if !ok {
				continue
			}
			_, _, lo, hi, _ := l.locateSpan(lsn)
			if lsn < lo || lsn > hi {
				t.Fatalf("trial %d: lsn %d outside its own span [%d, %d]", trial, lsn, lo, hi)
			}
			for q := lo; q <= hi; q++ {
				if y, u, ok := l.locate(q); !ok || y != x || u != s {
					t.Fatalf("trial %d: lsn %d in span [%d, %d] of %d locates elsewhere", trial, q, lo, hi, lsn)
				}
			}
			for _, q := range []uint64{lo - 1, hi + 1} {
				if y, u, ok := l.locate(q); ok && y == x && u == s {
					t.Fatalf("trial %d: span [%d, %d] of lsn %d is not maximal at %d", trial, lo, hi, lsn, q)
				}
			}
			c := newSrcCache(l)
			for _, q := range []uint64{lsn, lo, hi, lsn} {
				y, sq, ok := c.source(index.Version{LSN: q})
				wy, wsq, wok := l.source(index.Version{LSN: q})
				if y != wy || sq != wsq || ok != wok {
					t.Fatalf("trial %d: srcCache.source(%d) = %v %d %v, want %v %d %v", trial, q, y, sq, ok, wy, wsq, wok)
				}
			}
		}
	}
}
