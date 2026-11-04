package loget

import (
	"context"
	"hash/crc32"
	"sort"
	"sync"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Value read batching for iterators (design 28, part b). A page of keys
// whose values live in the log or a base chunk is read with one range read
// per run of nearby values in the same object, instead of one per value:
// keys written in key order (bulk loads, sequential ids) sit next to each
// other in segments. Reads are checked against each value's CRC as in
// readValue; anything that does not check out, or fails to read, is left
// for the one-value path (which retries and reports the error), so this
// changes speed only.
const (
	prefetchGap  = 4 << 10  // join two values if at most this many bytes lie between
	prefetchSpan = 1 << 20  // largest single range read
	prefetchMax  = 64 << 10 // values above this are read alone
	prefetchPar  = 4        // groups read concurrently
)

type pfReq struct {
	i      int // page index
	obj    string
	off, n int64
	sum    uint32
}

// valuePos returns where a referenced value is stored.
func (db *DB) valuePos(x *logState, seq uint64, ver index.Version) (vcKey, string) {
	if ver.Chunk != 0 { // in a compaction base's value chunk (x is the based log)
		b := x.meta.Base
		if b.Refs != nil && int(ver.Chunk) <= len(b.Refs) {
			// named by the object, so cached values survive a compaction
			// that shares the chunk
			r := b.Refs[ver.Chunk-1]
			return vcKey{r.Key, 1<<63 | uint64(r.N+1), ver.Off}, r.obj()
		}
		return vcKey{b.Key, 1<<63 | uint64(ver.Chunk), ver.Off}, chunkKey(b.Key, int(ver.Chunk-1))
	}
	return vcKey{x.meta.ID, seq, ver.Off}, segKey(x.meta.ID, seq)
}

// prefetch fills vals[i] with the stored bytes of page[i] where that is
// cheap to batch; vals[i] stays nil otherwise. vals has len(page).
func (db *DB) prefetch(ctx context.Context, l *logState, page []index.Pair, vals [][]byte) {
	var rs []pfReq
	for i := range page {
		v := &page[i].Version
		if !v.Ref() || v.Len > prefetchMax {
			continue
		}
		x, seq, ok := l.source(*v)
		if !ok {
			continue
		}
		_, obj := db.valuePos(x, seq, *v)
		rs = append(rs, pfReq{i: i, obj: obj, off: int64(v.Off), n: int64(v.Len), sum: v.Sum})
	}
	if len(rs) < 2 {
		return
	}
	sort.Slice(rs, func(a, b int) bool {
		if rs[a].obj != rs[b].obj {
			return rs[a].obj < rs[b].obj
		}
		return rs[a].off < rs[b].off
	})
	var groups [][]pfReq
	start := 0
	for j := 1; j <= len(rs); j++ {
		if j < len(rs) && rs[j].obj == rs[start].obj &&
			rs[j].off <= rs[j-1].off+rs[j-1].n+prefetchGap &&
			rs[j].off+rs[j].n-rs[start].off <= prefetchSpan {
			continue
		}
		if j-start > 1 { // a lone value gains nothing from batching
			groups = append(groups, rs[start:j])
		}
		start = j
	}
	read := func(g []pfReq) {
		lo := g[0].off
		hi := lo
		for _, r := range g {
			hi = max(hi, r.off+r.n)
		}
		b, err := store.GetRange(ctx, db.st, g[0].obj, lo, hi-lo)
		if err != nil || int64(len(b)) != hi-lo {
			return
		}
		for _, r := range g {
			v := b[r.off-lo : r.off-lo+r.n]
			if crc32.Checksum(v, index.Castagnoli) == r.sum {
				vals[r.i] = append([]byte(nil), v...)
			}
		}
	}
	if len(groups) == 1 {
		read(groups[0])
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, prefetchPar)
	for _, g := range groups {
		wg.Add(1)
		sem <- struct{}{}
		go func(g []pfReq) {
			defer func() { <-sem; wg.Done() }()
			read(g)
		}(g)
	}
	wg.Wait()
}
