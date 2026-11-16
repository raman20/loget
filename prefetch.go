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
	obj    pfObj
	off, n int64
	sum    uint32
}

// pfObj names the object holding a value without building its key: a
// segment (seq != 0) of log x, or a compaction-base value chunk (seq 0).
// The key string is made once per range read, not once per value.
type pfObj struct {
	x     *logState
	seq   uint64
	chunk uint32
}

func (o pfObj) less(p pfObj) bool {
	if o.x != p.x {
		return o.x.meta.ID < p.x.meta.ID
	}
	if o.seq != p.seq {
		return o.seq < p.seq
	}
	return o.chunk < p.chunk
}

// srcCache resolves the log and segment of many nearby versions (a page of
// keys loaded together shares a few segments): logState.source with the
// last segment's LSN span remembered, so most lookups take no lock and no
// binary search.
type srcCache struct {
	l      *logState
	based  *logState // l.based(), resolved once
	x      *logState // last segment located, valid for LSNs in [lo, hi]
	seq    uint64
	lo, hi uint64
	ok     bool
}

func newSrcCache(l *logState) srcCache { return srcCache{l: l, based: l.based()} }

// source is l.source(ver).
func (c *srcCache) source(ver index.Version) (*logState, uint64, bool) {
	if b := c.based; b != nil && (ver.Chunk != 0 || ver.LSN <= b.meta.Base.LSN) {
		return b, 0, true
	}
	if !c.ok || ver.LSN < c.lo || ver.LSN > c.hi {
		x, s, lo, hi, ok := c.l.locateSpan(ver.LSN)
		if !ok {
			return nil, 0, false
		}
		c.x, c.seq, c.lo, c.hi, c.ok = x, s.seq, lo, hi, true
	}
	return c.x, c.seq, true
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

// pfObjOf is where ver is stored (see valuePos), given its source log x
// and segment seq.
func pfObjOf(x *logState, seq uint64, ver index.Version) pfObj {
	if ver.Chunk != 0 {
		return pfObj{x: x, chunk: ver.Chunk}
	}
	return pfObj{x: x, seq: seq}
}

// key is the object's store key: the same as valuePos returns.
func (db *DB) pfKey(o pfObj, ver index.Version) string {
	_, k := db.valuePos(o.x, o.seq, ver)
	return k
}

// pfScratch is a caller's reusable prefetch working memory (an iterator
// prefetches one page at a time).
type pfScratch struct {
	rs     []pfReq
	groups [][]pfReq
}

// prefetch fills vals[i] with the stored bytes of page[i] where that is
// cheap to batch; vals[i] stays nil otherwise. vals has len(page). scr is
// working memory, reused across calls (nil: allocate).
func (db *DB) prefetch(ctx context.Context, l *logState, page []index.Pair, vals [][]byte, scr *pfScratch) {
	if scr == nil {
		scr = &pfScratch{}
	}
	defer func() { // keep the buffers, not what they point at
		clear(scr.rs)
		clear(scr.groups)
	}()
	rs := scr.rs[:0]
	sc := newSrcCache(l)
	for i := range page {
		v := &page[i].Version
		if !v.Ref() || v.Len > prefetchMax {
			continue
		}
		x, seq, ok := sc.source(*v)
		if !ok {
			continue
		}
		rs = append(rs, pfReq{i: i, obj: pfObjOf(x, seq, *v), off: int64(v.Off), n: int64(v.Len), sum: v.Sum})
	}
	scr.rs = rs
	if len(rs) < 2 {
		return
	}
	sorted := true // pages loaded in log order are already sorted
	for j := 1; j < len(rs) && sorted; j++ {
		a, b := &rs[j-1], &rs[j]
		sorted = a.obj == b.obj && a.off <= b.off || a.obj != b.obj && a.obj.less(b.obj)
	}
	if !sorted {
		sort.Slice(rs, func(a, b int) bool {
			if rs[a].obj != rs[b].obj {
				return rs[a].obj.less(rs[b].obj)
			}
			return rs[a].off < rs[b].off
		})
	}
	groups := scr.groups[:0]
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
	scr.groups = groups
	read := func(g []pfReq) {
		lo := g[0].off
		hi := lo
		for _, r := range g {
			hi = max(hi, r.off+r.n)
		}
		b, err := store.GetRange(ctx, db.st, db.pfKey(g[0].obj, page[g[0].i].Version), lo, hi-lo)
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
