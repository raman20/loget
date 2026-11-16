package loget

import (
	"fmt"
	"sort"

	"github.com/raman20/loget/index"
)

// SegMeta is the stored form of one retained history segment (design 30):
// a segment below the KV pointer base that is still in the store.
type SegMeta struct {
	Seq   uint64 `json:"seq"`
	First uint64 `json:"first"`
	N     uint64 `json:"n"`
}

func segsFromMeta(in []SegMeta) []segIdx {
	out := make([]segIdx, len(in))
	for i, s := range in {
		out[i] = segIdx{seq: s.Seq, first: s.First, n: s.N}
	}
	return out
}

func metaFromSegs(in []segIdx) []SegMeta {
	out := make([]SegMeta, len(in))
	for i, s := range in {
		out[i] = SegMeta{Seq: s.seq, First: s.first, N: s.n}
	}
	return out
}

// baseLSN is the KV pointer-base LSN (0 if none). This is not the
// history floor: retained segments may make earlier LSNs foldable.
func (l *logState) baseLSN() uint64 {
	if b := l.based(); b != nil {
		return b.meta.Base.LSN
	}
	return 0
}

// historyFloor is the user-facing bound for time travel: history at or
// below it was deleted by log GC, or a format-4 compact already swept
// it. 0 means the segment prefix from the start (or ParentLSN) is
// complete, so any LSN up to the head can be folded. A pointer-only
// compact of a never-swept log leaves the floor at 0. A compact of a
// format-4 log that already dropped its prefix keeps the floor at the
// new pointer base: mid-range views would need the previous pointer
// base, which v1 does not keep.
func (l *logState) historyFloor() uint64 {
	if l.completePrefix() {
		return 0
	}
	if b := l.baseLSN(); b != 0 {
		return b
	}
	return 0
}

// floor is the user-facing bound for time travel, branches and snapshots.
func (l *logState) floor() uint64 { return l.historyFloor() }

// completePrefix reports whether retained history plus the tail cover
// every LSN from the lineage start through the pointer base (or the
// head, if there is no base).
func (l *logState) completePrefix() bool {
	target := l.baseLSN()
	if target == 0 {
		l.mu.RLock()
		target = l.meta.ParentLSN + l.n
		l.mu.RUnlock()
	}
	if target == 0 {
		return true
	}
	return l.coveredThrough(target)
}

// canFold reports whether a view at upto can be built. A pointer-base
// plus tail always serves upto >= the base. Below the base, every
// segment from the lineage start through upto must still be present.
func (l *logState) canFold(upto uint64) bool {
	if upto == 0 {
		return true
	}
	if b := l.baseLSN(); b != 0 && upto >= b {
		return true
	}
	return l.coveredThrough(upto)
}

func (l *logState) coveredThrough(upto uint64) bool {
	if upto == 0 {
		return true
	}
	// LSNs at or below the fork live in the parent. A child with no
	// own frames must not vacuously report that those LSNs are covered
	// (want=ParentLSN+1 > upto was true and ViewAt built an empty view
	// instead of ErrCompacted).
	if upto <= l.meta.ParentLSN {
		return l.parent != nil && l.parent.coveredThrough(upto)
	}
	if l.meta.ParentLSN > 0 && (l.parent == nil || !l.parent.coveredThrough(l.meta.ParentLSN)) {
		return false
	}
	want := l.meta.ParentLSN + 1
	for _, s := range l.foldSegs() {
		if s.n == 0 {
			continue
		}
		if s.first > want {
			return false
		}
		if end := s.first + s.n; end > want {
			want = end
		}
		if want > upto {
			return true
		}
	}
	return want > upto
}

// foldSegs is retained history (below the pointer base) followed by the
// recoverable tail. Caller may use the result after the lock is dropped:
// both slices are immutable after load / addSeg (addSeg only appends to
// the tail).
func (l *logState) foldSegs() []segIdx {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.retained) == 0 {
		return l.segs
	}
	out := make([]segIdx, len(l.retained)+len(l.segs))
	copy(out, l.retained)
	copy(out[len(l.retained):], l.segs)
	return out
}

func lookupSegs(segs []segIdx, lsn uint64) (segIdx, bool) {
	i := sort.Search(len(segs), func(i int) bool { return segs[i].first+segs[i].n > lsn })
	if i < len(segs) && segs[i].first <= lsn {
		return segs[i], true
	}
	return segIdx{}, false
}

// coveredSegs lists every history segment this log already holds that
// the new pointer base at seq will cover (seq is the first tail
// segment, i.e. Base.Seq).
func (l *logState) coveredSegs(seq uint64) []segIdx {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []segIdx
	seen := map[uint64]bool{}
	add := func(s segIdx) {
		if s.seq > 0 && s.seq < seq && !seen[s.seq] {
			seen[s.seq] = true
			out = append(out, s)
		}
	}
	for _, s := range l.retained {
		add(s)
	}
	for _, s := range l.segs {
		add(s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

func filterLiveSegs(all []segIdx, live map[string]struct{}, logID string) (keep []segIdx, drop []uint64) {
	for _, s := range all {
		if _, ok := live[segKey(logID, s.seq)]; ok {
			keep = append(keep, s)
		} else {
			drop = append(drop, s.seq)
		}
	}
	return keep, drop
}

func (db *DB) objectOf(l *logState, ver index.Version) string {
	if !ver.Ref() {
		return ""
	}
	x, seq, ok := l.source(ver)
	if !ok {
		return ""
	}
	if ver.Chunk != 0 {
		_, obj := db.valuePos(x, 0, ver)
		return obj
	}
	return segKey(x.meta.ID, seq)
}

func (db *DB) addLiveFromIndex(live map[string]struct{}, l *logState, idx index.Index) {
	if kv, ok := idx.(*index.KV); ok {
		db.addLiveFromLatest(live, l, kv)
		return
	}
	r, ok := idx.(index.Referencer)
	if !ok {
		return
	}
	r.EachRef(func(ver index.Version) {
		if obj := db.objectOf(l, ver); obj != "" {
			live[obj] = struct{}{}
		}
	})
}

// addLiveFromLatest pins objects named by the latest (non-deleted,
// non-expired) version of each key. Historical MVCC versions must not
// pin segments: those are exactly what DropHistory exists to collect.
func (db *DB) addLiveFromLatest(live map[string]struct{}, l *logState, kv *index.KV) {
	_ = kv.Latest(func(_, _ string, ver index.Version) error {
		if ver.Del {
			return nil
		}
		if obj := db.objectOf(l, ver); obj != "" {
			live[obj] = struct{}{}
		}
		return nil
	})
}

func addLiveTail(live map[string]struct{}, l *logState, seq uint64) {
	// Every segment at or after the new pointer base stays, including
	// retained history now in the keep window (not just the old tail).
	l.mu.RLock()
	defer l.mu.RUnlock()
	add := func(s segIdx) {
		if s.seq >= seq {
			live[segKey(l.meta.ID, s.seq)] = struct{}{}
		}
	}
	for _, s := range l.retained {
		add(s)
	}
	for _, s := range l.segs {
		add(s)
	}
}

// retentionSatisfied reports whether the current pointer base already
// leaves at least o.KeepCommits and o.KeepFor of history above it, so a
// retain sweep may drop unreferenced segs below that base. If the base
// is still at the head (or too close to it), a new cut must be written
// first; sweeping against the head live set would delete the window.
func (l *logState) retentionSatisfied(o GCOptions, now int64) bool {
	b := l.baseLSN()
	if b == 0 {
		return false
	}
	head := l.head()
	if o.KeepCommits > 0 && (head <= o.KeepCommits || head-b < o.KeepCommits) {
		return false
	}
	if o.KeepFor > 0 {
		if bm := l.meta.Base; bm == nil || bm.TS > now-int64(o.KeepFor) {
			return false
		}
	}
	return true
}

func validateRetain(r []SegMeta) error {
	var prev uint64
	for i, s := range r {
		if s.Seq == 0 || s.First == 0 || s.N == 0 {
			return fmt.Errorf("retain[%d]: seq %d first %d n %d", i, s.Seq, s.First, s.N)
		}
		if i > 0 && s.Seq <= prev {
			return fmt.Errorf("retain: seqs not strictly increasing")
		}
		prev = s.Seq
	}
	return nil
}

// completeRetainMeta reports whether m.Retain covers every LSN from the
// lineage start through the pointer base, using only manifest metadata.
func completeRetainMeta(m *LogMeta) bool {
	if m == nil || m.Base == nil || len(m.Retain) == 0 {
		return false
	}
	want := m.ParentLSN + 1
	for _, s := range m.Retain {
		if s.N == 0 {
			continue
		}
		if s.First > want {
			return false
		}
		if end := s.First + s.N; end > want {
			want = end
		}
		if want > m.Base.LSN {
			return true
		}
	}
	return want > m.Base.LSN
}
