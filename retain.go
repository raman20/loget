package loget

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

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
	// Lowest pointer base in the inherited range from which the head
	// is still continuously foldable. An earlier ancestor cut does
	// not lower the floor when mid-history was dropped (Rewind/ViewAt
	// in the hole must be ErrCompacted, not a dangling ref).
	head := l.head()
	var floor uint64
	found := false
	limit := head
	if limit == 0 {
		limit = math.MaxUint64
	}
	for x := l; x != nil; x = x.parent {
		if x.meta.Base != nil {
			if b := x.meta.Base.LSN; b > 0 && b <= limit && l.coveredFrom(b, head) {
				if !found || b < floor {
					floor, found = b, true
				}
			}
		}
		if x.meta.ParentLSN == 0 {
			break
		}
		limit = min(limit, x.meta.ParentLSN)
	}
	if found {
		return floor
	}
	if b := l.baseLSN(); b != 0 {
		return b
	}
	return 0
}

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
	if l.foldStart(upto) != nil {
		return true
	}
	return l.coveredThrough(upto)
}

// foldStart is the log whose pointer base should start a view at upto:
// the newest pointer base in the lineage with Base.LSN <= upto that
// sits in this log's inherited range, and whose following segments
// through upto are still present. A child's own later base is ignored
// for a cut below it: the parent's cut base is the fold at that LSN
// (inlined or as refs) even if the cut's own segment was collected.
func (l *logState) foldStart(upto uint64) *logState {
	return l.foldStartWith(upto, nil)
}

// plannedBase is the pointer base to use for this log: a base this
// commit is writing replaces the current one (the old image will be
// garbage).
func (l *logState) plannedBase(bases map[string]*BaseMeta) *BaseMeta {
	if bases != nil {
		if b, ok := bases[l.meta.ID]; ok {
			return b
		}
	}
	return l.meta.Base
}

// foldStartWith is foldStart honouring planned bases (needed / gcReach
// during compactLocked). A parent base being moved past a child's fork
// is not a legal start for that child.
func (l *logState) foldStartWith(upto uint64, bases map[string]*BaseMeta) *logState {
	if upto == 0 {
		return nil
	}
	var best *logState
	var bestLSN uint64
	limit := upto
	for x := l; x != nil; x = x.parent {
		if b := x.plannedBase(bases); b != nil {
			if bl := b.LSN; bl > 0 && bl <= limit && l.coveredFrom(bl, upto) && (best == nil || bl >= bestLSN) {
				best, bestLSN = x, bl
			}
		}
		if x.meta.ParentLSN == 0 {
			break
		}
		limit = min(limit, x.meta.ParentLSN)
	}
	return best
}

// coveredFrom reports whether every LSN in (after, upto] still exists
// in this lineage (after itself is a pointer base and need not be a
// live segment).
func (l *logState) coveredFrom(after, upto uint64) bool {
	if upto <= after {
		return true
	}
	if l.meta.ParentLSN > 0 && after < l.meta.ParentLSN {
		if l.parent == nil || !l.parent.coveredFrom(after, min(upto, l.meta.ParentLSN)) {
			return false
		}
		after = min(upto, l.meta.ParentLSN)
		if upto <= after {
			return true
		}
	}
	want := after + 1
	if want <= l.meta.ParentLSN {
		want = l.meta.ParentLSN + 1
	}
	for _, s := range l.foldSegs() {
		if s.n == 0 {
			continue
		}
		if s.first+s.n-1 <= after {
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
	if ver.Ref() {
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
	// Inlined values (plain or subject) are self-describing in the
	// pointer image: subject ciphertext carries Version.Log so AAD
	// does not need the sealing segment. They do not pin it.
	return ""
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

// gcNewBase is a pointer base this compaction is committing.
type gcNewBase struct {
	id   string
	base *BaseMeta
	live map[string]struct{} // image refs + tail, as writeBase built them
	refs map[string]struct{} // image refs only (needed of a descendant using this base)
}

// gcReach is the single GC liveness set, the Go form of LogEtLogGCFork
// Live: needed(manifest) ∪ live Snapshots ∪ retention windows.
// needed is the set of objects Open / recover / ViewAt(head) will read
// for each kept log (pointer-base image refs plus the continuous fold
// prefix after that base). A planned drop is cancelled if the log is
// named by that set, or if it is an ancestor of an unbased kept log
// (loadLog and manifest validate walk that chain).
func (db *DB) gcReach(ctx context.Context, drop []string, newBases []gcNewBase, o GCOptions, now, snapMaxAge int64) (live map[string]struct{}, stillDrop []string, err error) {
	newBy := map[string]gcNewBase{}
	for _, b := range newBases {
		newBy[b.id] = b
	}
	gone := map[string]bool{}
	for _, id := range drop {
		gone[id] = true
	}
	based := func(id string, lm *LogMeta) bool {
		if lm != nil && lm.Base != nil {
			return true
		}
		_, ok := newBy[id]
		return ok
	}

	live = map[string]struct{}{}
	for round := 0; ; round++ {
		if round > 2*len(db.man.Logs)+2 {
			return nil, nil, fmt.Errorf("%w: gc reach does not converge", ErrCorrupt)
		}
		// Unbased kept logs need their parent chain.
		changed := false
		for id, lm := range db.man.Logs {
			if gone[id] || lm == nil {
				continue
			}
			cur, cid := lm, id
			for !based(cid, cur) && cur.Parent != "" {
				if gone[cur.Parent] {
					delete(gone, cur.Parent)
					changed = true
				}
				cid, cur = cur.Parent, db.man.Logs[cur.Parent]
				if cur == nil {
					break
				}
			}
		}
		// Recompute live from the current kept set.
		live = map[string]struct{}{}
		for _, id := range sortedMapKeys(db.man.Logs) {
			if gone[id] {
				continue
			}
			if err := db.addLiveFromKept(ctx, live, id, newBy, o); err != nil {
				return nil, nil, err
			}
		}
		if err := db.addLiveFromSnapshots(ctx, live, now, snapMaxAge, gone); err != nil {
			return nil, nil, err
		}
		// A live object may name a log we planned to drop.
		for k := range live {
			id, ok := liveLogID(k)
			if !ok || !gone[id] {
				continue
			}
			if _, ok := db.man.Logs[id]; ok {
				delete(gone, id)
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	stillDrop = drop[:0]
	for _, id := range drop {
		if gone[id] {
			stillDrop = append(stillDrop, id)
		}
	}
	return live, stillDrop, nil
}

func liveLogID(obj string) (string, bool) {
	const p = "log/"
	if !strings.HasPrefix(obj, p) {
		return "", false
	}
	rest := obj[len(p):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return "", false
	}
	return rest[:i], true
}

func (db *DB) addLiveFromKept(ctx context.Context, live map[string]struct{}, id string, newBy map[string]gcNewBase, o GCOptions) error {
	l, err := db.loadLog(ctx, id, false)
	if err != nil || l == nil {
		return err
	}
	if err := db.addNeeded(ctx, live, l, l.head(), plannedBases(newBy), newBy); err != nil {
		return err
	}
	// A child's keep window may extend below its own base into an
	// ancestor (LogEtLogGCFork: tail of the child lineage). Pin those
	// segments even if the parent cut could not be rewritten.
	// KeepFor is already expressed as a written cut + tail (needed /
	// b.live); do not pin [0, head] or the time window never shrinks.
	if o.KeepCommits > 0 {
		from := uint64(0)
		if l.head() > o.KeepCommits {
			from = l.head() - o.KeepCommits
		}
		addLiveWindow(live, l, from, l.head())
	}
	return nil
}

func plannedBases(newBy map[string]gcNewBase) map[string]*BaseMeta {
	if len(newBy) == 0 {
		return nil
	}
	bases := make(map[string]*BaseMeta, len(newBy))
	for id, b := range newBy {
		bases[id] = b.base
	}
	return bases
}

// needed is the set of objects Open / recover / ViewAt(head) will read
// for every kept log. gcReach is this set ∪ live Snapshots ∪ retention
// windows. newBy honours pointer bases this commit is writing.
func (db *DB) needed(ctx context.Context, newBy map[string]gcNewBase, gone map[string]bool) (map[string]struct{}, error) {
	live := map[string]struct{}{}
	bases := plannedBases(newBy)
	for _, id := range sortedMapKeys(db.man.Logs) {
		if gone[id] {
			continue
		}
		l, err := db.loadLog(ctx, id, false)
		if err != nil || l == nil {
			return nil, err
		}
		if err := db.addNeeded(ctx, live, l, l.head(), bases, newBy); err != nil {
			return nil, err
		}
	}
	return live, nil
}

// addNeeded pins the objects Open / recover / ViewAt(upto) will read
// for l: the fold-start pointer-base image's Ref() objects, plus every
// lineage segment overlapping (baseLSN, upto], or the prefix through
// upto if there is no usable base. A log getting a new base at upto
// contributes exactly writeBase's live set (image refs + tail after
// the new base), not the prefix the new base is about to drop.
func (db *DB) addNeeded(ctx context.Context, live map[string]struct{}, l *logState, upto uint64, bases map[string]*BaseMeta, newBy map[string]gcNewBase) error {
	if upto == 0 {
		return nil
	}
	var recoverFrom uint64
	if b, ok := newBy[l.meta.ID]; ok && b.base != nil && b.base.LSN == upto {
		for k := range b.live {
			live[k] = struct{}{}
		}
		recoverFrom = b.base.Seq
	} else {
		start := l.foldStartWith(upto, bases)
		after := uint64(0)
		if start != nil {
			bm := start.plannedBase(bases)
			after = bm.LSN
			if err := db.addLiveFromBaseImage(ctx, live, start, bm, newBy); err != nil {
				return err
			}
			if start.meta.ID == l.meta.ID {
				recoverFrom = bm.Seq
			}
		}
		if after < upto {
			addLiveWindow(live, l, after+1, upto)
		}
		if recoverFrom == 0 {
			if bm := l.plannedBase(bases); bm != nil && bm.Seq != 0 {
				recoverFrom = bm.Seq
			} else {
				l.mu.RLock()
				recoverFrom = l.nextSeq
				l.mu.RUnlock()
			}
		}
	}
	// Recover starts at Base.Seq (or nextSeq) and GETs until the first
	// hole, including empty fence segments that never enter foldSegs.
	if err := db.addLiveRecoverTail(ctx, live, l, recoverFrom); err != nil {
		return err
	}
	return nil
}

func (db *DB) addLiveRecoverTail(ctx context.Context, live map[string]struct{}, l *logState, from uint64) error {
	if from == 0 {
		from = 1
	}
	keys, err := db.st.List(ctx, "log/"+l.meta.ID+"/")
	if err != nil {
		return err
	}
	for _, k := range keys {
		if s, ok := parseNum(k, "log/"+l.meta.ID+"/", ".seg"); ok && s >= from {
			live[k] = struct{}{}
		}
	}
	return nil
}

func (db *DB) addLiveFromBaseImage(ctx context.Context, live map[string]struct{}, l *logState, bm *BaseMeta, newBy map[string]gcNewBase) error {
	if bm == nil {
		return nil
	}
	if b, ok := newBy[l.meta.ID]; ok && b.base != nil && b.base.LSN == bm.LSN {
		for k := range b.refs {
			live[k] = struct{}{}
		}
		return nil
	}
	img, err := db.loadBase(ctx, l)
	if err != nil {
		return err
	}
	if img != nil && img.kv != nil {
		db.addLiveFromLatest(live, l, img.kv)
	}
	return nil
}

// neededSegsHeld reports whether every log/*.seg in needed(manifest)
// is still in some kept log's foldSegs (not already swept). Used as
// the CompactP guard: a new parent base must not make Open need a
// segment a previous GC already dropped.
func (db *DB) neededSegsHeld(ctx context.Context, newBy map[string]gcNewBase, gone map[string]bool) bool {
	live, err := db.needed(ctx, newBy, gone)
	if err != nil {
		return false
	}
	for obj := range live {
		if !strings.HasPrefix(obj, "log/") || !strings.HasSuffix(obj, ".seg") {
			continue
		}
		if !db.segHeld(ctx, obj) {
			return false
		}
	}
	return true
}

func (db *DB) segHeld(ctx context.Context, obj string) bool {
	id, ok := liveLogID(obj)
	if !ok {
		return false
	}
	seq, ok := parseNum(obj, "log/"+id+"/", ".seg")
	if !ok {
		return false
	}
	l, err := db.loadLog(ctx, id, false)
	if err != nil || l == nil {
		return false
	}
	for _, s := range l.foldSegs() {
		if s.seq == seq {
			return true
		}
	}
	return false
}

// rebaseLeavesOpen is the CompactP guard for one planned base: installing
// it must not make a kept log unopenable that is openable without it.
func (db *DB) rebaseLeavesOpen(ctx context.Context, id string, base *BaseMeta, others map[string]*BaseMeta, gone map[string]bool) bool {
	with := make(map[string]*BaseMeta, len(others)+1)
	for k, v := range others {
		with[k] = v
	}
	with[id] = base
	for _, cid := range sortedMapKeys(db.man.Logs) {
		if gone[cid] {
			continue
		}
		l, err := db.loadLog(ctx, cid, false)
		if err != nil || l == nil {
			return false
		}
		head := l.head()
		okWithout := l.foldStartWith(head, others) != nil || l.coveredThrough(head)
		okWith := l.foldStartWith(head, with) != nil || l.coveredThrough(head)
		if okWithout && !okWith {
			return false
		}
	}
	return true
}

// addLiveWindow pins every segment in this lineage that overlaps
// [from, to], walking parents for inherited LSNs.
func addLiveWindow(live map[string]struct{}, l *logState, from, to uint64) {
	if to < from {
		return
	}
	for x := l; x != nil; x = x.parent {
		for _, s := range x.foldSegs() {
			if s.n == 0 {
				continue
			}
			end := s.first + s.n - 1
			if end < from || s.first > to {
				continue
			}
			live[segKey(x.meta.ID, s.seq)] = struct{}{}
		}
		if x.meta.ParentLSN == 0 || x.meta.ParentLSN < from {
			break
		}
	}
}

func (db *DB) addLiveFromSnapshots(ctx context.Context, live map[string]struct{}, now, maxAge int64, gone map[string]bool) error {
	type pin struct {
		at   uint64
		logs []string
	}
	var pins []pin
	db.snaps.mu.Lock()
	for _, s := range db.snaps.live {
		if maxAge > 0 && now-s.created > maxAge {
			continue
		}
		pins = append(pins, pin{at: s.at, logs: append([]string(nil), s.logs...)})
	}
	db.snaps.mu.Unlock()
	for _, p := range pins {
		if len(p.logs) == 0 {
			continue
		}
		id := p.logs[0]
		if gone[id] {
			continue
		}
		l, err := db.loadLog(ctx, id, false)
		if err != nil || l == nil {
			return err
		}
		if p.at > 0 && !l.canFold(p.at) {
			continue
		}
		v, err := db.buildView(ctx, l, p.at, nil)
		if err != nil {
			return err
		}
		v.mu.RLock()
		db.addLiveFromIndex(live, l, v.kv)
		for _, d := range v.derived {
			db.addLiveFromIndex(live, l, d)
		}
		v.mu.RUnlock()
		// Keep the segments needed to fold this LSN, including on
		// ancestors. The snapshot's logs[] is the parent chain.
		for _, lid := range p.logs {
			if gone[lid] {
				continue
			}
			xl, err := db.loadLog(ctx, lid, false)
			if err != nil || xl == nil {
				continue
			}
			for _, s := range xl.foldSegs() {
				if s.n == 0 {
					continue
				}
				if p.at == 0 || s.first <= p.at {
					live[segKey(xl.meta.ID, s.seq)] = struct{}{}
				}
			}
		}
	}
	return nil
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
