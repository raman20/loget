package loget

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"
)

// GC (docs/design/08-gc-retention.md, spec/LogEtGC.tla) is compaction
// with a retention window, plus collection of what compaction leaves:
// unreachable (retired) logs, old reflog entries and old manifests.

// GCOptions configures GC. The zero value keeps no history (every log is
// based at its head, as Compact), the newest 64 reflog entries and the
// newest 16 manifests.
type GCOptions struct {
	// Grace is how long replaced objects stay before a sweep may delete
	// them (see CompactOptions).
	Grace time.Duration
	// KeepFor keeps this much history on every log: time travel, Rewind,
	// CreateBranch and ViewLog keep working at any LSN committed within
	// it (frame timestamps, i.e. the writer's clock). Reflog entries
	// newer than this are kept too.
	KeepFor time.Duration
	// KeepCommits keeps at least this many newest LSNs on every log.
	KeepCommits uint64
	// KeepReflog keeps the newest this-many reflog entries regardless of
	// age (0 = 64). Logs that only pruned entries named are dropped.
	KeepReflog int
	// KeepManifests keeps the newest this-many manifests (0 = 16).
	KeepManifests int
	// DropHistory is set by Compact when the caller asked to delete
	// unreferenced history segments. GC implies it.
	DropHistory bool
}

// GC compacts every log at its retention cut (see GCOptions), drops logs
// no branch, kept reflog entry or dependent branch reaches, prunes the
// reflog and deletes old manifests. Commits wait while it runs.
func (db *DB) GC(ctx context.Context, o GCOptions) (CompactStats, error) {
	return db.runCompaction(ctx, o, true)
}

// cutInfo is where a log's new base goes: at its head (head), or at the
// end k of a segment below it, with the following segment's seq and the
// time of frame k. The zero value means "no new base".
type cutInfo struct {
	k, seq uint64
	ts     int64
	head   bool
}

func (c cutInfo) set() bool { return c.head || c.k > 0 }

// retentionCut picks the highest LSN at or below capLSN that keeps
// o.KeepCommits LSNs and o.KeepFor of history above it, rounded down to
// the end of a segment of l's own.
func (db *DB) retentionCut(ctx context.Context, l *logState, o GCOptions, now int64, capLSN uint64) (cutInfo, error) {
	head := l.head()
	lim := min(head, capLSN)
	if o.KeepCommits > 0 {
		if head <= o.KeepCommits {
			return cutInfo{}, nil
		}
		lim = min(lim, head-o.KeepCommits)
	}
	tcut := now - int64(o.KeepFor)
	// foldSegs: after a pointer compact the keep window may sit in
	// retained history, not only in the writable tail (design 30).
	segs := l.foldSegs()
	l.mu.RLock()
	lastTS, nextSeq := l.lastTS, l.nextSeq
	l.mu.RUnlock()
	if lim == head && (o.KeepFor == 0 || lastTS <= tcut) {
		if l.meta.Base != nil && l.meta.Base.LSN == head {
			return cutInfo{}, nil
		}
		return cutInfo{k: head, head: true}, nil
	}
	end := func(i int) uint64 { return segs[i].first + segs[i].n - 1 }
	i := sort.Search(len(segs), func(i int) bool { return end(i) > lim }) - 1
	if i < 0 {
		return cutInfo{}, nil
	}
	var rerr error
	segTS := func(i int) int64 {
		f, err := l.readSeg(ctx, segs[i])
		if err != nil {
			rerr = err
			return math.MaxInt64
		}
		return f[len(f)-1].Frame.TS
	}
	j := i
	if o.KeepFor > 0 {
		// frame times are non-decreasing within a writer; a clock that
		// went backwards only makes this keep more
		j = sort.Search(i+1, func(x int) bool { return segTS(x) > tcut }) - 1
		if rerr != nil {
			return cutInfo{}, rerr
		}
		if j < 0 {
			return cutInfo{}, nil
		}
	}
	ts := segTS(j)
	if rerr != nil {
		return cutInfo{}, rerr
	}
	seq := nextSeq
	if j+1 < len(segs) {
		seq = segs[j+1].seq
	}
	return cutInfo{k: end(j), seq: seq, ts: ts}, nil
}

// gcPlan decides GC's cuts, the logs to drop and the pruned reflog.
// Caller holds db.mu with writers stopped.
//
// Reachable: logs named by a ref or a kept reflog entry, and every log a
// reachable unbased branch reads through (its ancestors up to the first
// based one). Such a branch reads ancestor x only up to the smallest
// branch point on the way, so x's cut is capped there; a cut that drops
// to nothing can make x itself dependent, hence the fixpoint.
func (db *DB) gcPlan(ctx context.Context, o GCOptions) (map[string]cutInfo, []string, []RefChange, error) {
	now := db.opts.Clock()
	keepN := o.KeepReflog
	if keepN <= 0 {
		keepN = 64
	}
	var reflog []RefChange
	for i, r := range db.man.Reflog {
		if i >= len(db.man.Reflog)-keepN || (o.KeepFor > 0 && r.TS > 0 && r.TS >= now-int64(o.KeepFor)) {
			reflog = append(reflog, r)
		}
	}
	reach := map[string]bool{}
	for _, id := range db.man.Refs {
		reach[id] = true
	}
	for _, r := range reflog {
		for _, id := range []string{r.From, r.To} {
			if _, ok := db.man.Logs[id]; ok {
				reach[id] = true
			}
		}
	}
	caps := map[string]uint64{}
	cuts := map[string]cutInfo{}
	cutOf := func(id string) (cutInfo, error) {
		if c, ok := cuts[id]; ok {
			return c, nil
		}
		l, err := db.loadLog(ctx, id, false)
		if err != nil {
			return cutInfo{}, err
		}
		capLSN, ok := caps[id]
		if !ok {
			capLSN = math.MaxUint64
		}
		c, err := db.retentionCut(ctx, l, o, now, capLSN)
		cuts[id] = c
		return c, err
	}
	for round := 0; ; round++ {
		if round > 2*len(db.man.Logs)+2 {
			return nil, nil, nil, fmt.Errorf("%w: gc plan does not converge", ErrCorrupt)
		}
		clear(cuts)
		changed := false
		for _, id := range sortedMapKeys(reach) {
			c, err := cutOf(id)
			if err != nil {
				return nil, nil, nil, err
			}
			m := db.man.Logs[id]
			if m.Parent == "" {
				continue
			}
			// The keep window is [head-KeepCommits, head]. A branch
			// forked at the parent's head may have a pointer base at
			// the fork while the window still extends below it into
			// the parent (keep=1, child head=2, ViewAt(1)). Cap the
			// ancestor even when the child is already based.
			// KeepFor needs nothing extra: ancestor frames carry the
			// same times.
			cp, x := m.ParentLSN, m.Parent
			if o.KeepCommits > 0 {
				l, err := db.loadLog(ctx, id, false)
				if err != nil {
					return nil, nil, nil, err
				}
				if l.head() > o.KeepCommits {
					cp = min(cp, l.head()-o.KeepCommits)
				} else {
					cp = min(cp, l.floor())
				}
			}
			// keep=0: a based child's window is its head, served
			// from its pointer base; do not cap the parent at the
			// fork or retired parents never drop.
			// keep>0 and cp == ParentLSN: ViewAt at the fork is
			// not served by a child base above the fork, so cap.
			if o.KeepCommits == 0 {
				if m.Base != nil || c.set() {
					continue
				}
			} else if (m.Base != nil || c.set()) && cp > m.ParentLSN {
				continue
			}
			for x != "" {
				xm, ok := db.man.Logs[x]
				if !ok {
					// A based log's parent may already have been
					// dropped (design 30). History below the fork
					// is gone; there is nothing left to cap.
					break
				}
				if !reach[x] {
					reach[x], changed = true, true
				}
				if c, ok := caps[x]; !ok || cp < c {
					caps[x], changed = cp, true
					delete(cuts, x)
				}
				xc, err := cutOf(x)
				if err != nil {
					return nil, nil, nil, err
				}
				if xm.Base != nil || xc.set() {
					break
				}
				cp, x = min(cp, xm.ParentLSN), xm.Parent
			}
		}
		if !changed {
			break
		}
	}
	plan := map[string]cutInfo{}
	for id, c := range cuts {
		if reach[id] && c.set() {
			plan[id] = c
		}
	}
	var drop []string
	for _, id := range sortedMapKeys(db.man.Logs) {
		if !reach[id] {
			drop = append(drop, id)
		}
	}
	return plan, drop, reflog, nil
}

// deleteOldManifests deletes manifests more than keep below the current
// one (keep 0 = 16). Readers only ever load the highest id, and
// putManifest's post-create check keeps fencing intact (LogEtGC.tla).
// Caller holds db.mu.
func (db *DB) deleteOldManifests(ctx context.Context, keep int, stats *CompactStats) {
	if keep <= 0 {
		keep = 16
	}
	if db.failed != nil || db.man.ID <= uint64(keep) {
		return
	}
	db.deleteOldLeases(ctx, keep)
	keys, err := db.st.List(ctx, "manifest/")
	if err != nil {
		db.logf("loget: gc: list manifests: %v", err)
		return
	}
	for _, k := range keys {
		if n, ok := parseNum(k, "manifest/", ".mf"); ok && n+uint64(keep) <= db.man.ID {
			if err := db.st.Delete(ctx, k); err != nil {
				db.logf("loget: gc: delete %s: %v", k, err)
				return
			}
			stats.ManifestsDeleted++
		}
	}
}
