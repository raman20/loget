package loget

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Compaction (docs/design/03-compaction.md, docs/design/30-log-only.md,
// spec/LogEtCompact.tla, spec/LogEtLogGC.tla).
//
// Compact writes a pointer base at each log's head: the latest version
// of every live key, with values still in their original log segments
// (or leftover format-4 .val chunks). Covered segments are retained,
// not listed as garbage. Time travel, branching and rewind below the
// new base keep working while those segments remain. GC (and Compact
// with DropHistory) then deletes a segment only when no live index or
// snapshot refers to it. ErrCompacted means a needed segment was
// actually deleted, not that Compact ran.

const (
	baseMagic  = "LGETBAS1"
	chunkMagic = "LGETVAL1"
)

// chunkMax: target value chunk size (a var so tests can make many chunks).
var chunkMax = 64 << 20

var errPaused = errors.New("loget: writer paused for compaction")

// CompactOptions configures Compact.
type CompactOptions struct {
	// Grace is how long replaced objects stay before a sweep may delete
	// them, so readers holding an older manifest (another process, a
	// read-only open, a backup) can finish. 0 deletes them right away.
	Grace time.Duration
	// DropHistory, after writing the pointer base, deletes history
	// segments that no live value, leftover chunk, tail segment or
	// Snapshot refers to. The default Compact keeps them so a new
	// index can still fold the retained log. GC always drops unreferenced
	// history (this flag is implied).
	DropHistory bool
}

// CompactStats reports what Compact did.
type CompactStats struct {
	Logs      int   `json:"logs"`              // logs given a new base
	Keys      int   `json:"keys"`              // live keys written into bases
	BaseBytes int64 `json:"base_bytes"`        // bytes written (images + value chunks)
	Deleted   int   `json:"deleted"`           // objects deleted by sweeps
	Pending   int   `json:"pending"`           // garbage items still waiting for their grace period
	Pinned    int   `json:"pinned,omitempty"`  // logs held back by a live Snapshot
	Expired   int   `json:"expired,omitempty"` // expired keys left out of bases
	// design 28: value chunks of the previous base kept, not copied
	SharedChunks int   `json:"shared_chunks,omitempty"`
	SharedBytes  int64 `json:"shared_bytes,omitempty"` // live value bytes in them

	// GC only
	LogsDropped      int `json:"logs_dropped,omitempty"`      // unreachable logs removed from the manifest
	ReflogPruned     int `json:"reflog_pruned,omitempty"`     // reflog entries dropped
	ManifestsDeleted int `json:"manifests_deleted,omitempty"` // old manifests deleted
}

func baseObj(key string) string { return key + ".base" }
func chunkKey(key string, n int) string {
	if n < 0 { // never for a real chunk; keep fmt's rendering of a sign
		return fmt.Sprintf("%s.%06d.val", key, n)
	}
	b := make([]byte, 0, len(key)+1+6+len(".val"))
	b = appendPadded(append(append(b, key...), '.'), uint64(n), 6)
	return string(append(b, ".val"...))
}

// Compact compacts every log (see above). Commits wait while it runs;
// reads continue. It is a no-op for logs unchanged since their base.
func (db *DB) Compact(ctx context.Context, o CompactOptions) (CompactStats, error) {
	return db.runCompaction(ctx, GCOptions{Grace: o.Grace, DropHistory: o.DropHistory}, false)
}

// runCompaction is Compact (gc false) or GC (gc true). Due garbage is
// swept before the writers pause and again after they resume, so the
// commit pause covers building and committing the bases and the reload,
// not the deletes (two fsyncs each on a journal: 2,800 of them paused
// commits for 0.3 s).
func (db *DB) runCompaction(ctx context.Context, o GCOptions, gc bool) (stats CompactStats, err error) {
	defer func() {
		runs, errs := &db.met.compactions, &db.met.compactErrors
		if gc {
			runs, errs = &db.met.gcs, &db.met.gcErrors
		}
		runs.Add(1)
		if err != nil {
			errs.Add(1)
		}
	}()
	db.mu.Lock()
	switch {
	case db.closed:
		db.mu.Unlock()
		return stats, ErrClosed
	case db.failed != nil:
		db.mu.Unlock()
		return stats, db.failed
	case db.compacting:
		db.mu.Unlock()
		return stats, ErrBusy
	}
	db.compacting, db.compactDone = true, make(chan struct{})
	db.mu.Unlock()
	// Branch ops wait in lockIdle from here on; commits do not.
	if err := db.sweep(ctx, &stats, false); err != nil {
		db.endCompaction(time.Time{})
		return stats, err
	}
	resumed, err := db.compactPaused(ctx, o, gc, &stats)
	if resumed && err == nil {
		// The manifest is committed: the sweep finishes even if the
		// caller gave up.
		err = db.sweep(context.WithoutCancel(ctx), &stats, true)
	}
	if gc && err == nil {
		db.lockIdle()
		if !db.closed {
			db.deleteOldManifests(ctx, o.KeepManifests, &stats)
		}
		db.mu.Unlock()
	}
	return stats, err
}

// endCompaction lets branch ops and paused commits go on; paused (when
// set) is when the writers were paused, for the pause histogram.
func (db *DB) endCompaction(paused time.Time) {
	db.mu.Lock()
	db.compacting = false
	close(db.compactDone)
	db.mu.Unlock()
	if !paused.IsZero() {
		db.met.compactLat.Observe(time.Since(paused))
	}
}

// compactPaused pauses the writers, runs compactLocked and reloads the
// branches. resumed reports a successful reload onto fresh writers.
func (db *DB) compactPaused(ctx context.Context, o GCOptions, gc bool, stats *CompactStats) (resumed bool, err error) {
	t0 := time.Now()
	defer db.endCompaction(t0)
	db.mu.RLock()
	ws := make([]*writer, 0, len(db.writers))
	for _, id := range sortedMapKeys(db.writers) {
		ws = append(ws, db.writers[id])
	}
	db.mu.RUnlock()
	// Stop the writers without db.mu (a draining writer may need it to
	// commit a snapshot).
	for _, w := range ws {
		w.pause()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrClosed
	}
	// A writer halted by a storage error (ErrUnknownOutcome) stays halted
	// until reopen: compacting would rebuild from a state that may miss
	// its last segment, and a fresh writer would hide the halt.
	var committed bool
	for _, w := range ws {
		if w.failed != nil && err == nil {
			err = fmt.Errorf("loget: compaction refused: %w", w.failed)
		}
	}
	if err == nil {
		committed, err = db.compactLocked(ctx, o, gc, stats)
	}
	if !committed {
		// nothing changed: resume on the same logs and views
		for _, w := range ws {
			if db.writers[w.l.meta.ID] == w {
				nw := newWriter(db, w.l, w.v)
				nw.failed = w.failed // a halt survives (read after stop)
				db.writers[w.l.meta.ID] = nw
			}
		}
		db.bw.Store(nil)
		return false, err
	}
	// Reload every branch from its base: frees the history held in RAM.
	db.logs, db.views, db.writers = map[string]*logState{}, map[string]*view{}, map[string]*writer{}
	db.bw.Store(nil)
	// The manifest is committed: the reload finishes even
	// if the caller gave up; a failed reload would halt the DB, and an
	// error here would report a committed compaction as failed.
	rctx := context.WithoutCancel(ctx)
	for _, name := range sortedMapKeys(db.man.Refs) {
		if _, rerr := db.openWritable(rctx, db.man.Refs[name]); rerr != nil {
			if db.failed == nil {
				db.failed = rerr
			}
			for _, w := range ws { // callers get ErrClosed instead of retrying
				w.mu.Lock()
				w.paused = false
				w.mu.Unlock()
			}
			return false, rerr
		}
	}
	return err == nil, err
}

// compactLocked builds bases and commits them. Caller holds db.mu with
// all writers stopped. committed reports whether the manifest changed.
// Compact bases every log at its head; GC follows gcPlan (retention cuts,
// reflog pruning, unreachable logs dropped).
func (db *DB) compactLocked(ctx context.Context, o GCOptions, gc bool, stats *CompactStats) (committed bool, err error) {
	type built struct {
		id      string
		base    *BaseMeta
		head    uint64 // log head at the cut: snapshots up to it are replaced
		live    map[string]struct{}
		covered []segIdx
	}
	dropHist := gc || o.DropHistory
	pinned := map[string]bool{}
	plan := map[string]cutInfo{}
	var drop []string
	reflog := db.man.Reflog
	if gc {
		if plan, drop, reflog, err = db.gcPlan(ctx, o); err != nil {
			return false, err
		}
	} else {
		for id := range db.man.Logs {
			plan[id] = cutInfo{head: true}
		}
	}
	var bs []built
	for _, id := range sortedMapKeys(plan) {
		c := plan[id]
		l, err := db.loadLog(ctx, id, false)
		if err != nil {
			return false, err
		}
		var bm *BaseMeta
		var live map[string]struct{}
		cut := c.k
		if c.head {
			cut = l.head()
		}
		if dropHist {
			// Snapshots pin log GC / retention cuts, not pointer-only Compact
			// (design 30). A cut may sit in retained history below the
			// current pointer base.
			if !db.snaps.plan(id, cut, db.opts.Clock, int64(db.opts.SnapshotMaxAge)) {
				stats.Pinned++
				pinned[id] = true
				continue
			}
		}
		if c.head {
			head := l.head()
			v := db.views[id]
			if l.meta.Base != nil && l.meta.Base.LSN == head {
				continue
			}
			if v == nil || v.head != head {
				if v, err = db.buildView(ctx, l, head, db.loadSnapshot(ctx, l)); err != nil {
					return false, err
				}
			}
			bm, live, err = db.writeBase(ctx, l, v, head, nil, stats)
		} else {
			// A cut below the current pointer base needs a complete
			// foldable prefix. If history was already swept, keep the
			// current base (the window is already as tight as it can be).
			if !l.canFold(c.k) || !l.coveredFrom(c.k, l.head()) {
				// LogEtLogGCFork CompactP: a cut is only legal if
				// the fold at the cut and every LSN above it to the
				// head are still stored (no hole in the keep window).
				continue
			}
			var v *view
			if v, err = db.buildView(ctx, l, c.k, nil); err != nil {
				return false, err
			}
			bm, live, err = db.writeBase(ctx, l, v, c.k, &c, stats)
		}
		if err != nil {
			return false, err
		}
		bs = append(bs, built{id, bm, l.head(), live, l.coveredSegs(bm.Seq)})
	}
	if len(bs) == 0 && len(drop) == 0 && len(reflog) == len(db.man.Reflog) {
		return false, nil
	}
	// An unbased branch reading through a rebased ancestor has snapshots
	// whose versions point into that ancestor's replaced segments: they go
	// too (Compact rebases every log, so this is GC only).
	rebased := map[string]bool{}
	for _, b := range bs {
		rebased[b.id] = true
	}
	stale := map[string]uint64{} // log -> head
	for id, lm := range db.man.Logs {
		if lm.Base != nil || rebased[id] || lm.SnapKey == "" && lm.Ckpt == nil {
			continue
		}
		for x := db.man.Logs[lm.Parent]; x != nil && lm.Parent != ""; x = db.man.Logs[x.Parent] {
			if rebased[x.ID] {
				l, err := db.loadLog(ctx, id, false)
				if err != nil {
					return false, err
				}
				stale[id] = l.head()
				break
			}
			if x.Base != nil || x.Parent == "" {
				break
			}
		}
	}
	pruned := len(db.man.Reflog) - len(reflog)
	now := db.opts.Clock()
	after := now + int64(o.Grace)
	newBases := make([]gcNewBase, len(bs))
	for i, b := range bs {
		newBases[i] = gcNewBase{id: b.id, base: b.base, live: b.live}
	}
	globalLive, drop, err := db.gcReach(ctx, drop, newBases, o, now, int64(db.opts.SnapshotMaxAge))
	if err != nil {
		return false, err
	}
	err = db.commitManifest(ctx, func(m *Manifest) error {
		m.Format = max(m.Format, manifestFormatLogOnly)
		// A checkpoint's runs point into segments a new base replaces
		// (and are bound to the old base): it goes with them.
		dropCkpt := func(id string) {
			if c := m.Logs[id].Ckpt; c != nil {
				m.Garbage = append(m.Garbage, ckptGarbage(id, c, nil, after)...)
				m.Logs[id].Ckpt = nil
			}
		}
		for id, head := range stale {
			m.Logs[id].SnapLSN, m.Logs[id].SnapKey = 0, ""
			m.Garbage = append(m.Garbage, Garbage{Kind: "snaps", Log: id, Below: head + 1, After: after})
			dropCkpt(id)
		}
		for _, b := range bs {
			lm := m.Logs[b.id]
			if old := lm.Base; old != nil {
				m.Garbage = append(m.Garbage, baseGarbage(b.id, old, b.base, after)...)
			}
			if b.base.Refs != nil {
				m.Format = max(m.Format, manifestFormatRefs)
			}
			dropCkpt(b.id)
			keep, drop := b.covered, []uint64(nil)
			if dropHist {
				keep, drop = filterLiveSegs(b.covered, globalLive, b.id)
			}
			lm.Retain = metaFromSegs(keep)
			if len(drop) > 0 {
				m.Garbage = append(m.Garbage, Garbage{Kind: "segs", Log: b.id, Seqs: drop, After: after})
			}
			// On-disk snapshots are disposable folds of a now-replaced
			// pointer image; the retained log can rebuild any view.
			m.Garbage = append(m.Garbage,
				Garbage{Kind: "snaps", Log: b.id, Below: b.head + 1, After: after})
			lm.Base = b.base
			lm.SnapLSN, lm.SnapKey = 0, ""
		}
		handled := map[string]bool{}
		for _, b := range bs {
			handled[b.id] = true
		}
		if dropHist {
			for _, id := range sortedMapKeys(m.Logs) {
				if handled[id] || pinned[id] {
					continue
				}
				l := db.logs[id]
				if l == nil {
					// try the already-loaded set; loadLog needs ctx and may recurse
					var err error
					l, err = db.loadLog(ctx, id, false)
					if err != nil || l == nil {
						continue
					}
				}
				if l.meta.Base == nil && len(l.retained) == 0 {
					continue
				}
				// Only sweep if the current pointer base already
				// satisfies the retention window. Otherwise the
				// head live set would drop history still in the
				// window (the cut should have gone into bs).
				if !l.retentionSatisfied(o, db.opts.Clock()) {
					continue
				}
				if !db.snaps.plan(id, l.baseLSN(), db.opts.Clock, int64(db.opts.SnapshotMaxAge)) {
					stats.Pinned++
					continue
				}
				seq := l.nextSeq
				if l.meta.Base != nil {
					seq = l.meta.Base.Seq
				}
				keep, dropSeqs := filterLiveSegs(l.coveredSegs(seq), globalLive, id)
				m.Logs[id].Retain = metaFromSegs(keep)
				if len(dropSeqs) > 0 {
					m.Garbage = append(m.Garbage, Garbage{Kind: "segs", Log: id, Seqs: dropSeqs, After: after})
				}
			}
		}
		for _, id := range drop {
			lm := m.Logs[id]
			if lm.Base != nil {
				m.Garbage = append(m.Garbage, baseGarbage(id, lm.Base, nil, after)...)
			}
			dropCkpt(id)
			m.Garbage = append(m.Garbage,
				Garbage{Kind: "segs", Log: id, Below: math.MaxUint64, After: after},
				Garbage{Kind: "snaps", Log: id, Below: math.MaxUint64, After: after})
			delete(m.Logs, id)
		}
		m.Reflog = reflog
		return nil
	})
	if err != nil {
		// The new bases may be orphans now; a later epoch's sweep deletes
		// them (we cannot: the put of the manifest may yet be visible).
		return false, err
	}
	stats.Logs, stats.LogsDropped, stats.ReflogPruned = len(bs), len(drop), pruned
	return true, nil
}

// baseGarbage lists base b's objects as garbage, except value chunks the
// base replacing it (next, nil if none) still refers to.
func baseGarbage(id string, b, next *BaseMeta, after int64) []Garbage {
	g := Garbage{Kind: "base", Log: id, Key: b.Key, Chunks: b.Chunks, After: after}
	if b.Run != nil {
		g.Pieces = len(b.Run.Sums)
	}
	if b.Refs == nil && (next == nil || next.Refs == nil) {
		return []Garbage{g} // no chunk shared either way
	}
	g.Chunks = 0
	keep := map[string]bool{}
	if next != nil {
		for _, o := range next.chunkObjs() {
			keep[o] = true
		}
	}
	vals := Garbage{Kind: "vals", Log: id, After: after}
	for _, o := range b.chunkObjs() {
		if !keep[o] {
			vals.Objs = append(vals.Objs, o)
		}
	}
	if len(vals.Objs) == 0 {
		return []Garbage{g}
	}
	return []Garbage{g, vals}
}

// writeBase writes the base of log l at head from its view v.
// at is nil for a base at the log's head; for a retention cut below the
// head it gives the segment after the cut and the cut frame's time.
func (db *DB) writeBase(ctx context.Context, l *logState, v *view, head uint64, at *cutInfo, stats *CompactStats) (*BaseMeta, map[string]struct{}, error) {
	nonce, err := db.keys.randBytes(4)
	if err != nil {
		return nil, nil, err
	}
	key := fmt.Sprintf("base/%s/%020d-e%d-%s", l.meta.ID, head, db.epoch, hex.EncodeToString(nonce))
	id := l.meta.ID
	o := db.kvOptions()
	// The base is one run (design 24), built in key order in a run file
	// (or on the heap) and stored in pieces: no single object holds it.
	rw := index.NewRunWriter(o, 0)
	done := false
	defer func() {
		if !done {
			rw.Abort()
		}
	}()
	// The base's TS is the cut frame's commit time; a key expired at it is
	// invisible to every read at or after the cut, so the base drops it
	// (expiry GC is part of the fold: docs/design/27-ttl-snapshots.md).
	l.mu.RLock()
	seq := l.nextSeq
	l.mu.RUnlock()
	var ts int64
	if at != nil {
		ts, seq = at.ts, at.seq
	} else if ts, err = frameTS(ctx, l, head); err != nil {
		// the same as lastTS for a log with frames of its own; a branch
		// without any takes its parent's frame (or base) at the fork
		return nil, nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	// Pointer-only (design 30): keep segment positions and leftover
	// format-4 chunk refs as they are. Any live ref pins a leftover
	// chunk (no shareMin, no new .val, no reseal).
	shared, remap, err := db.leftoverChunks(l, v, ts)
	if err != nil {
		return nil, nil, err
	}
	live := map[string]struct{}{}
	err = v.kv.Latest(func(space, k string, ver index.Version) error {
		if ver.Del {
			return nil
		}
		if ver.Expired(ts) {
			stats.Expired++
			return nil
		}
		if ver.Ref() {
			if _, _, ok := l.source(ver); !ok {
				return fmt.Errorf("%w: LSN %d of %s/%s not in the log", ErrCorrupt, ver.LSN, space, k)
			}
		}
		if obj := db.objectOf(l, ver); obj != "" {
			live[obj] = struct{}{}
		}
		nv := index.Version{LSN: ver.LSN, Op: ver.Op, Subject: ver.Subject, Exp: ver.Exp, Val: ver.Val,
			Chunk: ver.Chunk, Off: ver.Off, Len: ver.Len, Sum: ver.Sum, Log: ver.Log}
		if nv.Log == "" {
			if id, ok := l.originLog(ver); ok {
				nv.Log = id
			}
		}
		if nc := remap[ver.Chunk]; nc != 0 && ownChunk(l, ver) {
			nv.Chunk = nc
			stats.SharedBytes += int64(ver.Len)
		}
		stats.Keys++
		return rw.Add(space, k, nv)
	})
	if err != nil {
		return nil, nil, err
	}
	chain, err := l.chainAt(ctx, head)
	if err != nil {
		return nil, nil, err
	}
	if at == nil {
		// every segment that exists now is covered (a read-only load may
		// stop before a torn tail)
		keys, err := db.st.List(ctx, "log/"+id+"/")
		if err != nil {
			return nil, nil, err
		}
		for _, k := range keys {
			if s, ok := parseNum(k, "log/"+id+"/", ".seg"); ok && s >= seq {
				seq = s + 1
			}
		}
	}
	cut, err := rw.Finish()
	done = true
	if err != nil {
		return nil, nil, err
	}
	run := &index.RunMeta{} // no live key: an empty base
	if cut != nil {
		defer cut.Release()
		cr := cut.Runs[0]
		m := cr.Meta()
		run = &m
		if err := db.putRun(ctx, key, cr.Bytes(), m.Piece); err != nil {
			return nil, nil, err
		}
	}
	stats.BaseBytes += run.Len
	addLiveTail(live, l, seq)
	bm := &BaseMeta{LSN: head, Seq: seq, Key: key, Chain: hex.EncodeToString(chain[:]), TS: ts, Run: run}
	if len(shared) > 0 {
		bm.Refs = shared
		stats.SharedChunks += len(shared)
	}
	return bm, live, nil
}

// leftoverChunks keeps every leftover format-4 value chunk that a live
// version still names (design 30: any live ref pins the whole chunk;
// numbers are remapped densely into the new base's Refs). WithCompactCopyAll
// is a no-op: we still do not copy values out of the log.
func (db *DB) leftoverChunks(l *logState, v *view, ts int64) ([]ChunkRef, map[uint32]uint32, error) {
	ob := l.meta.Base
	if ob == nil {
		return nil, nil, nil
	}
	old := ob.Refs
	if old == nil {
		n := ob.Chunks
		if n == 0 && len(ob.ChunkSizes) > 0 {
			n = len(ob.ChunkSizes)
		}
		for i := 0; i < n; i++ {
			sz := int64(0)
			if i < len(ob.ChunkSizes) {
				sz = ob.ChunkSizes[i]
			}
			old = append(old, ChunkRef{Key: ob.Key, N: i, Size: sz})
		}
	}
	if len(old) == 0 {
		return nil, nil, nil
	}
	used := make([]bool, len(old))
	err := v.kv.Latest(func(_, _ string, ver index.Version) error {
		if !ver.Del && !ver.Expired(ts) && ownChunk(l, ver) && int(ver.Chunk) <= len(used) {
			used[ver.Chunk-1] = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	var shared []ChunkRef
	remap := map[uint32]uint32{}
	for i, r := range old {
		if used[i] {
			shared = append(shared, r)
			remap[uint32(i+1)] = uint32(len(shared))
		}
	}
	return shared, remap, nil
}

// shareMin: a chunk of the previous base is shared, not copied, while at
// least this fraction of its bytes is still live (else its live values
// are copied into new chunks and it becomes garbage). Space amplification
// of value chunks is so bounded by 1/shareMin.
const shareMin = 0.5

// ownChunk: ver is stored in a value chunk of l's own current base.
func ownChunk(l *logState, ver index.Version) bool {
	if ver.Chunk == 0 || ver.Subject != "" {
		return false
	}
	x, _, ok := l.source(ver)
	return ok && x == l
}

// shareChunks decides which value chunks of l's current base the next
// base keeps (design 28): those at least shareMin live at time ts.
// remap maps a kept chunk's old number to its number in the new base
// (1..len(shared)). Subject values are always rewritten (resealed).
// Caller holds v.mu (read).
func (db *DB) shareChunks(l *logState, v *view, ts int64) ([]ChunkRef, map[uint32]uint32, error) {
	ob := l.meta.Base
	if ob == nil || db.opts.CompactCopyAll {
		return nil, nil, nil
	}
	old := ob.Refs
	if old == nil {
		if len(ob.ChunkSizes) != ob.Chunks {
			return nil, nil, nil // written before design 28: sizes unknown
		}
		for i, n := range ob.ChunkSizes {
			old = append(old, ChunkRef{Key: ob.Key, N: i, Size: n})
		}
	}
	live := make([]int64, len(old))
	err := v.kv.Latest(func(_, _ string, ver index.Version) error {
		if !ver.Del && !ver.Expired(ts) && ownChunk(l, ver) && int(ver.Chunk) <= len(live) {
			live[ver.Chunk-1] += int64(ver.Len)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	var shared []ChunkRef
	remap := map[uint32]uint32{}
	for i, r := range old {
		if r.Size > 0 && float64(live[i]) >= shareMin*float64(r.Size) {
			shared = append(shared, r)
			remap[uint32(i+1)] = uint32(len(shared))
		}
	}
	return shared, remap, nil
}

// chunkWriter packs base values into chunk objects of about chunkMax.
type chunkWriter struct {
	db    *DB
	key   string
	buf   []byte
	n     int // chunks written
	bytes int64
	sizes []int64 // of each chunk written
}

func (c *chunkWriter) add(ctx context.Context, val []byte) (int, int, error) {
	if len(c.buf) > len(chunkMagic) && len(c.buf)+len(val) > chunkMax {
		if err := c.flush(ctx); err != nil {
			return 0, 0, err
		}
	}
	if len(c.buf) == 0 {
		c.buf = append(c.buf, chunkMagic...)
	}
	off := len(c.buf)
	c.buf = append(c.buf, val...)
	return c.n, off, nil
}

func (c *chunkWriter) flush(ctx context.Context) error {
	if len(c.buf) == 0 {
		return nil
	}
	if err := putExactlyOnce(ctx, c.db.st, chunkKey(c.key, c.n), c.buf); err != nil {
		return err
	}
	c.bytes += int64(len(c.buf))
	c.sizes = append(c.sizes, int64(len(c.buf)))
	c.n++
	c.buf = c.buf[:0]
	return nil
}

// baseImage is a decoded base: a new record store (callers may modify it).
type baseImage struct {
	log *logState
	lsn uint64
	kv  *index.KV
}

// loadBase reads and checks the base of based log b.
func (db *DB) loadBase(ctx context.Context, b *logState) (*baseImage, error) {
	bm := b.meta.Base
	if bm.Run != nil { // run pieces (design 24)
		var runs []ckptRunDesc
		if bm.Run.Len > 0 {
			runs = append(runs, ckptRunDesc{key: bm.Key, meta: *bm.Run})
		}
		src := db.runSources(ctx, runs)
		for i := range src {
			src[i].Key = "" // not a checkpoint's run: a checkpoint stores it anew
		}
		kv, err := index.LoadKVRuns(db.kvOptions(), src)
		if err != nil {
			if errors.Is(err, index.ErrBadRun) {
				err = fmt.Errorf("%w: %w", ErrCorrupt, err)
			}
			return nil, fmt.Errorf("loget: log %s base %s: %w", b.meta.ID, bm.Key, err)
		}
		return &baseImage{log: b, lsn: bm.LSN, kv: kv}, nil
	}
	data, err := db.st.Get(ctx, baseObj(bm.Key))
	if err != nil {
		return nil, fmt.Errorf("loget: log %s base %s: %w", b.meta.ID, bm.Key, err)
	}
	bad := func(why string) error {
		return fmt.Errorf("%w: log %s base %s: %s", ErrCorrupt, b.meta.ID, bm.Key, why)
	}
	if len(data) >= 8 && string(data[:7]) == baseMagic[:7] && data[7] != baseMagic[7] {
		return nil, fmt.Errorf("%w: log %s base %s has version %q (this build reads %q; upgrade the binary)",
			ErrVersion, b.meta.ID, bm.Key, data[7], baseMagic[7])
	}
	if len(data) < 28 || string(data[:8]) != baseMagic {
		return nil, bad("bad header")
	}
	if binary.LittleEndian.Uint32(data[len(data)-4:]) != format.CRC(data[:len(data)-4]) {
		return nil, bad("checksum mismatch")
	}
	if binary.LittleEndian.Uint64(data[8:]) != bm.LSN {
		return nil, bad("LSN mismatch")
	}
	n := binary.LittleEndian.Uint64(data[16:])
	body := data[24 : len(data)-4]
	if n != uint64(len(body)) {
		return nil, bad("bad length")
	}
	kv, err := index.LoadKVBaseOptions(body, db.kvOptions())
	if err != nil {
		return nil, bad(err.Error())
	}
	return &baseImage{log: b, lsn: bm.LSN, kv: kv}, nil
}

// sweep deletes due garbage and unreferenced bases of older epochs, then
// commits the shorter garbage list. It holds db.mu only to read the
// manifest and to commit, so writers keep committing while it deletes.
// That is safe: due garbage is unreachable from every manifest from this
// one on (and its grace has passed), and only a compaction writes bases,
// which sweepOrphans skips for this epoch. sweepMu keeps one sweep at a
// time. idle: take db.mu through lockIdle (no compaction of ours runs).
func (db *DB) sweep(ctx context.Context, stats *CompactStats, idle bool) error {
	db.sweepMu.Lock()
	defer db.sweepMu.Unlock()
	db.mu.RLock()
	man, stopped := db.man, db.closed || db.failed != nil
	db.mu.RUnlock()
	if stopped {
		return nil
	}
	now := db.opts.Clock()
	var swept []Garbage
	pending := 0
	for _, g := range man.Garbage {
		if g.After > now {
			pending++
			continue
		}
		n, err := db.deleteGarbage(ctx, g)
		stats.Deleted += n
		if err != nil {
			db.logf("loget: sweep %s %s: %v", g.Kind, g.Log, err)
			pending++
			continue
		}
		swept = append(swept, g)
	}
	stats.Pending = pending
	n, err := db.sweepOrphans(ctx, man)
	stats.Deleted += n
	if err != nil {
		db.logf("loget: orphan sweep: %v", err)
	}
	if len(swept) == 0 {
		return nil
	}
	if idle {
		db.lockIdle()
	} else {
		db.mu.Lock()
	}
	defer db.mu.Unlock()
	if db.closed {
		return nil // the entries stay listed; the next sweep finds them gone
	}
	return db.commitManifest(ctx, func(m *Manifest) error {
		m.Garbage = withoutGarbage(m.Garbage, swept)
		return nil
	})
}

// withoutGarbage returns gs minus one occurrence of each entry of drop.
func withoutGarbage(gs, drop []Garbage) []Garbage {
	id := func(g Garbage) string { b, _ := json.Marshal(g); return string(b) }
	n := map[string]int{}
	for _, g := range drop {
		n[id(g)]++
	}
	var keep []Garbage
	for _, g := range gs {
		if k := id(g); n[k] > 0 {
			n[k]--
			continue
		}
		keep = append(keep, g)
	}
	return keep
}

// stopped reports whether the DB was closed (or crashed): sweeps stop
// deleting then, as a crashed DB must not write storage any more.
func (db *DB) stopped() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

func (db *DB) deleteGarbage(ctx context.Context, g Garbage) (int, error) {
	var keys []string
	switch g.Kind {
	case "segs", "snaps":
		dir, suf := "log/"+g.Log+"/", ".seg"
		if g.Kind == "snaps" {
			dir, suf = "snap/"+g.Log+"/", ".snap"
		}
		all, err := db.st.List(ctx, dir)
		if err != nil {
			return 0, err
		}
		for _, k := range all {
			n, ok := parseNum(k, dir, suf)
			if !ok && g.Kind == "segs" {
				n, ok = parseNum(k, dir, ".repair") // a repair note goes with its segment
			}
			if !ok && g.Kind == "segs" {
				n, ok = parseNum(k, dir, ".idx") // so does a checkpoint ending there
			}
			if !ok && g.Kind == "segs" {
				n, _, _, ok = parseIngest(k, dir) // so does an ingest run committed there
			}
			if !ok {
				continue
			}
			if len(g.Seqs) > 0 {
				keep := false
				for _, s := range g.Seqs {
					if n == s {
						keep = true
						break
					}
				}
				if keep {
					keys = append(keys, k)
				}
				continue
			}
			if n < g.Below {
				keys = append(keys, k)
			}
		}
	case "base":
		keys = append(keys, baseObj(g.Key)) // absent for a run base: deleting it is a no-op
		for i := 0; i < g.Chunks; i++ {
			keys = append(keys, chunkKey(g.Key, i))
		}
		for i := 0; i < g.Pieces; i++ {
			keys = append(keys, ckptPieceKey(g.Key, i))
		}
	case "vals":
		keys = append(keys, g.Objs...)
	case "ckpt":
		keys = append(keys, ckptDescObj(g.Key))
	case "run":
		for i := 0; i < g.Chunks; i++ {
			keys = append(keys, ckptPieceKey(g.Key, i))
		}
	default:
		return 0, fmt.Errorf("unknown garbage kind %q", g.Kind)
	}
	// In batches (design 23): on the journal one record and two syncs per
	// batch instead of per object. Any subset of a failed batch may be
	// gone; garbage is unreferenced, so a retry deletes the rest.
	n := 0
	sctx := stopCtx{ctx, db}
	for len(keys) > 0 {
		if db.stopped() {
			return n, ErrClosed
		}
		b := keys[:min(len(keys), deleteBatch)]
		if err := store.DeleteBatch(sctx, db.st, b); err != nil {
			return n, err
		}
		n += len(b)
		keys = keys[len(b):]
	}
	return n, nil
}

// stopCtx reports ErrClosed from Err once the DB is closed or crashed,
// so store.DeleteBatch's one-at-a-time fallback stops within a batch, as
// the sweep did before batching (a crashed DB deletes nothing more). Only
// Err changes: Done does not close on a stop.
type stopCtx struct {
	context.Context
	db *DB
}

func (c stopCtx) Err() error {
	if c.db.stopped() {
		return ErrClosed
	}
	return c.Context.Err()
}

// deleteBatch is how many garbage objects one store.DeleteBatch removes:
// large enough that the syncs no longer dominate, small enough that a
// batch holds the journal's write lock (and so blocks commits) for about
// as long as one commit's fsync: ~3 ms for 1024 keys on the reference
// box, ~10 ms for 4096 (design 23, part 2).
const deleteBatch = 1024

// sweepOrphans deletes base objects that no manifest from this one on
// can refer to: written in an older epoch and not in the manifest (a
// compaction whose commit lost to a newer opener).
func (db *DB) sweepOrphans(ctx context.Context, man *Manifest) (int, error) {
	live := map[string]bool{}
	for _, lm := range man.Logs {
		if lm.Base != nil {
			live[lm.Base.Key] = true
		}
	}
	liveObj := map[string]bool{} // value chunks shared from older bases
	for _, lm := range man.Logs {
		if lm.Base != nil && lm.Base.Refs != nil {
			for _, o := range lm.Base.chunkObjs() {
				liveObj[o] = true
			}
		}
	}
	for _, g := range man.Garbage {
		switch g.Kind {
		case "base":
			live[g.Key] = true
		case "vals":
			for _, o := range g.Objs {
				liveObj[o] = true
			}
		}
	}
	n := 0
	ids := map[string]bool{}
	for id := range man.Logs {
		ids[id] = true
	}
	for _, g := range man.Garbage {
		ids[g.Log] = true // dropped by GC: its orphans too
	}
	ckLive := map[string]bool{} // checkpoint objects named or listed
	for _, lm := range man.Logs {
		if lm.Ckpt != nil {
			for _, k := range ckptObjects(lm.Ckpt) {
				ckLive[k] = true
			}
		}
	}
	for _, g := range man.Garbage {
		switch g.Kind {
		case "ckpt":
			ckLive[ckptDescObj(g.Key)] = true
		case "run":
			for i := 0; i < g.Chunks; i++ {
				ckLive[ckptPieceKey(g.Key, i)] = true
			}
		}
	}
	for _, id := range sortedMapKeys(ids) {
		// Checkpoint objects of an older epoch that the manifest neither
		// names nor lists: a checkpoint whose commit lost or never came
		// (spec/LogEtCkpt.tla, SweepOrphan). Our own epoch's may still be
		// committed by a running checkpoint.
		keys, err := db.st.List(ctx, ckptPrefix(id))
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			if ep, ok := ckptEpoch(k); !ok || ep >= db.epoch || ckLive[k] {
				continue
			}
			if db.stopped() {
				return n, ErrClosed
			}
			if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
				return n, err
			}
			n++
		}
	}
	for _, id := range sortedMapKeys(ids) {
		c, err := db.sweepIngests(ctx, id)
		n += c
		if err != nil {
			return n, err
		}
	}
	for _, id := range sortedMapKeys(ids) {
		keys, err := db.st.List(ctx, "base/"+id+"/")
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			bk, ok := baseKeyOf(k)
			if !ok || live[bk] || liveObj[k] {
				continue
			}
			if ep, ok := baseEpoch(bk); !ok || ep >= db.epoch {
				continue
			}
			if db.stopped() {
				return n, ErrClosed
			}
			if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// baseKeyOf maps an object under base/ to its base key.
func baseKeyOf(obj string) (string, bool) {
	if k, ok := strings.CutSuffix(obj, ".base"); ok {
		return k, true
	}
	for _, suf := range []string{".val", ".run"} { // value chunk, run piece
		if k, ok := strings.CutSuffix(obj, suf); ok && len(k) > 7 && k[len(k)-7] == '.' {
			return k[:len(k)-7], true
		}
	}
	return "", false
}

// baseEpoch parses the epoch from base/<log>/<lsn>-e<epoch>-<nonce>.
func baseEpoch(key string) (uint64, bool) {
	// fields of the last path element: <lsn>, e<epoch>, <nonce>; the hex
	// nonce may itself start with "e", so split rather than search
	f := strings.Split(key[strings.LastIndexByte(key, '/')+1:], "-")
	if len(f) != 3 || !strings.HasPrefix(f[1], "e") {
		return 0, false
	}
	ep, err := strconv.ParseUint(f[1][1:], 10, 64)
	return ep, err == nil
}
