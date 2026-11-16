package loget

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// view holds the state of one log folded up to `head`: the record store
// (MVCC versions of every keyed record, in every space) and one instance
// of every derived index registered with WithIndex.
type view struct {
	mu      sync.RWMutex
	log     *logState
	head    uint64
	env     index.Env
	kv      *index.KV     // the record store
	derived []index.Index // in registry order (db.reg.Derived())
	// ckMu serialises index checkpoints of this view (ckpt.go): a run's
	// stored key is set by one and read by the next.
	ckMu sync.Mutex
}

// indexes returns every index of the view, the record store first.
func (v *view) indexes() []index.Index {
	return append([]index.Index{v.kv}, v.derived...)
}

// derivedIndex returns the derived index called name.
func (v *view) derivedIndex(name string) (index.Index, bool) {
	for _, l := range v.derived {
		if l.Name() == name {
			return l, true
		}
	}
	return nil, false
}

// apply folds one entry into every index. Caller holds v.mu.
func (v *view) apply(e *index.Entry) {
	for _, l := range v.indexes() {
		l.Apply(e, v.env)
	}
	v.head = e.LSN
}

// buildView folds the lineage of log up to `upto`. The record store
// starts from the snapshot (an index checkpoint) if it is at or after the
// compaction base, else from the base. Each
// derived index starts from its snapshot section if it has one, or else
// from the base's records in its spaces, and then folds the rest of the
// log.
func (db *DB) buildView(ctx context.Context, l *logState, upto uint64, snap *snapshot) (*view, error) {
	env := db.env
	kinds := db.reg.Derived()
	v := &view{log: l, env: env, kv: index.NewKVOptions(db.kvOptions()), derived: make([]index.Index, len(kinds))}
	b := l.foldStart(upto)
	if b == nil && upto != 0 && !l.coveredThrough(upto) {
		return nil, fmt.Errorf("%w: LSN %d: history was deleted by log GC", ErrCompacted, upto)
	}
	// foldStart may be an ancestor's cut below this log's own later
	// pointer base. A snapshot of a later image must not be used.
	if snap != nil && b != nil && snap.lsn < b.meta.Base.LSN {
		snap = nil
	}
	if snap != nil && b == nil {
		// Folding from scratch: a later pointer-image snapshot is
		// not a valid prefix of this view.
		if own := l.based(); own != nil && snap.lsn >= own.meta.Base.LSN && upto < own.meta.Base.LSN {
			snap = nil
		}
	}
	if snap != nil && (snap.lsn > upto || b != nil && snap.lsn < b.meta.Base.LSN) {
		snap = nil
	}
	// The base is read only when needed (design 24): for the record
	// store without a newer checkpoint, or for a derived index without
	// a section.
	var bimg *baseImage
	loadBase := func() error {
		if bimg != nil {
			return nil
		}
		var err error
		bimg, err = db.loadBase(ctx, b)
		return err
	}
	if b != nil && snap == nil {
		if err := loadBase(); err != nil {
			return nil, err
		}
		snap = &snapshot{lsn: bimg.lsn, kv: bimg.kv, drop: db.keys.shreddedList()}
	}
	var from uint64 // the record store holds LSNs ≤ from
	if snap != nil {
		v.kv, from = snap.kv, snap.lsn
	}
	// froms[i]: derived index i holds LSNs ≤ froms[i]
	froms := make([]uint64, len(kinds))
	restored := make([]bool, len(kinds))
	var fromBase []int // derived indexes to feed the base records
	for i, k := range kinds {
		if snap != nil && k.Restore != nil {
			if b, ok := snap.sections[k.SectionName()]; ok {
				if ls, err := k.Restore(b); err == nil && ls.Name() == k.Name {
					v.derived[i], froms[i], restored[i] = ls, snap.lsn, true
					continue
				} else if err != nil {
					db.logf("loget: index %s: ignoring its snapshot section (it is rebuilt by replay): %v", k.Name, err)
				}
			}
		}
		v.derived[i] = k.New()
		if b != nil {
			if err := loadBase(); err != nil {
				return nil, err
			}
			fromBase, froms[i] = append(fromBase, i), bimg.lsn
		}
	}
	if snap != nil {
		// The snapshot predates some shreds: redact now (DropSubject is
		// defined to equal folding with the subject redacted from the
		// start). Indexes fed from the base see shredded values as
		// unreadable through env.Open, like any replay.
		for _, s := range snap.drop {
			v.kv.DropSubject(s)
			for i := range kinds {
				if restored[i] {
					v.derived[i].DropSubject(s)
				}
			}
		}
	}
	if len(fromBase) > 0 {
		if err := db.eachBase(ctx, bimg, func(space string) bool {
			for _, i := range fromBase {
				if kinds[i].Owns(space) {
					return true
				}
			}
			return false
		}, func(e *index.Entry) error {
			for _, i := range fromBase {
				v.derived[i].Apply(e, env)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	after := from // replay only what some index lacks
	for _, f := range froms {
		after = min(after, f)
	}
	err := l.eachAfter(ctx, after, upto, func(e *index.Entry) error {
		if e.LSN > from {
			if err := db.resolveIngest(ctx, v.kv, e); err != nil {
				return err
			}
			v.kv.Apply(e, env)
		}
		for i, d := range v.derived {
			if e.LSN > froms[i] {
				d.Apply(e, env)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	v.head = upto
	return v, nil
}

// eachBase feeds a base's live records whose space passes keep to fn as
// synthetic entries: grouped by LSN, each op at its original index (other
// slots empty) so env.Open finds the AAD it was sealed with.
func (db *DB) eachBase(ctx context.Context, img *baseImage, keep func(space string) bool, fn func(*index.Entry) error) error {
	type item struct {
		space, key string
		ver        index.Version
	}
	var items []item
	_ = img.kv.Latest(func(space, key string, ver index.Version) error {
		if !ver.Del && keep(space) {
			items = append(items, item{space, key, ver})
		}
		return nil
	})
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i].ver, items[j].ver
		return a.LSN < b.LSN || a.LSN == b.LSN && a.Op < b.Op
	})
	for i := 0; i < len(items); {
		if err := ctx.Err(); err != nil {
			return err
		}
		j := i
		for j < len(items) && items[j].ver.LSN == items[i].ver.LSN {
			j++
		}
		// AAD is bound to the log that sealed the value (design 30:
		// values stay in their original segment, so a branch may name
		// an ancestor's log). Format-4 resealed under the based log;
		// source() still reports that log for inlined leftover values.
		logID := img.log.meta.ID
		if x, _, ok := img.log.source(items[i].ver); ok {
			logID = x.meta.ID
		}
		e := &index.Entry{Log: logID, LSN: items[i].ver.LSN, Ops: make([]format.Op, items[j-1].ver.Op+1)}
		for _, it := range items[i:j] {
			val := it.ver.Val
			if it.ver.Ref() {
				x, seq, ok := img.log.source(it.ver)
				if !ok {
					return fmt.Errorf("%w: LSN %d of %s/%s not in the log", ErrCorrupt, it.ver.LSN, it.space, it.key)
				}
				var err error
				if val, err = db.readValue(x, seq, it.ver); err != nil {
					return err
				}
			}
			e.Ops[it.ver.Op] = format.Op{Kind: format.OpPut, Subject: it.ver.Subject, Space: it.space, Key: it.key, Value: val}
		}
		if err := fn(e); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// spaceOK reports whether s is a space name records may be written to:
// non-empty and without the NUL byte the record store uses as separator.
func spaceOK(s string) bool { return s != "" && !strings.ContainsRune(s, 0) }

// viewForLSN returns a view that can serve reads at LSN at. The branch's
// head view is latest-only at and after a pointer base (design 30); below
// the base, history is folded from retained segments.
func (db *DB) viewForLSN(ctx context.Context, l *logState, head *view, at uint64) (*view, error) {
	// at==0 means the branch head (ViewAt), not LSN 0.
	if at == 0 || at >= head.head {
		return head, nil
	}
	return db.buildView(ctx, l, at, nil)
}
