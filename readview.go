package loget

import (
	"context"
	"errors"
	"fmt"
	"github.com/raman20/loget/store"
	"sync"
	"time"
)

// Read views (docs/design/27-ttl-snapshots.md, part b). A Snapshot is a
// stable point-in-time view of a branch: an LSN and a read time. Creating
// one costs a registry entry (no copy: the record store is multi-version);
// many transactions and iterators may read through it, concurrently. It
// pins history: compaction does not move a log's base past a live
// snapshot's LSN. The pin is bounded: once older than
// Options.SnapshotMaxAge a snapshot stops pinning and every read through
// it fails with ErrSnapshotTooOld. Release it when done.

const defaultSnapshotMaxAge = 10 * time.Minute

// SnapshotOptions configures Branch.Snapshot.
type SnapshotOptions struct {
	// At is the LSN to view (0 = the branch head).
	At uint64
	// Now, if non-zero, is the read time expiries are compared with
	// (unix nanoseconds). 0 = the commit timestamp of the frame at At.
	Now int64
}

// Snapshot is a released-explicitly, point-in-time read view.
type Snapshot struct {
	db      *DB
	branch  string
	at      uint64
	now     int64
	created int64
	logs    []string // the lineage it reads (pins each)
	id      uint64
}

// snapRegistry tracks live snapshots and, per log, the highest base LSN a
// compaction has planned (monotone). Creation and compaction planning are
// serialised by mu, so a snapshot is either seen by the planner or created
// at or above the planned cut (spec/LogEtSnapshot.tla).
type snapRegistry struct {
	mu      sync.Mutex
	next    uint64
	live    map[uint64]*Snapshot
	cutting map[string]uint64
}

// Snapshot creates a read view of the branch.
func (b *Branch) Snapshot(ctx context.Context, o SnapshotOptions) (*Snapshot, error) {
	for try := 0; ; try++ {
		s, err := b.snapshot(ctx, o)
		// The segment holding At's frame may have been replaced by a base
		// (and deleted, with no grace) since the log state was read: look
		// again at the current one.
		if err == nil || try >= 3 || !errors.Is(err, store.ErrNotFound) {
			return s, err
		}
	}
}

func (b *Branch) snapshot(ctx context.Context, o SnapshotOptions) (*Snapshot, error) {
	w, err := b.db.branchWriter(b.name)
	if err != nil {
		return nil, err
	}
	w.v.mu.RLock()
	head := w.v.head
	w.v.mu.RUnlock()
	at := o.At
	if at == 0 {
		at = head
	}
	if at > head {
		return nil, fmt.Errorf("%w: %d > head %d", ErrBadLSN, at, head)
	}
	if at > 0 && !w.l.canFold(at) {
		return nil, fmt.Errorf("%w: LSN %d", ErrSnapshotTooOld, at)
	}
	now := o.Now
	if now == 0 {
		if now, err = frameTS(ctx, w.l, at); errors.Is(err, ErrCompacted) {
			return nil, fmt.Errorf("%w: %v", ErrSnapshotTooOld, err)
		} else if err != nil {
			return nil, err
		}
	}
	var logs []string
	for x := w.l; x != nil; x = x.parent {
		logs = append(logs, x.meta.ID)
	}
	s := &Snapshot{db: b.db, branch: b.name, at: at, now: now, created: b.db.opts.Clock(), logs: logs}
	r := &b.db.snaps
	r.mu.Lock()
	defer r.mu.Unlock()
	if at > 0 && !w.l.canFold(at) {
		return nil, fmt.Errorf("%w: LSN %d", ErrSnapshotTooOld, at)
	}
	for _, id := range logs {
		if c := r.cutting[id]; at < c {
			return nil, fmt.Errorf("%w: LSN %d < compaction cut %d", ErrSnapshotTooOld, at, c)
		}
	}
	if r.live == nil {
		r.live = map[uint64]*Snapshot{}
	}
	r.next++
	s.id = r.next
	r.live[s.id] = s
	return s, nil
}

// Snapshot creates a read view of main.
func (db *DB) Snapshot(ctx context.Context, o SnapshotOptions) (*Snapshot, error) {
	return db.Branch("main").Snapshot(ctx, o)
}

// LSN returns the snapshot's LSN.
func (s *Snapshot) LSN() uint64 { return s.at }

// Now returns the snapshot's read time (compared with expiries).
func (s *Snapshot) Now() int64 { return s.now }

// Release unpins the snapshot; later reads fail with ErrSnapshotReleased.
// Idempotent.
func (s *Snapshot) Release() {
	r := &s.db.snaps
	r.mu.Lock()
	delete(r.live, s.id)
	r.mu.Unlock()
}

// check reports why the snapshot can no longer be read, if it cannot.
func (s *Snapshot) check(l *logState) error {
	r := &s.db.snaps
	r.mu.Lock()
	_, ok := r.live[s.id]
	r.mu.Unlock()
	if !ok {
		return ErrSnapshotReleased
	}
	if s.db.opts.Clock()-s.created > int64(s.db.opts.SnapshotMaxAge) {
		return fmt.Errorf("%w: older than %v", ErrSnapshotTooOld, s.db.opts.SnapshotMaxAge)
	}
	if s.at > 0 && !l.canFold(s.at) {
		return fmt.Errorf("%w: LSN %d", ErrSnapshotTooOld, s.at)
	}
	return nil
}

// View runs a read-only transaction on the snapshot. Iterators opened in
// fn read the same view; fn may run concurrently with other View calls.
// If fn fails while a compaction replaced the branch's view (objects the
// old view pointed at may be gone after a zero grace period), fn is run
// again on the new view, at the same LSN and read time: like Update's fn,
// it must be free of side effects.
func (s *Snapshot) View(ctx context.Context, fn func(*Tx) error) error {
	for try := 0; ; try++ {
		w, err := s.db.branchWriter(s.branch)
		if err != nil {
			return err
		}
		if err := s.check(w.l); err != nil {
			return err
		}
		v, err := s.db.viewForLSN(ctx, w.l, w.v, s.at)
		if err != nil {
			return err
		}
		tx := &Tx{db: s.db, v: v, at: s.at, lazyCtx: ctx, lazyLog: w.l, now: s.now, nowOK: true}
		ferr := fn(tx)
		// A compaction that passed an aged snapshot during fn may have
		// changed what fn read: report it rather than a silently mixed view.
		if err := s.check(w.l); err != nil {
			return err
		}
		if ferr == nil {
			return nil
		}
		if w2, err := s.db.branchWriter(s.branch); try >= 3 || err != nil || w2.v == w.v {
			return ferr
		}
	}
}

// plan records cut as the planned base LSN of log id unless a live,
// young-enough snapshot reading id is below it. ok = false: the cut is
// held back (the caller skips this log this round).
func (r *snapRegistry) plan(id string, cut uint64, clock func() int64, maxAge int64) (ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var now int64
	if len(r.live) > 0 {
		now = clock() // only then: a fake clock's sequence stays as before
	}
	for _, s := range r.live {
		if now-s.created > maxAge || s.at >= cut {
			continue
		}
		for _, l := range s.logs {
			if l == id {
				return false
			}
		}
	}
	if r.cutting == nil {
		r.cutting = map[string]uint64{}
	}
	r.cutting[id] = max(r.cutting[id], cut)
	return true
}

// LiveSnapshots returns how many snapshots are registered (released ones
// are not; aged ones are until released).
func (db *DB) LiveSnapshots() int {
	db.snaps.mu.Lock()
	defer db.snaps.mu.Unlock()
	return len(db.snaps.live)
}
