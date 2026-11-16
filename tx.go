package loget

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// Space names: every model is a key space in the one log.
// WriteOpt modifies a write.
type WriteOpt func(*format.Op)

// Subject encrypts the value under the subject's own key, so Shred(subject)
// makes it unreadable everywhere (all branches, snapshots, indexes).
// Subject ids are hashed before they reach the log.
func Subject(s string) WriteOpt { return func(o *format.Op) { o.Subject = s } }

// Tx is a transaction. Update transactions are serializable: reads come
// from a snapshot at LSN At(), and commit fails with ErrConflict if any
// record read (a key, or any key under a scanned prefix) changed since.
// Index read APIs declare what they read with ReadKey and ReadPrefix.
type Tx struct {
	db       *DB
	v        *view
	at       uint64
	writable bool
	ops      []format.Op
	reads    map[rkey]bool
	ranges   []*keyRange // scanned ranges and ReadPrefix (iter.go)
	local    map[rkey]*localVal
	// Exact view at `at` for derived indexes in a time-travel read, built
	// on first use by indexView (read-only branch views only).
	lazyCtx context.Context
	lazyLog *logState
	eph     *view
	// Read time for expiry (ttl.go): set lazily from the frame at `at`
	// unless a Snapshot fixed it. minExp: earliest expiry a read-write
	// transaction saw as live (0 = none), checked at commit.
	now    int64
	nowOK  bool
	minExp int64
}

type localVal struct {
	val []byte
	del bool
}

// At returns the snapshot LSN this transaction reads at.
func (tx *Tx) At() uint64 { return tx.at }

func (tx *Tx) write(op format.Op, opts []WriteOpt) error {
	if !tx.writable {
		return ErrReadOnly
	}
	for _, o := range opts {
		o(&op)
	}
	if err := checkExp(&op); err != nil {
		return err
	}
	if n := max(len(op.Key), len(op.Space), len(op.Subject)); n > MaxKeyBytes {
		return fmt.Errorf("%w: key or name of %d bytes (max %d)", ErrTooLarge, n, MaxKeyBytes)
	}
	if op.Subject != "" && tx.db.keys.isShredded(SubjectHash(op.Subject)) {
		return ErrShredded
	}
	// The caller may reuse its buffer once the call returns; small values
	// are kept inline in the index, so never retain the caller's slice.
	op.Value = bytes.Clone(op.Value)
	tx.ops = append(tx.ops, op)
	if op.Kind == format.OpPut || op.Kind == format.OpDel {
		tx.local[rkey{op.Space, op.Key}] = &localVal{val: op.Value, del: op.Kind == format.OpDel}
	}
	return nil
}

// get reads (space,key) at the snapshot, decrypting subject data.
func (tx *Tx) get(space, key string) ([]byte, error) {
	k := rkey{space, key}
	if l, ok := tx.local[k]; ok {
		if l.del {
			return nil, ErrNotFound
		}
		return l.val, nil
	}
	if tx.writable {
		tx.reads[k] = true
	}
	tx.v.mu.RLock()
	ver, ok := tx.v.kv.Get(space, key, tx.at)
	tx.v.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	if live, err := tx.live(&ver); err != nil {
		return nil, err
	} else if !live {
		return nil, ErrNotFound
	}
	return tx.open(ver, space, key)
}

// open returns the plaintext of a stored version: it reads the bytes from
// the log when the index keeps only their position, and decrypts subject
// data. Shredded data is indistinguishable from absent (ErrNotFound).
func (tx *Tx) open(ver index.Version, space, key string) ([]byte, error) {
	return tx.openWith(ver, space, key, nil)
}

// ctx is the context reads of stored values run under: the View's (a
// cancelled or timed-out read stops waiting on the store), else none.
func (tx *Tx) ctx() context.Context {
	if tx.lazyCtx != nil {
		return tx.lazyCtx
	}
	return context.Background()
}

// openWith is open with the stored bytes already read (by an iterator's
// prefetch, checked against ver.Sum and owned), or nil to read them.
func (tx *Tx) openWith(ver index.Version, space, key string, raw []byte) ([]byte, error) {
	if raw != nil && ver.Ref() && ver.Subject == "" {
		return raw, nil // prefetched (and checked) by the caller, owned
	}
	val, owned := ver.Val, false // ver.Val is shared index state
	var logID string
	if ver.Ref() || ver.Subject != "" {
		x, seq, ok := tx.v.log.source(ver)
		if !ok {
			return nil, fmt.Errorf("%w: LSN %d of %s/%s not in the log", ErrCorrupt, ver.LSN, space, key)
		}
		logID = x.meta.ID
		if ver.Ref() && raw != nil {
			val, owned = raw, true
		} else if ver.Ref() {
			var err error
			if val, err = tx.db.readValue(tx.ctx(), x, seq, ver); err != nil {
				return nil, err
			}
			owned = true // readValue returns a private copy
		}
	}
	if ver.Subject == "" {
		if owned {
			return val, nil
		}
		return bytes.Clone(val), nil // the caller owns what it gets back
	}
	if p, ok := tx.db.keys.open(ver.Subject, val, aad(logID, ver.LSN, int(ver.Op), space, key)); ok {
		return p, nil
	}
	return nil, ErrNotFound
}

// ---- running transactions ----

// Branch is a handle on a named branch ("main" is the default).
type Branch struct {
	db   *DB
	name string
}

// Branch returns a handle on a named branch.
func (db *DB) Branch(name string) *Branch { return &Branch{db, name} }

const maxRetries = 16

// escalateAfter is how many conflicts a transaction may lose optimistically.
// After that its body runs inside the branch's single writer, at the
// applied head and validated before anything else in its group commit, so
// it commits on that attempt: a hot key cannot starve a client. The log is
// unchanged (the same ops are committed); only where fn runs differs.
var escalateAfter = 3 // var only so tests can force the escalated path

// Update runs fn in a serializable read-write transaction on main.
// fn may be re-run on ErrConflict, so it must be free of side effects.
// After escalateAfter lost conflicts fn runs on the branch's writer
// goroutine (so it always commits); it must therefore not block or call
// Update on the same DB.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) error {
	return db.Branch("main").Update(ctx, fn)
}

// View runs fn in a read-only snapshot transaction on main.
func (db *DB) View(ctx context.Context, fn func(*Tx) error) error {
	return db.Branch("main").View(ctx, fn)
}

// UpdateLSN is Update but also returns the commit LSN.
func (b *Branch) UpdateLSN(ctx context.Context, fn func(*Tx) error) (uint64, error) {
	var last error
	for i := 0; i < maxRetries; i++ {
		w, err := b.db.branchWriter(b.name)
		if err != nil {
			return 0, err
		}
		if i >= escalateAfter {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			res := w.submit(&commitReq{attempt: i, exec: fn})
			if errors.Is(res.err, errPaused) { // compaction swapped the writer: not an attempt
				if err := b.db.waitCompaction(ctx); err != nil {
					return 0, err
				}
				i--
				continue
			}
			if res.panicked != nil {
				panic(res.panicked)
			}
			if !errors.Is(res.err, ErrConflict) {
				return res.lsn, res.err
			}
			last = res.err // cannot happen: validated first at the head
			continue
		}
		res, done, err := w.attempt(ctx, fn, i)
		if err != nil || done {
			return res.lsn, err
		}
		if errors.Is(res.err, errPaused) {
			if err := b.db.waitCompaction(ctx); err != nil {
				return 0, err
			}
			i--
			continue
		}
		if errors.Is(res.err, ErrConflict) {
			last = res.err
			if err := conflictBackoff(ctx, i); err != nil {
				return 0, err
			}
			continue
		}
		return res.lsn, res.err
	}
	return 0, last
}

// attempt runs fn once at the view head and submits its writes. The
// snapshot stays registered with the writer until submit returns, so the
// writer keeps every commit it could conflict with (design 23, part 3).
// done: fn wrote nothing (res.lsn is the snapshot) or failed (err).
func (w *writer) attempt(ctx context.Context, fn func(*Tx) error, i int) (res commitRes, done bool, err error) {
	at, shard := w.register()
	defer w.release(at, shard)
	tx := &Tx{db: w.db, v: w.v, at: at, writable: true, reads: map[rkey]bool{}, local: map[rkey]*localVal{}}
	if err := fn(tx); err != nil {
		return commitRes{}, true, err
	}
	if len(tx.ops) == 0 {
		return commitRes{lsn: tx.at}, true, nil
	}
	if err := ctx.Err(); err != nil {
		return commitRes{}, true, err
	}
	return w.submit(&commitReq{at: tx.at, ops: tx.ops, reads: tx.reads, ranges: tx.ranges, minExp: tx.minExp, attempt: i}), false, nil
}

// conflictBackoff waits a jittered, exponentially growing delay after the
// i-th lost conflict (none after the first: the retry already reads the
// winner's commit). It spreads clients contending for one hot key across
// group commits; the cap keeps the worst case of 16 attempts near 40 ms.
func conflictBackoff(ctx context.Context, i int) error {
	if i == 0 {
		return nil
	}
	d := time.Duration(rand.Int64N(int64(50*time.Microsecond) << min(i, 6)))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b *Branch) Update(ctx context.Context, fn func(*Tx) error) error {
	_, err := b.UpdateLSN(ctx, fn)
	return err
}

// View runs a read-only transaction at the branch head.
func (b *Branch) View(ctx context.Context, fn func(*Tx) error) error {
	return b.ViewAt(ctx, 0, fn)
}

// ViewAt runs a read-only transaction at LSN `at` (0 = head): time travel.
func (b *Branch) ViewAt(ctx context.Context, at uint64, fn func(*Tx) error) error {
	w, err := b.db.branchWriter(b.name)
	if err != nil {
		return err
	}
	w.v.mu.RLock()
	head := w.v.head
	w.v.mu.RUnlock()
	if at == 0 {
		at = head
	}
	if at > head {
		return fmt.Errorf("%w: %d > head %d", ErrBadLSN, at, head)
	}
	if f := w.l.floor(); at < f {
		return fmt.Errorf("%w: LSN %d < base %d", ErrCompacted, at, f)
	}
	// Read-only: no read set and no local writes, so no maps to allocate.
	tx := &Tx{db: b.db, v: w.v, at: at, lazyCtx: ctx, lazyLog: w.l}
	return fn(tx)
}

// Head returns the branch head LSN.
func (b *Branch) Head() (uint64, error) {
	w, err := b.db.branchWriter(b.name)
	if err != nil {
		return 0, err
	}
	w.v.mu.RLock()
	defer w.v.mu.RUnlock()
	return w.v.head, nil
}

// Name returns the branch name.
func (b *Branch) Name() string { return b.name }
