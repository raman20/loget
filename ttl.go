package loget

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// Per-key expiry (docs/design/27-ttl-snapshots.md). An expiry is an
// absolute time in the log's commit-timestamp unit (unix nanoseconds),
// logged with the put. A read decides "expired" against its read time,
// which is the commit timestamp of the frame at the read's snapshot LSN
// (or an explicit time given to a Snapshot), never the wall clock at read
// time: replay, checkpoint-open and live reads agree.

// expBad marks an invalid TTL/ExpiresAt option; tx.write rejects it.
const expBad = math.MinInt64

// TTL makes a put expire d after its commit timestamp: the writer turns it
// into an absolute expiry (commit TS + d) when it assigns the frame's TS.
// d must be > 0. Only puts into the KV space ("kv") may expire.
func TTL(d time.Duration) WriteOpt {
	return func(o *format.Op) {
		if d <= 0 {
			o.Exp = expBad
			return
		}
		o.Exp = -int64(d) // relative; resolved at commit (resolveExp)
	}
}

// ExpiresAt makes a put expire at t (compared with read times, which are
// commit timestamps). t must be after the Unix epoch. A t at or before the
// commit timestamp commits a put that is already invisible.
func ExpiresAt(t time.Time) WriteOpt {
	return func(o *format.Op) {
		if n := t.UnixNano(); n > 0 {
			o.Exp = n
		} else {
			o.Exp = expBad
		}
	}
}

// checkExp validates an op's expiry option.
func checkExp(op *format.Op) error {
	if op.Exp == 0 {
		return nil
	}
	if op.Exp == expBad {
		return fmt.Errorf("%w: TTL must be > 0 and ExpiresAt after 1970", ErrBadValue)
	}
	if op.Kind != format.OpPut || op.Space != spaceKV {
		return fmt.Errorf("%w: only KV puts may expire (space %q)", ErrBadValue, op.Space)
	}
	return nil
}

// resolveExp turns relative expiries (TTL) into absolute ones at commit
// timestamp ts. Called by the writer once the frame's TS is known.
func resolveExp(ops []format.Op, ts int64) {
	for i := range ops {
		if e := ops[i].Exp; e < 0 {
			d := -e
			if ts > math.MaxInt64-d {
				ops[i].Exp = math.MaxInt64
			} else if ops[i].Exp = ts + d; ops[i].Exp <= 0 {
				ops[i].Exp = 1 // before the epoch: already expired
			}
		}
	}
}

// frameTS returns the commit timestamp of the frame at lsn in l's lineage
// (the base's TS at the compaction base, 0 for LSN 0). The head of the
// lineage needs no I/O; older LSNs read one segment.
func frameTS(ctx context.Context, l *logState, lsn uint64) (int64, error) {
	if lsn == 0 {
		return 0, nil
	}
	l.mu.RLock()
	head, own, ts := l.meta.ParentLSN+l.n, l.n, l.lastTS
	l.mu.RUnlock()
	if lsn == head && own > 0 {
		return ts, nil
	}
	if b := l.based(); b != nil && lsn == b.meta.Base.LSN {
		return b.meta.Base.TS, nil
	}
	e, err := l.entryAt(ctx, lsn)
	if err != nil {
		return 0, err
	}
	return e.TS, nil
}

// Now returns the transaction's read time: the time expiries are compared
// with. It is the commit timestamp of the frame at At() unless the
// transaction runs on a Snapshot created with an explicit time.
func (tx *Tx) Now() (int64, error) {
	if !tx.nowOK {
		ctx := tx.lazyCtx
		if ctx == nil {
			ctx = context.Background()
		}
		ts, err := frameTS(ctx, tx.v.log, tx.at)
		if err != nil {
			return 0, err
		}
		tx.now, tx.nowOK = ts, true
	}
	return tx.now, nil
}

// live reports whether a stored version is visible at the read time, and
// for a read-write transaction records the earliest expiry it relied on:
// if a later commit's timestamp reaches it, validation fails (expiry acts
// as a write at that time).
func (tx *Tx) live(v *index.Version) (bool, error) {
	if v.Exp == 0 {
		return true, nil
	}
	now, err := tx.Now()
	if err != nil {
		return false, err
	}
	if v.Expired(now) {
		return false, nil
	}
	if tx.writable && (tx.minExp == 0 || v.Exp < tx.minExp) {
		tx.minExp = v.Exp
	}
	return true, nil
}
