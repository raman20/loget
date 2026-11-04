package loget

import (
	"bytes"
	"context"
	"fmt"

	"github.com/raman20/loget/format"
)

// Batch is an atomic group of blind writes (design 26): puts and deletes
// of KV keys that commit together as one log frame, at one LSN, or not at
// all. A batch reads nothing, so it never conflicts and is never retried;
// a transaction that read a key the batch writes conflicts with it as
// with any other commit. The zero value is an empty batch. Not safe for
// concurrent use.
type Batch struct {
	ops   []format.Op
	bytes int // estimated encoded size of ops
}

// batchOpOverhead estimates the encoding of one op besides its strings
// (kind, lengths, value offset). An over-estimate: the writer checks the
// exact encoded size at commit.
const batchOpOverhead = 16

// frameOverhead is a frame's encoding besides its ops (LSN, TS, Prev, count).
const frameOverhead = 64

func (b *Batch) add(op format.Op) error {
	if n := max(len(op.Key), len(op.Subject)); n > MaxKeyBytes {
		return fmt.Errorf("%w: key or subject of %d bytes (max %d)", ErrTooLarge, n, MaxKeyBytes)
	}
	sz := len(op.Key) + len(op.Value) + len(op.Space) + len(op.Subject) + batchOpOverhead
	if len(b.ops)+1 > MaxTxOps || frameOverhead+b.bytes+sz > MaxTxBytes {
		return fmt.Errorf("%w: batch would hold %d ops, about %d bytes (limits %d ops, %d bytes)",
			ErrTooLarge, len(b.ops)+1, frameOverhead+b.bytes+sz, MaxTxOps, MaxTxBytes)
	}
	op.Value = bytes.Clone(op.Value) // the caller may reuse its buffer
	b.ops = append(b.ops, op)
	b.bytes += sz
	return nil
}

// Put adds key = val. ErrTooLarge if the batch would exceed MaxTxOps or
// (about) MaxTxBytes; the batch is unchanged then.
func (b *Batch) Put(key string, val []byte, opts ...WriteOpt) error {
	op := format.Op{Kind: format.OpPut, Space: spaceKV, Key: key, Value: val}
	for _, o := range opts {
		o(&op)
	}
	return b.add(op)
}

// Delete adds a delete of key (a no-op at commit if it is absent).
func (b *Batch) Delete(key string) error {
	return b.add(format.Op{Kind: format.OpDel, Space: spaceKV, Key: key})
}

// Len returns the number of writes in the batch.
func (b *Batch) Len() int { return len(b.ops) }

// Size returns the batch's estimated encoded size in bytes.
func (b *Batch) Size() int { return frameOverhead + b.bytes }

// Reset empties the batch for reuse.
func (b *Batch) Reset() { b.ops, b.bytes = b.ops[:0], 0 }

// Apply commits b on main; see Branch.Apply.
func (db *DB) Apply(ctx context.Context, b *Batch) (uint64, error) {
	return db.Branch("main").Apply(ctx, b)
}

// Apply commits b atomically on the branch and returns its LSN: a
// snapshot before it sees none of its writes, one at or after it sees all
// of them (later writes of the same key in b win). An empty batch commits
// nothing and returns the head. The batch may be reused once Apply
// returns; on error nothing was committed.
func (br *Branch) Apply(ctx context.Context, b *Batch) (uint64, error) {
	ops := b.ops
	return br.UpdateLSN(ctx, func(tx *Tx) error {
		if !tx.writable {
			return ErrReadOnly
		}
		for _, op := range ops {
			if op.Subject != "" && tx.db.keys.isShredded(SubjectHash(op.Subject)) {
				return ErrShredded
			}
		}
		// Blind writes: no reads, so the commit cannot conflict. The ops
		// were copied when staged; the writer does not modify them.
		tx.ops = append(tx.ops[:0], ops...)
		return nil
	})
}
