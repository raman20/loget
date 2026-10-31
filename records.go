package loget

import (
	"fmt"
	"strings"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// The record API is how every index, KV included, reads and writes the
// log (docs/index-authoring.md). A record is (space, key) → value. Each
// space is owned by one registered index kind (index.Kind.Spaces), and
// writes are accepted only into owned spaces. Spaces starting with "_"
// are reserved for the engine.

// Record is one live record: a key and its value.
type Record struct {
	Key   string
	Value []byte
}

// checkSpace rejects a write into a space no registered index owns.
func (tx *Tx) checkSpace(space string) error {
	if !spaceOK(space) {
		return fmt.Errorf("%w: space %q", ErrBadValue, space)
	}
	if _, ok := tx.db.reg.Owner(space); !ok {
		return fmt.Errorf("%w: space %q is not owned by a registered index (WithIndex)", ErrBadValue, space)
	}
	return nil
}

// PutRecord sets (space, key) = val in this transaction. space must be
// owned by a registered index kind.
func (tx *Tx) PutRecord(space, key string, val []byte, opts ...WriteOpt) error {
	if err := tx.checkSpace(space); err != nil {
		return err
	}
	return tx.write(format.Op{Kind: format.OpPut, Space: space, Key: key, Value: val}, opts)
}

// DeleteRecord deletes (space, key) in this transaction.
func (tx *Tx) DeleteRecord(space, key string) error {
	if err := tx.checkSpace(space); err != nil {
		return err
	}
	return tx.write(format.Op{Kind: format.OpDel, Space: space, Key: key}, nil)
}

// GetRecord reads (space, key) at the snapshot, including this
// transaction's own writes, and adds the key to the read set.
// It returns ErrNotFound for a missing, deleted or shredded record.
func (tx *Tx) GetRecord(space, key string) ([]byte, error) { return tx.get(space, key) }

// ScanRecords returns the live records of space whose key starts with
// prefix, sorted by key, including this transaction's own writes. It
// adds the prefix to the read set: any later write under it conflicts.
// It is RangeRecords with IterOptions{Prefix: prefix}.
func (tx *Tx) ScanRecords(space, prefix string) ([]Record, error) {
	return tx.RangeRecords(space, IterOptions{Prefix: prefix})
}

// ReadKey adds (space, key) to the read set without reading it: commit
// fails with ErrConflict if a transaction committed after At() wrote it.
// A derived index's read API calls it for every record its answer
// depends on. It is a no-op in read-only transactions.
func (tx *Tx) ReadKey(space, key string) {
	if tx.writable {
		tx.reads[rkey{space, key}] = true
	}
}

// ReadPrefix adds every key of space starting with prefix to the read
// set ("" = the whole space). See ReadKey.
func (tx *Tx) ReadPrefix(space, prefix string) {
	if tx.writable {
		pe := index.PrefixEnd(prefix)
		tx.ranges = append(tx.ranges, &keyRange{space: space, lo: prefix, hi: pe, inf: pe == ""})
	}
}

// Index runs fn with the derived index called name, under its read lock.
// fn must not keep the index or anything it returns after it returns, and
// must not call back into the database.
//
// What fn sees: in a read-only transaction (View, ViewAt, ViewLog),
// exactly the state at At(); time travel below the head rebuilds the
// index by replay on first use. In an Update, the state at the branch
// head, which may be newer than At(): that is safe only for reads the
// caller declares with ReadKey / ReadPrefix (a newer write to them makes
// the commit fail with ErrConflict). Writes of this transaction are not
// visible to fn: an index folds committed records only.
func (tx *Tx) Index(name string, fn func(index.Index) error) error {
	v, err := tx.indexView()
	if err != nil {
		return err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	l, ok := v.derivedIndex(name)
	if !ok {
		return fmt.Errorf("%w: index %q is not registered", ErrNotFound, name)
	}
	return fn(l)
}

// indexView returns the view derived indexes should be read from: the live
// view when reading at its head or in an Update, else an exact replay at
// tx.at.
func (tx *Tx) indexView() (*view, error) {
	tx.v.mu.RLock()
	h := tx.v.head
	tx.v.mu.RUnlock()
	if tx.lazyLog == nil || h == tx.at || tx.writable {
		return tx.v, nil
	}
	if tx.eph == nil {
		v, err := tx.db.buildView(tx.lazyCtx, tx.lazyLog, tx.at, nil)
		if err != nil {
			return nil, err
		}
		tx.eph = v
	}
	return tx.eph, nil
}

// WithIndex registers an index kind (index.Kind) with the database. Every
// branch view gets one instance of it. Register the same kinds, in any
// order, every time the database is opened: records in spaces of an
// unregistered kind stay in the log but cannot be written or read
// through an index. Open fails if two kinds' spaces overlap.
func WithIndex(k index.Kind) Option {
	return func(o *Options) { o.Indexes = append(o.Indexes, k) }
}

// kvKind is the built-in KV index: a record store on space "kv" (kv.go).
var kvKind = index.Kind{Name: "kv", Spaces: []string{spaceKV}}

// newRegistry registers KV and then the kinds of WithIndex.
func newRegistry(kinds []index.Kind) (*index.Registry, error) {
	r := &index.Registry{}
	for _, k := range append([]index.Kind{kvKind}, kinds...) {
		for _, p := range k.Spaces {
			if strings.HasPrefix(p, "_") {
				return nil, fmt.Errorf("%w: index %s: space prefix %q: spaces starting with \"_\" belong to the engine", ErrBadValue, k.Name, p)
			}
		}
		if err := r.Register(k); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadValue, err)
		}
	}
	return r, nil
}
