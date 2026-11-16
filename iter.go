package loget

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sort"

	"github.com/raman20/loget/index"
)

// Range reads (docs/design/22-range-reads.md).

// IterOptions bounds a range read. The zero value reads every key in
// ascending order. Prefix, Start and End combine: the iterator visits the
// keys k with prefix Prefix and Start <= k < End.
type IterOptions struct {
	Prefix  string // only keys starting with Prefix ("" = any)
	Start   string // only keys >= Start ("" = no lower bound)
	End     string // only keys < End ("" = no upper bound)
	Reverse bool   // descending key order
	Limit   int    // stop after Limit keys (0 = no limit)
}

// After returns the options for the next page of a paginated read: pass
// the last key of the current page. Keys are compared bytewise.
func (o IterOptions) After(key string) IterOptions {
	if o.Reverse {
		o.End = key
		if key == "" { // nothing sorts below "": an empty range
			o.Start, o.End = "\x00", "\x00"
		}
	} else {
		o.Start = key + "\x00"
	}
	return o
}

// bounds returns the effective [lo, hi) of o (inf: no upper bound).
func (o IterOptions) bounds() (lo, hi string, inf bool) {
	lo, hi, inf = max(o.Start, o.Prefix), o.End, o.End == ""
	if o.Prefix != "" {
		if pe := index.PrefixEnd(o.Prefix); pe != "" && (inf || pe < hi) {
			hi, inf = pe, false
		}
	}
	return lo, hi, inf
}

// keyRange is a read-set range [lo, hi) of a space ([lo, ∞) if inf).
// Commit fails with ErrConflict if a newer commit wrote a key in it.
type keyRange struct {
	space, lo, hi string
	inf           bool
}

func (r *keyRange) has(k rkey) bool {
	return k.space == r.space && k.key >= r.lo && (r.inf || k.key < r.hi)
}

// Iterator is an ordered range read over a transaction's snapshot: it
// sees exactly the records committed at At(), plus this transaction's own
// writes made before the iterator was created. It holds no lock between
// calls and keeps at most one page of keys (up to 1024) in memory, so it
// can walk a space far larger than RAM; the values of a page are read in
// batches of nearby positions (prefetch.go).
//
// In an Update, the range the iterator has covered so far joins the read
// set: from the start bound through the last key returned, or the whole
// range once Next has returned false without an error. Commit fails with
// ErrConflict if another transaction committed a write in it after At(),
// including an insert into a gap the iterator walked past (a phantom).
// Keys beyond where the caller stopped are not read and do not conflict.
//
// An Iterator is not safe for concurrent use, and must not be used after
// its transaction's function returns.
type Iterator struct {
	tx           *Tx
	space        string
	lo, hi       string // the bounds: [lo, hi), or [lo, ∞) if inf
	inf          bool
	rev          bool
	limit, n     int
	resume       string // index position: next lo (forward) or hi (reverse)
	started      bool   // resume is set
	page         []index.Pair
	vals         [][]byte // prefetched stored bytes of page values (nil: read on use)
	pf           pfScratch
	pi, pageSize int
	more         bool // the index may have keys beyond page
	local        []localKV
	li           int
	track        bool      // add what is read to the read set (Update)
	read         *keyRange // covered so far; nil until the first key
	key          string
	val          []byte
	err          error
	done         bool
}

type localKV struct {
	key string
	*localVal
}

const (
	iterFirstPage = 16
	iterMaxPage   = 1024
)

// Iter returns an iterator over the KV keys selected by o.
func (tx *Tx) Iter(o IterOptions) *Iterator { return tx.IterRecords(spaceKV, o) }

// IterRecords returns an iterator over the records of space selected by o.
func (tx *Tx) IterRecords(space string, o IterOptions) *Iterator {
	lo, hi, inf := o.bounds()
	it := &Iterator{tx: tx, space: space, lo: lo, hi: hi, inf: inf, rev: o.Reverse, limit: max(o.Limit, 0),
		more: true, pageSize: iterFirstPage, track: tx.writable}
	if !inf && lo >= hi {
		it.done = true
		return it
	}
	for k, l := range tx.local {
		if k.space == space && it.in(k.key) {
			it.local = append(it.local, localKV{k.key, l})
		}
	}
	sort.Slice(it.local, func(i, j int) bool { return (it.local[i].key < it.local[j].key) != it.rev })
	return it
}

func (it *Iterator) in(k string) bool { return k >= it.lo && (it.inf || k < it.hi) }

// Next advances to the next key and reports whether there is one. It
// returns false at the end of the range, at the limit, or on an error
// (see Err).
func (it *Iterator) Next() bool {
	it.key, it.val = "", nil
	for !it.done {
		if it.limit > 0 && it.n >= it.limit {
			it.done = true
			break
		}
		if it.pi == len(it.page) && it.more {
			if it.fetch(); it.err != nil {
				break
			}
		}
		var p *index.Pair
		if it.pi < len(it.page) {
			p = &it.page[it.pi]
		}
		var l *localKV
		if it.li < len(it.local) {
			l = &it.local[it.li]
		}
		if p == nil && l == nil {
			it.done = true
			it.coverAll()
			break
		}
		if l != nil && (p == nil || l.key == p.Key || (l.key < p.Key) != it.rev) {
			if p != nil && p.Key == l.key {
				it.pi++ // shadowed by this transaction's write
			}
			it.li++
			if l.del {
				continue
			}
			it.key, it.val = l.key, l.val
		} else {
			var raw []byte
			if it.pi < len(it.vals) {
				raw, it.vals[it.pi] = it.vals[it.pi], nil
			}
			it.pi++
			v, err := it.tx.openWith(p.Version, it.space, p.Key, raw)
			if errors.Is(err, ErrNotFound) {
				continue // shredded
			}
			if err != nil {
				it.err, it.done = err, true
				break
			}
			it.key, it.val = p.Key, v
		}
		it.n++
		it.cover(it.key)
		return true
	}
	it.page = nil
	return false
}

// fetch reads the next page of index keys under the view's read lock.
func (it *Iterator) fetch() {
	lo, hi, inf := it.lo, it.hi, it.inf
	if it.started {
		if it.rev {
			hi, inf = it.resume, false
		} else {
			lo = it.resume
		}
	}
	if !inf && lo >= hi {
		it.page, it.pi, it.more = it.page[:0], 0, false
		return
	}
	if inf {
		hi = "" // KV.Range: no upper bound
	}
	n := it.pageSize
	if it.limit > 0 {
		n = min(n, it.limit-it.n)
	}
	n = max(n, 1)
	page := it.page[:0]
	v := it.tx.v
	// The read time is fixed before the read lock (it may read a segment);
	// only a view that has seen an expiry needs it.
	if !it.tx.nowOK {
		v.mu.RLock()
		may := v.kv.MayExpire() // read under the lock: the writer sets it
		v.mu.RUnlock()
		if may {
			if _, err := it.tx.Now(); err != nil {
				it.err, it.done, it.page, it.pi, it.more = err, true, page, 0, false
				return
			}
		}
	}
	v.mu.RLock()
	v.kv.Range(it.space, lo, hi, it.tx.at, it.rev, func(k string, ver index.Version) bool {
		if ver.Exp != 0 {
			if live, _ := it.tx.live(&ver); !live {
				return true
			}
		}
		page = append(page, index.Pair{Key: k, Version: ver})
		return len(page) < n
	})
	v.mu.RUnlock()
	it.page, it.pi, it.more = page, 0, len(page) == n
	clear(it.vals) // Next hands each value out once and drops it
	it.vals = it.vals[:0]
	if len(page) > 1 {
		it.vals = slices.Grow(it.vals, len(page))[:len(page)]
		ctx := it.tx.lazyCtx
		if ctx == nil {
			ctx = context.Background()
		}
		it.tx.db.prefetch(ctx, v.log, page, it.vals, &it.pf)
	}
	if len(page) > 0 {
		last := page[len(page)-1].Key
		it.started = true
		if it.rev {
			it.resume = last
		} else {
			it.resume = last + "\x00"
		}
	}
	it.pageSize = min(it.pageSize*2, iterMaxPage)
}

// cover extends the read set through k, the key just returned.
func (it *Iterator) cover(k string) {
	if !it.track {
		return
	}
	if it.read == nil {
		it.read = &keyRange{space: it.space, lo: it.lo, hi: it.hi, inf: it.inf}
		it.tx.ranges = append(it.tx.ranges, it.read)
	}
	if it.rev {
		it.read.lo = k
	} else {
		it.read.hi, it.read.inf = k+"\x00", false
	}
}

// coverAll adds the whole range to the read set: the iterator walked it.
func (it *Iterator) coverAll() {
	if !it.track {
		return
	}
	if it.read == nil {
		it.read = &keyRange{}
		it.tx.ranges = append(it.tx.ranges, it.read)
	}
	*it.read = keyRange{space: it.space, lo: it.lo, hi: it.hi, inf: it.inf}
}

// Key returns the current key (valid after Next returned true).
func (it *Iterator) Key() string { return it.key }

// Value returns the current value. The caller owns it.
func (it *Iterator) Value() []byte { return it.val }

// Err returns the error that stopped the iterator, if any.
func (it *Iterator) Err() error { return it.err }

// All returns the rest of the iterator as a range-over-func sequence:
//
//	for k, v := range tx.Iter(o).All() { ... }
//
// Check Err afterwards.
func (it *Iterator) All() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for it.Next() {
			if !yield(it.key, it.val) {
				return
			}
		}
	}
}

// Range returns the records selected by o as a slice (use Limit and
// IterOptions.After to page through a large range).
func (tx *Tx) Range(o IterOptions) ([]KV, error) { return tx.RangeRecords(spaceKV, o) }

// RangeRecords is Range over the records of space.
func (tx *Tx) RangeRecords(space string, o IterOptions) ([]Record, error) {
	var out []Record
	it := tx.IterRecords(space, o)
	for it.Next() {
		out = append(out, Record{it.key, it.val})
	}
	return out, it.Err()
}
