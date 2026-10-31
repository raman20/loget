// Package counter is the reference example of a derived LogEt index
// (docs/index-authoring.md walks through it).
//
// A counter groups items under a name and gives each item a value, say
// poll "lunch", voter "ana", choice "tacos". Its read API answers "how
// many items of a counter hold value v" without scanning the items:
// the index keeps the counts up to date as it folds the log.
//
// Items are records in space "ctr:<name>" (key = item, value = the
// item's value). They are written with the core record API, so they get
// transactions, branches, time travel, compaction and crypto-shredding
// from the engine. The index only maintains the counts.
//
//	db, _ := loget.Open(ctx, url, loget.WithIndex(counter.Kind()))
//	db.Update(ctx, func(tx *loget.Tx) error {
//		return counter.Set(tx, "lunch", "ana", "tacos", loget.Subject("ana"))
//	})
//	db.View(ctx, func(tx *loget.Tx) error {
//		n, _ := counter.Count(tx, "lunch", "tacos") // 1
//	})
package counter

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strings"

	"github.com/raman20/loget"
	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// Name is the index name (Tx.Index) and Prefix the space prefix it owns.
const (
	Name   = "counter"
	Prefix = "ctr:"
)

// Version is the index's fold and snapshot format version. A var only so
// the tests can bump it.
var Version uint32 = 1

// Kind is the index kind to register with loget.WithIndex.
func Kind() index.Kind {
	return index.Kind{Name: Name, Version: Version, Spaces: []string{Prefix}, New: newIndex, Restore: restore}
}

// Set gives item of counter name the value val (replacing its old one).
// opts may carry loget.Subject: shredding the subject removes the item.
func Set(tx *loget.Tx, name, item, val string, opts ...loget.WriteOpt) error {
	return tx.PutRecord(Prefix+name, item, []byte(val), opts...)
}

// Remove deletes item from counter name.
func Remove(tx *loget.Tx, name, item string) error {
	return tx.DeleteRecord(Prefix+name, item)
}

// Count returns how many items of counter name hold val. In an Update
// it declares the whole counter read, so a concurrent change to any of
// its items makes the commit fail with loget.ErrConflict.
func Count(tx *loget.Tx, name, val string) (n int, err error) {
	tx.ReadPrefix(Prefix+name, "")
	err = tx.Index(Name, func(l index.Index) error {
		n = l.(*state).counts[Prefix+name][val]
		return nil
	})
	return n, err
}

// Counts returns every value of counter name with its count (values
// with no items are absent). The map is the caller's.
func Counts(tx *loget.Tx, name string) (map[string]int, error) {
	tx.ReadPrefix(Prefix+name, "")
	out := map[string]int{}
	err := tx.Index(Name, func(l index.Index) error {
		for v, n := range l.(*state).counts[Prefix+name] {
			out[v] = n
		}
		return nil
	})
	return out, err
}

// state is the index. items is what it folded (needed to move an item's
// count when its value changes, and to drop a shredded subject); counts
// is the answer it serves. Both depend only on the live records, which
// is what lets the index survive compaction (index.Kind).
type state struct {
	items  map[string]map[string]item // space -> item key -> item
	counts map[string]map[string]int  // space -> value -> live items
}

type item struct{ val, subject string }

func newIndex() index.Index {
	return &state{items: map[string]map[string]item{}, counts: map[string]map[string]int{}}
}

func (*state) Name() string { return Name }

// Apply folds one committed transaction. It sees every op of the entry,
// including other indexes' spaces and empty slots (Kind 0) of a
// compaction base; it skips all of those.
func (s *state) Apply(e *index.Entry, env index.Env) {
	for i, op := range e.Ops {
		if !strings.HasPrefix(op.Space, Prefix) {
			continue
		}
		switch op.Kind {
		case format.OpPut:
			if p, ok := env.Open(e, i); ok {
				s.set(op.Space, op.Key, item{string(p), op.Subject})
			} else {
				s.del(op.Space, op.Key) // shredded subject: as if never written
			}
		case format.OpDel:
			s.del(op.Space, op.Key)
		}
	}
}

func (s *state) set(space, key string, it item) {
	s.del(space, key)
	if s.items[space] == nil {
		s.items[space], s.counts[space] = map[string]item{}, map[string]int{}
	}
	s.items[space][key] = it
	s.counts[space][it.val]++
}

func (s *state) del(space, key string) {
	it, ok := s.items[space][key]
	if !ok {
		return
	}
	delete(s.items[space], key)
	if s.counts[space][it.val]--; s.counts[space][it.val] == 0 {
		delete(s.counts[space], it.val)
	}
	if len(s.items[space]) == 0 {
		delete(s.items, space)
		delete(s.counts, space)
	}
}

// DropSubject removes every item written under subject, exactly as if
// those writes had been redacted from the start.
func (s *state) DropSubject(subject string) {
	for space, m := range s.items {
		for key, it := range m {
			if it.subject == subject {
				s.del(space, key)
			}
		}
	}
}

// Snapshot encodes the items, sorted, so equal states give equal bytes.
// The counts are derived from the items and not stored.
func (s *state) Snapshot() []byte {
	var b []byte
	str := func(x string) { b = binary.AppendUvarint(b, uint64(len(x))); b = append(b, x...) }
	for _, space := range sortedKeys(s.items) {
		m := s.items[space]
		for _, key := range sortedKeys(m) {
			str(space)
			str(key)
			str(m[key].val)
			str(m[key].subject)
		}
	}
	return b
}

var errSnapshot = errors.New("counter: bad snapshot")

func restore(b []byte) (index.Index, error) {
	s := newIndex().(*state)
	str := func() (string, error) {
		n, w := binary.Uvarint(b)
		if w <= 0 || w != len(binary.AppendUvarint(nil, n)) || n > uint64(len(b)-w) { // canonical lengths only
			return "", errSnapshot
		}
		x := string(b[w : w+int(n)])
		b = b[w+int(n):]
		return x, nil
	}
	var last [2]string
	for len(b) > 0 {
		var f [4]string
		for i := range f {
			x, err := str()
			if err != nil {
				return nil, err
			}
			f[i] = x
		}
		// strictly increasing (space, key): what Snapshot writes, so a
		// restored index re-snapshots to the same bytes
		if (last != [2]string{}) && (f[0] < last[0] || f[0] == last[0] && f[1] <= last[1]) || !strings.HasPrefix(f[0], Prefix) {
			return nil, errSnapshot
		}
		last = [2]string{f[0], f[1]}
		s.set(f[0], f[1], item{f[2], f[3]})
	}
	return s, nil
}

// Digest hashes the full state (the engine compares it with a replay).
func (s *state) Digest() [32]byte { return sha256.Sum256(s.Snapshot()) }

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
