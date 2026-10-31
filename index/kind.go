package index

import (
	"errors"
	"strconv"
	"strings"
)

// Snapshotter is implemented by an index that can save its state, so the
// engine can restore the state on open instead of replaying the log.
// Snapshot must be canonical: two indexes that folded the same log
// must return the same bytes.
type Snapshotter interface {
	Snapshot() []byte
}

// Kind describes one type of index to the engine. Register a Kind with
// loget.WithIndex, and the engine builds one instance of it for every
// branch view.
//
// Two shapes of index exist:
//
//   - A record store (New == nil) has no state of its own. Its records
//     are the state: the engine's MVCC record store stores them, and
//     its read API is Tx.GetRecord and Tx.ScanRecords on its spaces.
//     KV is a record store.
//   - A derived index (New != nil) folds its spaces' records into its own
//     state with Apply. Its read API runs under Tx.Index. The engine
//     builds the index by replaying records: first the compaction base
//     (the latest Put of every live key in the index's spaces, as
//     synthetic entries), then the log. If the index is a Snapshotter
//     and Restore is set, the engine restores its state from a snapshot
//     and replays only the log after the snapshot.
//
// Compaction keeps only the latest Put of each (space, key). A derived
// index's state therefore has to be a function of the live records alone,
// not of the order of updates or of overwritten values. TestIndexContract
// in the examples checks this by comparing a fold of the full log with a
// fold of its compacted form.
type Kind struct {
	// Name identifies the index: Tx.Index(name), digests, and snapshot
	// sections. Lowercase letters, digits, '-' and '_'.
	Name string
	// Version labels the index's fold and snapshot format. Bump it when
	// either changes: snapshots written by another version are ignored
	// and the index is rebuilt by replay.
	Version uint32
	// Spaces are the space prefixes this index owns (its declared record
	// types). Writes through Tx.PutRecord / Tx.DeleteRecord are allowed
	// only into owned spaces. A derived index is fed the base records of
	// these spaces on rebuild. Prefixes of different kinds must not
	// overlap.
	Spaces []string
	// New returns an empty derived index. nil means this is a record store.
	New func() Index
	// Restore rebuilds a derived index from its Snapshot bytes. It may
	// be nil; then the index is always rebuilt by replay.
	Restore func(b []byte) (Index, error)
}

// Owns reports whether space belongs to k.
func (k *Kind) Owns(space string) bool {
	for _, p := range k.Spaces {
		if strings.HasPrefix(space, p) {
			return true
		}
	}
	return false
}

// SectionName is the key of k's section in a snapshot.
func (k *Kind) SectionName() string { return k.Name + "@" + strconv.FormatUint(uint64(k.Version), 10) }

// Registry is the set of index kinds of one database.
type Registry struct {
	kinds []Kind
}

func errKind(msg string) error { return errors.New("index: bad kind: " + msg) }

func validName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Register adds k. It rejects a bad or duplicate name, a kind with no
// spaces, an empty space prefix, a prefix that overlaps a registered
// kind's prefix, and Restore without New.
func (r *Registry) Register(k Kind) error {
	if !validName(k.Name) {
		return errKind("name " + strconv.Quote(k.Name))
	}
	if len(k.Spaces) == 0 {
		return errKind(k.Name + " declares no spaces")
	}
	if k.New == nil && k.Restore != nil {
		return errKind(k.Name + " has Restore but no New")
	}
	for _, p := range k.Spaces {
		if p == "" {
			return errKind(k.Name + " declares an empty space prefix")
		}
	}
	for _, o := range r.kinds {
		if o.Name == k.Name {
			return errKind(k.Name + " registered twice")
		}
		for _, p := range k.Spaces {
			for _, q := range o.Spaces {
				if strings.HasPrefix(p, q) || strings.HasPrefix(q, p) {
					return errKind(k.Name + " space " + strconv.Quote(p) + " overlaps " + o.Name + " space " + strconv.Quote(q))
				}
			}
		}
	}
	k.Spaces = append([]string(nil), k.Spaces...)
	r.kinds = append(r.kinds, k)
	return nil
}

// Kinds returns the registered kinds in registration order.
func (r *Registry) Kinds() []Kind { return append([]Kind(nil), r.kinds...) }

// Owner returns the kind that owns space.
func (r *Registry) Owner(space string) (*Kind, bool) {
	for i := range r.kinds {
		if r.kinds[i].Owns(space) {
			return &r.kinds[i], true
		}
	}
	return nil, false
}

// Derived returns the derived kinds (New != nil) in registration order.
func (r *Registry) Derived() []Kind {
	var out []Kind
	for _, k := range r.kinds {
		if k.New != nil {
			out = append(out, k)
		}
	}
	return out
}
