package loget

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strings"

	"github.com/raman20/loget/index"
)

// tally is the engine tests' derived index (examples/counter is the
// documented one; it cannot be imported here without an import cycle).
// It keeps the live value and subject of every record in spaces "t:*"
// and answers "how many live records of a space hold value v". Its
// state depends only on the live records, so it survives compaction.
type tally struct {
	recs map[string]map[string]tallyRec // space -> key -> record
}

type tallyRec struct{ val, subj string }

const tallyPrefix = "t:"

func newTally() index.Index { return &tally{recs: map[string]map[string]tallyRec{}} }

// tallyKind is registered by mustOpen. version is a var so tests can
// bump it.
var tallyVersion uint32 = 1

func tallyKind() index.Kind {
	return index.Kind{Name: "tally", Version: tallyVersion, Spaces: []string{tallyPrefix}, New: newTally, Restore: restoreTally}
}

func (*tally) Name() string { return "tally" }

func (l *tally) Apply(e *index.Entry, env index.Env) {
	for i, op := range e.Ops {
		if !strings.HasPrefix(op.Space, tallyPrefix) {
			continue
		}
		m := l.recs[op.Space]
		if m == nil {
			m = map[string]tallyRec{}
			l.recs[op.Space] = m
		}
		switch op.Kind {
		case 1: // format.OpPut
			if p, ok := env.Open(e, i); ok {
				m[op.Key] = tallyRec{string(p), op.Subject}
			} else {
				delete(m, op.Key) // shredded: as if never written
			}
		case 2: // format.OpDel
			delete(m, op.Key)
		}
	}
}

func (l *tally) DropSubject(s string) {
	for _, m := range l.recs {
		for k, r := range m {
			if r.subj == s {
				delete(m, k)
			}
		}
	}
}

func (l *tally) Snapshot() []byte {
	var b []byte
	str := func(s string) { b = binary.AppendUvarint(b, uint64(len(s))); b = append(b, s...) }
	spaces := make([]string, 0, len(l.recs))
	for sp, m := range l.recs {
		if len(m) > 0 {
			spaces = append(spaces, sp)
		}
	}
	sort.Strings(spaces)
	for _, sp := range spaces {
		keys := make([]string, 0, len(l.recs[sp]))
		for k := range l.recs[sp] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			r := l.recs[sp][k]
			str(sp)
			str(k)
			str(r.val)
			str(r.subj)
		}
	}
	return b
}

var errTally = errors.New("tally: bad snapshot")

func restoreTally(b []byte) (index.Index, error) {
	l := newTally().(*tally)
	str := func() (string, error) {
		n, w := binary.Uvarint(b)
		if w <= 0 || n > uint64(len(b)-w) {
			return "", errTally
		}
		s := string(b[w : w+int(n)])
		b = b[w+int(n):]
		return s, nil
	}
	for len(b) > 0 {
		var f [4]string
		for i := range f {
			s, err := str()
			if err != nil {
				return nil, err
			}
			f[i] = s
		}
		if l.recs[f[0]] == nil {
			l.recs[f[0]] = map[string]tallyRec{}
		}
		l.recs[f[0]][f[1]] = tallyRec{f[2], f[3]}
	}
	return l, nil
}

func (l *tally) Digest() [32]byte { return sha256.Sum256(l.Snapshot()) }

// count is tally's read API: live records of space holding val.
func (l *tally) count(space, val string) int {
	n := 0
	for _, r := range l.recs[space] {
		if r.val == val {
			n++
		}
	}
	return n
}

// tallyCount reads the index in tx and declares the whole space read.
func tallyCount(tx *Tx, space, val string) (n int, err error) {
	tx.ReadPrefix(space, "")
	err = tx.Index("tally", func(l index.Index) error { n = l.(*tally).count(space, val); return nil })
	return
}
