package index

import (
	"fmt"
	"testing"

	"github.com/raman20/loget/format"
	"pgregory.net/rapid"
)

// testEnv: identity "encryption" + a shredded set.
type testEnv map[string]bool

func (t testEnv) Shredded(s string) bool { return t[s] }
func (t testEnv) Open(e *Entry, i int) ([]byte, bool) {
	op := e.Ops[i]
	if op.Subject != "" && t[op.Subject] {
		return nil, false
	}
	return op.Value, true
}

var spaces = []string{"kv", "s2"}

func genLog(t *rapid.T) []*Entry {
	n := rapid.IntRange(0, 40).Draw(t, "entries")
	var log []*Entry
	for lsn := uint64(1); lsn <= uint64(n); lsn++ {
		e := &Entry{Log: "L1", LSN: lsn, TS: int64(lsn)}
		nops := rapid.IntRange(1, 4).Draw(t, "nops")
		for j := 0; j < nops; j++ {
			sp := rapid.SampledFrom(spaces).Draw(t, "space")
			op := format.Op{Space: sp, Key: fmt.Sprint("k", rapid.IntRange(0, 4).Draw(t, "key")),
				Subject: rapid.SampledFrom([]string{"", "", "a", "b"}).Draw(t, "subj")}
			op.Kind = rapid.SampledFrom([]format.Kind{format.OpPut, format.OpPut, format.OpDel}).Draw(t, "kind")
			if op.Kind == format.OpDel {
				op.Subject = ""
			}
			op.Value = []byte(fmt.Sprint("v", lsn, ".", j))
			if op.Kind == format.OpDel {
				op.Value = nil
			} else {
				op.Exp = rapid.SampledFrom([]int64{0, 0, 0, int64(lsn) + 3}).Draw(t, "exp") // design 27
			}
			e.Ops = append(e.Ops, op)
		}
		log = append(log, e)
	}
	return log
}

func indexes() []Index { return []Index{NewKV()} }

func fold(log []*Entry, env Env) []Index {
	ls := indexes()
	for _, e := range log {
		for _, l := range ls {
			l.Apply(e, env)
		}
	}
	return ls
}

func digests(ls []Index) string {
	s := ""
	for _, l := range ls {
		d := l.Digest()
		s += fmt.Sprintf("%s=%x ", l.Name(), d[:6])
	}
	return s
}

// Replay determinism: the same log folds to bit-identical state.
func TestPropFoldDeterministic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		env := testEnv{"b": rapid.Bool().Draw(t, "shredB")}
		if a, b := digests(fold(log, env)), digests(fold(log, env)); a != b {
			t.Fatalf("non-deterministic fold: %s vs %s", a, b)
		}
	})
}

// Checkpoint = fold prefix: Load(Snapshot(fold(log[:k]))) then fold(log[k:])
// is identical to fold(log).
func TestPropSnapshotPlusTailEqualsFold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		k := rapid.IntRange(0, len(log)).Draw(t, "k")
		env := testEnv{}
		kv := NewKV()
		for _, e := range log[:k] {
			kv.Apply(e, env)
		}
		kv2, err := LoadKV(kv.Snapshot(), kv.Inline())
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range log[k:] {
			kv2.Apply(e, env)
		}
		full := fold(log, env)
		if kv2.Digest() != full[0].Digest() {
			t.Fatal("snapshot+tail != fold")
		}
	})
}

// Crypto-shred: DropSubject(s) on a live index == folding the log with s
// redacted from the very beginning, for every index.
func TestPropDropSubjectEqualsRedactedFold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		live := fold(log, testEnv{})
		for _, l := range live {
			l.DropSubject("a")
		}
		redacted := fold(log, testEnv{"a": true})
		if a, b := digests(live), digests(redacted); a != b {
			t.Fatalf("DropSubject != fold(redact):\n%s\n%s", a, b)
		}
	})
}

// Refinement of the KV index against a naive reference model built from the
// log prefix ≤ at (MVCC reads / time travel).
func TestPropIndexesRefineModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		env := testEnv{"b": rapid.Bool().Draw(t, "shredB")}
		ls := fold(log, env)
		kv := ls[0].(*KV)
		at := uint64(rapid.IntRange(0, len(log)).Draw(t, "at"))
		// reference model
		mkv := map[string]string{}
		for _, e := range log {
			if e.LSN > at {
				break
			}
			for _, op := range e.Ops {
				red := op.Subject != "" && env[op.Subject]
				switch op.Kind {
				case format.OpPut:
					if red {
						delete(mkv, op.Space+"/"+op.Key)
					} else {
						mkv[op.Space+"/"+op.Key] = string(op.Value)
					}
				case format.OpDel:
					delete(mkv, op.Space+"/"+op.Key)
				}
			}
		}
		for _, sp := range spaces {
			for i := 0; i < 5; i++ {
				k := fmt.Sprint("k", i)
				v, ok := kv.Get(sp, k, at)
				want, wok := mkv[sp+"/"+k]
				if ok != wok || (ok && string(v.Val) != want) {
					t.Fatalf("KV %s/%s@%d: got %q,%v want %q,%v", sp, k, at, v.Val, ok, want, wok)
				}
			}
		}
	})
}
