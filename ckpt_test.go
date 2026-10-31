package loget

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
	"pgregory.net/rapid"
)

func TestCkptRoundTrip(t *testing.T) {
	h := ckptHeader{log: "L3", lsn: 42, floor: 7, chain: [32]byte{1, 2, 3}}
	runs := []ckptRunDesc{
		{key: "ckpt/L3/x.r0", meta: index.RunMeta{Len: 10, N: 2, Vers: 3, Piece: 64, Sums: []uint32{9}}},
		{key: "base/L3/y", meta: index.RunMeta{Len: 0}},
	}
	secs := map[string][]byte{"tally@1": []byte("t"), "z@2": {}}
	b := encodeCkpt(h, 64, runs, secs)
	gh, inline, gr, gs, err := decodeCkpt(b)
	if err != nil || gh != h || inline != 64 || fmt.Sprint(gr) != fmt.Sprint(runs) || fmt.Sprint(gs) != fmt.Sprint(secs) {
		t.Fatalf("round trip: %+v %d %+v %v %v", gh, inline, gr, gs, err)
	}
	for i := range b { // every single-byte change is caught
		c := bytes.Clone(b)
		c[i] ^= 0x40
		if _, _, _, _, err := decodeCkpt(c); err == nil {
			t.Fatalf("flip at %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, _, _, _, err := decodeCkpt(b[:n]); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
}

func FuzzDecodeCkpt(f *testing.F) {
	f.Add(encodeCkpt(ckptHeader{log: "L1", lsn: 9}, 64, nil, nil))
	f.Add(encodeCkpt(ckptHeader{log: "L1", lsn: 9, floor: 3}, 0,
		[]ckptRunDesc{{key: "k", meta: index.RunMeta{Len: 5, N: 1, Vers: 1, Sums: []uint32{1}}}}, map[string][]byte{"a@1": []byte("x")}))
	f.Add([]byte(ckptMagic))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, inline, runs, secs, err := decodeCkpt(b)
		if err != nil {
			return
		}
		if again := encodeCkpt(h, inline, runs, secs); !bytes.Equal(again, b) {
			t.Fatalf("accepted bytes that do not re-encode identically")
		}
	})
}

// logCapture collects log lines that contain sub.
func logCapture(sub string, into *[]string) Option {
	return WithLogf(func(f string, a ...any) {
		if s := fmt.Sprintf(f, a...); strings.Contains(s, sub) {
			*into = append(*into, s)
		}
	})
}

// A checkpoint is used only if it is bound to this log's history and
// lineage and every piece checks out; anything else is ignored (replay).
func TestCkptBinding(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(2), WithCheckpointBytes(-1))
	for i := 0; i < 4; i++ {
		db.Update(ctx, func(tx *Tx) error {
			tx.Put("k", []byte{byte('a' + i)})
			return tx.Put(fmt.Sprint(tallyPrefix, i), []byte("1"))
		})
	}
	c := db.Manifest().Logs["L1"].Ckpt
	db.Close()
	if c == nil || len(c.Runs) == 0 {
		t.Fatalf("no checkpoint: %+v", c)
	}
	var logs []string
	open := func() string {
		logs = nil
		db, err := OpenStore(ctx, st, WithCheckpointBytes(-1), WithIndex(tallyKind()), logCapture("ignoring index checkpoint", &logs))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if v, err := db.Get(ctx, "k"); err != nil || string(v) != "d" {
			t.Fatalf("k = %q %v", v, err)
		}
		d1, _, _ := db.IndexDigests("main")
		d2, _ := db.RebuildDigests("main")
		if fmt.Sprint(d1) != fmt.Sprint(d2) {
			t.Fatal("checkpoint+tail != full fold")
		}
		return strings.Join(logs, ";")
	}
	if got := open(); got != "" {
		t.Fatalf("good checkpoint ignored: %s", got)
	}
	desc := ckptDescObj(c.Key)
	orig, _ := st.Get(ctx, desc)
	h, inline, runs, secs, err := decodeCkpt(orig)
	if err != nil {
		t.Fatal(err)
	}
	set := func(k string, b []byte) {
		st.Raw(func(m map[string][]byte) {
			if b == nil {
				delete(m, k)
			} else {
				m[k] = b
			}
		})
	}
	piece := ckptPieceKey(c.Runs[0].Key, 0)
	origPiece, _ := st.Get(ctx, piece)
	flipped := bytes.Clone(origPiece)
	flipped[len(flipped)/2] ^= 1
	for name, bad := range map[string]func(){
		"other chain":  func() { set(desc, encodeCkpt(ckptHeader{h.log, h.lsn, h.floor, [32]byte{9}}, inline, runs, secs)) },
		"other log":    func() { set(desc, encodeCkpt(ckptHeader{"L9", h.lsn, h.floor, h.chain}, inline, runs, secs)) },
		"other lsn":    func() { set(desc, encodeCkpt(ckptHeader{h.log, h.lsn - 1, h.floor, h.chain}, inline, runs, secs)) },
		"other floor":  func() { set(desc, encodeCkpt(ckptHeader{h.log, h.lsn, h.floor + 1, h.chain}, inline, runs, secs)) },
		"other inline": func() { set(desc, encodeCkpt(h, inline+1, runs, secs)) },
		"fewer runs":   func() { set(desc, encodeCkpt(h, inline, runs[1:], secs)) },
		"no desc":      func() { set(desc, nil) },
		"bad piece":    func() { set(piece, flipped) },
		"no piece":     func() { set(piece, nil) },
	} {
		bad()
		if got := open(); !strings.Contains(got, "ignoring index checkpoint") {
			t.Errorf("%s: checkpoint used", name)
		}
		set(desc, orig)
		set(piece, origPiece)
	}
	if got := open(); got != "" {
		t.Fatalf("restored checkpoint ignored: %s", got)
	}
}

// writeLegacySnapshot writes an LGSNAP2 snapshot of log id the way builds
// before design 24 did, so the read path for old snapshots stays tested.
func writeLegacySnapshot(t *testing.T, db *DB, id string) string {
	t.Helper()
	db.mu.RLock()
	w := db.writers[id]
	db.mu.RUnlock()
	w.v.mu.RLock()
	lsn, kv := w.v.head, w.v.kv.Snapshot()
	sections := map[string][]byte{}
	for i, k := range db.reg.Derived() {
		if sn, ok := w.v.derived[i].(index.Snapshotter); ok && k.Restore != nil {
			sections[k.SectionName()] = sn.Snapshot()
		}
	}
	w.v.mu.RUnlock()
	chain, err := w.l.chainAt(ctx, lsn)
	if err != nil {
		t.Fatal(err)
	}
	key := snapKey(id, lsn)
	if err := db.st.PutIfAbsent(ctx, key, encodeSnapshot(snapHeader{log: id, lsn: lsn, chain: chain}, kv, sections)); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.commitManifest(ctx, func(m *Manifest) error {
		m.Logs[id].SnapLSN, m.Logs[id].SnapKey = lsn, key
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return key
}

// Snapshots written before design 24 are still read; the first index
// checkpoint replaces (and garbage-lists) them.
func TestLegacySnapshotRead(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithCheckpointBytes(-1))
	for i := 0; i < 7; i++ {
		db.Update(ctx, func(tx *Tx) error {
			tx.Put(fmt.Sprint("k", i%3), []byte(fmt.Sprint(i)))
			return tx.Put(fmt.Sprint(tallyPrefix, i%2), []byte("1"))
		})
	}
	key := writeLegacySnapshot(t, db, "L1")
	db.Put(ctx, "after", []byte("x"))
	db.Close()
	var logs []string
	db = mustOpen(t, st, WithCheckpointBytes(-1), logCapture("ignoring", &logs))
	if v, err := db.Get(ctx, "k0"); err != nil || string(v) != "6" || len(logs) != 0 {
		t.Fatalf("k0 = %q %v; %v", v, err, logs)
	}
	d1, _, _ := db.IndexDigests("main")
	d2, _ := db.RebuildDigests("main")
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatal("legacy snapshot+tail != full fold")
	}
	db.Close()
	// it was really read: a damaged one is reported and replayed
	good, _ := st.Get(ctx, key)
	st.Raw(func(m map[string][]byte) { m[key] = []byte("garbage") })
	db = mustOpen(t, st, WithCheckpointBytes(-1), logCapture("ignoring snapshot", &logs))
	db.Close()
	if len(logs) != 1 {
		t.Fatalf("damaged legacy snapshot not reported: %v", logs)
	}
	// bound to its log and hash chain; v1 (unbound) snapshots are ignored
	h, kvb, tp, _ := decodeSnapshot(good)
	for name, bad := range map[string][]byte{
		"other chain": encodeSnapshot(snapHeader{log: h.log, lsn: h.lsn, chain: [32]byte{9}}, kvb, tp),
		"other log":   encodeSnapshot(snapHeader{log: "L9", lsn: h.lsn, chain: h.chain}, kvb, tp),
		"other lsn":   encodeSnapshot(snapHeader{log: h.log, lsn: h.lsn - 1, chain: h.chain}, kvb, tp),
		"v1":          append([]byte{8, 0, 0, 0, 0, 0, 0, 0}, good[8:]...),
	} {
		logs = nil
		st.Raw(func(m map[string][]byte) { m[key] = bad })
		db = mustOpen(t, st, WithCheckpointBytes(-1), logCapture("ignoring snapshot", &logs))
		if v, err := db.Get(ctx, "k0"); err != nil || string(v) != "6" || len(logs) != 1 {
			t.Errorf("%s: k0 = %q %v; %v", name, v, err, logs)
		}
		db.Close()
	}
	if _, _, _, err := decodeSnapshot(nil); !errors.Is(err, errSnapshot) {
		t.Fatal(err)
	}
	st.Raw(func(m map[string][]byte) { m[key] = good })
	// a checkpoint supersedes it
	db = mustOpen(t, st)
	db.Close()
	lm := db.Manifest().Logs["L1"]
	if lm.Ckpt == nil || lm.SnapKey != "" {
		t.Fatalf("checkpoint did not supersede the snapshot: %+v", lm)
	}
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "after"); err != nil || string(v) != "x" {
		t.Fatalf("after = %q %v", v, err)
	}
}

// cloneWithoutCkpts copies st and strips every checkpoint and snapshot
// from the newest manifest, so opening the copy replays the log (from the
// base, if any): the reference a checkpoint-open must equal.
func cloneWithoutCkpts(t ftb, st *store.Mem) *store.Mem {
	c := store.NewMem()
	var newest string
	st.Raw(func(src map[string][]byte) {
		c.Raw(func(dst map[string][]byte) {
			for k, v := range src {
				dst[k] = bytes.Clone(v)
				if strings.HasPrefix(k, "manifest/") && k > newest {
					newest = k
				}
			}
		})
	})
	c.Raw(func(m map[string][]byte) {
		man, err := decodeManifest(m[newest])
		if err != nil {
			t.Fatalf("%s: %v", newest, err)
		}
		for _, lm := range man.Logs {
			lm.Ckpt, lm.SnapKey, lm.SnapLSN = nil, "", 0
		}
		m[newest] = encodeManifest(man)
	})
	return c
}

// The property design 24 rests on (spec/LogEtCkpt.tla, CkptEqReplay):
// opening from a checkpoint gives exactly the state a full replay gives,
// on random histories with background and synchronous checkpoints,
// compactions, shreds, clean closes and crashes at any point (including
// in the middle of a background checkpoint).
func TestCkptOpenEqualsReplay(t *testing.T) {
	defer func(p int) { index.PieceBytes = p }(index.PieceBytes)
	rapid.Check(t, func(t *rapid.T) {
		index.PieceBytes = rapid.SampledFrom([]int{64, 512, 64 << 20}).Draw(t, "piece")
		seed := rapid.Int64().Draw(t, "seed")
		inline := rapid.SampledFrom([]int{0, 4, 64}).Draw(t, "inline")
		every := rapid.SampledFrom([]int{0, 0, 1, 3}).Draw(t, "snapEvery")
		ckb := rapid.SampledFrom([]int64{-1, 200, 2000, 1 << 30}).Draw(t, "ckptBytes")
		st := store.NewMem()
		opts := func(o ...Option) []Option {
			return append([]Option{quiet(), WithIndex(tallyKind()), WithRand(rand.New(rand.NewSource(seed))), WithInlineMax(inline),
				WithSnapshotEvery(every), WithCheckpointBytes(ckb)}, o...)
		}
		open := func(st store.Store) *DB {
			db, err := OpenStore(ctx, st, opts()...)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}
		check := func(db *DB, why string) {
			if r, err := db.Verify(ctx); err != nil || len(r.Warnings) != 0 {
				t.Fatalf("%s: verify: %+v %v", why, r.Warnings, err)
			}
			ref, err := OpenStore(ctx, cloneWithoutCkpts(t, st), opts(WithCheckpointBytes(-1))...)
			if err != nil {
				t.Fatalf("%s: reference open: %v", why, err)
			}
			defer ref.Close()
			d1, h1, err1 := db.IndexDigests("main")
			d2, h2, err2 := ref.IndexDigests("main")
			if err1 != nil || err2 != nil || h1 != h2 || fmt.Sprint(d1) != fmt.Sprint(d2) {
				t.Fatalf("%s: checkpoint-open (head %d, %v) != replay-open (head %d, %v)", why, h1, err1, h2, err2)
			}
			for k := 0; k < 6; k++ {
				v1, e1 := db.Get(ctx, fmt.Sprint("k", k))
				v2, e2 := ref.Get(ctx, fmt.Sprint("k", k))
				if !bytes.Equal(v1, v2) || (e1 == nil) != (e2 == nil) {
					t.Fatalf("%s: k%d = %q %v, replay %q %v", why, k, v1, e1, v2, e2)
				}
			}
		}
		db := open(st)
		defer func() { db.Close() }()
		n := rapid.IntRange(1, 60).Draw(t, "n")
		for i := 0; i < n; i++ {
			subj := rapid.SampledFrom([]string{"", "s1", "s2"}).Draw(t, "subj")
			var o []WriteOpt
			if subj != "" {
				o = append(o, Subject(subj))
			}
			k := rapid.IntRange(0, 5).Draw(t, "k")
			val := []byte(fmt.Sprint(i, strings.Repeat("x", rapid.SampledFrom([]int{1, 10, 100}).Draw(t, "size"))))
			switch rapid.IntRange(0, 12).Draw(t, "op") {
			case 0, 1, 2, 3:
				db.Update(ctx, func(tx *Tx) error {
					tx.Put(fmt.Sprint("k", k), val, o...)
					return tx.Put(fmt.Sprint(tallyPrefix, k%2), []byte("1"))
				})
			case 4:
				db.Update(ctx, func(tx *Tx) error { return tx.Delete(fmt.Sprint("k", k)) })
			case 5:
				db.Update(ctx, func(tx *Tx) error { return tx.PutRecord("t:d", fmt.Sprint("d", k), val, o...) })
			case 6:
				if subj != "" {
					db.Shred(ctx, subj)
				}
			case 7:
				if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
					t.Fatalf("compact: %v", err)
				}
			case 8:
				db.writeSnapshotNow(t)
			case 9, 10: // crash, possibly mid-checkpoint
				db.Crash()
				db = open(st)
				check(db, fmt.Sprint("after crash at op ", i))
			case 11, 12:
				db.Close()
				db = open(st)
				check(db, fmt.Sprint("after close at op ", i))
			}
		}
		db.Close()
		db = open(st)
		check(db, "final")
		if r, err := db.Verify(ctx); err != nil || !r.OK || len(r.Warnings) != 0 {
			t.Fatalf("verify: %+v %v", r, err)
		}
	})
}

// writeSnapshotNow checkpoints main synchronously (as SnapshotEvery does).
func (db *DB) writeSnapshotNow(t ftb) {
	w, err := db.branchWriter("main")
	if err != nil {
		return // closed
	}
	if err := db.writeSnapshot(ctx, w.l, w.v); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("checkpoint: %v", err)
	}
}

// A base larger than one piece is stored and read back in pieces (the
// single-object base capped bases at ~15M keys).
func TestShardedBase(t *testing.T) {
	defer func(p int) { index.PieceBytes = p }(index.PieceBytes)
	index.PieceBytes = 256
	st := store.NewMem()
	db := mustOpen(t, st, WithCheckpointBytes(-1))
	for i := 0; i < 200; i++ {
		db.Put(ctx, fmt.Sprintf("key-%04d", i), []byte(fmt.Sprint("v", i)))
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	b := db.Manifest().Logs[db.Manifest().Refs["main"]].Base
	db.Close()
	if b == nil || b.Run == nil || len(b.Run.Sums) < 5 {
		t.Fatalf("base not sharded: %+v", b)
	}
	for p := range b.Run.Sums {
		if o, err := st.Get(ctx, ckptPieceKey(b.Key, p)); err != nil || len(o) > 256 {
			t.Fatalf("piece %d: %d bytes, %v", p, len(o), err)
		}
	}
	db = mustOpen(t, st, WithCheckpointBytes(-1))
	defer db.Close()
	for i := 0; i < 200; i += 17 {
		if v, err := db.Get(ctx, fmt.Sprintf("key-%04d", i)); err != nil || string(v) != fmt.Sprint("v", i) {
			t.Fatalf("key %d = %q %v", i, v, err)
		}
	}
	// a damaged piece is corruption, not a shorter base
	db.Close()
	k := ckptPieceKey(b.Key, 2)
	st.Raw(func(m map[string][]byte) { m[k][10] ^= 1 })
	if _, err := OpenStore(ctx, st, quiet(), WithCheckpointBytes(-1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged base piece: %v, want ErrCorrupt", err)
	}
}
