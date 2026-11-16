package loget

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Index checkpoints (docs/design/24-open-checkpoints.md,
// spec/LogEtCkpt.tla) let open skip replaying a log: the record store's
// runs are stored as they are, in pieces, and named by a descriptor that
// the manifest points at (LogMeta.Ckpt). Runs are immutable, so a later
// checkpoint names the runs an earlier one stored instead of storing them
// again; only the hot set and runs made by merges since are new. Like a
// snapshot, a checkpoint is a cache: one that does not check out is
// ignored (with a log line) and the log is replayed instead.
//
// Objects:
//
//	ckpt/<log>/<lsn:020>-e<epoch>-<nonce>.ckpt             descriptor
//	ckpt/<log>/<lsn:020>-e<epoch>-<nonce>.r<i>.<p:06>.run  piece p of run i
//
// Descriptor (formats.md):
//
//	magic "LGCKPT1\n" | lsn u64 | floor u64 | chain [32]byte | idLen u8 | log id
//	| inline u32 | nRuns u32
//	| { keyLen u16 | key | len u64 | keys u64 | versions u64 | piece u32 | nSums u32 | sums u32* }*nRuns
//	| nSections u32 | { nameLen u8 | name | len u64 | bytes }*   (sorted by name)
//	| crc32c u32 (over everything before it)
//
// Runs are newest first (the record store's order); the first may be the
// hot set. floor is the lineage's compaction base LSN the record store was
// built on: a checkpoint is used only on that base, at that hash-chain head.

const ckptMagic = "LGCKPT1\n"

// CkptMeta locates a log's newest index checkpoint. Runs lists every run
// object the descriptor names, with its piece count, so GC never reads a
// descriptor.
type CkptMeta struct {
	LSN   uint64    `json:"lsn"`
	Floor uint64    `json:"floor"` // compaction base LSN of the lineage it was built on
	Key   string    `json:"key"`   // descriptor: Key + ".ckpt"
	Runs  []CkptRun `json:"runs"`
}

// CkptRun is one run object of a checkpoint.
type CkptRun struct {
	Key    string `json:"key"`
	Pieces int    `json:"pieces"`
}

func ckptDescObj(key string) string         { return key + ".ckpt" }
func ckptPieceKey(run string, p int) string { return fmt.Sprintf("%s.%06d.run", run, p) }
func ckptRunKey(key string, i int) string   { return key + ".r" + strconv.Itoa(i) }
func ckptPrefix(log string) string          { return "ckpt/" + log + "/" }
func ckptObjKey(log string, lsn, ep uint64, nonce []byte) string {
	return fmt.Sprintf("ckpt/%s/%020d-e%d-%s", log, lsn, ep, hex.EncodeToString(nonce))
}

// ckptObjects lists every object of checkpoint c.
func ckptObjects(c *CkptMeta) []string {
	keys := []string{ckptDescObj(c.Key)}
	for _, r := range c.Runs {
		for p := 0; p < r.Pieces; p++ {
			keys = append(keys, ckptPieceKey(r.Key, p))
		}
	}
	return keys
}

// ckptEpoch parses the epoch from an object under ckpt/<log>/.
func ckptEpoch(obj string) (uint64, bool) {
	name := obj[strings.LastIndexByte(obj, '/')+1:]
	name, _, _ = strings.Cut(name, ".")
	f := strings.Split(name, "-")
	if len(f) != 3 || !strings.HasPrefix(f[1], "e") {
		return 0, false
	}
	ep, err := strconv.ParseUint(f[1][1:], 10, 64)
	return ep, err == nil
}

type ckptHeader struct {
	log   string
	lsn   uint64
	floor uint64
	chain [32]byte
}

type ckptRunDesc struct {
	key  string
	meta index.RunMeta
}

func encodeCkpt(h ckptHeader, inline int, runs []ckptRunDesc, sections map[string][]byte) []byte {
	b := append([]byte(nil), ckptMagic...)
	b = binary.LittleEndian.AppendUint64(b, h.lsn)
	b = binary.LittleEndian.AppendUint64(b, h.floor)
	b = append(b, h.chain[:]...)
	b = append(b, byte(len(h.log)))
	b = append(b, h.log...)
	b = binary.LittleEndian.AppendUint32(b, uint32(inline))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(runs)))
	for _, r := range runs {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(r.key)))
		b = append(b, r.key...)
		b = binary.LittleEndian.AppendUint64(b, uint64(r.meta.Len))
		b = binary.LittleEndian.AppendUint64(b, uint64(r.meta.N))
		b = binary.LittleEndian.AppendUint64(b, uint64(r.meta.Vers))
		b = binary.LittleEndian.AppendUint32(b, uint32(r.meta.Piece))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(r.meta.Sums)))
		for _, s := range r.meta.Sums {
			b = binary.LittleEndian.AppendUint32(b, s)
		}
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(sections)))
	for _, name := range sortedMapKeys(sections) {
		b = append(b, byte(len(name)))
		b = append(b, name...)
		b = binary.LittleEndian.AppendUint64(b, uint64(len(sections[name])))
		b = append(b, sections[name]...)
	}
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

var errCkpt = errors.New("not a valid index checkpoint")

// ckptReader reads the descriptor's fields, failing once on underrun.
type ckptReader struct {
	b   []byte
	bad bool
}

func (r *ckptReader) take(n uint64) []byte {
	if r.bad || n > uint64(len(r.b)) {
		r.bad = true
		return nil
	}
	x := r.b[:n]
	r.b = r.b[n:]
	return x
}
func (r *ckptReader) u8() uint8 {
	if x := r.take(1); x != nil {
		return x[0]
	}
	return 0
}
func (r *ckptReader) u16() uint16 {
	if x := r.take(2); x != nil {
		return binary.LittleEndian.Uint16(x)
	}
	return 0
}
func (r *ckptReader) u32() uint32 {
	if x := r.take(4); x != nil {
		return binary.LittleEndian.Uint32(x)
	}
	return 0
}
func (r *ckptReader) u64() uint64 {
	if x := r.take(8); x != nil {
		return binary.LittleEndian.Uint64(x)
	}
	return 0
}

// decodeCkpt checks a descriptor's framing and checksum and splits it.
// It never panics on any input (FuzzDecodeCkpt).
func decodeCkpt(b []byte) (h ckptHeader, inline int, runs []ckptRunDesc, sections map[string][]byte, err error) {
	if len(b) < len(ckptMagic)+4 || string(b[:len(ckptMagic)]) != ckptMagic {
		return h, 0, nil, nil, errCkpt
	}
	body := b[:len(b)-4]
	if format.CRC(body) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return h, 0, nil, nil, fmt.Errorf("%w: checksum mismatch", errCkpt)
	}
	r := &ckptReader{b: body[len(ckptMagic):]}
	h.lsn, h.floor = r.u64(), r.u64()
	copy(h.chain[:], r.take(32))
	h.log = string(r.take(uint64(r.u8())))
	inline = int(r.u32())
	n := r.u32()
	for i := uint32(0); i < n && !r.bad; i++ {
		var d ckptRunDesc
		d.key = string(r.take(uint64(r.u16())))
		d.meta.Len, d.meta.N, d.meta.Vers = int64(r.u64()), int(r.u64()), int(r.u64())
		d.meta.Piece = int(r.u32())
		ns := r.u32()
		if uint64(ns)*4 > uint64(len(r.b)) {
			r.bad = true
			break
		}
		for j := uint32(0); j < ns; j++ {
			d.meta.Sums = append(d.meta.Sums, r.u32())
		}
		if d.meta.Len < 0 || d.meta.N < 0 || d.meta.Vers < 0 || d.meta.Piece < 0 {
			r.bad = true
		}
		runs = append(runs, d)
	}
	ns := r.u32()
	sections = map[string][]byte{}
	prev := ""
	for i := uint32(0); i < ns && !r.bad; i++ {
		name := string(r.take(uint64(r.u8())))
		data := r.take(r.u64())
		if i > 0 && name <= prev {
			return h, 0, nil, nil, fmt.Errorf("%w: section %q out of order", errCkpt, name)
		}
		sections[name], prev = data, name
	}
	if r.bad || len(r.b) != 0 {
		return h, 0, nil, nil, fmt.Errorf("%w: truncated or trailing bytes", errCkpt)
	}
	return h, inline, runs, sections, nil
}

// writeIndexCkpt checkpoints l's record store and derived indexes as of
// v's head (design 24). Not concurrent with itself for one view (v.ckMu).
// Returns nil without a commit when there is nothing new, or when the
// manifest moved on meanwhile (a compaction, a newer checkpoint): the
// objects written are then orphans for a later epoch's sweep.
func (db *DB) writeIndexCkpt(ctx context.Context, l *logState, v *view) error {
	v.ckMu.Lock()
	defer v.ckMu.Unlock()
	id := l.meta.ID
	db.mu.RLock()
	lm := db.man.Logs[id]
	var have map[string]bool
	var prev uint64
	if lm != nil && lm.Ckpt != nil {
		prev = lm.Ckpt.LSN
		have = map[string]bool{}
		for _, r := range lm.Ckpt.Runs {
			have[r.Key] = true
		}
	}
	db.mu.RUnlock()
	if lm == nil {
		return nil
	}
	v.mu.RLock()
	lsn := v.head
	if lsn <= prev || lsn <= l.baseLSN() {
		v.mu.RUnlock()
		return nil
	}
	cut := v.kv.Cut()
	sections := map[string][]byte{}
	for i, k := range db.reg.Derived() {
		if sn, ok := v.derived[i].(index.Snapshotter); ok && k.Restore != nil {
			sections[k.SectionName()] = sn.Snapshot()
		}
	}
	v.mu.RUnlock()
	defer cut.Release()
	floor := l.baseLSN()
	chain, err := l.chainAt(ctx, lsn)
	if err != nil {
		return err
	}
	// The segment index goes first, as for snapshots: open then reads
	// only the segments after it. Failing to write it only costs time.
	if err := db.writeCheckpoint(ctx, l); err != nil {
		db.logf("loget: segment-index checkpoint failed (non-fatal): %v", err)
	}
	nonce, err := db.keys.randBytes(4)
	if err != nil {
		return err
	}
	key := ckptObjKey(id, lsn, db.epoch, nonce)
	meta := &CkptMeta{LSN: lsn, Floor: floor, Key: key}
	var descs []ckptRunDesc
	newKeys := map[int]string{}
	for i, cr := range cut.Runs {
		m := cr.Meta()
		rk := cr.Key()
		if rk == "" || !have[rk] {
			rk = ckptRunKey(key, i)
			if err := db.putRun(ctx, rk, cr.Bytes(), m.Piece); err != nil {
				return err
			}
			newKeys[i] = rk
		}
		descs = append(descs, ckptRunDesc{key: rk, meta: m})
		meta.Runs = append(meta.Runs, CkptRun{Key: rk, Pieces: len(m.Sums)})
	}
	desc := encodeCkpt(ckptHeader{log: id, lsn: lsn, floor: floor, chain: chain}, cut.Inline, descs, sections)
	if err := putExactlyOnce(ctx, db.st, ckptDescObj(key), desc); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	stale := false
	err = db.commitManifest(ctx, func(m *Manifest) error {
		lm := m.Logs[id]
		if lm == nil || floorOf(m, id) != floor || lm.Ckpt != nil && lm.Ckpt.LSN >= lsn || lm.Base != nil && lm.Base.LSN >= lsn {
			stale = true
			return errStaleCkpt
		}
		// Reused runs must still be the live checkpoint's (not garbage).
		live := map[string]bool{}
		if lm.Ckpt != nil {
			for _, r := range lm.Ckpt.Runs {
				live[r.Key] = true
			}
		}
		for i, r := range meta.Runs {
			if _, isNew := newKeys[i]; !isNew && !live[r.Key] {
				stale = true
				return errStaleCkpt
			}
		}
		// No grace: only a writable open reads a checkpoint, and it fences
		// every older writer before it reads one (sweeps delete only what
		// the latest manifest lists as garbage).
		after := db.opts.Clock()
		if old := lm.Ckpt; old != nil {
			named := map[string]bool{}
			for _, r := range meta.Runs {
				named[r.Key] = true
			}
			m.Garbage = append(m.Garbage, ckptGarbage(id, old, named, after)...)
		}
		if lm.SnapKey != "" && lm.SnapLSN <= lsn { // replaced by the checkpoint
			m.Garbage = append(m.Garbage, Garbage{Kind: "snaps", Log: id, Below: lsn + 1, After: after})
			lm.SnapLSN, lm.SnapKey = 0, ""
		}
		lm.Ckpt = meta
		return nil
	})
	if stale {
		return nil
	}
	if err != nil {
		return err
	}
	for i, rk := range newKeys {
		cut.Runs[i].SetKey(rk) // the next checkpoint names it
	}
	db.met.snapshots.Add(1)
	return nil
}

// ckptGarbage lists checkpoint c's objects as garbage, except the runs in keep.
func ckptGarbage(id string, c *CkptMeta, keep map[string]bool, after int64) []Garbage {
	g := []Garbage{{Kind: "ckpt", Log: id, Key: c.Key, After: after}}
	for _, r := range c.Runs {
		if !keep[r.Key] {
			g = append(g, Garbage{Kind: "run", Log: id, Key: r.Key, Chunks: r.Pieces, After: after})
		}
	}
	return g
}

var errStaleCkpt = errors.New("loget: checkpoint superseded")

// floorOf is the compaction base LSN of id's lineage in manifest m.
func floorOf(m *Manifest, id string) uint64 {
	for hops := 0; hops <= len(m.Logs); hops++ {
		lm := m.Logs[id]
		if lm == nil {
			return 0
		}
		if lm.Base != nil {
			return lm.Base.LSN
		}
		if lm.Parent == "" {
			return 0
		}
		id = lm.Parent
	}
	return 0
}

// putRun stores a run's bytes in pieces of size bytes (its RunMeta.Piece).
func (db *DB) putRun(ctx context.Context, key string, b []byte, size int) error {
	for p := 0; p*size < len(b); p++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		piece := b[p*size : min((p+1)*size, len(b))]
		if err := putExactlyOnce(ctx, db.st, ckptPieceKey(key, p), piece); err != nil {
			if errors.Is(err, store.ErrExists) { // never: the names are new
				err = fmt.Errorf("%w: run piece %s exists", ErrCorrupt, ckptPieceKey(key, p))
			}
			return err
		}
	}
	return nil
}

// runSources returns loaders of stored runs, reading pieces from the store.
func (db *DB) runSources(ctx context.Context, runs []ckptRunDesc) []index.RunSource {
	src := make([]index.RunSource, len(runs))
	for i, r := range runs {
		key := r.key
		src[i] = index.RunSource{Key: key, Meta: r.meta, Piece: func(p int) ([]byte, error) {
			return db.st.Get(ctx, ckptPieceKey(key, p))
		}}
	}
	return src
}

// readIndexCkpt loads the checkpoint lm names for l, checked against l's
// history: same log, LSN, base and hash chain, every piece intact.
func (db *DB) readIndexCkpt(ctx context.Context, l *logState, c *CkptMeta) (*snapshot, error) {
	b, err := db.st.Get(ctx, ckptDescObj(c.Key))
	if err != nil {
		return nil, err
	}
	h, inline, runs, sections, err := decodeCkpt(b)
	if err != nil {
		return nil, err
	}
	switch {
	case h.log != l.meta.ID || h.lsn != c.LSN || h.floor != c.Floor:
		return nil, fmt.Errorf("%w: it holds log %s LSN %d on base %d", errCkpt, h.log, h.lsn, h.floor)
	case h.floor != l.baseLSN():
		return nil, fmt.Errorf("%w: built on base %d, the log's base is %d", errCkpt, h.floor, l.baseLSN())
	case h.lsn > l.head() || h.lsn <= l.baseLSN():
		return nil, fmt.Errorf("%w: LSN %d outside the log (%d..%d)", errCkpt, h.lsn, l.baseLSN(), l.head())
	case len(runs) != len(c.Runs):
		return nil, fmt.Errorf("%w: %d runs, the manifest lists %d", errCkpt, len(runs), len(c.Runs))
	}
	for i, r := range runs {
		if r.key != c.Runs[i].Key || len(r.meta.Sums) != c.Runs[i].Pieces {
			return nil, fmt.Errorf("%w: run %d is %s, the manifest lists %s", errCkpt, i, r.key, c.Runs[i].Key)
		}
	}
	o := db.kvOptions()
	if inline != o.Inline {
		return nil, fmt.Errorf("%w: written with inline limit %d, this DB uses %d", errCkpt, inline, o.Inline)
	}
	chain, err := l.chainAt(ctx, h.lsn)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(chain[:], h.chain[:]) {
		return nil, fmt.Errorf("%w: hash chain at LSN %d differs from the log's", errCkpt, h.lsn)
	}
	kv, err := index.LoadKVRuns(o, db.runSources(ctx, runs))
	if err != nil {
		return nil, err
	}
	return &snapshot{lsn: h.lsn, kv: kv, sections: sections, drop: db.keys.shreddedList()}, nil
}
