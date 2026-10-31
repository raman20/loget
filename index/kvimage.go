package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strconv"
	"strings"
)

// Persistent runs (docs/design/24-open-checkpoints.md). A run's entry
// bytes (kvrun.go) are its persistent form: a checkpoint stores them in
// pieces of at most PieceBytes, and loading copies the pieces into a run
// file (or the heap), checks them in one pass and rebuilds only the hash
// slots, instead of decoding every version into a new run. Runs are
// immutable, so a run already stored by one checkpoint is named, not
// stored again, by the next (the KV keeps its key).

// PieceBytes bounds one stored piece of a run (a var for tests).
var PieceBytes = 64 << 20

// RunMeta describes a stored run.
type RunMeta struct {
	Len  int64    `json:"len"`  // entry bytes
	N    int      `json:"n"`    // keys
	Vers int      `json:"vers"` // versions
	Sums []uint32 `json:"sums"` // CRC-32C of each piece, in order
}

// Pieces returns how many pieces a run of n bytes is stored in.
func Pieces(n int64) int { return int((n + int64(PieceBytes) - 1) / int64(PieceBytes)) }

// SharedRunFile is a RunFile that counts references: Ref adds one,
// Release drops one and unmaps at the last. A Cut takes a reference on
// every mapped run, so a checkpoint reads runs without the owner's lock
// while the KV replaces (and releases) them.
type SharedRunFile interface {
	RunFile
	Ref()
}

// Cut is a consistent read-only view of a KV's runs for a checkpoint,
// taken under the owner's read lock: the hot set as a heap run, then the
// runs newest first. Mapped runs stay mapped until Release, even when
// the KV replaces them meanwhile.
type Cut struct {
	Inline int
	Runs   []*CutRun
	done   bool
}

// CutRun is one run of a Cut.
type CutRun struct {
	r   *run
	b   []byte  // r.b at the cut (the KV may release r meanwhile)
	ref RunFile // a reference on r's file, or nil (heap)
	Hot bool    // the hot set, frozen for this cut (never has a key)
}

// Cut returns a view of the KV for a checkpoint. The caller holds the
// owner's read lock for the call only.
func (l *KV) Cut() *Cut {
	c := &Cut{Inline: l.inline}
	if len(l.m) > 0 {
		w := newBuilder()
		l.hotRun(w)
		if r, _ := w.finish(); r != nil {
			c.Runs = append(c.Runs, &CutRun{r: r, b: r.b, Hot: true})
		}
	}
	for _, r := range l.runs {
		cr := &CutRun{r: r, b: r.b}
		if r.mp != nil {
			if sf, ok := r.mp.(SharedRunFile); ok {
				sf.Ref()
				cr.ref = sf
			} else { // cannot outlive the KV's release: copy
				cr.b = bytes.Clone(r.b)
			}
		}
		c.Runs = append(c.Runs, cr)
	}
	return c
}

// Release drops the cut's references (idempotent; not concurrent).
func (c *Cut) Release() {
	if c.done {
		return
	}
	c.done = true
	for _, cr := range c.Runs {
		if cr.ref != nil {
			cr.ref.Release()
			cr.ref = nil
		}
	}
}

// Bytes returns the run's entry bytes: valid until the cut is released.
func (c *CutRun) Bytes() []byte { return c.b }

// Key returns the key the run was stored under, or "".
func (c *CutRun) Key() string { return c.r.key }

// SetKey records that the run is stored under key (a later cut of the
// same KV names it instead of storing it again). Never for the hot run.
func (c *CutRun) SetKey(key string) {
	if !c.Hot {
		c.r.key = key
	}
}

// Meta returns the run's description; its piece checksums are computed
// (one pass over the bytes).
func (c *CutRun) Meta() RunMeta {
	m := RunMeta{Len: int64(len(c.b)), N: c.r.n, Vers: c.r.vers}
	for p := 0; p < len(c.b); p += PieceBytes {
		m.Sums = append(m.Sums, crc32.Checksum(c.b[p:min(p+PieceBytes, len(c.b))], Castagnoli))
	}
	return m
}

// RunSource is a stored run to load: its description, the key to keep
// for it ("" for none, e.g. a hot run) and a reader of its pieces.
type RunSource struct {
	Key   string
	Meta  RunMeta
	Piece func(i int) ([]byte, error)
}

// ErrBadRun reports a stored run that fails its checks.
var ErrBadRun = errors.New("index: bad stored run")

type runError string

func (e runError) Error() string { return ErrBadRun.Error() + ": " + string(e) }
func (e runError) Unwrap() error { return ErrBadRun }

func badRun(why string) error { return runError(why) }

// LoadKVRuns builds a KV configured by o from stored runs, newest first
// (Cut's order). A run file error falls back to the heap, as loadKV.
func LoadKVRuns(o KVOptions, runs []RunSource) (*KV, error) {
	l := NewKVOptions(o)
	for i := 0; i < len(runs); i++ {
		r, err := l.loadRun(runs[i])
		if err == errRunFile { // l.files is now nil: this run again on the heap
			r, err = l.loadRun(runs[i])
		}
		if err != nil {
			l.dropRuns()
			return nil, err
		}
		l.runs = append(l.runs, r)
	}
	l.plan()
	return l, nil
}

func (l *KV) loadRun(src RunSource) (*run, error) {
	m := src.Meta
	if m.Len <= 0 || m.N <= 0 || m.Vers < m.N || int64(len(m.Sums)) != int64(Pieces(m.Len)) || m.Len > int64(maxRunBytes) {
		return nil, badRun("bad description")
	}
	w := l.builder(int(m.Len))
	ok := false
	defer func() {
		if !ok {
			w.abort()
		}
	}()
	var got int64
	for i := range m.Sums {
		p, err := src.Piece(i)
		if err != nil {
			return nil, err
		}
		want := min(int64(PieceBytes), m.Len-got)
		if int64(len(p)) != want {
			return nil, badRun("piece " + strconv.Itoa(i) + " has " + strconv.Itoa(len(p)) + " bytes, want " + strconv.FormatInt(want, 10))
		}
		if crc32.Checksum(p, Castagnoli) != m.Sums[i] {
			return nil, badRun("piece " + strconv.Itoa(i) + " checksum mismatch")
		}
		got += want
		w.buf = append(w.buf, p...)
		if w.f != nil && len(w.buf) >= fileBuf {
			w.flush()
		}
	}
	r := w.r
	w.r = nil
	if w.f != nil {
		w.flush()
		f, err := w.f, w.err
		w.f, w.buf = nil, nil
		if err == nil {
			r.b, r.slots, err = f.Map(nslots(m.N))
		}
		if err == nil && int64(len(r.b)) != m.Len {
			err = errors.New("index: run file holds " + strconv.Itoa(len(r.b)) + " bytes, want " + strconv.FormatInt(m.Len, 10))
		}
		if err != nil {
			f.Release()
			l.fileFailed(err)
			return nil, errRunFile
		}
		r.mp = f
	} else {
		r.b, w.buf = w.buf, nil
		r.slots = make([]uint64, nslots(m.N))
	}
	if err := r.check(m); err != nil {
		r.release()
		return nil, err
	}
	r.fillSlots()
	if r.mp != nil {
		if err := r.mp.Seal(); err != nil {
			r.release()
			l.fileFailed(err)
			return nil, errRunFile
		}
	}
	r.key = src.Key
	ok = true
	return r, nil
}

// check validates r.b as a run's entries in one pass (so the trusting
// decoders of kvrun.go never read out of bounds) and sets bofs, chunk
// and subj from them; n and vers must match m.
func (r *run) check(m RunMeta) error {
	bad := func(p int, why string) error { return badRun("entry at " + strconv.Itoa(p) + ": " + why) }
	b := r.b
	r.bofs, r.subj, r.n, r.vers, r.chunk = nil, map[string]int{}, 0, 0, false
	var first, key, prev []byte
	for p := 0; p < len(b); {
		if r.n%runBlock == 0 {
			r.bofs = append(r.bofs, p)
		}
		sh, n1 := binary.Uvarint(b[p:])
		if n1 <= 0 {
			return bad(p, "shared")
		}
		q := p + n1
		sl, n2 := binary.Uvarint(b[q:])
		if n2 <= 0 || sl > uint64(len(b)-q-n2) {
			return bad(p, "suffix")
		}
		q += n2
		if r.n%runBlock == 0 {
			if sh != 0 {
				return bad(p, "block start shares a prefix")
			}
			first = b[q : q+int(sl)]
		} else if sh > uint64(len(first)) {
			return bad(p, "shared too long")
		}
		key = append(append(key[:0], first[:sh]...), b[q:q+int(sl)]...)
		q += int(sl)
		if (r.n > 0 && bytes.Compare(key, prev) <= 0) || bytes.IndexByte(key, 0) < 0 {
			return bad(p, "key out of order or without a space")
		}
		prev = append(prev[:0], key...)
		bl, n3 := binary.Uvarint(b[q:])
		if n3 <= 0 || bl > uint64(len(b)-q-n3) {
			return bad(p, "body length")
		}
		q += n3
		nv, err := r.checkBody(b[q : q+int(bl)])
		if err != nil {
			return bad(p, err.Error())
		}
		r.n++
		r.vers += nv
		p = q + int(bl)
	}
	r.bofs = append(r.bofs, len(b))
	if r.n != m.N || r.vers != m.Vers {
		return badRun(strconv.Itoa(r.n) + " keys and " + strconv.Itoa(r.vers) + " versions, want " + strconv.Itoa(m.N) + " and " + strconv.Itoa(m.Vers))
	}
	return nil
}

// checkBody validates one body; it returns its version count.
func (r *run) checkBody(b []byte) (int, error) {
	errShort := errors.New("short version")
	u := func() (uint64, error) {
		x, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errShort
		}
		b = b[n:]
		return x, nil
	}
	nv, err := u()
	if err != nil || nv == 0 || nv > uint64(len(b)) {
		return 0, errors.New("bad version count")
	}
	var lastLSN, lastOp uint64
	for i := uint64(0); i < nv; i++ {
		lsn, err := u()
		if err != nil {
			return 0, err
		}
		op, err := u()
		if err != nil || op > 1<<32-1 {
			return 0, errShort
		}
		if i > 0 && (lsn < lastLSN || lsn == lastLSN && op <= lastOp) {
			return 0, errors.New("versions out of order")
		}
		lastLSN, lastOp = lsn, op
		if len(b) == 0 {
			return 0, errShort
		}
		f := b[0]
		b = b[1:]
		if f&^(fDel|fPos|fChunk|fVal|fSubj) != 0 {
			return 0, errors.New("unknown flags")
		}
		if f&fPos != 0 {
			for k := 0; k < 2; k++ {
				if x, err := u(); err != nil || x > 1<<32-1 {
					return 0, errShort
				}
			}
			if len(b) < 4 {
				return 0, errShort
			}
			b = b[4:]
		}
		if f&fChunk != 0 {
			x, err := u()
			if err != nil || x > 1<<32-1 {
				return 0, errShort
			}
			r.chunk = r.chunk || x != 0
		}
		if f&fVal != 0 {
			x, err := u()
			if err != nil || x > uint64(len(b)) {
				return 0, errShort
			}
			b = b[x:]
		}
		var subj string
		if f&fSubj != 0 {
			x, err := u()
			if err != nil || x > uint64(len(b)) {
				return 0, errShort
			}
			subj, b = string(b[:x]), b[x:]
		}
		if subj != "" && !(f&fDel != 0 && f&(fVal|fPos|fChunk) == 0) {
			r.subj[subj]++
		}
	}
	if len(b) != 0 {
		return 0, errors.New("trailing bytes in body")
	}
	return int(nv), nil
}

// RunWriter writes one run, keys in ascending order: a compaction base
// stored as a run (design 24), in a run file when o.Files is set.
type RunWriter struct {
	l    *KV
	w    *builder
	kb   []byte
	prev string
	n    int
}

// NewRunWriter returns a writer of a run of about est bytes.
func NewRunWriter(o KVOptions, est int) *RunWriter {
	l := NewKVOptions(o)
	return &RunWriter{l: l, w: l.builder(est)}
}

// Add writes a key's versions (oldest first, at least one).
func (rw *RunWriter) Add(space, key string, vs ...Version) error {
	k := ck(space, key)
	if rw.n > 0 && k <= rw.prev || len(vs) == 0 || !spaceOK(space) {
		return badRun("key " + strconv.Quote(k) + " out of order or without versions")
	}
	for i := range vs {
		rw.w.ver(&vs[i])
	}
	rw.kb = append(rw.kb[:0], k...)
	rw.w.end(rw.kb)
	rw.prev, rw.n = k, rw.n+1
	return rw.w.err
}

// Finish returns the run as a one-run Cut (nil if no key was added);
// releasing the cut frees the run. The writer is spent either way.
func (rw *RunWriter) Finish() (*Cut, error) {
	r, err := rw.w.finish()
	rw.w = nil
	if err != nil || r == nil {
		return nil, err
	}
	// the cut owns the run: its release is the run's
	return &Cut{Inline: rw.l.inline, Runs: []*CutRun{{r: r, b: r.b, ref: r.mp}}}, nil
}

// Abort frees an unfinished writer.
func (rw *RunWriter) Abort() {
	if rw.w != nil {
		rw.w.abort()
		rw.w = nil
	}
}

func spaceOK(s string) bool { return s != "" && strings.IndexByte(s, 0) < 0 }
