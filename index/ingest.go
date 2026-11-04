package index

import (
	"bytes"
	"errors"
	"hash/crc32"
	"strconv"
)

// Bulk ingest (docs/design/26-batch-ingest.md). Sorted puts are written
// straight into one run (no hot set, no merges); the engine stores the
// run's bytes beside the log and commits one OpIngest frame naming them.
// Applying that frame installs the run as the newest one: every key in it
// gets one version at the frame's LSN, value inline. Replay loads the
// same bytes (LoadIngest), checked against the description in the frame,
// so the KV stays a fold of the log.

// ErrUnsorted: ingest keys must be strictly increasing.
var ErrUnsorted = errors.New("index: ingest keys not strictly increasing")

// IngestRun is a run built by an Ingest or loaded for an OpIngest frame.
type IngestRun struct {
	r    *run
	lsn  uint64
	Key  string  // object key the run is stored under (the frame's op Key)
	Meta RunMeta // set by Finish and LoadIngest
}

// Ingest builds an IngestRun from sorted puts.
type Ingest struct {
	w     *builder
	lsn   uint64
	space string
	prev  []byte
	kb    []byte
}

// NewIngest starts an ingest of puts into space at LSN lsn: in a run file
// if o.Files is set, else on the heap. Safe to use concurrently with the
// KV the run will be installed in.
func NewIngest(o KVOptions, space string, lsn uint64) (*Ingest, error) {
	var w *builder
	if o.Files != nil {
		var err error
		if w, err = newFileBuilder(o.Files); err != nil {
			return nil, err
		}
	} else {
		w = newBuilder()
	}
	w.par = o.Parallel
	return &Ingest{w: w, lsn: lsn, space: space}, nil
}

// Add appends key = val; keys must be strictly increasing (ErrUnsorted).
// val is copied.
func (g *Ingest) Add(key string, val []byte) error {
	g.kb = append(append(append(g.kb[:0], g.space...), 0), key...)
	if g.prev != nil && bytes.Compare(g.kb, g.prev) <= 0 {
		return ErrUnsorted
	}
	if val == nil {
		val = []byte{} // a put, never "no value"
	}
	v := Version{LSN: g.lsn, Val: val}
	g.w.ver(&v)
	g.w.end(g.kb)
	g.prev = append(g.prev[:0], g.kb...)
	if int64(g.w.size()) > int64(maxRunBytes) {
		return errors.New("index: ingest run too large")
	}
	return g.w.err
}

// Len returns the keys added so far.
func (g *Ingest) Len() int { return g.w.r.n }

// Finish completes the run (nil, nil if no key was added).
func (g *Ingest) Finish() (*IngestRun, error) {
	r, err := g.w.finish()
	if err != nil || r == nil {
		return nil, err
	}
	x := &IngestRun{r: r, lsn: g.lsn}
	x.Meta = RunMeta{Len: int64(len(r.b)), N: r.n, Vers: r.vers, Piece: PieceBytes}
	for p := 0; p < len(r.b); p += PieceBytes {
		x.Meta.Sums = append(x.Meta.Sums, crc32.Checksum(r.b[p:min(p+PieceBytes, len(r.b))], Castagnoli))
		x.DropPiece(len(x.Meta.Sums) - 1) // a mapped run: read once, not held in RSS
	}
	return x, nil
}

// Abort discards an unfinished ingest.
func (g *Ingest) Abort() { g.w.abort() }

// Piece returns piece i of the run's bytes (Meta.Piece bytes each, the
// last shorter), valid until the run is released.
func (x *IngestRun) Piece(i int) []byte {
	p := x.Meta.Piece
	return x.r.b[i*p : min((i+1)*p, len(x.r.b))]
}

// DropPiece lets the OS take back piece i's pages from the process (a
// mapped run; the bytes stay in the file). A no-op on the heap.
func (x *IngestRun) DropPiece(i int) {
	if d, ok := x.r.mp.(pageDropper); ok {
		p := x.Meta.Piece
		d.Drop(i*p, min((i+1)*p, len(x.r.b))-i*p)
	}
}

// Release frees a run that will not be installed.
func (x *IngestRun) Release() {
	if x.r != nil {
		x.r.release()
		x.r = nil
	}
}

// LoadIngest loads a stored ingest run for the frame at lsn: the pieces
// must match src.Meta (from the frame), and every entry must be one inline
// put at lsn, so the result is exactly what Ingest built.
func (l *KV) LoadIngest(src RunSource, lsn uint64) (*IngestRun, error) {
	r, err := l.loadRun(src)
	if err == errRunFile {
		r, err = l.loadRun(src)
	}
	if err != nil {
		return nil, err
	}
	r.key = "" // a checkpoint stores its own copy (the ingest object goes with its segment)
	if err := r.checkIngest(lsn); err != nil {
		r.release()
		return nil, err
	}
	return &IngestRun{r: r, lsn: lsn, Key: src.Key, Meta: src.Meta}, nil
}

// checkIngest checks that every key has exactly one version: an inline
// put at lsn, op 0, no subject (r already passed check).
func (r *run) checkIngest(lsn uint64) error {
	c := r.cursor()
	for c.next() {
		b := c.body
		nv, b := uv(b)
		x, b := uv(b)
		op, b := uv(b)
		if nv != 1 || x != lsn || op != 0 || len(b) == 0 || b[0] != fVal {
			return badRun("ingest entry " + strconv.Quote(string(c.key)) + " is not one inline put at LSN " + strconv.FormatUint(lsn, 10))
		}
	}
	return nil
}

// install puts x in front of every other run: its versions are newer
// than everything in the KV. The hot set is frozen first.
func (l *KV) install(x *IngestRun) {
	if len(l.m) > 0 {
		l.freeze()
	}
	l.runs = append([]*run{x.r}, l.runs...)
	x.r = nil
	l.plan()
	for len(l.runs) > maxRuns && len(l.mgs) > 0 {
		l.work(-1)
	}
}
