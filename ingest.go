package loget

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Bulk ingest (docs/design/26-batch-ingest.md, part 2).
//
// Protocol, on one branch:
//
//  1. Exclusion: the ingest holds the DB's compaction slot (Compact, GC
//     and branch operations wait or get ErrBusy) and then the branch's
//     writer: commits on the branch queue until the ingest ends.
//  2. The writer tells the ingest the LSN (head+1) and segment number its
//     commit will get. The caller builds one run from the sorted input,
//     every key at that LSN, and stores its bytes in pieces next to that
//     segment: log/<log>/<seq>-e<epoch>-<nonce>.ingest.<piece>.
//  3. Commit point: the writer commits one frame holding one OpIngest op
//     that names the pieces and their CRCs. Before it is durable nothing
//     is visible and the pieces are orphans; after it, all of the run is.
//  4. Replay loads the named pieces, checks them against the op and
//     installs the same run; checkpoints and compaction bases hold the
//     data like any other.
//
// The pieces are deleted with their segment (GC), and orphans of an
// older epoch, which no frame names, by the orphan sweep.

// IngestSource feeds an ingest: it calls add for every key in strictly
// increasing order and returns nil when done. An error from add (ErrUnsorted,
// ErrTooLarge, a cancelled context) should be returned as is. It runs while
// the branch's writer waits, so it must not commit on that branch.
type IngestSource func(add func(key string, val []byte) error) error

// ErrUnsorted: ingest keys must be strictly increasing.
var ErrUnsorted = fmt.Errorf("loget: %w", index.ErrUnsorted)

// IngestStats describes a finished ingest.
type IngestStats struct {
	LSN    uint64 // commit LSN (the head, if nothing was added)
	Keys   int
	Bytes  int64 // run bytes stored
	Pieces int
}

// Ingest bulk-loads sorted puts into main; see Branch.Ingest.
func (db *DB) Ingest(ctx context.Context, src IngestSource) (IngestStats, error) {
	return db.Branch("main").Ingest(ctx, src)
}

// Ingest bulk-loads sorted puts (KV keys, no subjects) into the branch as
// one atomic commit: after it returns nil every key reads its new value at
// the returned LSN; on an error or a crash none does. Values are kept
// inline in the index (in run files when the DB has them). While it runs,
// other commits on the branch wait, Compact and GC return ErrBusy, and
// branch operations wait; an ingest itself waits for a running Compact or GC. A read-write transaction whose snapshot predates
// the ingest conflicts with it.
func (b *Branch) Ingest(ctx context.Context, src IngestSource) (st IngestStats, err error) {
	db := b.db
	for { // take the compaction slot, waiting out a running compaction
		db.mu.Lock()
		switch {
		case db.closed:
			db.mu.Unlock()
			return st, ErrClosed
		case db.failed != nil:
			db.mu.Unlock()
			return st, db.failed
		case !db.compacting:
			db.compacting, db.compactDone = true, make(chan struct{})
			db.mu.Unlock()
		default:
			db.mu.Unlock()
			if err := db.waitCompaction(ctx); err != nil {
				return st, err
			}
			continue
		}
		break
	}
	defer db.endCompaction(time.Time{})
	w, err := db.branchWriter(b.name)
	if err != nil {
		return st, err
	}
	job := &ingestJob{start: make(chan ingestStart, 1), ready: make(chan *builtIngest, 1)}
	resc := make(chan commitRes, 1)
	go func() { resc <- w.submit(&commitReq{ingest: job}) }()
	var s ingestStart
	select {
	case s = <-job.start:
	case res := <-resc: // the writer refused it (closed, halted)
		return st, res.err
	}
	var run *builtIngest
	sent := false
	send := func(x *builtIngest) {
		if !sent {
			sent = true
			job.ready <- x
		}
	}
	defer func() {
		send(nil) // never leave the writer waiting (also on a panic in src)
	}()
	run, err = db.buildIngest(ctx, s, src)
	if err != nil {
		send(nil)
		<-resc
		return st, err
	}
	if run == nil { // nothing added
		send(nil)
		res := <-resc
		return IngestStats{LSN: res.lsn}, nil
	}
	st = IngestStats{Keys: run.Meta.N, Bytes: run.Meta.Len, Pieces: len(run.Meta.Sums)}
	send(run)
	res := <-resc
	if res.err != nil {
		return IngestStats{}, res.err
	}
	st.LSN = res.lsn
	return st, nil
}

// buildIngest builds and stores the run (nil if src added nothing).
func (db *DB) buildIngest(ctx context.Context, s ingestStart, src IngestSource) (*builtIngest, error) {
	g, err := index.NewIngest(db.kvOptions(), spaceKV, s.lsn)
	if err != nil {
		return nil, err
	}
	n := 0
	err = src(func(key string, val []byte) error {
		if len(key) > MaxKeyBytes {
			return fmt.Errorf("%w: key of %d bytes (max %d)", ErrTooLarge, len(key), MaxKeyBytes)
		}
		if n++; n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if db.stopped() {
				return ErrClosed
			}
		}
		if err := g.Add(key, val); err != nil {
			if errors.Is(err, index.ErrUnsorted) {
				return fmt.Errorf("%w: %q", ErrUnsorted, key)
			}
			return err
		}
		return nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		g.Abort()
		return nil, err
	}
	run, err := g.Finish()
	if err != nil || run == nil {
		return nil, err
	}
	nonce := make([]byte, 8)
	if _, err := io.ReadFull(db.opts.Rand, nonce); err != nil {
		run.Release()
		return nil, err
	}
	run.Key = ingestKey(s.log, s.seq, db.epoch, nonce)
	h := sha256.New()
	for i := range run.Meta.Sums {
		h.Write(run.Piece(i))
		if err := putExactlyOnce(ctx, db.st, ingestPieceKey(run.Key, i), run.Piece(i)); err != nil {
			run.Release()
			for j := 0; j <= i; j++ { // not committed: best effort, else the sweep
				_ = db.st.Delete(context.WithoutCancel(ctx), ingestPieceKey(run.Key, j))
			}
			return nil, err
		}
		run.DropPiece(i) // stored: the pages need not stay in RSS
	}
	return &builtIngest{run, hex.EncodeToString(h.Sum(nil))}, nil
}

// ingestDesc is an OpIngest op's value: the run's layout and checksums,
// and the SHA-256 of its bytes, so the hash chain covers the ingested
// data as it covers values in frames.
type ingestDesc struct {
	Meta   index.RunMeta `json:"meta"`
	SHA256 string        `json:"sha256"`
}

// ingestJob is the hand-off between an ingest and its branch's writer.
type ingestJob struct {
	start chan ingestStart
	ready chan *builtIngest // nil: the ingest gave up
}

// builtIngest is a stored run and the SHA-256 of its bytes.
type builtIngest struct {
	*index.IngestRun
	sum string
}

type ingestStart struct {
	lsn uint64 // the commit's LSN
	seq uint64 // the commit's segment
	log string
}

// runIngest serves an ingest request on the writer goroutine: no other
// commit of this branch runs until it returns.
func (w *writer) runIngest(r *commitReq) {
	if w.crashed.Load() {
		r.done <- commitRes{err: ErrClosed}
		return
	}
	if w.failed != nil {
		r.done <- commitRes{err: w.failed}
		return
	}
	w.l.mu.RLock()
	seq := w.l.nextSeq
	w.l.mu.RUnlock()
	lsn := w.head + 1
	r.ingest.start <- ingestStart{lsn: lsn, seq: seq, log: w.l.meta.ID}
	run := <-r.ingest.ready
	if run == nil {
		r.done <- commitRes{lsn: w.head}
		return
	}
	defer run.Release() // a no-op once installed
	meta, _ := json.Marshal(ingestDesc{Meta: run.Meta, SHA256: run.sum})
	cr := &commitReq{at: w.head, ops: []format.Op{{Kind: format.OpIngest, Space: spaceKV, Key: run.Key, Value: meta}}, ingestRun: run.IngestRun, done: r.done}
	w.process([]*commitReq{cr})
	if w.head == lsn {
		// Transactions that read at an older snapshot conflict: the
		// ingest's keys are not listed in the OCC window.
		w.recentFloor = max(w.recentFloor, lsn)
	}
}

// ingestKey names an ingest run stored beside segment seq of log.
func ingestKey(log string, seq, ep uint64, nonce []byte) string {
	return fmt.Sprintf("log/%s/%020d-e%d-%s.ingest", log, seq, ep, hex.EncodeToString(nonce))
}

func ingestPieceKey(key string, p int) string { return fmt.Sprintf("%s.%06d", key, p) }

// parseIngest parses an object under dir (log/<log>/) that belongs to an
// ingest: its segment, epoch and run key.
func parseIngest(obj, dir string) (seq, ep uint64, key string, ok bool) {
	name, found := strings.CutPrefix(obj, dir)
	if !found {
		return 0, 0, "", false
	}
	base, rest, found := strings.Cut(name, ".")
	if !found || !strings.HasPrefix(rest, "ingest") {
		return 0, 0, "", false
	}
	f := strings.Split(base, "-")
	if len(f) != 3 || !strings.HasPrefix(f[1], "e") {
		return 0, 0, "", false
	}
	var err1, err2 error
	seq, err1 = strconv.ParseUint(f[0], 10, 64)
	ep, err2 = strconv.ParseUint(f[1][1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, "", false
	}
	return seq, ep, dir + base + ".ingest", true
}

// resolveIngest loads the run e's OpIngest op names (replay).
func (db *DB) resolveIngest(ctx context.Context, kv *index.KV, e *index.Entry) error {
	for _, op := range e.Ops {
		if op.Kind != format.OpIngest {
			continue
		}
		if len(e.Ops) != 1 || op.Space != spaceKV || !strings.HasPrefix(op.Key, "log/"+e.Log+"/") {
			return fmt.Errorf("%w: LSN %d: malformed ingest frame", ErrCorrupt, e.LSN)
		}
		var d ingestDesc
		if err := json.Unmarshal(op.Value, &d); err != nil || len(d.SHA256) != 64 {
			return fmt.Errorf("%w: LSN %d: ingest description: %v", ErrCorrupt, e.LSN, err)
		}
		key := op.Key
		h, nextP := sha256.New(), 0
		src := index.RunSource{Key: key, Meta: d.Meta, Piece: func(p int) ([]byte, error) {
			b, err := db.st.Get(ctx, ingestPieceKey(key, p))
			if err == nil {
				if p == 0 { // (a reload starts over)
					h.Reset()
					nextP = 0
				}
				if p == nextP {
					h.Write(b)
					nextP++
				}
			}
			return b, err
		}}
		x, err := kv.LoadIngest(src, e.LSN)
		if err == nil && (nextP != len(d.Meta.Sums) || hex.EncodeToString(h.Sum(nil)) != d.SHA256) {
			x.Release()
			err = fmt.Errorf("%w: pieces do not hash to the committed SHA-256", index.ErrBadRun)
		}
		if errors.Is(err, index.ErrBadRun) || errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: LSN %d: ingest run %s: %w", ErrCorrupt, e.LSN, key, err)
		} else if err != nil {
			return err
		}
		e.Ingest = x
	}
	return nil
}

// sweepIngests deletes the orphaned ingest runs of an older epoch in log
// id: runs whose segment exists (or, below the log's next segment, is
// gone) but whose frame does not name them, because the ingest stored
// its pieces and never committed. A run whose segment may still be
// written is kept (spec/LogEtIngest.tla, SweepOrphan).
func (db *DB) sweepIngests(ctx context.Context, id string) (int, error) {
	db.mu.Lock()
	l := db.logs[id]
	db.mu.Unlock()
	if l == nil {
		return 0, nil // a dropped log: GC deletes all of it
	}
	l.mu.RLock()
	next := l.nextSeq
	l.mu.RUnlock()
	dir := "log/" + id + "/"
	keys, err := db.st.List(ctx, dir)
	if err != nil {
		return 0, err
	}
	named := map[uint64]map[string]bool{} // per segment: run keys its frames name
	n := 0
	for _, k := range keys {
		seq, ep, run, ok := parseIngest(k, dir)
		if !ok || ep >= db.epoch || seq >= next {
			continue
		}
		m, seen := named[seq]
		if !seen {
			if m, err = db.ingestsNamed(ctx, id, seq); err != nil {
				return n, err
			}
			named[seq] = m
		}
		if m == nil || m[run] {
			continue // unreadable segment, or committed: keep
		}
		if db.stopped() {
			return n, ErrClosed
		}
		if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
			return n, err
		}
		n++
	}
	return n, nil
}

// ingestsNamed returns the ingest runs segment seq's frames name (empty
// if the segment is gone, nil if it is unreadable).
func (db *DB) ingestsNamed(ctx context.Context, id string, seq uint64) (map[string]bool, error) {
	b, err := db.st.Get(ctx, segKey(id, seq))
	if errors.Is(err, store.ErrNotFound) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, err
	}
	h, frames, _, derr := format.DecodeSegment(b)
	if derr != nil || h.Seq != seq {
		return nil, nil
	}
	m := map[string]bool{}
	for _, f := range frames {
		for _, op := range f.Frame.Ops {
			if op.Kind == format.OpIngest {
				m[op.Key] = true
			}
		}
	}
	return m, nil
}
