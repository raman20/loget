package loget

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

type rkey struct{ space, key string }

type commitReq struct {
	at      uint64
	ops     []format.Op
	reads   map[rkey]bool
	ranges  []*keyRange
	attempt int // conflicts this transaction already lost (fairness)
	// exec, if set, is the transaction body to run inside the writer at the
	// applied head (see escalateAfter); ops/reads/at are filled from it.
	exec func(*Tx) error
	// ingest, if set, is a bulk ingest (ingest.go) served alone by
	// runIngest; ingestRun is the run its OpIngest frame installs.
	ingest    *ingestJob
	ingestRun *index.IngestRun
	done      chan commitRes
}

// size estimates the bytes r adds to a segment (an escalated transaction's
// ops are not known until it runs: counted as small).
func (r *commitReq) size() int {
	n := 64
	for _, op := range r.ops {
		n += len(op.Key) + len(op.Value) + len(op.Space) + len(op.Subject) + 16
	}
	return n
}

type commitRes struct {
	lsn      uint64
	err      error
	panicked any // exec panicked; re-raised in the caller
}

type recentCommit struct {
	lsn    uint64
	writes []rkey
}

// writer is the single sequencer of one log: it validates transactions
// (OCC, serializable), assigns LSNs, encrypts subject data, and group-
// commits a batch as ONE immutable segment. A transaction is acknowledged
// only after the segment is durable (fsync'd file / S3 PUT returned 200).
type writer struct {
	db *DB
	l  *logState
	v  *view

	mu      sync.RWMutex
	closed  bool
	paused  bool // stopped for compaction: submit returns errPaused
	ch      chan *commitReq
	done    chan struct{}
	crashed atomic.Bool

	failed      error
	head        uint64
	lastTS      int64
	recent      []recentCommit
	recentBytes int          // estimated heap held by recent (recentCost)
	carry       []*commitReq // escalated requests deferred to the next batch
	recentFloor uint64
	sinceSnap   int
	sinceCk     int64         // segment bytes since the last background checkpoint started
	bg          chan struct{} // closed when the background checkpoint ends; nil: none started
	bgCtx       context.Context
	bgCancel    context.CancelFunc

	// reg counts the snapshot LSNs of Update attempts that may still
	// submit (design 23, part 3), sharded so attempts on many goroutines
	// do not contend on one lock. The writer drops recent commits no such
	// snapshot can conflict with.
	reg     [regShards]regShard
	regNext atomic.Uint32
}

// The recent-commit window: commits newer than the oldest active
// snapshot are kept; the floor is raised past older ones (an attempt
// older than the floor gets ErrConflict and retries) only when a long
// attempt pins more than recentMax*2 commits or recentMaxBytes. Vars:
// tests shrink them.
var (
	recentMax      = 8192
	recentMaxBytes = 64 << 20
)

const regShards = 16

type regShard struct {
	mu     sync.Mutex
	active map[uint64]int
	_      [48]byte // own cache line
}

// recentCost estimates the heap one recent commit holds.
func recentCost(c recentCommit) int {
	n := 64 + 32*len(c.writes)
	for _, k := range c.writes {
		n += len(k.space) + len(k.key)
	}
	return n
}

// register records an Update attempt's snapshot (the view head, read
// under the shard lock so the writer cannot scan that shard between the
// read and the insert) until release, after the attempt's submit
// returns. An attempt registering after the writer scanned its shard
// reads a head at or past every commit the writer holds, so it needs
// none of the ones being dropped.
func (w *writer) register() (at uint64, shard int) {
	shard = int(w.regNext.Add(1) % regShards)
	r := &w.reg[shard]
	r.mu.Lock()
	w.v.mu.RLock()
	at = w.v.head
	w.v.mu.RUnlock()
	if r.active == nil {
		r.active = map[uint64]int{}
	}
	r.active[at]++
	r.mu.Unlock()
	return at, shard
}

func (w *writer) release(at uint64, shard int) {
	r := &w.reg[shard]
	r.mu.Lock()
	if r.active[at]--; r.active[at] == 0 {
		delete(r.active, at)
	}
	r.mu.Unlock()
}

// oldestActive is the lowest registered snapshot, or MaxUint64.
func (w *writer) oldestActive() uint64 {
	bound := ^uint64(0)
	for i := range w.reg {
		r := &w.reg[i]
		r.mu.Lock()
		for at := range r.active {
			bound = min(bound, at)
		}
		r.mu.Unlock()
	}
	return bound
}

// trimRecent drops recent commits at or below the oldest active
// snapshot: an attempt can only conflict with commits after its own
// snapshot, and one that starts later reads a view head at or past every
// commit held here (they are applied first). Past the count or byte
// budget it also drops newer ones and raises the floor.
func (w *writer) trimRecent() {
	bound := w.oldestActive()
	cut, bytes := 0, w.recentBytes
	for cut < len(w.recent) && w.recent[cut].lsn <= bound {
		bytes -= recentCost(w.recent[cut])
		cut++
	}
	if len(w.recent)-cut > 2*recentMax || bytes > recentMaxBytes {
		for cut < len(w.recent) && (len(w.recent)-cut > recentMax || bytes > recentMaxBytes/2) {
			bytes -= recentCost(w.recent[cut])
			cut++
		}
		w.recentFloor = max(w.recentFloor, w.recent[cut-1].lsn)
	}
	if cut == 0 {
		return
	}
	clear(w.recent[:cut]) // release the keys
	if cut == len(w.recent) {
		w.recent = w.recent[:0]
	} else {
		w.recent = w.recent[cut:]
	}
	w.recentBytes = bytes
}

// maxSegBytes caps one group-commit segment: transactions that would push
// a segment past it wait for the next one (the journal store takes objects
// up to 1 GiB; one transaction alone is at most MaxTxBytes).
var maxSegBytes = 256 << 20 // var: tests shrink it

func newWriter(db *DB, l *logState, v *view) *writer {
	w := &writer{db: db, l: l, v: v, ch: make(chan *commitReq, 1024), done: make(chan struct{})}
	w.bgCtx, w.bgCancel = context.WithCancel(context.Background())
	w.head = l.head()
	w.recentFloor = w.head
	l.mu.RLock()
	w.lastTS = l.lastTS
	l.mu.RUnlock()
	go w.loop()
	return w
}

func (w *writer) submit(r *commitReq) commitRes {
	r.done = make(chan commitRes, 1)
	w.mu.RLock()
	if w.closed {
		err := ErrClosed
		if w.paused {
			err = errPaused
		}
		w.mu.RUnlock()
		return commitRes{err: err}
	}
	t0 := time.Now()
	w.ch <- r
	w.mu.RUnlock()
	res := <-r.done
	m := &w.db.met
	switch {
	case res.err == nil:
		m.commits.Add(1)
		m.commitLat.Observe(time.Since(t0))
	case errors.Is(res.err, ErrConflict):
		m.conflicts.Add(1)
	case errors.Is(res.err, errPaused):
	default:
		m.commitErrors.Add(1)
		m.commitLat.Observe(time.Since(t0))
	}
	return res
}

func (w *writer) stop() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()
	<-w.done
}

// pause stops the writer for compaction; later submits get errPaused
// and retry on the writer that replaces it.
func (w *writer) pause() {
	w.mu.Lock()
	w.paused = true
	w.mu.Unlock()
	w.stop()
}

func (w *writer) crash() {
	w.crashed.Store(true)
	w.bgCancel() // a crashed DB writes nothing more: stop the checkpoint
	w.stop()
}

// startCkpt starts a background index checkpoint unless one is running.
func (w *writer) startCkpt() {
	if w.bg != nil {
		select {
		case <-w.bg:
		default:
			return
		}
	}
	w.sinceCk = 0
	done := make(chan struct{})
	w.bg = done
	go func() {
		defer close(done)
		w.ckpt(w.bgCtx)
	}()
}

func (w *writer) ckpt(ctx context.Context) {
	if err := w.db.writeIndexCkpt(ctx, w.l, w.v); err != nil {
		w.db.met.snapshotErrors.Add(1)
		w.db.logf("loget: index checkpoint of %s failed (non-fatal): %v", w.l.meta.ID, err)
	}
}

// referenced reports whether a branch names log id (a retired log, only
// reachable through the reflog, is not worth a checkpoint).
func (db *DB) referenced(id string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, r := range db.man.Refs {
		if r == id {
			return true
		}
	}
	return false
}

// finish runs when the loop ends: it waits for a background checkpoint,
// then, on a clean stop (Close; not a crash, a halt or a compaction
// pause), checkpoints so the next open replays nothing.
func (w *writer) finish() {
	if w.bg != nil {
		<-w.bg
	}
	w.mu.RLock()
	paused := w.paused
	w.mu.RUnlock()
	if !paused && !w.crashed.Load() && w.failed == nil && w.db.opts.CheckpointBytes > 0 && w.db.referenced(w.l.meta.ID) {
		w.ckpt(w.bgCtx)
	}
	w.bgCancel()
}

// CommitPolicy is the group-commit budget: after taking what is queued, a
// batch waits up to MaxDelay (from its first transaction's arrival) for
// more transactions, until it holds TargetBytes of keys and values or
// MaxBatch transactions. Waiting trades commit latency for fewer, larger
// segment writes: on an object store each segment is one billed PUT.
type CommitPolicy struct {
	MaxDelay    time.Duration // 0: never wait (the default)
	TargetBytes int           // 0 with MaxDelay set: 1 MiB
}

const defaultTargetBytes = 1 << 20

func (w *writer) loop() {
	defer close(w.done)
	defer w.finish()
	var timer *time.Timer
	if w.db.opts.Commit.MaxDelay > 0 {
		timer = time.NewTimer(time.Hour)
		timer.Stop()
	}
	for {
		batch := w.carry
		w.carry = nil
		if len(batch) == 0 {
			r, ok := <-w.ch
			if !ok {
				return
			}
			batch = append(batch, r)
		}
		start := time.Now()
		batch = w.drain(batch)
		if timer != nil {
			batch = w.linger(batch, start, timer)
		}
		for len(batch) > 0 { // ingests run alone, in arrival order
			i := 0
			for i < len(batch) && batch[i].ingest == nil {
				i++
			}
			if i > 0 {
				w.process(batch[:i])
			}
			if i < len(batch) {
				w.runIngest(batch[i])
				i++
			}
			batch = batch[i:]
		}
	}
}

// drain adds the requests already queued, up to MaxBatch.
func (w *writer) drain(batch []*commitReq) []*commitReq {
	for len(batch) < w.db.opts.MaxBatch {
		select {
		case r, ok := <-w.ch:
			if !ok {
				return batch
			}
			batch = append(batch, r)
		default:
			return batch
		}
	}
	return batch
}

// linger waits for more requests within the CommitPolicy budget. A closed
// queue ends the wait; loop sees the close on its next receive.
func (w *writer) linger(batch []*commitReq, start time.Time, timer *time.Timer) []*commitReq {
	p := w.db.opts.Commit
	size := 0
	for _, r := range batch {
		size += r.size()
	}
	for len(batch) < w.db.opts.MaxBatch && size < p.TargetBytes {
		wait := p.MaxDelay - time.Since(start)
		if wait <= 0 {
			break
		}
		timer.Reset(wait)
		select {
		case r, ok := <-w.ch:
			timer.Stop()
			if !ok {
				return batch
			}
			batch = append(batch, r)
			size += r.size()
		case <-timer.C:
			return batch
		}
	}
	return batch
}

// runExec runs an escalated transaction body at the applied head. It
// reports whether r now carries ops to validate and commit; otherwise r
// has been answered (error, panic, or read-only).
func (w *writer) runExec(r *commitReq) (ok bool) {
	tx := &Tx{db: w.db, v: w.v, at: w.head, writable: true, reads: map[rkey]bool{}, local: map[rkey]*localVal{}}
	defer func() {
		if p := recover(); p != nil {
			r.done <- commitRes{panicked: p}
			ok = false
		}
	}()
	if err := r.exec(tx); err != nil {
		r.done <- commitRes{err: err}
		return false
	}
	if len(tx.ops) == 0 {
		r.done <- commitRes{lsn: tx.at}
		return false
	}
	r.at, r.ops, r.reads, r.ranges = tx.at, tx.ops, tx.reads, tx.ranges
	return true
}

func (w *writer) conflict(r *commitReq, pending []recentCommit) bool {
	if len(r.reads) == 0 && len(r.ranges) == 0 {
		return false // blind writes are always serializable
	}
	if r.at < w.recentFloor {
		return true // history needed for validation was trimmed
	}
	check := func(c recentCommit) bool {
		if c.lsn <= r.at {
			return false
		}
		for _, wr := range c.writes {
			if r.reads[wr] {
				return true
			}
			for _, rg := range r.ranges {
				if rg.has(wr) {
					return true
				}
			}
		}
		return false
	}
	for i := len(w.recent) - 1; i >= 0 && w.recent[i].lsn > r.at; i-- {
		if check(w.recent[i]) {
			return true
		}
	}
	for _, c := range pending {
		if check(c) {
			return true
		}
	}
	return false
}

func (w *writer) seal(ctx context.Context, ops []format.Op, lsn uint64) ([]format.Op, error) {
	out := make([]format.Op, len(ops))
	for i, op := range ops {
		if op.Subject != "" && op.Kind != format.OpShred && op.Kind != format.OpDel {
			a, err := w.db.keys.dek(ctx, op.Subject)
			if err != nil {
				return nil, err
			}
			ct, err := w.db.keys.seal(a, op.Value, aad(w.l.meta.ID, lsn, i, op.Space, op.Key))
			if err != nil {
				return nil, err
			}
			op.Value = ct
			op.Subject = SubjectHash(op.Subject)
		} else if op.Subject != "" {
			op.Subject = SubjectHash(op.Subject)
		}
		out[i] = op
	}
	return out, nil
}

func (w *writer) process(batch []*commitReq) {
	ctx := context.Background()
	fail := func(rs []*commitReq, err error) {
		for _, r := range rs {
			r.done <- commitRes{err: err}
		}
	}
	if w.crashed.Load() {
		fail(batch, ErrClosed)
		return
	}
	if w.failed == nil {
		w.db.mu.Lock()
		w.failed = w.db.failed
		w.db.mu.Unlock()
	}
	if w.failed != nil {
		fail(batch, w.failed)
		return
	}
	w.l.mu.RLock()
	seq := w.l.nextSeq
	chain := w.l.tip()
	w.l.mu.RUnlock()

	seg := format.EncodeHeader(format.SegHeader{Epoch: w.db.epoch, Seq: seq})
	head := w.head
	ts := w.lastTS
	var accepted []*commitReq
	var entries []*index.Entry
	var pending []recentCommit
	// Validate transactions that already lost conflicts first: within a
	// batch the oldest retry wins a contended key, so a hot key cannot
	// starve one client while newcomers keep committing. The order only
	// chooses which of several conflicting transactions commits; each
	// accepted one is still validated against everything before it.
	sort.SliceStable(batch, func(i, j int) bool { return batch[i].attempt > batch[j].attempt })
	// Escalated transactions run here, at the applied head, before the batch
	// is validated. The first one is validated first and cannot conflict;
	// a later one that conflicts with what this batch already accepted is
	// carried to the next batch (and re-run there) instead of failing.
	kept := batch[:0]
	for _, r := range batch {
		if r.exec == nil || w.runExec(r) {
			kept = append(kept, r)
		}
	}
	batch = kept
	for _, r := range batch {
		if w.conflict(r, pending) {
			if r.exec != nil {
				w.carry = append(w.carry, r)
				continue
			}
			r.done <- commitRes{err: ErrConflict}
			continue
		}
		lsn := head + 1
		ops, err := w.seal(ctx, r.ops, lsn)
		if err != nil {
			r.done <- commitRes{err: err}
			continue
		}
		if now := w.db.opts.Clock(); now > ts {
			ts = now
		}
		f := &format.Frame{LSN: lsn, TS: ts, Prev: chain, Ops: ops}
		prev := len(seg)
		seg2, payload := format.AppendFrame(seg, f)
		if len(payload) > format.MaxFrameSize || len(ops) > format.MaxOps {
			// The decoder rejects such a frame: writing it would make the
			// log unreadable (docs/design/13-durability.md).
			seg = seg2[:prev]
			r.done <- commitRes{err: fmt.Errorf("%w: %d bytes, %d ops encoded (limits %d bytes, %d ops)",
				ErrTooLarge, len(payload), len(ops), format.MaxFrameSize, format.MaxOps)}
			continue
		}
		if len(seg2) > maxSegBytes && len(accepted) > 0 {
			seg = seg2[:prev] // full: commit it in the next segment
			w.carry = append(w.carry, r)
			continue
		}
		seg = seg2
		chain = format.ChainNext(chain, payload)
		head = lsn
		accepted = append(accepted, r)
		entries = append(entries, &index.Entry{Log: w.l.meta.ID, LSN: lsn, TS: ts, Ops: ops, Ingest: r.ingestRun})
		rc := recentCommit{lsn: lsn}
		for _, op := range ops {
			if op.Kind == format.OpPut || op.Kind == format.OpDel {
				rc.writes = append(rc.writes, rkey{op.Space, op.Key})
			}
		}
		pending = append(pending, rc)
	}
	if len(accepted) == 0 {
		return
	}
	if w.crashed.Load() {
		fail(accepted, ErrClosed)
		return
	}
	// Durability point: the whole batch becomes visible atomically or not at all.
	t0 := time.Now()
	err := putExactlyOnce(ctx, w.db.st, segKey(w.l.meta.ID, seq), seg)
	w.db.met.segmentLat.Observe(time.Since(t0))
	if err != nil {
		if errors.Is(err, store.ErrExists) {
			w.failed = w.db.fenced()
		} else if errors.Is(err, ErrUnknownOutcome) {
			w.failed = fmt.Errorf("segment %d, writer halted until reopen: %w", seq, err)
		} else {
			w.failed = fmt.Errorf("segment %d, writer halted until reopen: %w: %w", seq, ErrUnknownOutcome, err)
		}
		halt := w.failed
		w.db.met.halted.CompareAndSwap(nil, &halt)
		w.db.logf("loget: writer for %s halted: %v", w.l.meta.ID, w.failed)
		fail(accepted, w.failed)
		return
	}
	w.db.met.segments.Add(1)
	w.db.met.segmentBytes.Add(uint64(len(seg)))
	w.l.mu.Lock()
	w.l.addSeg(seq, entries[0].LSN, uint64(len(entries)), ts, chain)
	w.l.nextSeq = seq + 1
	w.l.lastEp = w.db.epoch
	w.l.mu.Unlock()
	w.v.mu.Lock()
	for _, e := range entries {
		w.v.apply(e)
	}
	w.v.mu.Unlock()
	w.db.bump()
	w.head, w.lastTS = head, ts
	for _, c := range pending {
		w.recentBytes += recentCost(c)
	}
	w.recent = append(w.recent, pending...)
	w.trimRecent()
	// Snapshots are taken before acking so that, with one client, every
	// store call happens in a deterministic order (simulation replayability).
	w.sinceSnap += len(accepted)
	w.sinceCk += int64(len(seg))
	if every := w.db.opts.SnapshotEvery; every > 0 {
		if w.sinceSnap >= every {
			w.sinceSnap = 0
			w.ckpt(ctx)
		}
	} else if cb := w.db.opts.CheckpointBytes; cb > 0 && w.sinceCk >= cb {
		w.startCkpt()
	}
	for i, r := range accepted {
		r.done <- commitRes{lsn: entries[i].LSN}
	}
}
