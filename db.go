package loget

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/store"
)

// Options configure a DB. Every field has a working default.
type Options struct {
	// Clock returns the commit timestamp (unix nanos). Recorded in the log,
	// so indexes never read a clock themselves. Fake it in simulations.
	Clock func() int64
	// Rand is the entropy source for keys and nonces (crypto/rand default).
	Rand io.Reader
	// Holder names this process in its lease record (WithHolder).
	Holder string
	// KEK is the 32-byte key-encryption key. Nil = dev mode: generated and
	// stored in the bucket at keys/_kek (NOT safe against bucket backups).
	KEK []byte
	// Keys, if set, supplies the KEK at open (overrides KEK).
	Keys KeyProvider
	// SnapshotEvery writes an index checkpoint and a segment-index
	// checkpoint every N commits per log, synchronously in the commit path
	// (<= 0 = never, the default; then CheckpointBytes decides). Open
	// recovers the log from the segment-index checkpoint (reading only the
	// segments after it) and restores the record store from the stored
	// runs and every derived index that can snapshot itself
	// (index.Kind.Restore). A derived index that cannot is rebuilt by
	// reading every segment once. An index checkpoint stores only the
	// runs no earlier checkpoint stored (design 24).
	SnapshotEvery int
	// CheckpointBytes starts an index checkpoint in the background once a
	// log has written this many segment bytes since the last one, unless
	// SnapshotEvery is set; Close also checkpoints every log with commits
	// past its checkpoint. 0 = default (512 MiB); negative = never (open
	// then replays from the base or a SnapshotEvery checkpoint).
	CheckpointBytes int64
	// ReadAhead is how many segment reads a replay (open, time travel,
	// index builds) keeps in flight. 0 = default (8); 1 = one at a time.
	ReadAhead int
	// MaxBatch bounds how many transactions one group commit may contain.
	MaxBatch int
	// Commit sets how long a group commit may wait to grow (CommitPolicy).
	// The zero value never waits: a batch is whatever queued up while the
	// previous segment was being written.
	Commit CommitPolicy
	// Logf receives operational messages (default: log.Printf; nil-safe).
	Logf func(format string, args ...any)
	// InlineMax: KV values up to this many bytes are kept in memory; larger
	// ones are kept as (segment, offset, length) positions and read from
	// the log on demand. 0 = default (64); negative = keep none inline.
	InlineMax int
	// ValueCacheBytes bounds the cache of values read from segments. The
	// cache lives off the Go heap (anonymous mmap where available), so RSS
	// grows by about this much once it fills, not by a GC multiple.
	// 0 = default: 1/16 of the memory limit (cgroup v2, else MemTotal),
	// between 16 and 256 MiB, and at most 32 MiB on a local store, where
	// the OS page cache already holds segment bytes (design 25);
	// negative = no cache.
	ValueCacheBytes int
	// IndexDir is where the KV index keeps its large runs (design 21):
	// memory-mapped files, unlinked as soon as they are created, so the
	// directory never shows them and a crash leaves nothing behind. The
	// pages are file-backed, so the kernel can evict them under memory
	// pressure instead of the index holding them on the Go heap. Use a
	// local disk: on tmpfs the pages are RAM again. "" = os.TempDir();
	// if that is unusable the index stays on the heap (logged). An
	// explicit directory that is unusable fails Open. CLI: --index-dir.
	IndexDir string
	// IndexResidentBytes bounds the KV index's mapped run pages counted in
	// this process's RSS (design 25). 0 = unbounded: pages a lookup
	// touched stay mapped until the kernel reclaims them. > 0: a
	// background check every 100 ms drops every run file's pages from the
	// process when its file-backed RSS exceeds n; the data stays in the
	// OS page cache and later lookups fault it back (minor faults, a cost
	// on reads). Linux only (other systems ignore it).
	IndexResidentBytes int64
	// IndexInMemory keeps the whole KV index on the Go heap (no run
	// files), as before design 21.
	IndexInMemory bool
	// IndexFiles, if set, supplies the run files and puts every run in
	// one, however small (simulations and tests inject faults with it).
	// Overrides IndexDir.
	IndexFiles index.RunFiles
	// RepairTail lets open repair a damaged final segment by keeping its
	// valid prefix. The damaged object stays in place; a create-only
	// repair note (log/<id>/<seq>.repair) records which prefix is kept.
	// Every supported store writes objects atomically, so a damaged tail is
	// corruption, not a torn write, and may hold acknowledged commits: by
	// default open refuses with ErrCorrupt and changes nothing. Set it (CLI:
	// --repair-tail) only to salvage a database you cannot restore.
	RepairTail bool
	// Indexes are the index kinds registered with WithIndex.
	Indexes []index.Kind
	// SnapshotMaxAge bounds how long a Snapshot (readview.go) holds back
	// compaction: older ones stop pinning and fail with
	// ErrSnapshotTooOld. Measured with Clock. <= 0 = default (10 min).
	SnapshotMaxAge time.Duration
}

type Option func(*Options)

func WithClock(f func() int64) Option { return func(o *Options) { o.Clock = f } }

// WithSnapshotMaxAge sets Options.SnapshotMaxAge.
func WithSnapshotMaxAge(d time.Duration) Option { return func(o *Options) { o.SnapshotMaxAge = d } }
func WithRand(r io.Reader) Option               { return func(o *Options) { o.Rand = r } }
func WithKEK(k []byte) Option                   { return func(o *Options) { o.KEK = k } }

// WithCommitPolicy sets the group-commit budget (see CommitPolicy).
func WithCommitPolicy(p CommitPolicy) Option { return func(o *Options) { o.Commit = p } }

func WithSnapshotEvery(n int) Option         { return func(o *Options) { o.SnapshotEvery = n } }
func WithCheckpointBytes(n int64) Option     { return func(o *Options) { o.CheckpointBytes = n } }
func WithLogf(f func(string, ...any)) Option { return func(o *Options) { o.Logf = f } }
func WithInlineMax(n int) Option             { return func(o *Options) { o.InlineMax = n } }
func WithValueCacheBytes(n int) Option       { return func(o *Options) { o.ValueCacheBytes = n } }
func WithRepairTail(on bool) Option          { return func(o *Options) { o.RepairTail = on } }
func WithReadAhead(n int) Option             { return func(o *Options) { o.ReadAhead = n } }
func WithIndexDir(dir string) Option         { return func(o *Options) { o.IndexDir = dir } }
func WithIndexResidentBytes(n int64) Option {
	return func(o *Options) { o.IndexResidentBytes = n }
}
func WithIndexInMemory(on bool) Option { return func(o *Options) { o.IndexInMemory = on } }

// WithIndexFiles supplies the KV index run files (Options.IndexFiles).
func WithIndexFiles(f index.RunFiles) Option { return func(o *Options) { o.IndexFiles = f } }

const (
	defaultReadAhead  = 8
	defaultInlineMax  = 64
	defaultValueCache = 256 << 20
	// defaultCheckpointBytes: an index checkpoint per this much log
	// written (design 24).
	defaultCheckpointBytes = 512 << 20
)

// DB is an open database. One process holds the write lease (epoch); a newer
// Open anywhere fences this one, after which writes fail with ErrFenced.
type DB struct {
	st    store.Store
	opts  Options
	epoch uint64
	// fullScan: recovery ignores segment-index checkpoints (Verify reads
	// every segment and cross-checks the checkpoints instead)
	fullScan bool

	snaps snapRegistry // live Snapshots (readview.go)

	reg  *index.Registry // index kinds (WithIndex); fixed after Open
	keys *keyring
	// devKEK: the KEK lives in the bucket (no key supplied).
	devKEK bool
	// chg is closed when a branch head may have moved (notify.go).
	chgMu sync.Mutex
	chg   chan struct{}
	env   env

	mu      sync.RWMutex // guards manifest, logs, views, writers
	man     *Manifest
	logs    map[string]*logState
	views   map[string]*view   // live views of writable logs
	writers map[string]*writer // by log id
	vcache  *valueCache
	// runFiles holds the KV index's large runs (nil = Go heap; design 21).
	runFiles  index.RunFiles
	stopBound func() // stops the IndexResidentBytes check
	met       metrics
	// bw caches branch name -> writer for the read path; nil = rebuild.
	// Reset (under db.mu held exclusively) whenever refs, writers or
	// closed change, so readers never need db.mu on a hit.
	bw     atomic.Pointer[map[string]*writer]
	failed error
	closed bool
	owned  io.Closer // store opened by Open from a URL; closed by Close
	// compacting is set while Compact runs; branch ops wait on
	// compactDone (lockIdle) and paused writers' callers retry after it.
	compacting  bool
	compactDone chan struct{}
	sweepMu     sync.Mutex // one garbage sweep at a time (compact.go)
}

// lockIdle takes db.mu once no compaction is running.
func (db *DB) lockIdle() {
	for {
		db.mu.Lock()
		if !db.compacting {
			return
		}
		ch := db.compactDone
		db.mu.Unlock()
		<-ch
	}
}

// waitCompaction blocks until a running compaction finishes.
func (db *DB) waitCompaction(ctx context.Context) error {
	db.mu.RLock()
	ch := db.compactDone
	busy := db.compacting
	db.mu.RUnlock()
	if !busy {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Open opens (or creates) the database at url: a directory path or
// file:///path (its existing layout, else the journal layout on Unix; see
// store.OpenLocal), journal:///path, fs:///path, mem://name or
// s3://bucket/prefix?endpoint=...
func Open(ctx context.Context, url string, opts ...Option) (*DB, error) {
	st, err := store.Open(url)
	if err != nil {
		return nil, err
	}
	if _, isS3 := st.(*store.S3); isS3 {
		if err := store.CheckConditional(ctx, st); err != nil {
			return nil, err
		}
	}
	db, err := OpenStore(ctx, st, opts...)
	if c, ok := st.(io.Closer); ok {
		if err != nil {
			c.Close()
			return nil, err
		}
		db.owned = c
	}
	return db, err
}

// OpenStore opens a database on an explicit Store.
func OpenStore(ctx context.Context, st store.Store, opts ...Option) (*DB, error) {
	o := Options{}
	for _, f := range opts {
		f(&o)
	}
	if o.Clock == nil {
		o.Clock = func() int64 { return time.Now().UnixNano() }
	}
	if o.Rand == nil {
		o.Rand = rand.Reader
	}
	if o.SnapshotMaxAge <= 0 {
		o.SnapshotMaxAge = defaultSnapshotMaxAge
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
	if o.Commit.MaxDelay > 0 && o.Commit.TargetBytes <= 0 {
		o.Commit.TargetBytes = defaultTargetBytes
	}
	if o.ReadAhead <= 0 {
		o.ReadAhead = defaultReadAhead
	}
	if o.CheckpointBytes == 0 {
		o.CheckpointBytes = defaultCheckpointBytes
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	switch {
	case o.InlineMax == 0:
		o.InlineMax = defaultInlineMax
	case o.InlineMax < 0:
		o.InlineMax = 0
	}
	if o.ValueCacheBytes == 0 {
		o.ValueCacheBytes = defaultValueCacheBytes(localStore(st))
	}
	reg, err := newRegistry(o.Indexes)
	if err != nil {
		return nil, err
	}
	files, err := openRunFiles(o)
	if err != nil {
		return nil, err
	}
	db := &DB{runFiles: files, st: &holdStore{Store: st}, opts: o, reg: reg, vcache: newValueCache(o.ValueCacheBytes), logs: map[string]*logState{}, views: map[string]*view{}, writers: map[string]*writer{}}
	db.stopBound = boundResident(files, o.IndexResidentBytes)
	if err := db.start(ctx, st); err != nil {
		// Writers of branches opened before the failure are running, and
		// the value cache holds memory: stop both without further writes.
		db.Crash()
		return nil, err
	}
	return db, nil
}

// start takes the write lease, opens the keyring and starts a writer per
// branch (OpenStore).
func (db *DB) start(ctx context.Context, st store.Store) error {
	o := db.opts
	if err := db.acquire(ctx); err != nil {
		return err
	}
	kek := o.KEK
	if o.Keys != nil {
		var err error
		if kek, err = o.Keys.KEK(ctx); err != nil {
			return err
		}
		if len(kek) != 32 {
			return fmt.Errorf("loget: key provider returned %d bytes, want 32", len(kek))
		}
	}
	db.devKEK = kek == nil
	if db.devKEK {
		devKEKWarn.Do(func() {
			db.logf("loget: WARNING: dev-mode key: the key-encryption key is stored in the bucket at keys/_kek; anyone with a copy of the bucket can read unshredded data. Supply a KEK (WithKeyProvider, LOGET_KEK, --kek-file) in production.")
		})
	}
	var err error
	if db.keys, err = openKeyring(ctx, st, o.Rand, kek); err != nil {
		return err
	}
	db.env = env{db.keys}
	for _, name := range sortedMapKeys(db.man.Refs) {
		if _, err := db.openWritable(ctx, db.man.Refs[name]); err != nil {
			return err
		}
	}
	return nil
}

// devKEKWarn: Open warns once per process (through Logf) in dev-key mode.
var devKEKWarn sync.Once

// DevKEK reports whether this database runs on the dev-mode key stored in
// the bucket (no KEK or key provider was supplied).
func (db *DB) DevKEK() bool { return db.devKEK }

func (db *DB) logf(f string, a ...any) {
	if db.opts.Logf != nil {
		db.opts.Logf(f, a...)
	}
}

// acquire takes the write lease: create manifest id+1 with epoch+1.
func (db *DB) acquire(ctx context.Context) error {
	for i := 0; i < 64; i++ {
		m, err := loadLatestManifest(ctx, db.st)
		if err != nil {
			return err
		}
		var next *Manifest
		if m == nil {
			idb := make([]byte, 16)
			if _, err := io.ReadFull(db.opts.Rand, idb); err != nil {
				return err
			}
			next = &Manifest{Format: manifestFormat, ID: 1, Epoch: 1, DBID: hex.EncodeToString(idb), NextLog: 2,
				Logs: map[string]*LogMeta{"L1": {ID: "L1"}}, Refs: map[string]string{"main": "L1"}}
		} else {
			next = m.clone()
			next.ID, next.Epoch = m.ID+1, m.Epoch+1
		}
		err = putManifest(ctx, db.st, next)
		if errors.Is(err, store.ErrExists) {
			continue // someone else advanced; re-read and try again
		}
		if err != nil {
			return err
		}
		db.man, db.epoch = next, next.Epoch
		db.bw.Store(nil)
		db.writeLease(ctx, next.Epoch-1)
		return nil
	}
	return errors.New("loget: could not acquire write lease (manifest contention)")
}

// manifestPutTimeout bounds a manifest commit, which ignores the
// caller's cancellation once started (commitManifest).
const manifestPutTimeout = 2 * time.Minute

// commitManifest applies change to a copy of the manifest and creates the
// next manifest. ErrExists => a newer writer exists => we are fenced; the
// error also wraps ErrUnknownOutcome when the change may still be committed
// (putManifest). Any failure halts the DB until reopen. Caller holds db.mu.
func (db *DB) commitManifest(ctx context.Context, change func(m *Manifest) error) error {
	if db.failed != nil {
		return db.failed
	}
	// Waiters re-read heads under db.mu, which the caller holds until the
	// views match the new manifest, so waking them here is safe.
	defer db.bump()
	next := db.man.clone()
	if err := change(next); err != nil {
		return err
	}
	next.ID = db.man.ID + 1
	// A caller that has given up gets nothing committed and no halt. Once
	// the put starts it runs to the end on its own deadline: a cancel in
	// the middle would leave the outcome unknown, which halts the DB (an
	// HTTP client timing out on /v1/compact did, in a soak).
	if err := ctx.Err(); err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), manifestPutTimeout)
	defer cancel()
	err := putManifest(pctx, db.st, next)
	if errors.Is(err, store.ErrExists) {
		db.failed = db.fenced()
		if errors.Is(err, ErrUnknownOutcome) {
			return fmt.Errorf("%w (change may be committed: %w)", db.failed, err)
		}
		return db.failed
	}
	if err != nil {
		db.failed = err
		return err
	}
	db.man = next
	db.bw.Store(nil)
	return nil
}

// loadLog returns the (cached) logState of id, recovering it from storage.
// Caller holds db.mu.
func (db *DB) loadLog(ctx context.Context, id string, writable bool) (*logState, error) {
	if l, ok := db.logs[id]; ok {
		return l, nil
	}
	meta, ok := db.man.Logs[id]
	if !ok {
		return nil, fmt.Errorf("%w: unknown log %s", ErrCorrupt, id)
	}
	l := &logState{meta: *meta, st: db.st, readAhead: db.opts.ReadAhead}
	if b := meta.Base; b != nil {
		// A compacted log starts from its base: no parent, no older segments.
		c, err := hex.DecodeString(b.Chain)
		if err != nil || len(c) != 32 || b.LSN < meta.ParentLSN {
			return nil, fmt.Errorf("%w: log %s: bad base", ErrCorrupt, id)
		}
		copy(l.base[:], c)
		l.n, l.lastTS, l.nextSeq = b.LSN-meta.ParentLSN, b.TS, b.Seq
	} else if meta.Parent != "" {
		p, err := db.loadLog(ctx, meta.Parent, false)
		if err != nil {
			return nil, err
		}
		if p.head() < meta.ParentLSN {
			return nil, fmt.Errorf("%w: log %s parent %s shorter than branch point %d", ErrCorrupt, id, meta.Parent, meta.ParentLSN)
		}
		l.parent = p
		if l.base, err = p.chainAt(ctx, meta.ParentLSN); err != nil {
			return nil, err
		}
	}
	if err := db.loadCheckpoint(ctx, l); err != nil {
		return nil, err
	}
	if err := db.recoverLog(ctx, l, writable); err != nil {
		return nil, err
	}
	if meta.Ckpt != nil && meta.Ckpt.LSN > l.head() {
		// as for snapshots below: written after its LSN was durable
		return nil, fmt.Errorf("%w: log %s ends at LSN %d but its index checkpoint is at LSN %d: newest segments are missing", ErrCorrupt, id, l.head(), meta.Ckpt.LSN)
	}
	if meta.SnapLSN > l.head() {
		// The snapshot was written after a commit at SnapLSN was durable:
		// the log's newest segments are gone.
		return nil, fmt.Errorf("%w: log %s ends at LSN %d but its snapshot is at LSN %d: newest segments are missing", ErrCorrupt, id, l.head(), meta.SnapLSN)
	}
	db.logs[id] = l
	return l, nil
}

// openWritable recovers, fences and starts the writer for a log. Caller
// holds db.mu, except OpenStore: the DB is not shared yet, so no writer it
// started has requests and nothing else touches the maps.
func (db *DB) openWritable(ctx context.Context, id string) (*writer, error) {
	if w, ok := db.writers[id]; ok {
		return w, nil
	}
	l, err := db.loadLog(ctx, id, true)
	if err != nil {
		return nil, err
	}
	if err := db.fence(ctx, l); err != nil {
		return nil, err
	}
	snap := db.loadSnapshot(ctx, l)
	v, err := db.buildView(ctx, l, l.head(), snap)
	if err != nil {
		return nil, err
	}
	db.views[id] = v
	w := newWriter(db, l, v)
	db.writers[id] = w
	db.bw.Store(nil)
	return w, nil
}

// branchWriter returns the writer of a named branch.
func (db *DB) branchWriter(name string) (*writer, error) {
	if m := db.bw.Load(); m != nil {
		if w, ok := (*m)[name]; ok {
			return w, nil
		}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.bw.Load() == nil {
		m := make(map[string]*writer, len(db.man.Refs))
		for ref, id := range db.man.Refs {
			if w, ok := db.writers[id]; ok {
				m[ref] = w
			}
		}
		db.bw.Store(&m) // still under db.mu: no reset can interleave
	}
	id, ok := db.man.Refs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoBranch, name)
	}
	w, ok := db.writers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q not open", ErrNoBranch, name)
	}
	return w, nil
}

// Close stops all writers after draining queued commits.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.bw.Store(nil)
	ws := db.writers
	db.mu.Unlock()
	db.bump()
	for _, id := range sortedMapKeys(ws) {
		ws[id].stop()
	}
	db.vcache.close()
	if db.stopBound != nil {
		db.stopBound()
	}
	if db.owned != nil {
		return db.owned.Close()
	}
	return nil
}

// Crash abandons the DB without any further storage writes (for crash tests
// and simulations). Pending commits fail with ErrClosed.
func (db *DB) Crash() {
	db.mu.Lock()
	db.closed = true
	db.bw.Store(nil)
	db.failed = ErrClosed
	ws := db.writers
	db.mu.Unlock()
	db.bump()
	for _, id := range sortedMapKeys(ws) {
		ws[id].crash()
	}
	db.vcache.close()
	if db.stopBound != nil {
		db.stopBound()
	}
}

// Epoch returns this writer's fencing epoch.
func (db *DB) Epoch() uint64 { return db.epoch }

// Manifest returns a copy of the current manifest.
func (db *DB) Manifest() *Manifest {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.man.clone()
}

// Branches returns branch name -> head LSN.
func (db *DB) Branches() map[string]uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := map[string]uint64{}
	for n, id := range db.man.Refs {
		if v, ok := db.views[id]; ok {
			v.mu.RLock()
			out[n] = v.head
			v.mu.RUnlock()
		}
	}
	return out
}

// Entries returns the committed log entries visible on a branch (oldest
// first). Values of subject data are ciphertext.
func (db *DB) Entries(branch string) ([]*index.Entry, error) {
	w, err := db.branchWriter(branch)
	if err != nil {
		return nil, err
	}
	w.v.mu.RLock()
	h := w.v.head
	w.v.mu.RUnlock()
	return w.l.lineage(context.Background(), h)
}
