// Package sim is a deterministic simulation harness for loget.
//
// One seed fully determines a run: the workload, the fake clock, every
// random byte the engine consumes (keys, nonces), and every injected fault
// (failed / lost / delayed+reordered / duplicated responses, torn writes,
// crashes, a second writer taking over). The engine is driven by a single
// client goroutine and the writer acks only after all of its store calls,
// so the sequence of store calls — and therefore the run — is a pure
// function of the seed. Run reports a trace hash so determinism itself is
// checked (same seed twice ⇒ identical trace).
//
// The reference model is a plain sequence of transaction ids. After every
// recovery the harness checks refinement against it:
//
//	acked ⊆ recovered ⊆ acked ∪ ambiguous      (durability, no phantoms)
//	recovered is strictly increasing           (no reordering, no double-apply)
//	indexes == fold(model ops of recovered)     (KV + counter index, shred-redacted)
//	index digests == full replay digests        (index = pure fold over log)
//	a fenced (zombie) writer never commits     (no split brain)
//	shredded subjects are never readable       (crypto-shred)
//	Verify(): hash chains + manifest history OK
package sim

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/loget"
	"github.com/raman20/loget/examples/counter"
	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
	"github.com/raman20/loget/internal/runfile"
	"github.com/raman20/loget/store"
)

type Config struct {
	Steps int
	Probs store.FaultProbs
}

type Result struct {
	Seed             int64
	Steps            int
	Acked            int
	Ambiguous        int
	Crashes          int
	ReadFaultCorrupt int // opens that said ErrCorrupt under read faults (then opened clean without them)
	CalmOpens        int // recoveries that needed the fault storm to pass (100 failed opens)
	Closes           int // clean closes (final snapshot) instead of a crash
	Takeovers        int
	Shreds           int
	Rewinds          int
	Compacts         int
	GCs              int
	Faults           int
	IndexFileFaults  int // injected KV index run-file failures (every 4th seed keeps runs in files)
	Expired          int // keys a check found expired (design 27)
	Expiring         int // keys a check found live with an expiry still ahead
	TraceHash        [32]byte
}

// the counter the sim drives (examples/counter, a derived index) and the
// values its items take
const simCounter = "c"

var simCounterVals = []string{"0", "1", "2"}

type mop struct {
	kind    byte // 'p' put, 'd' del, 'c' counter set, 'r' counter remove
	key     string
	val     string
	subject string
	exp     int64 // 'p': absolute expiry (loget.ExpiresAt), 0 = none (design 27)
}

type sim struct {
	ctx     context.Context
	rng     *rand.Rand
	seed    int64
	mem     *store.Mem
	st      *store.Faulty
	db      *loget.DB
	clock   atomic.Int64
	rnd     *lockedRand
	txs     map[int][]mop  // all issued txns
	acked   map[int]uint64 // tx id -> lsn
	amb     map[int]bool
	shred   map[string]bool // subject -> definitely shredded
	shredQ  map[string]bool // shred outcome unknown
	rewindQ uint64          // ambiguous rewind target (+1), 0 = none
	// prefix: tx ids (and LSNs) in each compaction base, by "log@LSN";
	// the log only has entries above the base
	prefix map[string]txPrefix
	nextID int
	lsns   []uint64 // LSNs of the ids last returned by recovered
	// TS of main's head frame as recovered saw it (headOK: there is one
	// above the base)
	headTS int64
	headOK bool
	res    Result
	trace  []string
	// files: the KV index run files (nil = heap), in indexDir
	files      index.RunFiles
	indexDir   string
	fileFaults atomic.Int64
}

type txPrefix struct {
	ids  []int
	lsns []uint64
}

type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) Read(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Read(p)
}

func (s *sim) fail(f string, a ...any) error {
	tail := s.trace
	if len(tail) > 25 {
		tail = tail[len(tail)-25:]
	}
	st := s.st.Trace
	if len(st) > 15 {
		st = st[len(st)-15:]
	}
	return fmt.Errorf("seed %d step %d: %s\n  recent: %s\n  store: %s", s.seed, s.res.Steps, fmt.Sprintf(f, a...), strings.Join(tail, " | "), strings.Join(st, " | "))
}

func (s *sim) log(f string, a ...any) { s.trace = append(s.trace, fmt.Sprintf(f, a...)) }

// indexFiles puts every KV run in a run file whose operations fail with a
// per-seed probability (0, 2% or 20%).
func (s *sim) indexFiles() error {
	if !runfile.Supported {
		return nil
	}
	dir, err := os.MkdirTemp("", "loget-sim-index-")
	if err != nil {
		return err
	}
	d, err := runfile.New(dir)
	if err != nil {
		os.RemoveAll(dir)
		return err
	}
	p := []float64{0, 0.02, 0.2}[s.seed/4%3]
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(s.seed ^ 0x1dec5))
	d.Fault = func(op string) error {
		mu.Lock()
		defer mu.Unlock()
		if rng.Float64() < p {
			s.fileFaults.Add(1)
			return fmt.Errorf("sim: injected run-file %s fault", op)
		}
		return nil
	}
	s.files, s.indexDir = simFiles{d}, dir
	return nil
}

// simFiles adapts runfile.Dir to index.RunFiles.
type simFiles struct{ d *runfile.Dir }

func (f simFiles) Create() (index.RunFile, error) {
	rf, err := f.d.Create()
	if err != nil {
		return nil, err
	}
	return rf, nil
}

func (s *sim) open() (*loget.DB, error) {
	var last error
	on, probs := s.st.On, s.st.P
	defer func() { s.st.On, s.st.P = on, probs }()
	for i := 0; i < 200; i++ {
		if i == 100 {
			// Open re-reads every object, so on a large store under a
			// read-fault storm it fails with probability ~1-(1-p)^objects:
			// a liveness limit of the harness, not a safety question. Let
			// the storm pass; the open itself is still checked.
			s.st.On = false
			s.log("storm passed after 100 failed opens")
			s.res.CalmOpens++
		}
		db, err := loget.OpenStore(s.ctx, s.st,
			loget.WithClock(func() int64 { return s.clock.Add(1) }),
			loget.WithRand(s.rnd), loget.WithRepairTail(true), loget.WithSnapshotEvery(1+int(s.seed%7)), loget.WithReadAhead(1),
			loget.WithLogf(func(string, ...any) {}), loget.WithIndex(counter.Kind()),
			// even seeds keep every value as a log position (none inline),
			// and every third seed reads them without the value cache
			loget.WithInlineMax(int(s.seed%2)*64-1), loget.WithValueCacheBytes(map[bool]int{true: -1}[s.seed%3 == 0]),
			loget.WithIndexFiles(s.files))
		if err == nil {
			return db, nil
		}
		if errors.Is(err, loget.ErrCorrupt) {
			p := &s.st.P
			if !s.st.On || p.BitFlip+p.Misdirect+p.ShortRead+p.StaleList == 0 {
				return nil, err
			}
			// Oracle for read-path faults: ErrCorrupt or the right answer,
			// never a wrong one. The faults are transient, so with them off
			// the same store must open cleanly and pass every check.
			s.log("corrupt under read faults: %v", err)
			s.res.ReadFaultCorrupt++
			p.BitFlip, p.Misdirect, p.ShortRead, p.StaleList = 0, 0, 0, 0
		}
		last = err
	}
	return nil, fmt.Errorf("could not open after 200 attempts: %v", last)
}

// Run executes one seeded simulation.
func Run(seed int64, cfg Config) (Result, error) {
	s := &sim{ctx: context.Background(), rng: rand.New(rand.NewSource(seed)), seed: seed,
		txs: map[int][]mop{}, acked: map[int]uint64{}, amb: map[int]bool{},
		shred: map[string]bool{}, shredQ: map[string]bool{}, prefix: map[string]txPrefix{}}
	s.res.Seed = seed
	s.rnd = &lockedRand{r: rand.New(rand.NewSource(seed ^ 0x5eed))}
	s.mem = store.NewMem()
	s.st = store.NewFaulty(s.mem, seed*7919+1, cfg.Probs)
	if seed%4 == 1 {
		if err := s.indexFiles(); err != nil {
			return s.res, err
		}
		defer os.RemoveAll(s.indexDir)
	}
	var err error
	if s.db, err = s.open(); err != nil {
		return s.res, s.fail("initial open: %v", err)
	}
	for s.res.Steps = 0; s.res.Steps < cfg.Steps; s.res.Steps++ {
		var err error
		switch r := s.rng.Intn(100); {
		case r < 66:
			err = s.txn()
		case r < 70:
			err = s.compact()
		case r < 80:
			err = s.crash(true)
		case r < 85:
			err = s.takeover()
		case r < 90:
			err = s.doShred()
		case r < 95:
			err = s.rewind()
		default:
			err = s.check()
		}
		if err != nil {
			return s.res, err
		}
	}
	if err := s.crash(false); err != nil {
		return s.res, err
	}
	s.db.Close()
	s.res.IndexFileFaults = int(s.fileFaults.Load())
	if s.files != nil {
		if ents, err := os.ReadDir(s.indexDir); err != nil || len(ents) != 0 {
			return s.res, s.fail("index dir: %d files left, %v", len(ents), err)
		}
	}
	h := sha256.New()
	for _, t := range s.trace {
		h.Write([]byte(t))
		h.Write([]byte{0})
	}
	for _, t := range s.st.Trace {
		h.Write([]byte(t))
		h.Write([]byte{0})
	}
	copy(s.res.TraceHash[:], h.Sum(nil))
	s.res.Acked, s.res.Ambiguous = len(s.acked), len(s.amb)
	for _, t := range s.st.Trace {
		if !strings.HasPrefix(t, "put ") {
			s.res.Faults++
		}
	}
	return s.res, nil
}

var subjects = []string{"", "", "alice", "bob"}

func (s *sim) txn() error {
	id := s.nextID
	s.nextID++
	n := 1 + s.rng.Intn(3)
	ops := []mop{}
	for i := 0; i < n; i++ {
		m := mop{key: "k" + strconv.Itoa(s.rng.Intn(6)), val: fmt.Sprintf("v%d.%d", id, i), subject: subjects[s.rng.Intn(4)]}
		switch r := s.rng.Intn(14); {
		case r < 6:
			m.kind = 'p'
			if s.rng.Intn(3) == 0 {
				// expires soon after now (the clock ticks once per call)
				m.exp = max(1, s.clock.Load()+int64(s.rng.Intn(60))-5)
			}
		case r < 8:
			m.kind, m.val, m.subject = 'd', "", ""
		case r < 12:
			m.kind, m.key, m.val = 'c', "i"+strconv.Itoa(s.rng.Intn(10)), simCounterVals[s.rng.Intn(len(simCounterVals))]
		default:
			m.kind, m.key, m.val, m.subject = 'r', "i"+strconv.Itoa(s.rng.Intn(10)), "", ""
		}
		ops = append(ops, m)
	}
	s.txs[id] = ops
	blind := true // only puts and deletes: may go as a loget.Batch
	for _, m := range ops {
		blind = blind && (m.kind == 'p' || m.kind == 'd')
	}
	var lsn uint64
	var err error
	ingest := blind
	for _, m := range ops {
		ingest = ingest && m.kind == 'p' && m.subject == "" && m.exp == 0
	}
	if ingest && s.rng.Intn(3) == 0 { // puts only: a bulk ingest
		last := map[string]string{"__tx": strconv.Itoa(id)}
		for _, m := range ops {
			last[m.key] = m.val
		}
		keys := make([]string, 0, len(last))
		for k := range last {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var st loget.IngestStats
		st, err = s.db.Branch("main").Ingest(s.ctx, func(add func(string, []byte) error) error {
			for _, k := range keys {
				if err := add(k, []byte(last[k])); err != nil {
					return err
				}
			}
			return nil
		})
		lsn = st.LSN
		s.log("ingest")
	} else if blind && s.rng.Intn(2) == 0 {
		var b loget.Batch
		b.Put("__tx", []byte(strconv.Itoa(id)))
		for _, m := range ops {
			var o []loget.WriteOpt
			if m.subject != "" {
				o = append(o, loget.Subject(m.subject))
			}
			if m.exp != 0 {
				o = append(o, loget.ExpiresAt(time.Unix(0, m.exp)))
			}
			if m.kind == 'p' {
				b.Put(m.key, []byte(m.val), o...)
			} else {
				b.Delete(m.key)
			}
		}
		lsn, err = s.db.Branch("main").Apply(s.ctx, &b)
		s.log("batch")
	} else {
		lsn, err = s.update(ops)
	}
	s.log("tx%d%v=%v", id, ops, err)
	switch {
	case err == nil:
		s.acked[id] = lsn
		return nil
	case errors.Is(err, loget.ErrShredded):
		for _, m := range ops {
			if m.subject != "" && (s.shred[m.subject] || s.shredQ[m.subject]) {
				return nil
			}
		}
		return s.fail("tx%d: ErrShredded but no shredded subject", id)
	case errors.Is(err, loget.ErrConflict):
		return s.fail("tx%d: conflict with a single client", id)
	default:
		s.amb[id] = true
		if errors.Is(err, loget.ErrUnknownOutcome) || errors.Is(err, loget.ErrFenced) {
			return s.crash(true) // writer is poisoned; recover
		}
		return nil
	}
}

// update commits ops (and the __tx marker) in a transaction.
func (s *sim) update(ops []mop) (uint64, error) {
	id := s.nextID - 1
	return s.db.Branch("main").UpdateLSN(s.ctx, func(tx *loget.Tx) error {
		tx.Put("__tx", []byte(strconv.Itoa(id)))
		for _, m := range ops {
			var o []loget.WriteOpt
			if m.subject != "" {
				o = append(o, loget.Subject(m.subject))
			}
			if m.exp != 0 {
				o = append(o, loget.ExpiresAt(time.Unix(0, m.exp)))
			}
			var err error
			switch m.kind {
			case 'p':
				err = tx.Put(m.key, []byte(m.val), o...)
			case 'd':
				err = tx.Delete(m.key)
			case 'c':
				err = counter.Set(tx, simCounter, m.key, m.val, o...)
			case 'r':
				err = counter.Remove(tx, simCounter, m.key)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// crash kills the process (no clean close) and recovers.
func (s *sim) crash(faulty bool) error {
	s.res.Crashes++
	if s.rng.Intn(3) == 0 {
		s.db.Close() // clean: final snapshot (through the faulty store)
		s.res.Closes++
	} else {
		s.db.Crash()
	}
	if s.rng.Intn(2) == 0 || !faulty {
		s.st.Flush(s.ctx) // in-flight requests may still land after a crash
	}
	s.st.On = faulty
	db, err := s.open()
	s.st.On = true
	if err != nil {
		return s.fail("recovery: %v", err)
	}
	s.db = db
	s.log("recovered epoch=%d", db.Epoch())
	return s.check()
}

// takeover: a second writer opens the same bucket while the first is alive.
func (s *sim) takeover() error {
	s.res.Takeovers++
	old := s.db
	nw, err := s.open()
	if err != nil {
		return s.fail("takeover open: %v", err)
	}
	s.log("takeover epoch %d->%d", old.Epoch(), nw.Epoch())
	id := s.nextID
	s.nextID++
	s.txs[id] = nil
	err = old.Branch("main").Update(s.ctx, func(tx *loget.Tx) error {
		return tx.Put("__tx", []byte(strconv.Itoa(id)))
	})
	if err == nil {
		return s.fail("SPLIT BRAIN: zombie epoch %d committed after epoch %d opened", old.Epoch(), nw.Epoch())
	}
	old.Crash()
	s.db = nw
	return s.check()
}

func (s *sim) doShred() error {
	subj := subjects[2+s.rng.Intn(2)]
	_, err := s.db.Shred(s.ctx, subj)
	s.log("shred %s=%v", subj, err)
	if err == nil {
		s.res.Shreds++
		s.shred[subj] = true
		return s.check()
	}
	s.shredQ[subj] = true
	return s.crash(true)
}

func (s *sim) rewind() error {
	es, err := s.db.Entries("main")
	if err != nil || len(es) == 0 {
		return nil
	}
	at := uint64(s.rng.Intn(len(es) + 1))
	if at > 0 {
		at = es[at-1].LSN
	}
	err = s.db.Rewind(s.ctx, "main", at)
	s.log("rewind %d=%v", at, err)
	if errors.Is(err, loget.ErrCompacted) {
		return nil // below the base: refused, nothing changed
	}
	if err == nil {
		s.res.Rewinds++
		s.truncate(at)
		return s.check()
	}
	s.rewindQ = at + 1
	return s.crash(true)
}

func (s *sim) truncate(at uint64) {
	for id, l := range s.acked {
		if l > at {
			delete(s.acked, id)
		}
	}
}

// recovered returns the tx ids present on main, in log order.
func (s *sim) recovered() ([]int, error) {
	es, err := s.db.Entries("main")
	if err != nil {
		return nil, err
	}
	var ids []int
	var lsns []uint64
	s.headOK = len(es) > 0
	if s.headOK {
		s.headTS = es[len(es)-1].TS
	}
	// After a head compact, DropHistory may delete the segment that
	// held the head frame if no live value lives there. Entries then
	// ends before the pointer base; Now() is still the base's TS.
	if lm := s.db.Manifest().Logs[s.db.Manifest().Refs["main"]]; lm != nil && lm.Base != nil {
		if !s.headOK || es[len(es)-1].LSN < lm.Base.LSN {
			s.headTS, s.headOK = lm.Base.TS, true
		}
	}
	var floorLSN uint64
	if key := s.floorKey(); key != "" {
		p, ok := s.prefix[key]
		if !ok {
			return nil, fmt.Errorf("main starts from base %s that no compaction made", key)
		}
		ids, lsns = append(ids, p.ids...), append(lsns, p.lsns...)
		floorLSN = s.floorLSN()
	}
	for _, e := range es {
		// Design 30: pointer compact retains history segments, so
		// Entries still returns frames at or below the pointer base.
		// Those txns are already in the prefix recorded at compact.
		if floorLSN != 0 && e.LSN <= floorLSN {
			continue
		}
		for _, op := range e.Ops {
			if op.Kind == format.OpPut && op.Space == "kv" && op.Key == "__tx" {
				id, _ := strconv.Atoi(string(op.Value))
				ids = append(ids, id)
				lsns = append(lsns, e.LSN)
			}
			if op.Kind == format.OpIngest { // its __tx is in the run: read it at the LSN
				var v []byte
				if err := s.db.Branch("main").ViewAt(s.ctx, e.LSN, func(tx *loget.Tx) (err error) {
					v, err = tx.Get("__tx")
					return err
				}); err != nil {
					return nil, fmt.Errorf("ingest at %d: %v", e.LSN, err)
				}
				id, _ := strconv.Atoi(string(v))
				ids = append(ids, id)
				lsns = append(lsns, e.LSN)
			}
		}
	}
	// resolve an ambiguous rewind: all acked txns past the target vanish, or none
	if s.rewindQ > 0 {
		at := s.rewindQ - 1
		s.rewindQ = 0
		present := map[int]bool{}
		for _, id := range ids {
			present[id] = true
		}
		var past, gone int
		for id, l := range s.acked {
			if l > at {
				past++
				if !present[id] {
					gone++
				}
			}
		}
		if gone != 0 && gone != past {
			return nil, fmt.Errorf("ambiguous rewind to %d half-applied: %d of %d gone", at, gone, past)
		}
		if gone > 0 {
			s.truncate(at)
		}
	}
	s.lsns = lsns
	return ids, nil
}

// floorKey names main's compaction base ("" if none).
func (s *sim) floorKey() string {
	id, lsn := s.floorBase()
	if id == "" {
		return ""
	}
	return fmt.Sprintf("%s@%d", id, lsn)
}

func (s *sim) floorLSN() uint64 {
	_, lsn := s.floorBase()
	return lsn
}

func (s *sim) floorBase() (string, uint64) {
	m := s.db.Manifest()
	// A rewind or fork cuts the lineage at ParentLSN: an ancestor's
	// pointer base only applies if it sits at or below that cut.
	var limit uint64 = ^uint64(0)
	for lm := m.Logs[m.Refs["main"]]; lm != nil; lm = m.Logs[lm.Parent] {
		if lm.Base != nil && lm.Base.LSN <= limit {
			return lm.ID, lm.Base.LSN
		}
		if lm.Parent == "" {
			break
		}
		if lm.ParentLSN < limit {
			limit = lm.ParentLSN
		}
	}
	return "", 0
}

// compact records which txns are at main's head, then compacts. A
// failed compaction may still have committed: recover and check.
func (s *sim) compact() error {
	if err := s.check(); err != nil {
		return err
	}
	s.st.On = false // bookkeeping read, not under test
	head, err := s.db.Branch("main").Head()
	if err != nil {
		return s.fail("%v", err)
	}
	ids, err := s.recovered()
	s.st.On = true
	if err != nil {
		return s.fail("%v", err)
	}
	m := s.db.Manifest()
	s.prefix[fmt.Sprintf("%s@%d", m.Refs["main"], head)] = txPrefix{ids, s.lsns}
	op := "compact"
	if s.rng.Intn(2) == 0 {
		// GC at the head: same floor, plus reflog, retired-log and
		// old-manifest deletion under crash injection.
		op = "gc"
		_, err = s.db.GC(s.ctx, loget.GCOptions{KeepReflog: 1, KeepManifests: 1 + s.rng.Intn(3)})
	} else {
		_, err = s.db.Compact(s.ctx, loget.CompactOptions{})
	}
	s.log("%s @%d=%v", op, head, err)
	if err == nil {
		s.res.Compacts++
		if op == "gc" {
			s.res.GCs++
		}
		return s.check()
	}
	return s.crash(true)
}

func (s *sim) check() error {
	s.st.On = false
	defer func() { s.st.On = true }()
	ids, err := s.recovered()
	if err != nil {
		return s.fail("%v", err)
	}
	present := map[int]bool{}
	for i, id := range ids {
		if i > 0 && id <= ids[i-1] {
			return s.fail("order/double-apply violation: tx%d after tx%d", id, ids[i-1])
		}
		present[id] = true
		if _, ok := s.acked[id]; !ok && !s.amb[id] {
			return s.fail("phantom tx%d (never acked nor in flight)", id)
		}
		if s.txs[id] == nil && s.amb[id] {
			// zombie probe transactions must never be present
			return s.fail("zombie write tx%d is durable", id)
		}
	}
	for id := range s.acked {
		if !present[id] {
			return s.fail("LOST acknowledged tx%d (lsn %d)", id, s.acked[id])
		}
	}
	// shred status: resolve ambiguous shreds from durable state
	for subj := range s.shredQ {
		if s.db.IsShredded(subj) {
			s.shred[subj] = true
		}
	}
	// expected state = fold of model ops over recovered ids
	kv := map[string]string{}
	exp := map[string]int64{}    // kv key -> expiry of its live put
	items := map[string]string{} // counter item -> value
	for _, id := range ids {
		kv["__tx"] = strconv.Itoa(id)
		for _, m := range s.txs[id] {
			gone := m.subject != "" && s.shred[m.subject]
			switch m.kind {
			case 'p':
				if gone {
					delete(kv, m.key)
				} else {
					kv[m.key], exp[m.key] = m.val, m.exp
				}
			case 'd':
				delete(kv, m.key)
			case 'c':
				if gone {
					delete(items, m.key)
				} else {
					items[m.key] = m.val
				}
			case 'r':
				delete(items, m.key)
			}
		}
	}
	counts := map[string]string{}
	for _, v := range items {
		n, _ := strconv.Atoi(counts[v])
		counts[v] = strconv.Itoa(n + 1)
	}
	got := map[string]string{}
	gotItems := map[string]string{}
	gotCounts := map[string]string{}
	var now int64
	err = s.db.View(s.ctx, func(tx *loget.Tx) error {
		var err error
		if now, err = tx.Now(); err != nil {
			return err
		}
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		for _, p := range kvs {
			got[p.Key] = string(p.Value)
		}
		// range reads agree with the scan: descending, in pages (own
		// RNG, so the simulation's schedule for a seed is unchanged)
		pr := rand.New(rand.NewSource(s.seed ^ 0x17e7))
		o := loget.IterOptions{Reverse: true, Limit: 1 + pr.Intn(7)}
		var back []loget.KV
		for {
			page, err := tx.Range(o)
			if err != nil {
				return err
			}
			back = append(back, page...)
			if len(page) < o.Limit {
				break
			}
			o = o.After(page[len(page)-1].Key)
		}
		if len(back) != len(kvs) {
			return fmt.Errorf("reverse pages: %d keys, scan %d", len(back), len(kvs))
		}
		for i, p := range back {
			if q := kvs[len(kvs)-1-i]; p.Key != q.Key || string(p.Value) != string(q.Value) {
				return fmt.Errorf("reverse pages: key %d %q, scan %q", i, p.Key, q.Key)
			}
		}
		rs, err := tx.ScanRecords(counter.Prefix+simCounter, "")
		if err != nil {
			return err
		}
		for _, r := range rs {
			gotItems[r.Key] = string(r.Value)
		}
		c, err := counter.Counts(tx, simCounter)
		for v, n := range c {
			gotCounts[v] = strconv.Itoa(n)
		}
		return err
	})
	if err != nil {
		return s.fail("view: %v", err)
	}
	// expiry is decided at the read time: the head frame's commit TS
	if s.headOK && s.headTS != now {
		return s.fail("read time %d != head frame TS %d", now, s.headTS)
	}
	for k, e := range exp {
		if _, ok := kv[k]; !ok || e == 0 {
			continue
		}
		if e <= now {
			delete(kv, k)
			s.res.Expired++
		} else {
			s.res.Expiring++
		}
	}
	if !mapEq(kv, got) {
		return s.fail("KV index != model fold\n  want %v\n  got  %v", sorted(kv), sorted(got))
	}
	if !mapEq(items, gotItems) {
		return s.fail("counter records != model fold\n  want %v\n  got  %v", sorted(items), sorted(gotItems))
	}
	if !mapEq(counts, gotCounts) {
		return s.fail("counter index != model fold\n  want %v\n  got  %v", sorted(counts), sorted(gotCounts))
	}
	d1, _, err := s.db.IndexDigests("main")
	if err != nil {
		return s.fail("%v", err)
	}
	d2, err := s.db.RebuildDigests("main")
	if err != nil {
		return s.fail("%v", err)
	}
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		return s.fail("index digests != fold(log) digests")
	}
	r, err := s.db.Verify(s.ctx)
	if err != nil || !r.OK {
		return s.fail("verify: %+v %v", r, err)
	}
	// a durable shred leaves no key material behind (after the recovery pass)
	s.log("check ok ids=%d", len(ids))
	return nil
}

func mapEq(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func sorted(m map[string]string) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
