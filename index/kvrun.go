package index

import (
	"bytes"
	"encoding/binary"
	"sort"
)

// A run is an immutable, sorted, compressed slice of the KV index: the
// interim key index of docs/research (item 4). Keys are front-coded in
// blocks of runBlock entries; a hash table maps a key to its block, so a
// lookup costs one probe and one block scan, and a key absent from the run
// usually costs one probe. Runs are built only by this package, from
// versions that already passed loadKV's checks or came from Apply, so the
// decoders below trust their input.
//
// Entry:   uvarint shared, uvarint suffix len, suffix, uvarint body len, body
//
//	(shared: bytes in common with the block's first key, so any
//	entry decodes on its own given that key)
//
// Body:    uvarint version count, versions oldest first
// Version: uvarint LSN, uvarint Op, flags,
//
//	[uvarint Off, uvarint Len, Sum u32le] if fPos,
//	[uvarint Chunk] if fChunk,
//	[uvarint len, Val] if fVal, [uvarint len, Subject] if fSubj,
//	[Exp i64le] if fExp (design 27),
//	[uvarint len, Log] if fLog (design 30: origin log id for AAD)
//
// Versions carry absolute LSNs, so the bodies of one key in two runs merge
// by concatenation.
type run struct {
	b      []byte
	bofs   []int    // block i is b[bofs[i]:bofs[i+1]]
	slots  []uint64 // open addressing, 0 = empty: hash>>fpShift | block offset+1 | offset in block
	mp     RunFile  // non-nil: b and slots live in a mapped run file (kvfile.go)
	n      int      // keys
	vers   int      // versions
	chunk  bool     // some version has Chunk != 0
	exp    bool     // some version has Exp != 0
	hasLog bool     // some version has Log != ""
	subj   map[string]int
	// key is the key a checkpoint stored the run under (kvimage.go).
	// Written and read only by checkpoints, which are serialised.
	key string
}

const (
	fpShift  = 56        // a slot keeps the top 8 bits of the key's hash,
	maxInOff = 1<<16 - 1 // 40 bits of block start + 1 and 16 bits of offset in the block
)

// maxRunBytes bounds a run so its block starts fit a slot (a var for tests).
var maxRunBytes = 1 << 39

const (
	runBlock = 16
	fDel     = 1 << iota
	fPos
	fChunk
	fVal
	fSubj
	fExp
	fLog
)

// subjects counts, per subject, the versions DropSubject would still change.
func liveSubject(v *Version) bool {
	return v.Subject != "" && !(v.Del && v.Val == nil && v.Off == 0 && v.Len == 0 && v.Sum == 0 && v.Chunk == 0)
}

func khash[T string | []byte](k T) uint64 {
	h := uint64(14695981039346656037) // FNV-1a, then a murmur3 finaliser
	for i := 0; i < len(k); i++ {
		h ^= uint64(k[i])
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	return h ^ h>>33
}

func appendVer(b []byte, v *Version) []byte {
	var f byte
	if v.Del {
		f |= fDel
	}
	if v.Off != 0 || v.Len != 0 || v.Sum != 0 {
		f |= fPos
	}
	if v.Chunk != 0 {
		f |= fChunk
	}
	if v.Val != nil {
		f |= fVal
	}
	if v.Subject != "" {
		f |= fSubj
	}
	if v.Exp != 0 {
		f |= fExp
	}
	if v.Log != "" {
		f |= fLog
	}
	b = binary.AppendUvarint(b, v.LSN)
	b = binary.AppendUvarint(b, uint64(v.Op))
	b = append(b, f)
	if f&fPos != 0 {
		b = binary.AppendUvarint(b, uint64(v.Off))
		b = binary.AppendUvarint(b, uint64(v.Len))
		b = binary.LittleEndian.AppendUint32(b, v.Sum)
	}
	if f&fChunk != 0 {
		b = binary.AppendUvarint(b, uint64(v.Chunk))
	}
	if f&fVal != 0 {
		b = binary.AppendUvarint(b, uint64(len(v.Val)))
		b = append(b, v.Val...)
	}
	if f&fSubj != 0 {
		b = binary.AppendUvarint(b, uint64(len(v.Subject)))
		b = append(b, v.Subject...)
	}
	if f&fExp != 0 {
		b = binary.LittleEndian.AppendUint64(b, uint64(v.Exp))
	}
	if f&fLog != 0 {
		b = binary.AppendUvarint(b, uint64(len(v.Log)))
		b = append(b, v.Log...)
	}
	return b
}

func uv(b []byte) (uint64, []byte) {
	v, n := binary.Uvarint(b)
	return v, b[n:]
}

// decVer decodes one version. Val aliases the run (capacity capped, so an
// append by a caller cannot write into it); the run is never modified.
func decVer(b []byte) (Version, []byte) {
	var v Version
	var x uint64
	v.LSN, b = uv(b)
	x, b = uv(b)
	v.Op = uint32(x)
	f := b[0]
	b = b[1:]
	v.Del = f&fDel != 0
	if f&fPos != 0 {
		x, b = uv(b)
		v.Off = uint32(x)
		x, b = uv(b)
		v.Len = uint32(x)
		v.Sum, b = binary.LittleEndian.Uint32(b), b[4:]
	}
	if f&fChunk != 0 {
		x, b = uv(b)
		v.Chunk = uint32(x)
	}
	if f&fVal != 0 {
		x, b = uv(b)
		v.Val, b = b[:x:x], b[x:]
	}
	if f&fSubj != 0 {
		x, b = uv(b)
		v.Subject, b = string(b[:x]), b[x:]
	}
	if f&fExp != 0 {
		v.Exp, b = int64(binary.LittleEndian.Uint64(b)), b[8:]
	}
	if f&fLog != 0 {
		x, b = uv(b)
		v.Log, b = string(b[:x]), b[x:]
	}
	return v, b
}

// skipVer returns the LSN of the first version in b and the rest of b,
// without allocating.
func skipVer(b []byte) (uint64, []byte) {
	lsn, b := uv(b)
	_, b = uv(b)
	f := b[0]
	b = b[1:]
	if f&fPos != 0 {
		_, b = uv(b)
		_, b = uv(b)
		b = b[4:]
	}
	if f&fChunk != 0 {
		_, b = uv(b)
	}
	if f&fVal != 0 {
		x, r := uv(b)
		b = r[x:]
	}
	if f&fSubj != 0 {
		x, r := uv(b)
		b = r[x:]
	}
	if f&fExp != 0 {
		b = b[8:]
	}
	if f&fLog != 0 {
		x, r := uv(b)
		b = r[x:]
	}
	return lsn, b
}

// bodyAt returns the version of a body visible at `at` and whether one is.
func bodyAt(body []byte, at uint64) (Version, bool) {
	n, b := uv(body)
	var best []byte
	for i := uint64(0); i < n; i++ {
		lsn, rest := skipVer(b)
		if lsn > at {
			break
		}
		best, b = b, rest
	}
	if best == nil {
		return Version{}, false
	}
	v, _ := decVer(best)
	return v, true
}

// bodyLast returns the newest version of a body.
func bodyLast(body []byte) Version { v, _ := bodyAt(body, ^uint64(0)); return v }

// bodyVers decodes every version of a body, oldest first, appending to vs.
func bodyVers(vs []Version, body []byte) []Version {
	n, b := uv(body)
	for i := uint64(0); i < n; i++ {
		var v Version
		v, b = decVer(b)
		vs = append(vs, v)
	}
	return vs
}

// builder writes a run from keys in ascending order, to the heap or, if f
// is set, to a run file (kvfile.go).
type builder struct {
	r       *run
	buf     []byte  // heap: the run's bytes; file: bytes not yet written
	f       RunFile // nil: heap
	off     int     // file: bytes written before buf
	err     error   // file: first write error
	prev    []byte  // first key of the current block
	scratch []byte  // the versions of the current key
	nv      int
	par     func([]func()) // KVOptions.Parallel
}

func newBuilder() *builder { return &builder{r: &run{subj: map[string]int{}}} }

// grow reserves room for about b bytes (heap builders).
func (w *builder) grow(b int) {
	if w.f == nil {
		w.buf = make([]byte, 0, b)
	}
}

// size returns the bytes written so far.
func (w *builder) size() int { return w.off + len(w.buf) }

// ver adds a version to the current key.
func (w *builder) ver(v *Version) {
	w.scratch = appendVer(w.scratch, v)
	w.nv++
	w.r.chunk = w.r.chunk || v.Chunk != 0
	w.r.exp = w.r.exp || v.Exp != 0
	w.r.hasLog = w.r.hasLog || v.Log != ""
	if liveSubject(v) {
		w.r.subj[v.Subject]++
	}
}

// end writes the current key with the versions given to ver.
func (w *builder) end(key []byte) {
	w.entry(key, w.nv, w.scratch)
	w.scratch, w.nv = w.scratch[:0], 0
}

// entry writes a key whose nv versions are the concatenation of parts.
// Callers of entry directly (merges) account for chunk and subjects.
func (w *builder) entry(key []byte, nv int, parts ...[]byte) {
	r := w.r
	shared := 0
	if r.n%runBlock == 0 {
		r.bofs = append(r.bofs, w.size())
	} else {
		for shared < len(key) && shared < len(w.prev) && key[shared] == w.prev[shared] {
			shared++
		}
	}
	bl := uvLen(uint64(nv))
	for _, p := range parts {
		bl += len(p)
	}
	w.buf = binary.AppendUvarint(w.buf, uint64(shared))
	w.buf = binary.AppendUvarint(w.buf, uint64(len(key)-shared))
	w.buf = append(w.buf, key[shared:]...)
	w.buf = binary.AppendUvarint(w.buf, uint64(bl))
	w.buf = binary.AppendUvarint(w.buf, uint64(nv))
	for _, p := range parts {
		w.buf = append(w.buf, p...)
	}
	if r.n%runBlock == 0 {
		w.prev = append(w.prev[:0], key...) // the block's first key
	}
	r.n++
	r.vers += nv
	if w.f != nil && len(w.buf) >= fileBuf {
		w.flush()
	}
}

func uvLen(x uint64) int {
	n := 1
	for ; x >= 0x80; x >>= 7 {
		n++
	}
	return n
}

// nslots sizes a run's hash table: load factor ~0.77.
func nslots(n int) int { return n + n*3/10 + 1 }

// finish returns the run, or nil if it has no keys. Only a file builder
// can fail; the builder is spent either way.
func (w *builder) finish() (*run, error) {
	r := w.r
	w.r = nil
	if r.n == 0 {
		w.abort()
		return nil, nil
	}
	r.bofs = append(r.bofs, w.size())
	if cap(r.bofs) > len(r.bofs)+len(r.bofs)/8 { // trim slices that grew by doubling
		r.bofs = append([]int(nil), r.bofs...)
	}
	if w.f != nil {
		return w.finishFile(r)
	}
	r.b = w.buf
	if cap(r.b) > len(r.b)+len(r.b)/8 {
		r.b = bytes.Clone(r.b)
	}
	w.buf = nil
	r.slots = make([]uint64, nslots(r.n))
	r.fillSlots(nil, w.par)
	return r, nil
}

// fillSlots builds the hash table from the entries, in key order (so the
// table is the same wherever the run lives and whatever the machine).
// f, if set, is the run's file: entry pages read are dropped behind.
//
// Large runs (design 25) can insert in parallel (KVOptions.Parallel): the table is split into
// fillParts regions; each batch of entries is bucketed by its home
// region and every region inserted by its own goroutine, probing only
// inside the region. An entry that would probe past its region's end is
// set aside and inserted, in key order, after all others. The result is a
// valid linear-probing table (every slot between an entry's home and its
// place is full) and depends only on the entries.
func (r *run) fillSlots(f RunFile, par func([]func())) {
	if r.n < parFillMin {
		r.fillSlotsSeq(f)
		return
	}
	parts := fillParts
	L := len(r.slots)
	bound := func(w int) int { return int(uint64(w) * uint64(L) / uint64(parts)) }
	region := func(j int) int { // inverse of bound
		w := int(uint64(j) * uint64(parts) / uint64(L))
		for w+1 < parts && bound(w+1) <= j {
			w++
		}
		for bound(w) > j {
			w--
		}
		return w
	}
	type ent struct {
		v uint64 // slot value
		j int    // home slot
	}
	buckets := make([][]ent, parts)
	var over []ent
	overs := make([][]ent, parts)
	flush := func() {
		var tasks []func()
		for w := range parts {
			if len(buckets[w]) == 0 {
				continue
			}
			tasks = append(tasks, func() {
				end := bound(w + 1)
				for _, e := range buckets[w] {
					j := e.j
					for j < end && r.slots[j] != 0 {
						j++
					}
					if j == end {
						overs[w] = append(overs[w], e)
						continue
					}
					r.slots[j] = e.v
				}
			})
		}
		runTasks(par, tasks)
		for w := range parts {
			buckets[w] = buckets[w][:0]
		}
	}
	var key []byte
	freed, pending := 0, 0
	for bi := 0; bi < r.blocks(); bi++ {
		freed = dropBehind(f, freed, r.bofs[bi])
		start := r.bofs[bi]
		first := r.keyAt(start)
		for p := start; p < r.bofs[bi+1]; {
			b := r.b[p:]
			n := len(b)
			sh, b := uv(b)
			sl, b := uv(b)
			key = append(append(key[:0], first[:sh]...), b[:sl]...)
			bl, b := uv(b[sl:])
			at := min(p-start, maxInOff)
			h := khash(key)
			j := r.slot(h)
			w := region(j)
			buckets[w] = append(buckets[w], ent{h>>fpShift<<fpShift | uint64(start+1)<<16 | uint64(at), j})
			p += n - len(b) + int(bl)
			if pending++; pending == fillBatch {
				flush()
				pending = 0
			}
		}
	}
	flush()
	// Set-aside entries, region by region (each region's in key order):
	// deterministic, and correctness does not depend on the order.
	for w := range parts {
		over = append(over, overs[w]...)
	}
	for _, e := range over {
		j := e.j
		for r.slots[j] != 0 {
			if j++; j == L {
				j = 0
			}
		}
		r.slots[j] = e.v
	}
}

// fillParts is how many regions a large run's table is filled in at once
// (fixed, not the CPU count, so the table does not depend on the machine);
// parFillMin and fillBatch: the smallest run filled in parallel and the
// entries bucketed per round (bounds the extra memory to 16 B each).
const fillParts = 8

// vars so tests can take the parallel path with small runs
var (
	parFillMin = 1 << 17
	fillBatch  = 1 << 20
)

// runTasks runs tasks with par, or one after another if par is nil. The
// tasks touch disjoint memory, so the result does not depend on which.
func runTasks(par func([]func()), tasks []func()) {
	if par != nil {
		par(tasks)
		return
	}
	for _, t := range tasks {
		t()
	}
}

// fillSlotsSeq is fillSlots one entry at a time.
func (r *run) fillSlotsSeq(f RunFile) {
	var key []byte
	freed := 0
	for bi := 0; bi < r.blocks(); bi++ {
		freed = dropBehind(f, freed, r.bofs[bi])
		start := r.bofs[bi]
		first := r.keyAt(start)
		for p := start; p < r.bofs[bi+1]; {
			b := r.b[p:]
			n := len(b)
			sh, b := uv(b)
			sl, b := uv(b)
			key = append(append(key[:0], first[:sh]...), b[:sl]...)
			bl, b := uv(b[sl:])
			at := min(p-start, maxInOff)
			h := khash(key)
			j := r.slot(h)
			for r.slots[j] != 0 {
				if j++; j == len(r.slots) {
					j = 0
				}
			}
			r.slots[j] = h>>fpShift<<fpShift | uint64(start+1)<<16 | uint64(at)
			p += n - len(b) + int(bl)
		}
	}
}

// own returns b, copied if it points into a mapped file: values handed
// out of the index must not alias a run that a later write may unmap.
func (r *run) own(b []byte) []byte {
	if r.mp != nil {
		return bytes.Clone(b)
	}
	return b
}

// release unmaps a file run; the run must no longer be reachable by
// readers (its owner holds the write lock).
func (r *run) release() {
	if r.mp != nil {
		r.mp.Release()
	}
	r.b, r.slots = nil, nil
}

func (r *run) slot(h uint64) int { return int(uint64(uint32(h)) * uint64(len(r.slots)) >> 32) }

func (r *run) blocks() int { return len(r.bofs) - 1 }

// keyAt returns the key of the block-first entry at offset p (shared 0).
func (r *run) keyAt(p int) []byte {
	_, b := uv(r.b[p:])
	sl, b := uv(b)
	return b[:sl]
}

func (r *run) firstKey(i int) []byte { return r.keyAt(r.bofs[i]) }

// cursor iterates a run in key order. key is reused between steps.
type cursor struct {
	r    *run
	p    int
	end  int
	key  []byte
	body []byte
	held bool // seek stopped on an entry: next returns it again
	bi   int  // block of the entry at p
}

func (r *run) cursor() *cursor { return &cursor{r: r, end: len(r.b)} }

func (c *cursor) next() bool {
	if c.held {
		c.held = false
		return true
	}
	if c.p >= c.end {
		return false
	}
	for c.p >= c.r.bofs[c.bi+1] {
		c.bi++
	}
	b := c.r.b[c.p:]
	n := len(b)
	sh, b := uv(b)
	sl, b := uv(b)
	c.key = append(append(c.key[:0], c.r.firstKey(c.bi)[:sh]...), b[:sl]...)
	b = b[sl:]
	bl, b := uv(b)
	c.body = b[:bl]
	c.p += n - len(b) + int(bl)
	return true
}

// seek positions the cursor so that next returns the first key >= k.
func (c *cursor) seek(k []byte) {
	r := c.r
	i := sort.Search(r.blocks(), func(i int) bool { return bytes.Compare(r.firstKey(i), k) > 0 }) - 1
	if i < 0 {
		i = 0
	}
	c.p, c.held, c.bi = r.bofs[i], false, i
	for c.next() {
		if bytes.Compare(c.key, k) >= 0 {
			c.held = true
			return
		}
	}
}

// lookup returns the body of key k (hash h), if the run has it.
func (r *run) lookup(k string, h uint64) ([]byte, bool) {
	j := r.slot(h)
	return r.lookupAt(k, h, j, r.slots[j])
}

// lookupAt is lookup given the first slot to probe, j, and its value s
// (Get loads the first slot of every run before probing any, so the
// cache misses overlap).
func (r *run) lookupAt(k string, h uint64, j int, s uint64) ([]byte, bool) {
	fp := h >> fpShift
	for first := true; ; first = false {
		if !first {
			s = r.slots[j]
		}
		if s == 0 {
			return nil, false
		}
		if s>>fpShift == fp {
			start, at := int(s>>16&(1<<40-1))-1, int(s&maxInOff)
			if at != maxInOff {
				b := r.b[start+at:]
				sh, b := uv(b)
				sl, b := uv(b)
				if first := r.keyAt(start); int(sh+sl) == len(k) && string(first[:sh]) == k[:sh] && string(b[:sl]) == k[sh:] {
					bl, b := uv(b[sl:])
					return b[:bl], true
				}
			} else {
				c := r.cursor()
				c.seek([]byte(k))
				if c.next() && string(c.key) == k {
					return c.body, true
				}
			}
		}
		if j++; j == len(r.slots) {
			j = 0
		}
	}
}

// rebuildInto writes the run to w with fn applied to every key's versions
// (oldest first); fn returns nil to drop the key.
func (r *run) rebuildInto(w *builder, fn func(key []byte, vs []Version) []Version) {
	c := r.cursor()
	var vs []Version
	for c.next() {
		vs = fn(c.key, bodyVers(vs[:0], c.body))
		for i := range vs {
			w.ver(&vs[i])
		}
		if len(vs) > 0 {
			w.end(c.key)
		}
	}
}

// merger merges adjacent runs (newest first) into one, a bounded number of
// keys per step, so no single write pays for a whole merge.
type merger struct {
	in    []*run
	cur   []*cursor // nil once exhausted
	freed []int     // per input: bytes already dropped (dropBehind)
	w     *builder  // nil: only the cursors are used (KV.each)
}

func newMerger(in []*run, w *builder) *merger {
	m := &merger{in: in, w: w, freed: make([]int, len(in))}
	for _, r := range in {
		c := r.cursor()
		c.next() // runs are never empty
		m.cur = append(m.cur, c)
	}
	return m
}

// step merges up to n keys and reports whether the merge is complete.
func (m *merger) step(n int) bool {
	var parts [][]byte
	var key []byte
	for ; n > 0; n-- {
		key = key[:0]
		have := false
		for _, c := range m.cur {
			if c != nil && (!have || bytes.Compare(c.key, key) < 0) {
				key, have = append(key[:0], c.key...), true
			}
		}
		if !have {
			return true
		}
		parts = parts[:0]
		nv := 0
		for i := len(m.cur) - 1; i >= 0; i-- { // oldest run first
			c := m.cur[i]
			if c != nil && bytes.Equal(c.key, key) {
				k, rest := uv(c.body)
				parts, nv = append(parts, rest), nv+int(k)
				if !c.next() {
					m.cur[i] = nil
				}
			}
		}
		m.w.entry(key, nv, parts...)
	}
	done := true
	for i, c := range m.cur {
		p := len(m.in[i].b)
		if c != nil {
			p, done = c.p, false
		}
		m.freed[i] = dropBehind(m.in[i].mp, m.freed[i], p)
	}
	return done
}

// dropStride is how much consumed input a merge accumulates before it
// drops those pages from the process (design 25).
const dropStride = 4 << 20

// pageDropper is implemented by run files that can drop resident pages
// (internal/runfile).
type pageDropper interface{ Drop(off, n int) }

// dropBehind drops bytes [freed, p) of a file run once they reach
// dropStride and returns the new freed offset.
func dropBehind(f RunFile, freed, p int) int {
	d, ok := f.(pageDropper)
	if !ok || p-freed < dropStride {
		return freed
	}
	d.Drop(freed, p-freed)
	return p
}

// result returns the merged run (nil if empty).
func (m *merger) result() (*run, error) {
	for _, r := range m.in {
		m.w.r.chunk = m.w.r.chunk || r.chunk
		m.w.r.exp = m.w.r.exp || r.exp
		m.w.r.hasLog = m.w.r.hasLog || r.hasLog
		for s, k := range r.subj {
			m.w.r.subj[s] += k
		}
	}
	return m.w.finish()
}
