package index

import (
	"bytes"
	"runtime"
	"sort"
)

// Range reads (design 22). A range read merges the hot set and every run
// (newest first), each positioned with a seek, so it touches only the keys
// it returns plus one block per run: it never materialises the space or
// holds more than one decoded block per run, which is what lets a scan of
// a file-backed index (design 21) run in bounded memory.

// Range calls fn in key order (descending if reverse) for each live key k
// of space with lo <= k < hi (hi "" = no upper bound) and its version
// visible at LSN at, until fn returns false. The caller holds the read
// lock for the whole call, so fn must be short and must not call back
// into the index; v.Val is safe to keep after the lock is released (as
// with Get). Callers that hand keys to user code page through the range:
// resume after the last key with lo = key+"\x00" (or hi = key in reverse).
func (l *KV) Range(space, lo, hi string, at uint64, reverse bool, fn func(key string, v Version) bool) {
	klo := ck(space, lo)
	khi := space + "\x01" // every key of space sorts below this
	if hi != "" {
		khi = ck(space, hi)
	}
	if klo >= khi {
		return
	}
	srcs := make([]source, 0, 1+len(l.runs))
	if hs := l.hotSource(klo, khi, reverse); hs.valid() {
		srcs = append(srcs, hs)
	}
	for _, r := range l.runs {
		var s source
		if reverse {
			s = newRCursor(r, klo, khi)
		} else {
			s = newFCursor(r, klo, khi)
		}
		if s.valid() {
			srcs = append(srcs, s)
		}
	}
	defer runtime.KeepAlive(l.runs)
	strip := len(space) + 1
	for len(srcs) > 0 {
		// the next key: smallest (largest in reverse) over the sources
		var k []byte
		for i, s := range srcs {
			c := bytes.Compare(s.key(), k)
			if i == 0 || (!reverse && c < 0) || (reverse && c > 0) {
				k = s.key()
			}
		}
		key := string(k)
		var v Version
		found := false
		// sources are newest first: the first with a version at `at` wins
		live := srcs[:0]
		for _, s := range srcs {
			if string(s.key()) == key {
				if !found {
					v, found = s.at(at)
				}
				s.step()
			}
			if s.valid() {
				live = append(live, s)
			}
		}
		srcs = live
		if found && !v.Del && !fn(key[strip:], v) {
			return
		}
	}
}

// source is one sorted input of a range read, already bounded.
type source interface {
	valid() bool
	key() []byte                  // valid until step
	at(at uint64) (Version, bool) // the version visible at `at`, owned
	step()
}

// hotSrc walks the hot set's keys in range: the sorted keys slice and the
// (few) unsorted recent keys, sorted here, merged on the fly.
type hotSrc struct {
	l       *KV
	a, b    []string // in range, ascending
	i, j    int      // forward: next unread; reverse: count left
	rev     bool
	cur     string
	buf     []byte // cur as bytes
	isValid bool
}

func (l *KV) hotSource(lo, hi string, rev bool) *hotSrc {
	i := sort.SearchStrings(l.keys, lo)
	j := sort.SearchStrings(l.keys, hi)
	h := &hotSrc{l: l, a: l.keys[i:j], rev: rev}
	for _, k := range l.recent {
		if k >= lo && k < hi {
			h.b = append(h.b, k)
		}
	}
	sort.Strings(h.b)
	if rev {
		h.i, h.j = len(h.a), len(h.b)
	}
	h.step()
	return h
}

func (h *hotSrc) valid() bool { return h.isValid }

// key copies into a reused buffer: the index package stays free of unsafe
// (purity_test), and the copy is no allocation once the buffer has grown.
func (h *hotSrc) key() []byte { return h.buf }
func (h *hotSrc) at(at uint64) (Version, bool) {
	vs := h.l.m[h.cur]
	if i := vs.find(at); i >= 0 {
		return vs.at(i).unpack(), true
	}
	return Version{}, false
}

func (h *hotSrc) step() {
	if !h.rev {
		switch {
		case h.i < len(h.a) && (h.j >= len(h.b) || h.a[h.i] < h.b[h.j]):
			h.cur, h.i = h.a[h.i], h.i+1
		case h.j < len(h.b):
			h.cur, h.j = h.b[h.j], h.j+1
		default:
			h.isValid = false
			return
		}
	} else {
		switch {
		case h.i > 0 && (h.j == 0 || h.a[h.i-1] > h.b[h.j-1]):
			h.i--
			h.cur = h.a[h.i]
		case h.j > 0:
			h.j--
			h.cur = h.b[h.j]
		default:
			h.isValid = false
			return
		}
	}
	h.buf = append(h.buf[:0], h.cur...)
	h.isValid = true
}

// fCursor is a run cursor bounded to [lo, hi).
type fCursor struct {
	c  *cursor
	hi []byte
	ok bool
}

func newFCursor(r *run, lo, hi string) *fCursor {
	f := &fCursor{c: r.cursor(), hi: []byte(hi)}
	f.c.seek([]byte(lo))
	f.step()
	return f
}

func (f *fCursor) valid() bool                  { return f.ok }
func (f *fCursor) key() []byte                  { return f.c.key }
func (f *fCursor) step()                        { f.ok = f.c.next() && bytes.Compare(f.c.key, f.hi) < 0 }
func (f *fCursor) at(at uint64) (Version, bool) { return runAt(f.c.r, f.c.body, at) }

func runAt(r *run, body []byte, at uint64) (Version, bool) {
	v, ok := bodyAt(body, at)
	if ok {
		v.Val = r.own(v.Val)
	}
	return v, ok
}

// rCursor walks a run backwards from below hi down to lo. Runs are
// front-coded forwards, so it decodes one block at a time (at most
// runBlock entries) and steps through it from the end.
type rCursor struct {
	r    *run
	lo   []byte
	bi   int // block decoded into ents
	ents []rent
	keys []byte // backing store of the decoded keys
	i    int    // current entry; -1 = exhausted
}

type rent struct {
	k0, k1 int // key = keys[k0:k1]
	body   []byte
}

func newRCursor(r *run, lo, hi string) *rCursor {
	c := &rCursor{r: r, lo: []byte(lo), i: -1}
	hb := []byte(hi)
	// the last block whose first key is below hi
	bi := sort.Search(r.blocks(), func(i int) bool { return bytes.Compare(r.firstKey(i), hb) >= 0 }) - 1
	if bi < 0 {
		return c
	}
	c.load(bi)
	for c.i >= 0 && bytes.Compare(c.key(), hb) >= 0 {
		c.i--
	}
	c.check()
	return c
}

func (c *rCursor) load(bi int) {
	c.bi, c.ents, c.keys = bi, c.ents[:0], c.keys[:0]
	fc := &cursor{r: c.r, p: c.r.bofs[bi], end: c.r.bofs[bi+1], bi: bi}
	for fc.next() {
		k0 := len(c.keys)
		c.keys = append(c.keys, fc.key...)
		c.ents = append(c.ents, rent{k0, len(c.keys), fc.body})
	}
	c.i = len(c.ents) - 1
}

func (c *rCursor) check() {
	if c.i >= 0 && bytes.Compare(c.key(), c.lo) < 0 {
		c.i = -1
	}
}

func (c *rCursor) valid() bool { return c.i >= 0 }
func (c *rCursor) key() []byte { e := c.ents[c.i]; return c.keys[e.k0:e.k1] }
func (c *rCursor) at(at uint64) (Version, bool) {
	return runAt(c.r, c.ents[c.i].body, at)
}

func (c *rCursor) step() {
	if c.i--; c.i < 0 && c.bi > 0 {
		c.load(c.bi - 1)
	}
	c.check()
}

// PrefixEnd returns the smallest string above every string with prefix p,
// or "" if there is none (p is empty or all 0xff bytes).
func PrefixEnd(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 0xff {
			return p[:i] + string([]byte{p[i] + 1})
		}
	}
	return ""
}
