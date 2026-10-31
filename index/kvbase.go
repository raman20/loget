package index

import (
	"encoding/binary"
	"errors"
	"math"
)

// BaseEncoder streams the body of a compaction base: exactly the bytes
// Snapshot() returns for a fresh KV after SetLatest(space, key, v) was
// called once per key in ascending key order (design 23). A base body can
// be gigabytes; with the encoder the caller writes it out in pieces
// instead of building the KV, its snapshot and copies of both on the heap.
//
// Entries are buffered in KV3 form (with a chunk number per version)
// because whether the body is KV2 or KV3 is known only at the end: KV3 iff
// some version carries a base chunk. If none does, the caller converts the
// entries with StripChunks before writing Header.
type BaseEncoder struct {
	inline  int
	n       uint64
	chunked bool
	prev    string
	e       enc
}

// BaseHeaderLen is the size of the body header Header returns.
const BaseHeaderLen = 8 + 3 + 8 + 8

// NewBaseEncoder returns an encoder for a base of a KV with inline limit
// inline (as Inline reports it).
func NewBaseEncoder(inline int) *BaseEncoder { return &BaseEncoder{inline: inline} }

var errBaseOrder = errors.New("index: base keys must be added in ascending order")

// Add appends the only version of (space, key). Keys must be strictly
// ascending by (space, key) and v must not be a delete (a base holds live
// keys only).
func (b *BaseEncoder) Add(space, key string, v Version) error {
	k := ck(space, key)
	if (b.n > 0 && k <= b.prev) || v.Del {
		return errBaseOrder
	}
	b.prev = k
	b.n++
	b.chunked = b.chunked || v.Chunk != 0
	e := &b.e
	e.str(k)
	e.u64(1)
	e.u64(v.LSN)
	e.u64(uint64(v.Off)<<32 | uint64(v.Len))
	e.u64(uint64(v.Op)<<32 | uint64(v.Sum))
	e.u64(uint64(v.Chunk))
	e.bytes(v.Val)
	e.str(v.Subject)
	e.bool(false)
	return nil
}

// Buffered is the number of entry bytes not yet taken.
func (b *BaseEncoder) Buffered() int { return len(b.e.b) }

// Take returns the buffered entry bytes, valid until the next Add.
func (b *BaseEncoder) Take() []byte {
	out := b.e.b
	b.e.b = b.e.b[:0]
	return out
}

// Chunked reports whether some version added so far has a base chunk
// (the body is then KV3 and its entries are written as Add produced them).
func (b *BaseEncoder) Chunked() bool { return b.chunked }

// Header returns the body header (BaseHeaderLen bytes) for the entries
// added: format, inline limit and key count.
func (b *BaseEncoder) Header() []byte {
	e := &enc{}
	if b.chunked {
		e.str("KV3")
	} else {
		e.str("KV2")
	}
	e.u64(uint64(b.inline))
	e.u64(b.n)
	return e.b
}

// StripChunks converts entries written by Add (KV3 form) to KV2 form in
// place, dropping every version's chunk number (which must be 0), and
// returns the new length. It reads ahead of where it writes, so p can be
// a memory-mapped file.
func StripChunks(p []byte) (int, error) {
	r, w := 0, 0
	bad := errors.New("index: StripChunks: not entries written by BaseEncoder")
	u64 := func(at int) (uint64, bool) {
		if at+8 > len(p) {
			return 0, false
		}
		return binary.LittleEndian.Uint64(p[at:]), true
	}
	// field returns the end of a length-prefixed field at at (nil allowed).
	field := func(at int, nilOK bool) (int, bool) {
		n, ok := u64(at)
		if !ok {
			return 0, false
		}
		if n == math.MaxUint64 {
			return at + 8, nilOK
		}
		if n > uint64(len(p)-at-8) {
			return 0, false
		}
		return at + 8 + int(n), true
	}
	for r < len(p) {
		start := r
		end, ok := field(r, false) // key
		if !ok {
			return 0, bad
		}
		c, ok := u64(end)
		if !ok || c == 0 || c > uint64(len(p)) {
			return 0, bad
		}
		r = end + 8
		// key and count move as they are
		w += copy(p[w:], p[start:r])
		for i := uint64(0); i < c; i++ {
			ch, ok := u64(r + 24)
			if !ok || ch != 0 {
				return 0, bad
			}
			w += copy(p[w:], p[r:r+24])
			r += 32
			vs := r
			if r, ok = field(r, true); !ok { // value
				return 0, bad
			}
			if r, ok = field(r, false); !ok { // subject
				return 0, bad
			}
			if r >= len(p) || p[r] > 1 {
				return 0, bad
			}
			r++
			w += copy(p[w:], p[vs:r])
		}
	}
	return w, nil
}
