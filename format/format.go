// Package format defines loget's on-storage log format: immutable segments
// containing CRC32C-checksummed frames. One frame = one committed transaction
// (an atomic batch of ops) at one LSN. Frames carry the previous hash-chain
// head so the log is tamper-evident; the chain is computed over the stored
// (possibly encrypted) bytes, so crypto-shredding never breaks verification.
package format

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC returns the CRC32C of b.
func CRC(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Op kinds. The log has five verbs; every model is an index over them.
type Kind uint8

const (
	OpPut    Kind = 1 // set (space,key) = value
	OpDel    Kind = 2 // delete (space,key)
	OpAppend Kind = 3 // append to stream `space`: written only by the topics index (branch index/topics); main never writes it
	OpShred  Kind = 4 // audit marker: subject (Key) was crypto-shredded
	// OpIngest installs a sorted run of puts into space Space, built by bulk
	// ingest and stored beside the log under object key Key; Value is the
	// run's description (pieces and their CRC-32C), so the frame pins the
	// exact bytes. Every put in the run is at the frame's LSN (design 26).
	OpIngest Kind = 5
)

func (k Kind) String() string {
	switch k {
	case OpPut:
		return "PUT"
	case OpDel:
		return "DEL"
	case OpAppend:
		return "APPEND"
	case OpShred:
		return "SHRED"
	case OpIngest:
		return "INGEST"
	}
	return fmt.Sprintf("KIND(%d)", uint8(k))
}

// Op is one operation inside a transaction frame.
type Op struct {
	Kind    Kind
	Subject string // "" = not subject-scoped; else Value is AES-GCM ciphertext under the subject DEK
	Space   string
	Key     string
	Value   []byte
	// ValOff is not encoded: it is the byte offset of Value inside the
	// segment object that holds the frame (set by AppendFrame and
	// DecodeSegment; relative to the payload after EncodePayload or
	// DecodePayload alone; 0 = unknown). It lets indexes keep a position
	// instead of a copy of the value.
	ValOff uint32
	// Exp is an absolute expiry in the frame's TS unit (unix nanoseconds);
	// 0 = never expires. Only OpPut may carry one. Encoded as the kind byte
	// with KindExp set followed by Exp as a little-endian i64
	// (docs/design/27-ttl-snapshots.md); a build older than design 27
	// rejects such a frame as corrupt.
	Exp int64
}

// KindExp flags an op kind byte whose op carries an expiry.
const KindExp = 0x80

// Frame is one committed transaction.
type Frame struct {
	LSN  uint64
	TS   int64 // commit timestamp (from the DB clock), part of the log => deterministic for indexes
	Prev [32]byte
	Ops  []Op
}

// Segment header layout (little-endian, SegHeaderSize bytes):
//
//	0..8   magic "LGETSEG1"
//	8..10  version (SegVersion)
//	10..12 reserved, zero (rejected otherwise, so a future use is never misread)
//	12..20 epoch
//	20..28 seq
//	28..32 CRC32C of bytes 0..28
//
// Frames follow: len uint32 | CRC32C(payload) uint32 | payload.
const (
	SegMagic      = "LGETSEG1"
	SegVersion    = 1
	SegHeaderSize = 8 + 2 + 2 + 8 + 8 + 4
	FrameHdrSize  = 8
	MaxFrameSize  = 64 << 20
	MaxOps        = 1 << 20
)

var (
	ErrTorn    = errors.New("format: torn or corrupt tail")
	ErrHeader  = errors.New("format: bad segment header")
	ErrCorrupt = errors.New("format: corrupt frame")
	// ErrVersion: an intact header (CRC ok) with a version this build does
	// not read, e.g. written by a newer loget. Never repaired.
	ErrVersion = errors.New("format: unsupported segment version")
)

// SegHeader is the fixed header of a segment object.
type SegHeader struct {
	Epoch uint64 // writer epoch that created the segment (fencing)
	Seq   uint64 // dense sequence number within the log, starting at 1
}

// EncodeHeader encodes a segment header (layout above).
func EncodeHeader(h SegHeader) []byte {
	b := make([]byte, SegHeaderSize)
	copy(b, SegMagic)
	binary.LittleEndian.PutUint16(b[8:], SegVersion)
	binary.LittleEndian.PutUint64(b[12:], h.Epoch)
	binary.LittleEndian.PutUint64(b[20:], h.Seq)
	binary.LittleEndian.PutUint32(b[28:], CRC(b[:28]))
	return b
}

// DecodeHeader decodes and checks a segment header: ErrHeader if it is
// damaged or not canonical, ErrVersion if intact but of another version.
func DecodeHeader(b []byte) (SegHeader, error) {
	if len(b) < SegHeaderSize || string(b[:8]) != SegMagic {
		return SegHeader{}, ErrHeader
	}
	if binary.LittleEndian.Uint32(b[28:]) != CRC(b[:28]) {
		return SegHeader{}, ErrHeader
	}
	if v := binary.LittleEndian.Uint16(b[8:]); v != SegVersion {
		return SegHeader{}, fmt.Errorf("%w %d (this build reads %d; written by a newer loget? upgrade the binary)", ErrVersion, v, SegVersion)
	}
	if b[10]|b[11] != 0 {
		return SegHeader{}, ErrHeader
	}
	return SegHeader{Epoch: binary.LittleEndian.Uint64(b[12:]), Seq: binary.LittleEndian.Uint64(b[20:])}, nil
}

// EncodePayload encodes a frame body (without the len/crc frame header).
// It records each op's value offset (relative to the payload) in f.Ops.
func EncodePayload(f *Frame) []byte {
	return appendPayload(make([]byte, 0, payloadSize(f)), f)
}

// payloadSize is the exact encoded size of f's payload.
func payloadSize(f *Frame) int {
	n := 8 + 8 + 32 + uvarintLen(uint64(len(f.Ops)))
	for _, op := range f.Ops {
		if op.Exp != 0 {
			n += 8
		}
		n += 1 + strLen(len(op.Subject)) + strLen(len(op.Space)) + strLen(len(op.Key)) + strLen(len(op.Value))
	}
	return n
}

func strLen(n int) int { return uvarintLen(uint64(n)) + n }

// appendPayload appends f's payload to b. ValOff is relative to the start
// of the payload, which begins at len(b) on entry.
func appendPayload(b []byte, f *Frame) []byte {
	start := len(b)
	b = binary.LittleEndian.AppendUint64(b, f.LSN)
	b = binary.LittleEndian.AppendUint64(b, uint64(f.TS))
	b = append(b, f.Prev[:]...)
	b = binary.AppendUvarint(b, uint64(len(f.Ops)))
	for i, op := range f.Ops {
		if op.Exp != 0 {
			b = append(b, byte(op.Kind)|KindExp)
			b = binary.LittleEndian.AppendUint64(b, uint64(op.Exp))
		} else {
			b = append(b, byte(op.Kind))
		}
		b = appendStr(b, op.Subject)
		b = appendStr(b, op.Space)
		b = appendStr(b, op.Key)
		b = binary.AppendUvarint(b, uint64(len(op.Value)))
		f.Ops[i].ValOff = uint32(len(b) - start)
		b = append(b, op.Value...)
	}
	return b
}

func appendStr[T string | []byte](b []byte, s T) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func rebase(ops []Op, base int) {
	for i := range ops {
		ops[i].ValOff += uint32(base)
	}
}

// AppendFrame appends a framed (len|crc|payload) encoding of f to dst and
// returns the new slice plus the payload (for hash chaining). The payload
// aliases the returned slice; ValOff in f.Ops becomes relative to dst[0].
func AppendFrame(dst []byte, f *Frame) ([]byte, []byte) {
	hdr := len(dst)
	dst = slices.Grow(dst, FrameHdrSize+payloadSize(f))
	dst = append(dst, make([]byte, FrameHdrSize)...)
	dst = appendPayload(dst, f)
	p := dst[hdr+FrameHdrSize:]
	binary.LittleEndian.PutUint32(dst[hdr:], uint32(len(p)))
	binary.LittleEndian.PutUint32(dst[hdr+4:], CRC(p))
	rebase(f.Ops, hdr+FrameHdrSize)
	return dst, p
}

type reader struct {
	b   []byte
	err bool
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.err = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) uv() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 || n != uvarintLen(v) { // reject non-minimal encodings: one payload, one byte string
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func (r *reader) bytes() []byte {
	n := r.uv()
	if r.err || n > uint64(len(r.b)) {
		r.err = true
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

// DecodePayload decodes a frame body. It never panics on arbitrary input.
func DecodePayload(p []byte) (*Frame, error) { return decodePayload(p, nil) }

// names interns Subject and Space strings across the frames of one segment:
// a batch has few distinct spaces, and each conversion would allocate.
type names map[string]string

const maxNames = 256 // bound the table for segments with many subjects

func (m names) str(b []byte) string {
	if m == nil {
		return string(b)
	}
	if s, ok := m[string(b)]; ok { // no allocation for the lookup
		return s
	}
	s := string(b)
	if len(m) < maxNames {
		m[s] = s
	}
	return s
}

func decodePayload(p []byte, in names) (*Frame, error) {
	f := &Frame{}
	if err := parsePayload(p, in, f, true); err != nil {
		return nil, err
	}
	return f, nil
}

// parsePayload checks a frame body and, if build, decodes it. Without
// build it allocates nothing and returns a Frame holding only LSN and TS
// (ScanSegment); it accepts and rejects exactly the same bodies. It fills
// f (without build: only LSN, TS and Prev).
func parsePayload(p []byte, in names, f *Frame, build bool) error {
	r := &reader{b: p}
	f.LSN = r.u64()
	f.TS = int64(r.u64())
	if r.err || len(r.b) < 32 {
		return ErrCorrupt
	}
	copy(f.Prev[:], r.b[:32])
	r.b = r.b[32:]
	n := r.uv()
	if r.err || n > MaxOps || n > uint64(len(r.b)) {
		return ErrCorrupt
	}
	if build {
		f.Ops = make([]Op, 0, n)
	}
	for i := uint64(0); i < n; i++ {
		if len(r.b) < 1 {
			return ErrCorrupt
		}
		k := Kind(r.b[0])
		r.b = r.b[1:]
		var exp int64
		if k&KindExp != 0 {
			k &^= KindExp
			if k != OpPut || len(r.b) < 8 {
				return ErrCorrupt
			}
			exp = int64(binary.LittleEndian.Uint64(r.b))
			r.b = r.b[8:]
			if exp == 0 {
				return ErrCorrupt
			}
		}
		if k < OpPut || k > OpIngest {
			return ErrCorrupt
		}
		subj, space, key := r.bytes(), r.bytes(), r.bytes()
		valOff := len(p) - len(r.b)
		val := r.bytes()
		if r.err {
			return ErrCorrupt
		}
		if !build {
			continue
		}
		var v []byte
		if val != nil {
			v = append([]byte{}, val...)
		}
		f.Ops = append(f.Ops, Op{Kind: k, Subject: in.str(subj), Space: in.str(space), Key: string(key), Value: v, Exp: exp, ValOff: uint32(valOff + uvarintLen(uint64(len(val))))})
	}
	if len(r.b) != 0 {
		return ErrCorrupt
	}
	return nil
}

// Decoded is one frame plus its raw payload (for chain verification).
type Decoded struct {
	Frame   *Frame
	Payload []byte
}

// DecodeSegment decodes a whole segment. It returns every frame up to the
// first damaged one, the number of valid bytes, and ErrTorn if anything after
// that is not a valid frame (a torn or corrupt tail). It never panics.
func DecodeSegment(b []byte) (SegHeader, []Decoded, int, error) {
	var out []Decoded
	h, off, err := walkSegment(b, true, func(f *Frame, p []byte) { out = append(out, Decoded{Frame: f, Payload: p}) })
	return h, out, off, err
}

// SegScan summarises a segment checked by ScanSegment.
type SegScan struct {
	Frames   int    // valid frames
	FirstLSN uint64 // LSN of the first one (0 if none)
}

// ScanSegment checks a segment exactly as DecodeSegment does (same header,
// same frames accepted, same valid-byte count and error) without
// decoding the frames' operations, so it allocates almost nothing. Opens
// use it for segments a checkpoint already covers (design 25).
func ScanSegment(b []byte) (SegHeader, SegScan, int, error) {
	var sc SegScan
	h, off, err := walkSegment(b, false, func(f *Frame, _ []byte) {
		if sc.Frames == 0 {
			sc.FirstLSN = f.LSN
		}
		sc.Frames++
	})
	return h, sc, off, err
}

// walkSegment checks b's header and frames, calling fn for each valid
// frame (with build, a fully decoded fresh Frame; without, a reused one
// holding LSN, TS and Prev).
func walkSegment(b []byte, build bool, fn func(*Frame, []byte)) (SegHeader, int, error) {
	h, err := DecodeHeader(b)
	if err != nil {
		return h, 0, err
	}
	off := SegHeaderSize
	in := names{}
	var tmp Frame
	for off < len(b) {
		if len(b)-off < FrameHdrSize {
			return h, off, ErrTorn
		}
		n := int(binary.LittleEndian.Uint32(b[off:]))
		c := binary.LittleEndian.Uint32(b[off+4:])
		if n > MaxFrameSize || n > len(b)-off-FrameHdrSize {
			return h, off, ErrTorn
		}
		p := b[off+FrameHdrSize : off+FrameHdrSize+n]
		if CRC(p) != c {
			return h, off, ErrTorn
		}
		f := &tmp
		if build {
			f = &Frame{}
		}
		if err := parsePayload(p, in, f, build); err != nil {
			return h, off, ErrTorn
		}
		if build {
			rebase(f.Ops, off+FrameHdrSize)
		}
		fn(f, p)
		off += FrameHdrSize + n
	}
	return h, off, nil
}

// ChainNext computes the next hash-chain head: H(prev || H(payload)).
// The payload contains the stored (possibly encrypted) bytes, so the chain
// is over ciphertext digests and survives crypto-shredding.
func ChainNext(prev [32]byte, payload []byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], prev[:])
	d := sha256.Sum256(payload)
	copy(buf[32:], d[:])
	return sha256.Sum256(buf[:])
}
