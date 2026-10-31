package loget

import (
	"encoding/binary"
	"errors"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// baseStream assembles a base object (encodeBase's bytes) as a stream
// (design 23): the header is a placeholder until the body is complete, the
// body goes to a run file (unlinked, memory-mapped once complete) where
// the KV index keeps its runs, or to the heap when it keeps them there.
// Before, the whole key set was built as a KV index, snapshotted and
// copied into the image on the heap: three copies of the base at once.
type baseStream struct {
	be   *index.BaseEncoder
	f    index.RunFile // nil: heap
	heap []byte
	err  error
	n    int64
}

// baseHdr is the image's header before the body: magic, LSN, body length,
// then the body's own header.
const baseHdr = 24 + index.BaseHeaderLen

// baseFlushAt is how many entry bytes the encoder buffers between writes.
const baseFlushAt = 1 << 20

func (db *DB) newBaseStream(inline int) *baseStream {
	b := &baseStream{be: index.NewBaseEncoder(inline)}
	if db.runFiles != nil {
		f, err := db.runFiles.Create()
		if err == nil {
			b.f = f
		} else {
			db.kvFileError(err) // this base on the heap
		}
	}
	b.write(make([]byte, baseHdr))
	return b
}

func (b *baseStream) write(p []byte) {
	if b.err != nil {
		return
	}
	b.n += int64(len(p))
	if b.f == nil {
		b.heap = append(b.heap, p...)
		return
	}
	b.err = b.f.Write(p)
}

// add appends one key's version (see index.BaseEncoder.Add).
func (b *baseStream) add(space, k string, v index.Version) error {
	if err := b.be.Add(space, k, v); err != nil {
		return err
	}
	if b.be.Buffered() >= baseFlushAt {
		b.write(b.be.Take())
	}
	return b.err
}

// finish completes the image for a base at lsn and returns it; release
// frees it (the mapping) once the caller is done with the bytes.
func (b *baseStream) finish(lsn uint64) (img []byte, release func(), err error) {
	b.write(b.be.Take())
	b.write(make([]byte, 4)) // the CRC
	release = func() {}
	if b.err == nil && b.f != nil {
		f := b.f
		release = f.Release
		img, _, b.err = f.Map(0)
		if b.err == nil && int64(len(img)) != b.n {
			b.err = errShortBase
		}
	} else {
		img = b.heap
	}
	if b.err != nil {
		release()
		return nil, nil, b.err
	}
	body := img[baseHdr : len(img)-4]
	if !b.be.Chunked() { // KV2: drop the chunk numbers in place
		n, err := index.StripChunks(body)
		if err != nil {
			release()
			return nil, nil, err
		}
		img = img[:baseHdr+n+4]
	}
	copy(img, baseMagic)
	binary.LittleEndian.PutUint64(img[8:], lsn)
	binary.LittleEndian.PutUint64(img[16:], uint64(len(img)-24-4))
	copy(img[24:], b.be.Header())
	binary.LittleEndian.PutUint32(img[len(img)-4:], format.CRC(img[:len(img)-4]))
	return img, release, nil
}

var errShortBase = errors.New("loget: base image file is short")

// abandon frees the image after a failure.
func (b *baseStream) abandon() {
	if b.f != nil {
		b.f.Release()
	}
	b.heap = nil
}
