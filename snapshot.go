package loget

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/raman20/loget/format"
	"github.com/raman20/loget/index"
)

// Snapshots checkpoint a log's record store and the state of every
// derived index that can snapshot itself, so open need not replay
// the whole log. They are a cache: a missing, damaged or mismatched
// snapshot is ignored (with a log line) and the log is replayed instead.
//
// Layout (v2; formats.md):
//
//	magic "LGSNAP2\n" | lsn u64 | chain [32]byte | idLen u8 | log id
//	| kvLen u64 | kv | nSections u32
//	| { nameLen u8 | name | len u64 | bytes }*nSections   (sorted by name)
//	| crc32c u32 (over everything before it)
//
// A section holds one derived index's Snapshot; its name is the index
// kind's SectionName ("name@version"). Sections of kinds that are not
// registered, or of another version, are ignored.
//
// The log id and the hash-chain head at lsn bind a snapshot to the exact
// history it summarises: bytes that belong to another log (or another
// branch of the same history) are rejected, not loaded. v1 snapshots
// (no magic, no binding) are ignored.

var snapMagic = [8]byte{'U', 'R', 'S', 'N', 'A', 'P', '2', '\n'}

type snapshot struct {
	lsn      uint64
	kv       *index.KV
	sections map[string][]byte // derived index state by section name
	drop     []string
}

// snapHeader is what binds a snapshot to its history.
type snapHeader struct {
	log   string
	lsn   uint64
	chain [32]byte
}

func snapKey(log string, lsn uint64) string { return fmt.Sprintf("snap/%s/%020d.snap", log, lsn) }

func encodeSnapshot(h snapHeader, kv []byte, sections map[string][]byte) []byte {
	n := len(snapMagic) + 8 + 32 + 1 + len(h.log) + 8 + len(kv) + 4 + 4
	for name, b := range sections {
		n += 1 + len(name) + 8 + len(b)
	}
	b := make([]byte, 0, n)
	b = append(b, snapMagic[:]...)
	b = binary.LittleEndian.AppendUint64(b, h.lsn)
	b = append(b, h.chain[:]...)
	b = append(b, byte(len(h.log)))
	b = append(b, h.log...)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(kv)))
	b = append(b, kv...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(sections)))
	for _, name := range sortedMapKeys(sections) {
		b = append(b, byte(len(name)))
		b = append(b, name...)
		b = binary.LittleEndian.AppendUint64(b, uint64(len(sections[name])))
		b = append(b, sections[name]...)
	}
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

var errSnapshot = errors.New("not a valid v2 snapshot")

// decodeSnapshot checks a snapshot's framing and checksum and splits it.
// It never panics on any input (FuzzDecodeSnapshot).
func decodeSnapshot(b []byte) (h snapHeader, kv []byte, sections map[string][]byte, err error) {
	const fixed = len(snapMagic) + 8 + 32 + 1
	if len(b) < fixed+8+4+4 || !bytes.Equal(b[:8], snapMagic[:]) {
		return h, nil, nil, errSnapshot
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if format.CRC(body) != sum {
		return h, nil, nil, fmt.Errorf("%w: checksum mismatch", errSnapshot)
	}
	h.lsn = binary.LittleEndian.Uint64(body[8:])
	copy(h.chain[:], body[16:48])
	idLen := int(body[48])
	rest := body[fixed:]
	if len(rest) < idLen+8 {
		return h, nil, nil, fmt.Errorf("%w: truncated header", errSnapshot)
	}
	h.log, rest = string(rest[:idLen]), rest[idLen:]
	n := binary.LittleEndian.Uint64(rest)
	rest = rest[8:]
	if n > uint64(len(rest)) {
		return h, nil, nil, fmt.Errorf("%w: kv length %d > %d", errSnapshot, n, len(rest))
	}
	kv, rest = rest[:n], rest[n:]
	if len(rest) < 4 {
		return h, nil, nil, fmt.Errorf("%w: truncated section count", errSnapshot)
	}
	count := binary.LittleEndian.Uint32(rest)
	rest = rest[4:]
	sections = map[string][]byte{}
	prev := ""
	for i := uint32(0); i < count; i++ {
		if len(rest) < 1 || len(rest) < 1+int(rest[0])+8 {
			return h, nil, nil, fmt.Errorf("%w: truncated section %d", errSnapshot, i)
		}
		name := string(rest[1 : 1+int(rest[0])])
		rest = rest[1+int(rest[0]):]
		m := binary.LittleEndian.Uint64(rest)
		rest = rest[8:]
		if m > uint64(len(rest)) {
			return h, nil, nil, fmt.Errorf("%w: section %q length %d > %d", errSnapshot, name, m, len(rest))
		}
		if i > 0 && name <= prev {
			return h, nil, nil, fmt.Errorf("%w: section %q out of order", errSnapshot, name)
		}
		sections[name], rest, prev = rest[:m], rest[m:], name
	}
	if len(rest) != 0 {
		return h, nil, nil, fmt.Errorf("%w: %d trailing bytes", errSnapshot, len(rest))
	}
	return h, kv, sections, nil
}

// writeSnapshot checkpoints l's indexes (an index checkpoint, ckpt.go:
// snapshots in the format above are only read now).
func (db *DB) writeSnapshot(ctx context.Context, l *logState, v *view) error {
	return db.writeIndexCkpt(ctx, l, v)
}

// loadSnapshot returns l's index checkpoint, else its snapshot (written
// before design 24), or nil to replay the log.
func (db *DB) loadSnapshot(ctx context.Context, l *logState) *snapshot {
	lm := db.man.Logs[l.meta.ID]
	if lm == nil {
		return nil
	}
	if lm.Ckpt != nil && !db.fullScan {
		snap, err := db.readIndexCkpt(ctx, l, lm.Ckpt)
		if err == nil {
			return snap
		}
		db.met.ckptFallbacks.Add(1)
		db.logf("loget: ignoring index checkpoint %s (the log is replayed instead): %v", lm.Ckpt.Key, err)
	}
	if lm.SnapKey == "" {
		return nil
	}
	snap, err := db.readSnapshot(ctx, l, lm.SnapKey)
	if err != nil {
		db.logf("loget: ignoring snapshot %s (the log is replayed instead): %v", lm.SnapKey, err)
		return nil
	}
	return snap
}

func (db *DB) readSnapshot(ctx context.Context, l *logState, key string) (*snapshot, error) {
	b, err := db.st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	h, kvb, sections, err := decodeSnapshot(b)
	if err != nil {
		return nil, err
	}
	if err := l.checkSnapshot(ctx, key, h); err != nil {
		return nil, err
	}
	kv, err := index.LoadKVOptions(kvb, db.kvOptions())
	if err != nil {
		return nil, err
	}
	return &snapshot{lsn: h.lsn, kv: kv, sections: sections, drop: db.keys.shreddedList()}, nil
}

// checkSnapshot verifies that a snapshot header belongs at key in l's
// history: same log and LSN as its key, inside the live range, and the
// same hash-chain head at that LSN.
func (l *logState) checkSnapshot(ctx context.Context, key string, h snapHeader) error {
	if key != snapKey(h.log, h.lsn) || h.log != l.meta.ID {
		return fmt.Errorf("%w: it holds log %s LSN %d", errSnapshot, h.log, h.lsn)
	}
	if h.lsn > l.head() || h.lsn < l.floor() {
		return fmt.Errorf("%w: LSN %d outside the log (%d..%d)", errSnapshot, h.lsn, l.floor(), l.head())
	}
	chain, err := l.chainAt(ctx, h.lsn)
	if err != nil {
		return err
	}
	if chain != h.chain {
		return fmt.Errorf("%w: hash chain at LSN %d differs from the log's", errSnapshot, h.lsn)
	}
	return nil
}
