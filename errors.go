// Package loget is a single-writer storage engine where one append-only log on
// object storage (or a local directory) is the only source of truth, and
// key-value, topics/consumer groups, documents, secondary indexes and vectors
// are "indexes": pure folds over that log.
package loget

import (
	"errors"

	"github.com/raman20/loget/format"
)

var (
	ErrNotFound       = errors.New("loget: not found")
	ErrConflict       = errors.New("loget: transaction conflict (serializability); retry")
	ErrFenced         = errors.New("loget: fenced: another writer took over this database")
	ErrUnknownOutcome = errors.New("loget: commit outcome unknown (storage error); reopen to learn whether it committed")
	ErrShredded       = errors.New("loget: subject has been crypto-shredded")
	ErrClosed         = errors.New("loget: database closed")
	ErrCorrupt        = errors.New("loget: corrupt log")
	// ErrVersion: an object (manifest, segment, base, journal file) carries
	// a format version this build does not know, i.e. it was written by a
	// newer loget. Nothing is changed; upgrade the binary
	// (docs/compatibility.md).
	ErrVersion        = format.ErrVersion
	ErrReadOnly       = errors.New("loget: read-only transaction")
	ErrBranchExists   = errors.New("loget: branch already exists")
	ErrNoBranch       = errors.New("loget: no such branch")
	ErrNotFastForward = errors.New("loget: promote is not a fast-forward")
	ErrBadLSN         = errors.New("loget: LSN out of range")
	ErrMainBranch     = errors.New("loget: branch main cannot be deleted")
	// ErrCompacted: the requested history (time travel, branch point,
	// rewind target) is older than the log's compaction base.
	ErrCompacted = errors.New("loget: history before the compaction base was compacted away")
	// ErrWrongKEK: the key-encryption key does not match the one this
	// database was created with.
	ErrWrongKEK = errors.New("loget: wrong key-encryption key for this database")
	// ErrTooLarge: the transaction does not fit in one log frame
	// (MaxTxBytes encoded, MaxTxOps ops). Nothing was written.
	ErrTooLarge = errors.New("loget: transaction too large")
	// ErrBusy: a compaction or GC is already running on this database;
	// retry later. HTTP 409.
	ErrBusy = errors.New("loget: compaction already running")

	// ErrBadName: a name the API cannot store unambiguously (a consumer
	// group name with a NUL byte), or a message acked in another topic's
	// group.
	ErrBadName = errors.New("loget: invalid name")

	// ErrBadValue: a value the API refuses: a document that is not a JSON
	// object, or a Find value that cannot be encoded as JSON. HTTP 400.
	ErrBadValue = errors.New("loget: invalid value")
)

// Size limits of one transaction, checked at commit (after encryption).
const (
	MaxTxBytes = format.MaxFrameSize
	MaxTxOps   = format.MaxOps
)

// MaxKeyBytes bounds keys, and topic, collection and subject names
// (checked when the write is staged; ErrTooLarge). Every key lives in the
// in-memory index, so this also bounds what one write can cost in RAM.
const MaxKeyBytes = 8 << 10

// Op re-exports the log op type for advanced users (Entries).
type Op = format.Op
