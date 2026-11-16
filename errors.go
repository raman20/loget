// Package loget (LogEt) is a single-writer storage engine where one
// append-only log on object storage (or a local directory) is the only
// source of truth, and indexes are pure folds over that log: one log,
// different indexes over it. Key-value is the built-in index; others plug
// in through package index (WithIndex).
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
	// rewind target) needs a segment that log GC has deleted.
	ErrCompacted = errors.New("loget: the requested history was deleted by log GC")
	// ErrWrongKEK: the key-encryption key does not match the one this
	// database was created with.
	ErrWrongKEK = errors.New("loget: wrong key-encryption key for this database")
	// ErrTooLarge: the transaction does not fit in one log frame
	// (MaxTxBytes encoded, MaxTxOps ops). Nothing was written.
	ErrTooLarge = errors.New("loget: transaction too large")
	// ErrBusy: a compaction or GC is already running on this database;
	// retry later. HTTP 409.
	ErrBusy = errors.New("loget: compaction already running")

	// ErrBadName: a name the API cannot store unambiguously. Reserved for
	// indexes that name things (the topics index on branch index/topics).
	ErrBadName = errors.New("loget: invalid name")

	// ErrBadValue: a value or record the API refuses, such as a record in a
	// space no registered index owns, or an index whose spaces are invalid.
	// HTTP 400.
	ErrBadValue = errors.New("loget: invalid value")

	// ErrSnapshotTooOld: a Snapshot outlived Options.SnapshotMaxAge, or
	// its LSN is below the history floor (a needed segment was GC'd, or
	// a compaction in progress is about to delete one).
	ErrSnapshotTooOld = errors.New("loget: snapshot too old")
	// ErrSnapshotReleased: the Snapshot was released.
	ErrSnapshotReleased = errors.New("loget: snapshot released")
)

// Size limits of one transaction, checked at commit (after encryption).
const (
	MaxTxBytes = format.MaxFrameSize
	MaxTxOps   = format.MaxOps
)

// MaxKeyBytes bounds keys, record spaces and subject names
// (checked when the write is staged; ErrTooLarge). Every key lives in the
// in-memory index, so this also bounds what one write can cost in RAM.
const MaxKeyBytes = 8 << 10

// Op re-exports the log op type for advanced users (Entries).
type Op = format.Op
