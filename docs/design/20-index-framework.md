# Design 20: the generic index framework (2026-10-31)

## Problem

Before the split, the engine knew each index by name: `view.go`,
`snapshot.go` and `compact.go` had fields and code paths for KV, topics,
documents and vectors. Adding an index meant editing the core, and every
index's bugs were the core's bugs. LogEt's goal is one log with different
indexes over it, added one at a time and each finished before the next
(PIPELINE.md). That needs the core to know nothing about any index but KV.

## Design

- **Records.** The log carries `(space, key, value, subject)` puts and
  deletes. Spaces are owned by index kinds (`Kind.Spaces`, prefixes that
  may not overlap; `_` prefixes are the engine's). Writes go through
  `Tx.PutRecord` / `DeleteRecord` and are refused outside owned spaces.
- **One MVCC record store.** Every record, whatever index owns it, is
  stored in the engine's MVCC record store (the KV structure). This gives
  every index time travel, branches, compaction, shredding and
  `GetRecord` / `ScanRecords` with no work of its own.
- **Derived indexes** (`Kind.New`) keep extra state folded from their
  records. The engine replays the compaction base (latest put per live
  key, as synthetic entries) and then the log into them, or restores a
  snapshot section and replays the tail.
- **Snapshots** hold the KV image plus one section per derived index
  that implements `Snapshotter`, named `name@version`, sorted, decoded
  strictly (strictly increasing names) so bytes are canonical. Unknown or
  other-version sections are ignored and that index is rebuilt.
- **Transactions** stay in the core. OCC validates the read set at
  commit; derived indexes add to it through `ReadKey` / `ReadPrefix`.
  `Update` retries on `ErrConflict`.

## Why not more

- No plugin loading: indexes are Go packages compiled in, registered
  with `WithIndex`. Simple and type-safe.
- No index-private storage: everything durable is a record in the one
  log. That is what makes shred, branches and compaction uniform. The
  cost: a derived index's state must be rebuildable from live records
  alone (the contract test enforces it).
- `Tx.Index` in an `Update` reads the branch head, not the transaction's
  pending writes. Read-your-writes for derived indexes would need an
  overlay per index; deferred until an index needs it (topics will).

## Evidence

- `examples/counter`: 8 tests and a fuzzer, including the contract test
  over 20 random workloads with compaction and reopen; the fuzzer found
  a non-canonical length encoding in `Restore` (fixed, corpus kept).
- The simulator (`sim/`) drives KV and the counter with crashes and
  faults and checks KV, the counter's records and its counts against a
  model fold after every recovery.
- Root tests use a test-only `tally` index for compaction, crash,
  shred and snapshot paths; the golden test uses a record-store index
  (`notes`).
- TLA+: `LogEtTxn` models OCC independently of which index reads; no new
  spec was needed for the framework itself.
