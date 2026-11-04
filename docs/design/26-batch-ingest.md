# Design 26: atomic write batches and sorted bulk ingest (2026-11-04)

ROADMAP KV-completion item 2.

## Part 1: `Batch`

A transaction (`Update`) already commits all its writes as one log frame
at one LSN. What was missing is a write path that does not need a closure
and is never re-run: `Batch` collects puts and deletes of KV keys, and
`DB.Apply` / `Branch.Apply` commit them.

- **Atomic**: the batch is one frame. A snapshot (point read, `Scan`,
  iterator, `ViewAt`) before its LSN sees none of it, at or after sees all
  of it. A later write of a key in the same batch wins (ops are applied in
  order, as in a transaction).
- **No OCC cost, no retries**: a batch reads nothing; the writer's
  validation of blind writes is trivially true (`writer.conflict`), so a
  batch never fails with `ErrConflict` and is submitted once. A
  read-write transaction whose snapshot predates a batch and that read a
  key the batch wrote conflicts with it, like with any commit
  (`TestBatchAtomicUnderConcurrency`).
- **Limits**: the frame limits of every commit, `MaxTxOps` (2^20 ops) and
  `MaxTxBytes` (64 MiB encoded), plus `MaxKeyBytes` per key. `Put` and
  `Delete` refuse the op that would cross a limit (`ErrTooLarge`; the batch
  keeps what it had), using an over-estimate of the encoded size; the
  writer still checks the exact size at commit. Larger loads use ingest
  (part 2).
- **Subjects**: `Put(k, v, Subject(s))` works as in a transaction; a
  batch with a shredded subject fails whole (`ErrShredded`).
- **Branches**: `Branch.Apply` commits on that branch.
- **HTTP**: `POST /v1/tx` was already an atomic list of put/del ops; it
  now builds a `Batch` (so it is never retried), and `POST /v1/batch` is
  the same route under its new name. A bad op anywhere refuses the whole
  request (400) before anything is written.

No new log format, no new commit step: a batch is a transaction without
reads, so the TLA+ model of commits (`spec/LogEtTxn.tla`) covers it.

Tests: `batch_test.go` (basic, limits at exactly `MaxTxOps`, shredded
subject, readers never see part of a batch under concurrency (race),
property test vs a model map over 20 seeds), `server/batch_test.go`, and
the simulator sends half of its blind transactions as batches, across
crashes, takeovers, rewinds and compaction.
