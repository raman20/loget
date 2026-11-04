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

## Part 2: sorted bulk ingest

`DB.Ingest` / `Branch.Ingest(ctx, src)` loads sorted puts (KV keys, no
subjects, no deletes) as **one commit** that is not limited by
`MaxTxOps`/`MaxTxBytes`. `src` calls `add(key, val)` in strictly
increasing key order (`ErrUnsorted` otherwise); it returns
`IngestStats{LSN, Keys, Bytes, Pieces}`.

### Protocol (ingest.go)

1. **Exclusion.** The ingest takes the DB's compaction slot (it waits for
   a running Compact/GC; Compact/GC started during it get `ErrBusy`;
   branch operations wait), then its branch's writer: the writer hands it
   the commit's LSN (head+1) and segment number and serves nothing else
   on that branch until the ingest says ready or gives up.
2. **Build and store.** The index builds one run directly from the input
   (`index.Ingest`: a run file when the DB has run files, else the heap),
   every key one inline put at that LSN. Its bytes are stored in 64 MiB
   pieces beside the segment the commit will take:
   `log/<log>/<seq>-e<epoch>-<nonce>.ingest.<p:%06d>`. Each piece's pages
   are dropped from RSS once stored.
3. **Commit point.** The writer commits one frame with one op of the new
   kind `5=INGEST` (space `kv`, key = the run's object key, value = JSON
   `{meta: {len, n, vers, piece, sums}, sha256}`). Before that frame is
   durable nothing of the ingest is visible and its pieces are orphans;
   after it, all of it is. The writer installs the run in front of the
   KV's runs (the hot set is frozen first) and raises its OCC floor to
   the ingest's LSN, so a read-write transaction whose snapshot predates
   the ingest conflicts (the ingested keys are not listed in the OCC
   window).
4. **Replay.** Opening (or `RebuildDigests`) loads the named pieces,
   checks the per-piece CRC32C, the SHA-256 the frame commits to (so the
   hash chain covers ingested data), and that every entry is exactly one
   inline put at the frame's LSN (`checkIngest`), then installs the same
   run: the index after replay equals the index at commit. A missing or
   damaged piece is `ErrCorrupt`; `Verify` checks every live ingest too.
5. **Space.** Checkpoints and compaction bases store their own copy of
   the data, as for any run. GC deletes ingest objects with their segment.
   The orphan sweep (run by Compact/GC) deletes ingest objects of an
   older epoch whose segment exists (or, below the log's next segment,
   was GC'd) and does not name them; a failed ingest deletes its own
   pieces right away when it can.

`spec/LogEtIngest.tla` models stored pieces, the frame commit (by
PutIfAbsent of the segment), fenced/crashed writers of older epochs, GC
and the sweep: a live frame's pieces are always all present
(`IngestIntact`), 279,519 distinct states at MaxSeq=5, MaxEpoch=4, two
processes. `LogEtIngest_early.cfg` (commit before all pieces are stored)
is rejected. Dropping the sweep's "below our next segment" guard is *not*
rejected: the open-time fence already keeps an older writer from
committing at any absent segment, so the guard is defence in depth.

### Tradeoffs (honest list)

- Commits on the branch wait for the whole ingest (build + upload), and
  Compact/GC cannot run during it. Ingest in chunks of what the
  application can afford to block.
- Values stay inline in the index (in run files when the DB has them):
  an ingest of large values makes a large index, unlike `Put`, which
  moves large values to value chunks.
- The data is stored twice until GC (the pieces, and the first
  checkpoint or base that copies the run). A later merge may rewrite it.
- Old binaries reject a log with kind 5 as corrupt: after the first
  ingest the store needs this version or newer.
- `Entries` and `/v1/log` report an ingest as one op of kind `ingest`
  (the keys are not listed).

### Tests

- `index/ingest_test.go`: ingests mixed with puts/deletes vs the map
  model at every LSN (heap and run files, 30 seeds); a run loaded from its
  stored bytes has the same digest as the built one; wrong LSN rejected;
  `FuzzLoadIngest`.
- `ingest_test.go`: basic, unsorted commits nothing, property test (12
  seeds: ingests, puts, deletes, compaction, reopen; live index ==
  full-log replay == reopened checkpoint), OCC conflict, orphan sweep,
  damaged and missing pieces (`ErrCorrupt`, `Verify`).
- `TestCrashKill9Ingest`: SIGKILL of a child ingesting 3,000-key batches
  while 4 goroutines commit and compactions and checkpoints run; each
  recovery checks every ingest is whole or absent and every acknowledged
  one present, plus the existing invariants.
- The simulator sends a third of its put-only transactions as ingests
  (`__tx` marker included) under faults, crashes, takeovers, rewinds and
  compaction; 2,000 seeds × 200 steps passed.

### Numbers (bench/results/2026-11-04-ingest, one run each, fsync on)

10M sorted keys, 100-byte values: `Ingest` 1,446,309 keys/s, peak RSS
172 MiB (1,124 MiB before `Finish` dropped each piece's pages after its
checksum); Pebble sstable + `Ingest` 3,963,331 keys/s, 46 MiB. Batches of
1,000: LogEt 335,073 keys/s, 249 MiB; Pebble 2,088,630 keys/s, 35 MiB.
