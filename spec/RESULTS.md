# TLA+ model-checking results

**LogEt main (2026-10-31).** main keeps the specs for the log, transactions,
branches, compaction, GC, migration, journal growth, notification and
shredding. LogEtANN (persisted HNSW graphs) and LogEtRetain (topic retention
vs consumer groups) moved with their indexes to the `index/vectors` and
`index/topics` branches. The specs were renamed from `Fresh*` (freshDB) to
`LogEt*` with no change to their logic; the rename re-run is recorded
below. LogEtTxn still models its transaction as
"consume a message + write KV + advance a cursor"; on main that is the shape
of any read-modify-write over two keys and a counter, so the model stands.

**Rename re-run (2026-10-31 21:17–21:20 IST).** All nine `LogEt*` specs
on main, `spec/run_tlc.sh` with the pinned v1.7.4 jar (sha 936a2620…),
Java 21, `-workers auto` on the 8-vCPU dev box: no error, and every state
count and depth equals the table below (LogEtLog 22,587,747 / 5,697,022,
depth 32; LogEtGC 6,582,415 / 1,598,697, depth 28). The mutant configs
were not re-run under the new names.

TLC 2.19 (tla2tools v1.8.0), Java 21, 8 workers, on the dev box (2026-10-27).

**Jar pin (2026-10-29).** `v1.8.0` turned out to be a prerelease tag whose
`tla2tools.jar` is rebuilt from master every night (the asset was updated
2026-10-28 01:50 UTC), so CI's pinned checksum only passed because the jar
was cached, and the box had a later build with a different checksum. Both
were "v1.8.0" nightlies; neither was a fixed release. CI and local runs now
use `spec/get_tla.sh`, which downloads the stable **v1.7.4** release
(TLC 2.19 of 08 August 2024, sha256 `936a2620…0e88`) and verifies it. All
seven specs pass with it (LogEtLog: same 10,415,103 / 2,475,220 states).
Run with `spec/run_tlc.sh` (CI runs it on every push). Every run below explored the **complete** bounded
state space (0 states left on queue). Bounds are in each `.cfg`.

Last full rerun: 2026-10-27, after the rename to LogEt. State counts were
identical to the first run. Wall times on this 8-vCPU box: LogEtLog 31 s,
LogEtTxn 24 s, LogEtBranch 22 s, LogEtShred 1 s. LogEtLog reported depth 30;
the first run reported 32 with identical state counts.

| Spec | What it models | States generated | Distinct | Depth | Result |
|---|---|---:|---:|---:|---|
| LogEtLog | object-store log: create-only manifests + epochs, recovery scan, fence segment, group commit, lost/delayed/duplicated PUTs, give-up (unknown outcome), crash, power-loss torn tail + repair note, segment-index checkpoints (2026-10-29), 2 racing writers | 22,587,747 | 5,697,022 | 32 | no error |
| LogEtTxn | OCC serializable txns; consume-message + write-KV + ack-cursor in one txn; 3 concurrent consumers + blind writer; durable-prefix crash | 21,685,744 | 2,421,454 | 20 | no error |
| LogEtBranch | append-only logs with parents; create branch, rewind (fork + ref swap + reflog), time travel | 3,430,206 | 1,845,287 | 16 | no error |
| LogEtMigrate | offline layout migration (files ↔ journal): copy, verify, fsync, rename to `.migrated`, move original to backup, rename into place; volatile vs durable directory state; up to 4 crashes at any step with resume | 149 | 88 | 17 | no error (incl. liveness) |
| LogEtCompact | compaction/GC of a log: base image write, create-only manifest commit with garbage list, garbage + orphan sweeps, 2 processes with crashes and reopen (≤3 epochs), a fenced compactor, a reader holding an old manifest; grace modelled as "readers that started before an object was listed finish first". CI config MaxLSN=2; MaxLSN=3 checked locally: 63,131,325 / 11,134,091 states, depth 32, no error | 1,557,725 | 315,903 | 25 | no error |
| LogEtNotify | consumer change notification: a writer moves the head (commit or rewind) then closes and replaces the channel; 2 waiters run take-channel / read-head / block; MaxHead=2, MaxSteps=5 | 87,149 | 29,427 | 19 | no error |
| LogEtGC | GC with retention (item 8): two logs (main's log and a rewind child forked at P=1, kept by the reflog), retention window W=1 commits, a base at the cut capped by the dependent child's branch point, reflog pruning, dropping unreachable logs, sweeping, deleting old manifests (Keep=1), 2 processes with crashes, ≤2 epochs, a fenced writer that keeps running (incl. committing into a deleted manifest slot), create-only manifest put + post-create List check; MaxLSN=3, MaxMan=6, process symmetry | 6,582,415 | 1,598,697 | 28 | no error |
| LogEtGrow | journal preallocation (item 9): 2 appenders, 2 background growers, a file of ≤6 blocks; appends extend synchronously when needed, growers write one zero block at the current size under the writer lock | 97 | 28 | 8 | no error |
| LogEtShred | DEK per subject, marker → delete → audit entry + certificate; crash between steps; DEK create landing late; sweep; hash chain | 25,570 | 6,481 | 16 | no error (incl. liveness) |
| LogEtCkpt | index checkpoints and the sharded base (design 24, 2026-11-01): runs as LSN intervals; freeze and merge, piece and descriptor uploads, conditional checkpoint commit, compaction dropping the checkpoint, garbage sweeps with grace, orphan sweep by epoch, 2 processes with crashes, ≤2 epochs, open from a checkpoint, a reader of an older manifest; MaxLSN=2, MaxMans=5, process symmetry. Run with `-workers 2` (3 min 14 s). Larger bounds did not finish and are partial only (below) | 33,127,486 | 6,621,739 | 36 | no error |
| LogEtIngest | bulk ingest (design 26, 2026-11-04): pieces stored beside the segment, frame committed by segment PutIfAbsent, open-time fence, fenced/crashed writers of older epochs, abort, GC with the segment, orphan sweep by epoch; 2 processes, process symmetry. CI config MaxSeq=3, MaxEpoch=3 (14,318 / 2,677 states, depth 17); `LogEtIngest_big.cfg` MaxSeq=5, MaxEpoch=4 shown | 2,480,463 | 279,519 | 27 | no error |
| LogEtSnapshot | snapshot retention (design 27, 2026-11-04): writers append, snapshot creation (check floor and planned cut, register) and compaction planning (skip a cut below a live snapshot, record the cut, monotone) under one mutex, then commit or fail; snapshots age out (stop pinning) and are released; 3 snapshots, MaxLSN=4 | 768,903 | 151,967 | 17 | no error |
| LogEtTTL | per-key expiry in the fold (design 27): frames with non-decreasing commit timestamps, puts with absolute expiries, reads at LSN L using TS(L), compaction to a base at any cut dropping keys expired at TS(cut); checks base+tail = full replay at every L ≥ cut and that OCC validation with the expiry rule implies the read is unchanged at commit; 2 keys, 4 frames, timestamps 0..3 | 5,085,081 | 1,833,081 | 6 | no error |
| LogEtChunks | value chunks shared between bases (design 28): compactions sharing any subset of the current base's chunks and uploading fresh ones, crashes before commit (orphans), garbage = old refs minus new refs, due-garbage sweeps under grace, orphan sweep, readers of older manifests; checks no chunk a manifest, reader or in-flight compaction refers to is deleted; 5 chunks, 4 compactions | 9,826,032 | 2,158,748 | 15 | no error |
| LogEtLogGC | log-only values and segment GC (design 30, 2026-11-16, retention-cut extension the same day): values live in segments; a KV compact writes only a pointer base (no new chunks); a commit may start the recoverable tail at any still-stored cut (pointer compact at head+1, or a KeepCommits/KeepFor cut that extends the tail back into history so SweepHist cannot delete the window); history segments and leftover format-4 chunks are deleted only when no index, snapshot or recoverable tail refers to them; a second index may fold retained segments; crashes before commit leave orphan bases; grace for readers of older live-sets. CI config MaxLSN=3, MaxBase=2, MaxSnaps=1, MaxReaders=1, one leftover chunk | 1,164,659 | 173,628 | 19 | no error |

## Properties checked

- **LogEtLog** (invariants): `Durability` (acked ⊆ recovered log), `NoHoles`,
  `EpochMonotone` (no split brain: an older writer can't append after a newer
  fence), `NoDoubleApply`, `NoCorruption` (recovery never meets damage below the
  tail), `TornOnlyUnacked`, `SingleLeader`, `ManifestMonotone`, `NotesTorn`,
  `CkptSound` (everything a checkpoint covers is present and undamaged or
  noted, so recovery may skip re-checking it).
  Temporal: `AckedStable`, `Immutable` (segments never change; a torn tail
  is accepted with a separate repair note).
  Checkpoints (2026-10-29) grew LogEtLog from 10,415,103 / 2,475,220 to
  22,587,747 / 5,697,022 states (139 s on the shared box under load).
- **LogEtTxn**: `ExactlyOnce` (each message processed ≤ once), `AtomicCursor`
  (processed set = {0..cursor-1}, i.e. consume+KV+ack are atomic), `Serializable`
  (every committed read equals the serial-order state before it), `DurablePrefix`.
- **LogEtBranch**: `RewindExact`, `RewindKeepsOld` (pre-rewind head still readable),
  `DenseLSN`; temporal `AppendOnly`, `TimeTravelStable` (no step ever changes the
  contents of any (log, lsn) read).
- **LogEtMigrate** (2026-10-28): `OriginalSafe` (the original data is
  reachable under the DB directory or its backup, both in the live and in the
  crash-surviving state), `CopyReachable` (if the DB directory is missing, a
  complete copy waits under `.migrated`), `NoPartialVisible` (neither name ever
  holds an incomplete copy), `DoneCorrect`; temporal `Finishes` (resuming
  always completes). Sanity check: removing the data fsync before the first
  rename makes TLC report a `NoPartialVisible` violation.
- **LogEtCompact** (2026-10-28): `NoLoss` (every object the latest
  manifest needs exists), `EpochMonotone` (no fenced compactor commits),
  `GarbageFinal` (nothing listed as garbage is referenced by the same or a
  later manifest), `ReaderOK` (under the grace assumption a reader of an
  older manifest never misses an object).
- **LogEtNotify** (2026-10-28): `NoLostWakeup` (a waiter blocked with a
  stale view of the head always has a newer channel generation, or a
  pending close, coming) and `TypeOK`.
- **LogEtShred**: `ShreddedNeverRevealed`, `NoWritesAfterShred`, `ChainVerifiable`,
  `CertsVerify`; temporal `AppendOnly`, `KeyEventuallyGone` (under weak fairness
  of sweep/land: `<>[](shredded ⇒ no DEK)`).

## Bugs found by the models (and fixed)

1. **Real design + code bug — LogEtLog, `EpochMonotone` violated.** A slow opener
   (epoch 1) that created its manifest before a newer opener (epoch 2), but
   scanned the log *after* epoch 2 wrote its fence, appended its own fence with
   the older epoch. The next recovery would then refuse the log as corrupt
   ("epoch went backwards"), bricking the database. Counterexample: 9 steps.
   Fix: recovery fails with `ErrFenced` and writes nothing if it sees any epoch
   newer than its own (`log.go recoverLog`, also before torn-tail repair; spec
   `Superseded`). Regression test `TestConcurrentOpenersNeverBrickLog` **fails
   without the fix and passes with it**. The deterministic simulator had not
   caught this because it never interleaves two concurrent `Open`s.
2. **Spec bug — LogEtBranch, `AppendOnly` violated** at `Rewind("main",0)`: the
   property quantified over unallocated log slots, so initialising a fresh log
   looked like mutating one. Fixed by quantifying over `1..nlogs`. The code
   was not affected (no existing log's parent ever changes).
3. The shred spec models a race the **simulator** found first (seed 769): a
   DEK-create PUT that timed out but was still in flight can land after the
   shred deleted the DEK. Reads stay blocked by the shred marker
   (`ShreddedNeverRevealed`), and the sweep at open/Verify gives
   `KeyEventuallyGone`.

4. **Real design bug — LogEtCompact, `GarbageFinal` violated** on the
   first run (7 steps): compacting a log twice at the same head in the same
   epoch wrote the same base key and listed the live base as garbage, so a
   sweep would delete it. Fix before any code was written: a log with no
   frame since its base is skipped, and base keys carry a random nonce.

- **LogEtGC**: `NoLoss` (everything the latest manifest needs exists: bases,
  segments above each base, a dependent branch's parent prefix), `Retained`
  (every kept log keeps W commits above its base), `NoZombie` (no manifest
  from a fenced epoch is ever the latest; a writer of the newest epoch
  always holds the latest manifest), `GarbageFinal` (no present manifest
  lists anything the latest needs as garbage).

- **LogEtGrow**: `NoLostRecord` (no zero write lands on a record block),
  `Covered` (the file covers `end` and every block below it is a record).
  A small model: it exists to show the locking rule is needed, see the
  mutant below.

- **LogEtLogGC** (design 30): `NoDangling` (every ref from KV, a second
  folded index, live snapshots, readers of older live-sets, the
  recoverable tail, and an in-flight or committed pointer base exists
  in the store), `TailPinned` (the recoverable tail is never history-GC'd),
  `NoNewChunks` (no compaction creates a value chunk), `ValuesOnly`
  (indexes and snapshots point at segments or leftover format-4 chunks,
  never at a pointer base). Fencing of the manifest commit is the same
  create-only rule already checked in LogEtLog and LogEtCompact; this
  model is a single writer plus a crash that leaves orphan pointer
  bases. LogEtChunks remains the model of the format-4 chunk protocol,
  which format-5 databases can still read.
  The first LogEtLogGC omitted retention cuts: `Commit` always set
  `tailFrom` to `head+1`, so in-window history after a pointer compact
  was not in `Tail` and `SweepHist` could delete it (the Go
  `retentionCut` walked only the writable tail). The extension allows
  any `newTail` whose `newTail..head` segments are still stored; the
  keep window is `Tail` again. The model still has no forks or rewind,
  so it would not have caught a child inheriting a parent's pointer
  base below `ParentLSN`, or a parent GC ignoring a child's live refs.
  The sim double-count of retained `Entries` after a pointer compact
  is a test-harness assumption, not a model property.

## Mutation checks (are the properties strong enough?)

| Mutation | Result |
|---|---|
| LogEtLog without the `Superseded` check | `EpochMonotone` violated (bug #1 above) |
| LogEtLog with a checkpoint allowed at any dense prefix, not only the writer's own acknowledged commit (spec edit, 2026-10-29) | `CkptSound` violated (found after 2,291 states) |
| LogEtTxn with OCC validation removed | `ExactlyOnce` violated (911 states) |
| LogEtCompact without the grace guard on sweeps | `ReaderOK` violated |
| LogEtNotify with the head read before taking the channel (`LogEtNotifyBug.cfg`) | `NoLostWakeup` violated |
| LogEtIngest committing the frame before every piece is stored (`LogEtIngest_early.cfg`) | `IngestIntact` violated (66 distinct states) |
| LogEtIngest without the sweep's "below our next segment" guard | not violated: the open-time fence already stops older writers; the guard is defence in depth |
| LogEtCompact with the manifest commit not create-only (no fencing) | `EpochMonotone` violated |
| LogEtGC without the post-create manifest List (`LogEtGC_nopost.cfg`) | `GarbageFinal` violated in 10 steps: a fenced writer commits its stale GC manifest into a deleted slot below the latest, listing a segment the live child branch needs as garbage |
| LogEtGC with the parent's cut ignoring a dependent branch (`LogEtGC_nodeps.cfg`) | `GarbageFinal` violated |
| LogEtGC deleting the manifest it holds (`Keep = 0`, `LogEtGC_keep0.cfg`) | `TypeOK` violated (no manifest left) |
| LogEtGrow with the grower writing at a size read before releasing the lock (`LogEtGrow_unlocked.cfg`) | `NoLostRecord` violated in 4 steps |
| LogEtCkpt without the base-LSN (floor) check at checkpoint commit and open (`LogEtCkpt_nobase.cfg`, MaxLSN=3) | `CkptEqReplay` violated (1,244 states generated, depth 7; the count varies a little with worker interleaving) |
| LogEtCkpt with the orphan sweep ignoring the epoch rule (`LogEtCkpt_noepoch.cfg`, MaxLSN=3) | `CkptIntact` violated (1,751 states generated, depth 8) |
| LogEtSnapshot with creation checking and registering in two steps (`LogEtSnapshot_unlocked.cfg`) | `SnapshotReadable` violated (564 distinct states, depth 7) |
| LogEtSnapshot with creation ignoring the planned cut (`LogEtSnapshot_nocut.cfg`) | `SnapshotReadable` violated (175 distinct states, depth 6) |
| LogEtTTL with compaction dropping keys expired at a later (wall-clock) time than TS(cut) (`LogEtTTL_wallclock.cfg`) | `BaseEqReplay` violated in 2 steps |
| LogEtTTL with OCC validation ignoring expiries (`LogEtTTL_noexpcheck.cfg`) | `OCCSound` violated in 2 steps |
| LogEtChunks with garbage listing all old chunks, shared ones too (`LogEtChunks_nosub.cfg`) | `NoDangling` violated in 6 steps |
| LogEtChunks with the orphan sweep keeping only chunks the current base wrote (`LogEtChunks_byowner.cfg`) | `NoDangling` violated in 6 steps |
| LogEtLogGC with history GC ignoring index and snapshot refs (`LogEtLogGC_sweepref.cfg`) | `NoDangling` violated in 4 steps (a leftover chunk still named by KV is deleted) |
| LogEtLogGC with a pointer compact listing all covered segments as garbage (`LogEtLogGC_dropsegs.cfg`, the design-03 behaviour) | `NoDangling` violated in 6 steps (KV still points at a deleted segment) |
| LogEtShred with `Sweep` disabled | `KeyEventuallyGone` violated |
| LogEtShred with reads ignoring the shred marker | `ShreddedNeverRevealed` violated |

## Limits (honest)

- These are bounded models (2 writers, 3 txns, 5 segments, etc.). Bounded
  model checking finds small counterexamples well; it is not a proof for all sizes.
- The specs are written by hand to match the Go code. Nothing mechanically
  checks that the code matches them. The deterministic simulator and property
  tests (see README) check the code itself against reference models.
- Torn writes are over-approximated as any subset of a batch's frames (the
  real format keeps a prefix).
- Snapshot handles are modelled (LogEtSnapshot) for one log; lineage (a
  snapshot pinning parent logs) is not. Derived indexes (the index framework) and the HTTP layer are not modelled.
- LogEtCkpt at larger bounds was stopped, not completed: `LogEtCkpt_big.cfg`
  (MaxLSN=3, MaxMans=7) after 25 min, 58,595,423 distinct states, 29.9M
  still queued; `LogEtCkpt_e3.cfg` (MaxLSN=2, 3 epochs) after 13.2M distinct.
  Neither reported a violation, which says nothing about the unexplored rest.
  LogEtCkpt models runs as LSN intervals, not their bytes: piece CRCs,
  descriptor decoding and the hash-chain binding are checked by Go tests.
