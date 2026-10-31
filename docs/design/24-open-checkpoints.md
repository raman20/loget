# Design 24: open without full replay — incremental checkpoints and a sharded base (2026-11-01)

ROADMAP item 5. Two problems, one mechanism:

1. **Open replays.** Opening a log folds every entry since its base (or since
   the last snapshot) into a fresh record store. With 10M keys and no
   snapshot that took 28.4 s; from a compaction base 5.5 s with a 2.2 GiB peak,
   because the base is one object read into memory, decoded and rebuilt into
   runs. The old snapshot (`SnapshotEvery`) did not help: it encoded the whole
   record store into one object every N commits (10M keys: load 61.8 s instead
   of 38.5 s, 4.1 GiB peak) and reopening from it took longer than from the
   base (7.9 s, 3.2 GiB) because it is decoded and rebuilt the same way.
2. **One base object per log.** A journal object holds at most 1 GiB, so a
   base of more than ~15M keys of 100 B could not be written at all.

## Before (reference box, 8 vCPU / 16 GiB, ext4, journal layout, fsync on)

`loget-bench ycsb -keys N -value 100 -workloads C -keep`, then `loget-bench
open` on the kept directory (script `open.sh`), page cache warm (dropping it
needs root, which the box does not give), commit f63ec57 plus the bench's new
`-snapshot-every` flag. Single runs.

| case | open | peak RSS |
|---|---:|---:|
| 1M, replay (no base, no snapshot) | 1.92 s | 93 MiB |
| 1M, from a compaction base | 0.47 s | 349 MiB |
| 10M, replay | 28.4 s | 473 MiB |
| 10M, from a compaction base | 5.5 s | 2,209 MiB |
| 10M, from a snapshot (`-snapshot-every 2000`) | 7.9 s | 3,185 MiB |

(1M with `-snapshot-every 2000` wrote no snapshot: the load is 1,000 commits.)

## Options considered

**A. Keep the one-object snapshot, write it more often.** Cheapest; it is
what existed. Every snapshot re-encodes and re-uploads every key, so the cost
grows with the database, not with what changed, and open still decodes and
rebuilds every run. Rejected: it is the slow case in the table.

**B. Persist the record store's runs as they are (chosen).** The record store
is already an LSM of immutable, sorted, front-coded runs (design 21). A
checkpoint stores each run's entry bytes in pieces of at most 64 MiB, plus a
small descriptor; runs are immutable, so the next checkpoint *names* the runs
it already stored instead of uploading them again. Only the hot set and runs
made by merges since the last checkpoint are new. Open copies each run's
bytes back into a run file and rebuilds only its hash slots (a linear pass,
no sorting, no re-encoding); it checks every piece's CRC-32C and walks every
entry before use. There is no size limit beyond the run format's own.

**C. Map the store's objects directly (zero copy).** Only possible when the
store is a local file system laid out like a run file; on S3 and the journal
an object is not a file. The open path would differ by store. Rejected for
now; B copies pieces into a run file once, which is sequential I/O.

**D. A per-key-range sharded base and incremental compaction (rewrite only
changed shards).** This would also make compaction cost proportional to the
change. It needs a range partitioning of the record store and a manifest
that maps ranges to objects, and GC of a shard needs to know no other shard
version refers to its value chunks. Larger change; see "Not done".

**E. Use an external engine (Pebble) as the record store.** Pebble opens in
under 0.2 s because its SSTs are its storage. It would replace the index
framework's own record store and its determinism (simulation) guarantees and
add a large dependency. Rejected.

## The checkpoint (B)

### Objects

    ckpt/<log>/<lsn:020>-e<epoch>-<nonce>.ckpt       descriptor
    ckpt/<log>/<lsn:020>-e<epoch>-<nonce>.r<i>.<p:06>.run   piece p of run i

A run keeps the key it was first stored under (`<…>.r<i>`) for as long as the
record store keeps the run, so a later checkpoint's descriptor may name runs
first stored by earlier checkpoints.

The descriptor (`LGCKPT1\n`, formats.md) holds the LSN, the base LSN it was
cut from, the hash-chain head at the LSN, the log id, the record store's
inline limit, the runs newest first (key, length, keys, versions, one CRC-32C
per piece) and the derived indexes' snapshot sections (as before; they are
not incremental). It ends with a CRC-32C.

The manifest's `LogMeta.Ckpt` records LSN, base LSN, descriptor key and every
run key with its piece count, so GC never reads a descriptor.

### Protocol (spec/LogEtCkpt.tla)

1. Cut the record store under the view's read lock: the hot set is frozen into
   a new heap run; every run in a file gets a reference (`runfile.File.Ref`),
   so merges that replace and release runs meanwhile do not unmap them.
2. Upload the pieces of every run the current manifest's checkpoint does not
   already name, then the descriptor (all create-only, all new names).
3. Commit a manifest with the new `Ckpt` if, in the latest manifest, the log
   still exists, its base LSN is still the one cut from, and its checkpoint is
   older. The old descriptor and the old runs the new checkpoint does not name
   become garbage (grace period as for compaction). Otherwise (compaction ran,
   or a newer checkpoint) the new objects were never referenced; they are left
   to the orphan sweep.
4. Compaction and GC drop a log's checkpoint (all its objects become garbage):
   its versions point into segments the base replaces.
5. The orphan sweep deletes `ckpt/` objects of an *older* epoch that the
   manifest neither references nor lists as garbage, like bases.

Open uses the checkpoint when the manifest names one whose descriptor binds
to the same log, LSN, base LSN and hash-chain head, and whose every piece
passes its checks; it then replays only the segments after the LSN. Any
failure logs a line, counts in `CheckpointFallbacks`
(`loget_checkpoint_fallbacks_total`), and falls back to an older LGSNAP2
snapshot, the base, or full replay. A checkpoint is a cache.

Each run's piece size is recorded with it (`RunMeta.Piece`, 64 MiB by
default), so a reader does not depend on the writer's build constant; the
kill -9 test found this when parent and child used different piece sizes.

### When checkpoints are written

- In the background once `CheckpointBytes` of segment bytes were written
  since the last one (default 512 MiB; `WithCheckpointBytes(-1)` turns
  checkpoints off). The writer cuts under the view lock and uploads on its
  own goroutine; at most one runs at a time; a crash or close cancels it
  (its objects are then orphans for the sweep).
- Synchronously every `SnapshotEvery` commits when that is set (the
  simulator uses it so runs stay deterministic; background checkpoints are
  then off).
- At a clean close (not after a crash or a writer failure such as being
  fenced, and not for a retired log). LGSNAP2 snapshots are no longer written; existing ones are read
  until a checkpoint replaces them.

### Model check

`spec/LogEtCkpt.tla` models runs as LSN intervals: freeze, merge, uploads,
descriptor, checkpoint commit, compaction (which drops the checkpoint), sweeps
with grace, orphan sweeps by epoch, crashes, opens from the checkpoint and a
reader of an older manifest. Invariants: `CkptEqReplay` (what open folds from
the checkpoint plus the replayed suffix is exactly the log since the base,
each LSN once), `CkptIntact`, `ViewOK`, `NoLoss`, `EpochMonotone`,
`GarbageFinal`, `ReaderOK`.

TLC found a bug in the first version of the model: descriptors were named by
log and epoch only, so a compaction and a new checkpoint at the same LSN in
the same epoch reused a name that was already garbage (`GarbageFinal`). The
name now includes the base LSN (and a nonce in the code).

Result (2026-11-01 03:11–03:14 IST, TLC v1.7.4, 2 workers): `LogEtCkpt.cfg`
(2 processes, MaxLSN=2, MaxMans=5, ≤2 epochs, process symmetry) explored
33,127,486 states, 6,621,739 distinct, depth 36, no error, in 3 min 14 s.
Larger bounds did not finish: `LogEtCkpt_big.cfg` (MaxLSN=3, MaxMans=7) was
stopped after 25 min at 58.6M distinct states with 29.9M queued, and
`LogEtCkpt_e3.cfg` (3 epochs) at 13.2M distinct; neither had reported a
violation, which proves nothing about the rest of their state space.

Mutants: without the base-LSN check at commit and open
(`LogEtCkpt_nobase.cfg`) `CkptEqReplay` fails after 1,244 states generated;
without the epoch rule in the orphan sweep (`LogEtCkpt_noepoch.cfg`)
`CkptIntact` fails after 1,751 (counts vary a little between runs with the
worker interleaving).

The model has runs as LSN intervals, not bytes. Piece CRCs, descriptor
decoding, the hash-chain binding and fallbacks are covered by Go tests
instead: `TestCkptBinding`, `FuzzDecodeCkpt`, `FuzzLoadKVRuns`, and
`TestCkptOpenEqualsReplay`, a rapid property test that runs random
histories of puts, deletes, crashes, clean closes, compactions, shreds and
synchronous checkpoints with piece sizes of 64 B, 512 B and 64 MiB, and
after every reopen compares every index digest and `Get` with a replay-open
of a copy whose checkpoints and snapshots were removed, and requires
`Verify` to pass without warnings (3,000 checks passed locally, 100 in the
gate). `TestCrashKill9Ckpt` kills a child that checkpoints every 4 KiB of
segments (with compactions) 25 times and checks every acknowledged write and prefix consistency.

## The sharded base

A compaction base is written the same way: one run, built by `RunWriter` in
key order, stored as pieces `<base>.<p:06>.run`, with its description in the
manifest's `BaseMeta.Run`, and the manifest becomes format 3 (builds before
this refuse it with `ErrVersion`). Bases written before (one `.base` object)
are still read; golden databases `d557e72` and `d557e72-compacted` keep that
covered. The 1 GiB per-object limit no longer bounds the base; a test writes
a base of ≥ 5 pieces (`TestShardedBase`) and checks that a damaged piece
fails with `ErrCorrupt`. A 15M+-key base was not run on this box.

## After

Same box, method and workload as "Before", commit 677521b plus the bench's
new `-checkpoint-bytes` flag, 2026-11-01 03:23–03:29 IST. Single runs.
"replay" disables checkpoints (`-checkpoint-bytes -1`); "checkpoint" is the
default (background checkpoints during the load and one at close).

| case | before open | after open | before peak RSS | after peak RSS |
|---|---:|---:|---:|---:|
| 1M, replay | 1.92 s | 1.79 s | 93 MiB | 95 MiB |
| 1M, checkpoint (default) | — | 0.32 s | — | 90 MiB |
| 1M, compaction base (no checkpoint) | 0.47 s | 0.29 s | 349 MiB | 160 MiB |
| 1M, base + close checkpoint | — | 0.28 s | — | 161 MiB |
| 10M, replay | 28.4 s | 26.0 s | 473 MiB | 487 MiB |
| 10M, checkpoint (default) | 7.9 s (snapshot) | 3.43 s | 3,185 MiB (snapshot) | 580 MiB |
| 10M, compaction base (no checkpoint) | 5.5 s | 3.32 s | 2,209 MiB | 385 MiB |
| 10M, base + close checkpoint | — | 3.77 s | — | 387 MiB |

Throughput: the 10M load with background checkpoints took 37.7 s (replay
case 36.1 s, before 38.5–39.2 s); YCSB C c=8 1.75M ops/s with checkpoints
vs 1.80M without (before 1.83M), within this box's run-to-run noise. 1M C
3.16–3.25M ops/s in both. Compaction of 10M keys: 34.1 s → 31.9 s, peak
RSS 2,521 → 1,946 MiB (the base is no longer one encoded object). Data
directory at 10M: 1.5 GiB without checkpoints, 1.6 GiB with them.

The single 8.3 s p99 seen once in item 6 (1M keys, workload A after
compaction) did not reproduce: three runs of the same script with this
build (03:31–03:34 IST, `p99-A/` in the results) gave A p99 37, 45 and
56 ms, p99.9 58–80 ms, max 123–135k ops/s. Its cause is still unknown.

A replay-open is unchanged, as it should be: the gain comes from not
replaying. Open from a checkpoint is still O(index size): every piece is
read, checked and copied into a run file, and hash slots are rebuilt.

## Costs and limits

- The first checkpoint after a compaction stores the base's run again
  (the checkpoint does not yet name base pieces), i.e. one more copy of the
  live index until the next compaction.
- Background checkpoints upload while commits continue; on a disk-bound
  box that competes with segment writes. The measured load time did not
  change, but S3 was not benchmarked.

## Not done

- Compaction is still whole-log: it rewrites every live key into the new base
  (option D). The base is sharded into pieces, but not incremental.
- A checkpoint after compaction re-uploads the base run instead of naming
  its pieces.
- Derived index sections are still whole snapshots in the descriptor.
- Pieces are copied into a run file on open, not mapped in place (option C).
