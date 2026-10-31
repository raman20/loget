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
failure logs a line and falls back to the older snapshot, the base, or full
replay. A checkpoint is a cache.

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

Mutants: without the base-LSN check at commit and open
(`LogEtCkpt_nobase.cfg`) `CkptEqReplay` fails after 1,359 states; without the
epoch rule in the orphan sweep (`LogEtCkpt_noepoch.cfg`) `CkptIntact` fails
after 1,683 states.

## The sharded base

A compaction base is written the same way: one run, built by `RunWriter` in
key order, stored as pieces `<base>.<p:06>.run`, with its description in the
manifest's `BaseMeta.Run`. Bases written before (one `.base` object) are still
read. The 1 GiB per-object limit no longer bounds the base.

## Not done

- Compaction is still whole-log: it rewrites every live key into the new base
  (option D). The base is sharded into pieces, but not incremental.
- Derived index sections are still whole snapshots in the descriptor.
- Pieces are copied into a run file on open, not mapped in place (option C).
