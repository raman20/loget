# Design 23: compaction memory, batched journal deletes, bounded OCC window (2026-11-01)

ROADMAP item 6, the three problems design 21 measured and left open.
Each part was fixed, tested, measured and committed on its own. All
numbers come from the reference box (8 vCPU, 16 GiB, ext4, journal store
layout, fsync on), single runs of `loget-bench ycsb -keys N -value 100
-workloads C,A -clients 8 -compact-between` (script `run.sh`). Raw files:
`bench/results/2026-11-01-item6/`. Peaks are sampled every 20 ms during
`Compact` (new bench field `compact` in the JSON, `peak rss/anon/heap` in
the log). Heap is Go heap objects, live and not yet collected garbage.

## Part 1: stream the compaction base

### Problem

`writeBase` built the base of a log on the heap, three times:

1. a fresh `index.KV` with `SetLatest` for every live key (10M entries);
2. `kv.Snapshot()`, the whole body as one slice;
3. `encodeBase`, the body copied again behind the header and CRC;

and the journal store copied the image a fourth time into one record
buffer in `PutIfAbsent`. At 10M × 100 B the body is ~0.6 GiB, so each of
these is large, and they were all live at once at the end.

Before (commit 94b2362 + the bench sampler, 00:31-00:38 IST):

| | compact after C | compact after A |
|---|---|---|
| 10M keys: time | 76.8 s | 137.9 s |
| 10M keys: peak RSS / anon / heap MiB | **6,416** / 5,801 / 4,170 | 5,254 / 5,141 / 3,578 |
| 1M keys: time | 6.0 s | 62.3 s |
| 1M keys: peak RSS / heap MiB | 968 / 576 | 1,356 / 803 |

### Design

- `index.BaseEncoder` produces the body entry by entry: exactly the bytes
  `SetLatest` per key + `Snapshot` produced (keys come from `Latest`, so
  they are already in order; the encoder rejects disorder and deletes).
  Whether the body is KV2 or KV3 is only known at the end (KV3 iff some
  value went to a base chunk), so entries are written in KV3 form and
  `index.StripChunks` converts them to KV2 in place if no version has a
  chunk. It reads ahead of where it writes, so it works on a mapping.
  The index package stays pure (no I/O).
- `baseStream` (basewrite.go) writes a placeholder header, then the
  entries in 1 MiB pieces, to an unlinked run file in the index directory
  (`Options.IndexDir`, the same `index.RunFiles` the KV index uses since
  design 21). At the end it maps the file, strips chunk numbers if
  needed, patches the header and CRC in place and hands the mapped bytes
  to `putExactlyOnce`. The file is released after the put. Pages are
  file-backed and reclaimable, not heap.
- With the index in memory (`WithIndexInMemory`, no mmap support) or if
  the file cannot be created, the same stream goes to one heap slice:
  one copy of the base instead of three. A create failure is counted in
  `loget_kv_index_file_errors_total` and logged. A write or map failure
  fails that compaction (the chunks already written are orphans of this
  epoch, exactly as when the base put itself fails).
- `Journal.PutIfAbsent` no longer copies objects over 1 MiB into a record
  buffer: it writes the header and key, then the caller's bytes, with the
  record CRC computed incrementally over both (CRC-32 is streaming, so the
  on-disk record is byte-identical). The record is still not atomic on
  disk; it was not before either (a torn put is a torn tail, design 09).

Not changed: the base is still one object, so the journal's 1 GiB object
limit (`jrnMaxData`) still bounds the base at about 15M keys of 100 B
with values in chunks. Multi-part bases belong with multi-part
snapshots (ROADMAP item 5).

### Tests

- `index.TestBaseEncoderMatchesSnapshot` (rapid): random keys, spaces,
  subjects, nil vs empty values, chunked or not, any inline limit; the
  streamed body equals `SetLatest` + `Snapshot` byte for byte and loads
  with the strict base loader.
- `TestBaseStreamMatchesOldImage` (rapid): heap and run-file paths, up to
  2,000 keys over several 1 MiB flushes, KV2 and KV3, equal to the old
  `encodeBase(lsn, kv.Snapshot())` image (kept in the test as the
  reference).
- `TestBaseStreamFileFaults`: a failed create falls back to the heap and
  is counted; failed writes or maps fail the base and leave no file.
- Existing compaction, crash (kill -9) and golden-bytes tests unchanged
  and green: the base format did not change.

### Result

After (00:40-00:46 IST, same box and script):

| | before | after |
|---|---|---|
| 10M, compact after C: peak RSS / anon / heap MiB | 6,416 / 5,801 / 4,170 | **3,216** / 2,599 / 2,151 |
| 10M, compact after C: time | 76.8 s | 36.4 s |
| 10M, compact after A: peak RSS / heap MiB | 5,254 / 3,578 | 3,059 / 1,980 |
| 10M, compact after A: time | 137.9 s | 83.6 s |
| 1M, compact after C: peak RSS / heap MiB, time | 968 / 576, 6.0 s | 695 / 357, 3.4 s |
| 1M, compact after A: peak RSS / heap MiB, time | 1,356 / 803, 62.3 s | 972 / 537, 57.3 s |

Workload throughput is within run-to-run noise (10M C 1.88M → 1.95M,
A 116k → 123k ops/s; 1M C 3.22M → 2.91M, A 144k → 136k).

What is left in the 10M peak: a heap profile at the peak (scratch
`LOGET_BENCH_PEAK_HEAPPROF`) shows the compaction itself holding ~75 MiB
(the 64 MiB chunk buffer); the live heap is the OCC recent-commit window
of the bulk load (~555 MiB of `writer.process` and the key strings it
keeps, part 3) and the value cache. The rest of the heap peak is garbage
from per-key work (key strings, value reads) that the GC pacer lets grow
to twice the live heap. The 1M compaction after A is still dominated by
~490k garbage deletes, one journal delete each (part 2).

## Part 2: batched journal deletes

### Problem

Compaction deletes every superseded segment, base, snapshot and
checkpoint. On the journal store each `Delete` appended a delete record
and fsynced, then marked the put dead, punched its data and fsynced
again: two `fdatasync`s per object. After workload A at 1M keys that is
~490k objects and 57.3 s of a 57.3 s compaction (part 1 result).

Design 22's sweep work rejected batching deletes as several delete
records with one sync: recovery would see a torn run of delete records
and, when a damaged record is followed by a valid one, has to call it
corruption (a torn tail can only be at the end).

### Design

One record per batch, so a batch is torn or not as a whole:

- New record kind 4, *batch delete*: key `\0dels`, data a list of
  (u16 length, key). One CRC covers the header, key and list. At most
  1 MiB of keys per record (`jrnMaxBatch`); a longer call writes several
  records, each atomic.
- `Journal.DeleteBatch(keys)`: take the write lock, drop missing and
  duplicate keys, append the record, fsync (the batch is now durable),
  erase every put (dead header + punch, as for `Delete`), fsync once.
  Two syncs per batch instead of two per object.
- Recovery treats a batch like a run of delete records that cannot be
  torn apart:
  - a damaged batch record with nothing valid after it is a torn tail:
    zeroed, none of its deletes apply, the objects are still there
    (all-or-nothing);
  - a valid batch whose puts are not all dead yet (crash during erase)
    is an interrupted delete: open erases the rest, same as for one
    delete record;
  - a damaged put followed by a batch naming its key is that same
    interrupted delete;
  - a damaged batch followed by valid records, or a valid batch whose
    key list does not parse, is `ErrCorrupt` (as before for any record).
- The record needs a new journal version: `LOGETJ2`. New files are
  version 2. A version 1 file is still read and stays version 1 (its
  batches fall back to one delete each), so the build that wrote it can
  still open it after a newer build did. Older builds refuse a version 2
  file with `ErrVersion` (docs/compatibility.md).
- `store.DeleteBatch(ctx, st, keys)` uses an optional `BatchDeleter`
  interface and otherwise loops over `Delete` (ignoring `ErrNotFound`).
  `FS` removes all files, then fsyncs each directory once; `Mem` and
  `Faulty` implement it (Faulty injects a failure after a random prefix).
  S3 keeps the loop (no multi-object delete in the minimal client yet).
- The DB's backup-hold wrapper forwards batches: while a backup holds
  deletes, it queues them and releases them as one batch.
- Compaction's garbage sweep deletes in batches of 1024 and checks for
  `Close` between batches; on a store without batches the per-object
  fallback also stops within a batch once the DB is closed or crashed
  (`TestSweepStopsAfterCrash` caught the first version deleting the rest
  of the batch after `Crash`). A batch holds the journal's write lock, so
  commits wait for it: 0.8 ms for 256 keys, 3.2 ms for 1024, 10.5 ms for
  4096 (300 B objects). 1024 keeps that near one commit's fsync. The
  orphan sweep and manifest deletes stay per object (few objects).

### Tests

- `store/journal_batch_test.go`:
  - sync count (one batch record per 1 MiB of keys), secrets erased,
    reopen;
  - a version 1 file never gets a batch record;
  - every subset of the batch record's 512-byte sectors torn: open
    succeeds, either every key or none is deleted, appends work;
  - a durable batch with only some puts erased and a torn dead header:
    open finishes the deletes and leaves no secret bytes;
  - single delete records torn out of order, a damaged batch followed by
    a valid record, and a malformed key list are `ErrCorrupt`.
- `store/powerloss_test.go`: the power-loss workload also issues
  batches of up to 40 keys; every crash image (≈ 800-870 per seed, 3
  seeds) must show an in-flight batch all-or-nothing. Applying a batch as
  per-key deletes fails this check ("torn batch: 1 of 2 keys still
  there").
- Store conformance (`Mem`, `FS`, `Journal`, S3 on MinIO): missing and
  duplicate keys, other keys untouched, empty batch; `Faulty`'s injected
  failure deletes a prefix and returns the error.
- `compact_batch_test.go`: compaction garbage reaches the store's
  `DeleteBatch` through the DB's wrapper (no garbage deleted one by one;
  removing the wrapper's `DeleteBatch` fails it), and deletes held by a
  backup are released as a batch.
- The kill -9 crash tests on the journal compact while being killed.

Mutation: disabling the batch branch in recovery fails the torn and
interrupted tests. Treating a damaged batch like a damaged put survives
them; the two paths give the same result (equivalent mutant).

### Result

After (01:08-01:12 IST, same script, `fix2-batched-deletes/`):

| | part 1 | part 2 |
|---|---|---|
| 1M, compact after A: time (deleted) | 57.3 s (489,308) | **6.8 s** (472,875) |
| 1M, compact after A: peak RSS / heap MiB | 972 / 537 | 1,036 / 651 |
| 10M, compact after A: time (deleted) | 83.6 s (435,009) | **40.6 s** (433,271) |
| 10M, compact after A: peak RSS / heap MiB | 3,059 / 1,980 | 2,953 / 1,882 |
| 10M / 1M, compact after C: time | 36.4 / 3.4 s | 34.9 / 3.4 s |
| `BenchmarkCompactSweepPause` (~2,500 deletes): run | 313 ms | 20-23 ms |
| same: pause / max commit latency | 10 / 11 ms | 10-13 / 10-13 ms |

The 1M peak heap and RSS are higher (+114 / +64 MiB); not investigated,
single runs.
Throughput is within noise (10M C 1.92M, A 123k; 1M C 2.95M, A 136k
ops/s). One part-2 sweep benchmark run with batches of 4096 showed a
56 ms worst commit; four later runs did not, and the batch size was
lowered to 1024 anyway. The 10M compaction after C (10k deletes)
takes 34.9 s, so most of the 40.6 s after A is the base write and the
log reads, not deletes.
