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
