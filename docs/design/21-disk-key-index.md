# Design 21: the key index on disk (2026-10-31)

ROADMAP item 1. Status: implemented (ccc4147 index layer, af29ecf
wiring). Numbers marked "before" are measured at d013307 on the
reference box (8 vCPU, 16 GiB, ext4), "after" with the bench binary at 9c5f706 on
the same box the same evening (22:59-23:31 IST); raw files in
`bench/results/2026-10-31-disk-key-index/{before,after}/`.

## Problem

Every key and version is indexed in Go heap memory. The index is already
compact (immutable front-coded runs with a hash table, design notes in
`docs/components/kv.md`), but it is still heap, and the Go collector lets
the heap grow to about twice the live set:

| run (same box, same hour) | LogEt peak RSS | LogEt end RSS | Pebble peak | Pebble end |
|---|---|---|---|---|
| YCSB 1M × 1 KiB, A,B,C × 1/8/64 clients | 958 MiB | 774 MiB | 743 MiB | 80 MiB |
| same, LogEt value cache 8 MiB (Pebble's block cache size) | 478 MiB | 289 MiB | | |
| YCSB 10M × 100 B, C,A × 8 clients | 3,546 MiB | 1,133 MiB | 798 MiB | 51 MiB |

Heap profiles (10M keys): after the load 616 MiB of live heap is run
bytes, hash slots and the output buffers of merges in progress
(`builder.grow`/`finish`); at the end of the run 320 MiB (32 B/key). A
further 316 MiB after the load is OCC's recent-commit window (one entry
per written key of each commit still visible to an open transaction),
released by the next workload; it is not the key index and is out of
scope here (noted in ROADMAP). With the value cache at Pebble's size,
what remains at 1M keys is the key index plus GC headroom.

Goal: the key index should cost a bounded, small amount of anonymous
memory per key, independent of the number of keys, without changing the
log, the snapshot/base formats, Digest, or the root API beyond one option.

## Options considered

A. **Status quo, tuned.** Smaller ver structs, GOGC tuning. Already done
   in design 07 and component review B; the index is ~30 B/key and the
   collector's multiple is the rest. No structural fix.

B. **Runs in memory-mapped local files (chosen).** The run layout is
   already an immutable, position-independent byte array plus a hash
   table of `uint64` slots. Write both to a file and map it read-only:
   the bytes leave the Go heap (no GC scanning, no GC multiple) and live
   in the page cache, which the kernel can evict and refill from the file
   under memory pressure. RAM that must stay resident per key: the block
   offsets (8 B per 16 keys) and nothing else. Reads keep their current
   path (one slot probe + one block scan) at memory speed when pages are
   cached. Cost: page faults on cold pages, file I/O on freeze/merge.

C. **pread + a bounded block cache (Pebble/RocksDB style).** Strictly
   bounded resident memory even if the kernel is not under pressure, but
   every miss is a syscall and a copy, the cache needs its own eviction
   and concurrency, and the hash-slot table would have to become a
   sparse index + Bloom filter (the slots are random-access by design).
   More code, slower hits, same order of saving. Kept as a follow-up if
   page-cache residency proves to be a problem (e.g. on hosts where the
   page cache counts against a tight cgroup limit and is not reclaimed
   early enough).

D. **Persist runs as objects next to bases (an LSM over the object
   store).** Would also cut open time (no rebuild from the snapshot) but
   makes the runs a second durable truth: they need manifest references,
   GC, validation against the log, a format version, and a TLA+
   extension of the compaction model. That is the right shape for
   ROADMAP item 5 (multi-part checkpoint snapshots) and is deferred to it.
   Runs here stay a cache derived from the log, rebuilt at open.

E. **Embed Pebble/Badger as the key index.** A large dependency doing
   what the run code already does, with its own WAL we would not use.
   Rejected.

## Design (option B)

- **Where.** `Options.IndexDir` (`WithIndexDir`, CLI `--index-dir`, env
  `LOGET_INDEX_DIR`). Default: `os.TempDir()` on Unix; if that directory
  is unusable Open logs it and keeps the index on the heap, while an
  explicit `IndexDir` that is unusable fails Open. `WithIndexInMemory`
  keeps the old heap behaviour; non-Unix builds always use the heap. The
  directory should be on disk, not tmpfs (on tmpfs the pages are shmem
  and nothing is saved; documented in operations.md).
- **Code split.** `index` stays a pure fold (its purity test allows no
  I/O imports; `runtime` is allowed for `KeepAlive` only, and the test
  checks that). It sees run files through two small interfaces,
  `index.RunFiles` (`Create`) and `index.RunFile` (`Write`, `Map`, `Seal`,
  `Release`), passed in `index.KVOptions{Files, MinFileRun, OnFileError}`.
  `internal/runfile` implements them with `os.CreateTemp` + unlink, and
  `mmap`/`mprotect`/`madvise(MADV_RANDOM)` from `golang.org/x/sys/unix`
  (already a dependency). The root package adapts one `runfile.Dir` per
  DB. `Options.IndexFiles` lets tests and the simulator supply their own
  implementation (with fault injection) and put every run in a file.
- **Files.** One file per run, created with a random name, unlinked
  immediately after creation (the open descriptor and later the mapping
  keep it alive). Nothing is left behind by a crash or kill -9, and no
  other process can open it. Layout: `[run bytes][pad to 8][slots]`; the
  block-offset array (`bofs`) and the subject counts stay on the heap.
- **Small runs stay on the heap.** Runs estimated below `minFileRun`
  (1 MiB, `KVOptions.MinFileRun`) are built in memory as today: freezes
  of a small hot set, temporary runs used by `each`. Tests force every run
  into a file.
- **Write path.** Unchanged above the run layer: the hot map takes
  writes, freezes at 65,536 versions, adjacent runs merge a few keys per
  write. The builder appends entries to a 1 MiB buffer flushed to the
  file (heap use per build: O(buffer) instead of O(run) plus 12 B/key for
  hashes and offsets). `finish` sizes the file for the slots, maps it,
  fills the slots in a second pass over the mapped entries, and
  `mprotect`s it read-only.
- **Read path.** Unchanged: `Get` probes the hot set, then each run's
  slot and block, newest first. `Version.Val` returned by public methods
  (`Get`, `Scan`, `Latest`, `Snapshot` is bytes) is now a copy, never an
  alias of a run, because a run can be unmapped once it is replaced (the
  engine already copies before use; the copy is ≤ InlineMax = 64 B by
  default). Internal walks (encode, merges, rebuilds) alias under the
  owner's lock.
- **Retirement.** A run is unmapped when a merge, `DropSubject` rewrite
  or `SetLatest` replaces it, and a merge's output file is closed when the
  merge is abandoned. These happen only inside `Apply`/`DropSubject`/
  `SetLatest`, which the engine calls under the view's write lock, while
  every reader holds the read lock, so no reader can still be inside a
  replaced run. A KV that is dropped whole (a branch reload, a base image
  replaced by a snapshot) releases its maps through a finalizer on each
  mapping; methods keep the KV alive across their use of run memory
  (`runtime.KeepAlive`).
- **Failures.** Any error creating, writing, sizing or mapping a run
  file (disk full, EIO, directory gone) switches that KV to heap runs
  for good and rebuilds the run in memory from its inputs, which are
  still intact at that point (hot set, merge inputs, snapshot bytes).
  A merge whose output file fails is abandoned and planned again on the
  heap; a snapshot load that fails restarts on the heap from the
  snapshot bytes. The error is counted (`Stats.KVIndexFileErrors`,
  metric `loget_kv_index_file_errors_total`) and logged; later failures
  cannot happen for that KV because it no longer uses files. Correctness never
  depends on the files; memory use does.
- **Open / crash recovery.** No change: runs are never read back across
  processes. Open restores the snapshot or base and replays the tail as
  today; `loadKV` writes the restored runs straight to files. A crash
  leaves no index files (they were unlinked at creation).
- **Snapshots, bases, Digest.** Unchanged bytes: they are produced by
  walking the runs, wherever the runs live.
- **Shred.** Unchanged semantics: runs hold ciphertext for subject data,
  `DropSubject` rewrites affected runs and unmaps the old ones. The old
  file's disk blocks are freed, not wiped; they only ever held
  ciphertext whose key is destroyed by the shred.
- **S3.** Index files are always local; the object store is not touched.
  A database on S3 with no writable local directory uses
  `WithIndexInMemory`.

## Why no TLA+ model

The files carry no durable state and no protocol: they are written and
read by one process, are never referenced from the manifest, and vanish
with the process. There is no atomicity or ordering property for a model
to check. The properties that matter are (1) fold equivalence: a KV with
file runs answers every query exactly as the reference map does, and (2)
memory safety: no read of an unmapped run. (1) is checked by property
tests against the reference map with files forced on, fuzzing and the
simulator; (2) by the lock discipline above, the copy-out rule, and a
stress test that reads concurrently with merges and shreds under the
race detector. If option D is built later, it gets a model.

## Tests

All green at af29ecf and again at 9c5f706 in the full local gate (gofmt, vet, staticcheck, API diff, S3 against MinIO, `-race`, simulator, kill -9, fuzz smoke):

- `index/kvfile_test.go`: the refinement properties against the map model
  (`TestKVRunsRefineMap`, `TestKVSetLatestRefineMap`, with DropSubject,
  Scan, snapshot round trips) rerun with every run in a mapped file, and
  a check that runs really are mapped; `TestKVFilesFaults` makes each file
  operation fail with probability 0, 5 %, 30 % or 100 % and still checks
  the model; `TestKVFilesLoadFault` fails each operation during a
  snapshot load (one error reported, no mapped run, identical snapshot);
  `TestKVFilesValuesOwned` (values returned never alias a run);
  `TestKVFilesConcurrent` (4 readers against 20k writes, merges and
  shreds, under `-race`). Every test also checks the directory is empty.
- `FuzzLoadKVFiles`: FuzzLoadKV with every run in a file (30 s smoke:
  ~308k execs, no failure).
- `internal/runfile`: round trip, unlinking, sealing, fault hooks.
- Root: `TestIndexFiles` (write, compact, snapshot, reopen; index ==
  fold(log)), `TestIndexFileFaults` (each operation failing: reads stay
  right, errors counted), `TestIndexDirOption`.
- `TestCrashKill9IndexFiles`: 25 kill -9 cycles of a committing,
  compacting child with every run in a file; recovery also maps its runs;
  the index directory is empty after each kill.
- Simulator: every fourth seed keeps every run in a file with 0, 2 % or
  20 % of file operations failing; it checks its usual invariants and an
  empty directory at the end. 2,000 seeds × 60 steps: 3,989 injected
  run-file faults, no failure. `TestDeterminism` still holds.

## Benchmarks

Single runs, one engine at a time, same box as "before". Raw output and
a README: `bench/results/2026-10-31-disk-key-index/after/`.

**A bug found on the way.** The first after-run (af29ecf) showed YCSB-C
20 % slower and YCSB-A at 648 ops/s after a compaction. Neither was the
run files: the engine's store wrapper hid `GetRange`, so every value
cache miss read a whole segment or 64 MiB base chunk (fixed in 9c5f706,
`TestValueReadsAreRanged`; docs/components/kv.md). "before" (d013307)
has that bug; both "after" columns have the fix. So before → after mixes
the two changes, and **files vs heap (the same binary with
`WithIndexInMemory`) is the measure of design 21 alone.**

Memory (peak RSS from the kernel, end RSS split into anon / file-backed):

| run | before | after, heap | after, files | Pebble |
|---|---|---|---|---|
| 1M × 1 KiB, A,B,C × 1/8/64: peak | 958 MiB | 712 MiB | 640 MiB | 761 MiB |
| same: end (anon / file) | 774 | 662 (644 / 17) | 582 (509 / 72) | 82 |
| same, 8 MiB value cache, A,C: peak / end | 478 / 289 | 239 / 177 | 196 / 136 | |
| 10M × 100 B, C,A × 8: peak | 3,546 MiB | 2,632 MiB | 1,916 MiB | 740 MiB |
| same: end (anon / file) | 1,133 | 1,329 (1,311 / 17) | 920 (601 / 319) | 53 |
| 10M: Go live heap after load | 1,169 MiB | 1,041 MiB | 561 MiB | 113 MiB |

With files, the index's anon cost at 10M keys
fell from ~620 MiB to ~15 MiB (heap profile); the 319 MiB of index pages
are file-backed and reclaimable. What remains of LogEt's peak is OCC's
recent-commit window during the load (~310 MiB plus the keys it holds),
the 256 MiB value cache, and Go GC headroom.

Throughput (ops/s, 5 s per 1M cell, 10 s per 10M cell):

| workload | before | after, heap | after, files | Pebble (after) |
|---|---|---|---|---|
| 1M A c=1 / 8 / 64 | 8.8k / 70k / 139k | 41k / 102k / 211k | 38k / 99k / 200k | 19k / 89k / 97k |
| 1M B c=1 / 8 / 64 | 27k / 212k / 245k | 160k / 483k / 1.06M | 151k / 482k / 943k | 51k / 133k / 180k |
| 1M C c=1 / 8 / 64 | 52k / 327k / 306k | 344k / 1.60M / 1.92M | 349k / 1.62M / 2.02M | 82k / 159k / 228k |
| 10M C c=8 | 448k | 1.82M | 1.79M / 1.80M | 192k |
| 10M A c=8 | 116k | 100k | 88k / 87k | 112k |
| load 1M × 1 KiB / 10M × 100 B | 6.0 / 35.0 s | 5.8 / 38.7 s | 6.6 / 40.0 s | 4.1 / 6.1 s |

Read A/B (1M × 1 KiB, C × 8, two interleaved pairs): files 1.70M and
1.69M, heap 1.87M and 1.72M ops/s; CPU profiles put `KV.Get` at 11.7 %
of samples in both. Files cost up to ~10 % on reads in these runs, mostly
within this box's spread; writes (A) 3-12 % and load 3-14 %, from writing
and mapping run files during merges.

Open (10M × 100 B directory, 200k uniform reads after open):

| open from | mode | open | peak RSS | RSS after reads (anon / file) | reads |
|---|---|---|---|---|---|
| log only (no snapshot, full replay) | files | 32.7 / 33.6 s | 719 MiB | 455 (136 / 319) | 165-180k/s |
| | heap | 32.3 / 32.3 s | 1,167 MiB | 559 (542 / 16) | 177-222k/s |
| | Pebble | 0.13 / 0.01 s | 64-66 MiB | 59-64 | 55-61k/s |
| compaction base | files | 6.6 / 5.2 s | 2,208 MiB | 1,146-1,275 (820-948 / 326) | 227-317k/s |
| | heap | 6.2 / 4.8 s | 2,183 MiB | 2,183 (2,166 / 17) | 217-278k/s |

Open time is unchanged by design 21 (the index is still rebuilt at open;
persisting runs is option D / ROADMAP item 5) and is the largest gap to
Pebble. Opening from a base peaks at ~2.2 GiB in both modes because the
base is one object decoded in memory (item 5).

Compaction (pre-existing, not changed here; ROADMAP item 6). 1M × 100 B,
files vs heap: compaction after C 6.4 vs 5.9 s, after A 60.4 vs 59.8 s,
A after compaction 138k vs 140k ops/s. The 60 s is one `fdatasync` per
deleted object on the journal (480k segments). At 10M keys, compaction
after C took 97.8 s (324.6 s before the GetRange fix) and RSS reached
6.3 GiB (go-resident 5.6 GiB, live heap 82 MiB): `writeBase` holds the
new base image and chunk buffers in memory.

## Limits that remain

- The page cache holds the runs; RSS as `ps` reports it includes mapped
  pages that were touched (file-backed, reclaimable). We report anon and
  file separately.
- Snapshot write and load still hold the whole snapshot in memory
  (single object); multi-part snapshots are ROADMAP item 5.
- MVCC history is still indexed until compaction, now on disk.
- OCC's recent-commit window and the segment indexes are separate RAM
  users and are not changed here.
- Open time is unchanged: ~33 s for 10M keys without a snapshot (Pebble:
  under 0.2 s). Persisting runs (option D) belongs to ROADMAP item 5.
- Run files cost up to ~10 % read and 3-14 % write/load throughput in
  the runs above.
