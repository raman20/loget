# Design 21: the key index on disk (2026-10-31)

ROADMAP item 1. Status: design; numbers below marked "before" are measured
at d013307 on the reference box (8 vCPU, 16 GiB, ext4), raw files in
`bench/results/2026-10-31-disk-key-index/before/`.

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
  `LOGET_INDEX_DIR`). Default: `os.TempDir()` on Unix. `WithIndexInMemory`
  keeps the old heap behaviour; non-Unix builds always use the heap. The
  directory should be on disk, not tmpfs (on tmpfs the pages are shmem
  and nothing is saved; documented in operations.md).
- **Files.** One file per run, created with a random name, unlinked
  immediately after creation (the open descriptor and later the mapping
  keep it alive). Nothing is left behind by a crash or kill -9, and no
  other process can open it. Layout: `[run bytes][pad to 8][slots]`; the
  block-offset array (`bofs`) and the subject counts stay on the heap.
- **Small runs stay on the heap.** Runs estimated below `minFileRun`
  (1 MiB) are built in memory as today: freezes of a small hot set,
  temporary runs used by `each`, tests. Tests force files with the var.
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
  The error is counted (`Stats.KVIndexFileErrors`, metric
  `loget_kv_index_file_errors_total`) and logged once. Correctness never
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

- Property tests vs the reference map with `minFileRun = 0` (every run a
  file), including DropSubject, SetLatest, Scan, snapshot round trips and
  Digest equality with the heap KV.
- Fault injection: every file operation can fail (hook); the KV must
  fall back and still match the reference.
- FuzzLoadKV with files on.
- Root tests and the simulator with an index directory (fault seeds
  included); crash test with kill -9 checks the index directory is empty
  afterwards.
- Concurrent read + merge + shred stress under `-race`.

## Benchmarks (after: to be filled in)

Same three YCSB runs as above (peak and end RSS, anon vs file-backed),
read/write throughput, and open time (reopen of a 10M-key database with
and without a snapshot) for LogEt before/after and Pebble.

## Limits that remain

- The page cache holds the runs; RSS as `ps` reports it includes mapped
  pages that were touched (file-backed, reclaimable). We report anon and
  file separately.
- Snapshot write and load still hold the whole snapshot in memory
  (single object); multi-part snapshots are ROADMAP item 5.
- MVCC history is still indexed until compaction, now on disk.
- OCC's recent-commit window and the segment indexes are separate RAM
  users and are not changed here.
