# Design 25: the memory gap at 10M keys (2026-11-04)

ROADMAP KV-completion item 1. Goal: find where loget's RSS goes at 10M keys,
cut what can be cut cheaply, and state honestly what remains versus Pebble.

## Method

`loget-bench ycsb -keys 10000000 -value 100 -workloads C,A -clients 8
-warmup 1s -duration 10s` (script `bench/results/2026-11-04-memory/run.sh`),
reference box (8 vCPU / 16 GiB, ext4, journal layout, fsync on). The bench
samples RSS split into anonymous and file-backed pages (`/proc/self/status`
RssAnon / RssFile) plus Go runtime figures after each phase, and keeps the
highest sample (`peak_by_rss`). Pebble uses its default options (8 MiB block
cache). One run each; A-phase throughput varies ~±8% run to run.

## Where the memory was (HEAD before this note, 256 MiB value cache)

| | peak RSS (anon / file) | end RSS (anon / file) |
|---|---|---|
| loget | 1,053 MiB (481 / 572), in A | 850 MiB (530 / 319) |
| Pebble | 737 MiB (719 / 18), in load | 52 MiB (34 / 18) |

The earlier "1.9 GiB peak" figure predates the OCC-window cut (item 6); the
peak at HEAD was ~1.05 GiB.

Breakdown of loget's end state:

- **Value cache: ~250 MiB anon off-heap arena + ~80 MiB of Go heap** for its
  per-shard maps, plus the GC headroom those maps cause. On a local store the
  values it holds are also in the OS page cache: the cache duplicates memory.
- **File-backed ~320 MiB: the mmapped key-index runs** (design 21), ~32 B per
  key. This is the index's real working set under a uniform-random workload:
  with `MADV_DONTNEED` after a run is built, file RSS after load fell from 305
  to 148 MiB but climbed back to 306–320 MiB once C/A touched the keys.
- **Go heap ~60–150 MiB live** (heap profile after A with a 16 MiB cache:
  journal segment bookkeeping 21 MB, segment-key strings 11.5 MB, per-log
  segment state 8.6 MB, cache maps 5.6 MB), GC goal ~2× live.
- **Peak (in A): the file side, ~570 MiB**, during index merges when input and
  output runs are mapped and touched at the same time.

## Value-cache size sweep

| cache | peak (anon / file) | end RSS | C ops/s | A ops/s |
|---|---|---|---|---|
| 256 MiB (old default) | 1,053 (481 / 572) | 850 | 1.90M | 94.5k |
| 64 MiB | 766 (180 / 585) | 548 | 1.67M | 89.2k |
| 32 MiB | 704 (130 / 573) | 500 | 1.75M | 103.3k |
| 16 MiB | 682 (102 / 579) | 474 | 1.55M | 98.6k |
| Pebble | 737 (719 / 18) | 52 | 0.20M | 137.6k |

## Changes

1. **Local stores default to a 32 MiB value cache** (`localStore`: `*store.FS`,
   `*store.Journal`, `*store.Mem`). Remote/wrapped stores keep 1/16 of memory,
   16–256 MiB, because there a miss is an S3 range read. An explicit
   `WithValueCacheBytes` still wins. Cost: C (read-only, uniform) ~−8 to −13%
   from the 256 MiB run, still ~8× Pebble.
2. **`MADV_DONTNEED` after sealing a run** (`internal/runfile`): building a run
   touches every page; dropping them from the page tables makes RSS report
   the index's working set rather than its build history. Pages stay in the
   page cache (MAP_SHARED, file-backed), so this costs only minor faults. It
   lowers after-load RSS (353 → ~195 MiB) but not the A-phase peak.

## After (default options, this commit)

| | peak RSS (anon / file) | after load | end RSS (anon / file) | C ops/s | A ops/s |
|---|---|---|---|---|---|
| loget before | 1,053 (481 / 572) | 353 | 850 (530 / 319) | 1.90M | 94.5k |
| loget after | 701 (130 / 570) | 195 | 490 (173 / 317) | 1.65M | 92.3k |
| Pebble | 737 (719 / 18) | 710 | 52 (34 / 18) | 0.20M | 137.6k |

Peak is now below Pebble's (Pebble peaks while loading its memtables). The
steady-state gap remains ~10×: ~320 MiB is mmapped index pages and ~150 MiB
Go heap, against Pebble's 8 MiB block cache.

## Part 2 (2026-11-04, same day): merge peak, a bounded mode, open time

### Merge peak

A merge reads its input runs front to back while it writes the output; at
the end the output's hash table is filled by reading every entry. Both
left every page they touched mapped. Now (`index.dropBehind`, run files'
`Drop`): every 4 MiB of input a merge has consumed, and of entries the
table fill has read, is dropped from the process (`MADV_DONTNEED` on a
shared file mapping: the pages stay in the page cache, a later read is a
minor fault). Safe while readers use the run: the bytes do not change.
What remains of the peak is the index working set plus the output's
table, which is filled at random and cannot be streamed.

### Bounded mode (`Options.IndexResidentBytes`, opt-in)

The brief asked for a prototype of reading runs with `pread` into a
bounded block cache. That path would replace every direct access to a
run's bytes and slots (17 sites in `index/kvrun.go` and the iterators)
with block fetches. A cheaper prototype gives the same memory behaviour:
`pread` also serves from the OS page cache and keeps only its own buffer
in RSS, and dropping mapped pages does the same for a mapping. So
bounded mode is: every 100 ms read `RssFile` from `/proc/self/status`;
above the budget, drop every sealed run file's pages (`runfile.DropAll`).
It is a soft bound: under saturated uniform reads the working set comes
back within one interval (measured below at both 100 ms and 20 ms).
`RssFile` also counts the binary's text (~18 MiB) and any other file
mappings, so a budget below that drops on every check.

### Open time

A profile of the 3.40 s open from a checkpoint (10M x 100 B) showed:

- **every segment was read and fully decoded** although the checkpoint
  covers almost all of them; `buildView` skipped their entries only
  after decoding (~1.2 s wall, 2.2 s CPU). Design 24 meant to replay only
  segments after the checkpoint. Now segments holding only LSNs the
  record store and every derived index already have are read and checked
  by `format.ScanSegment`, which accepts and rejects exactly what
  `DecodeSegment` does (same CRC and payload checks, the fuzz target
  `FuzzDecodeSegment` compares the two) without building operations. A
  damaged middle segment still refuses the open
  (`TestCorruptMiddleSegmentRefusesOpen`).
- **`fillSlots` spent 0.85 s in random probes** into a 100 MiB table.
  Large runs (>= 128k keys) now fill in 8 fixed regions: each batch of
  1M entries is bucketed by home region and each region filled by its own
  task (`KVOptions.Parallel`; the index package starts no goroutines, the
  DB passes a runner); an entry that would probe past its region's end is
  inserted after all others. The table is a valid linear-probing table,
  is the same with or without concurrency and on any machine (fixed
  region count; `TestParallelFillSameTable`), and the refinement test
  runs with every run filled this way (`TestKVRunsRefineMapParallelFill`).

Lazy, on-demand mapping of runs was considered and not done: after these
two changes the open is the journal's own recovery scan (0.38 s), the
run copy and check (~0.5 s) and the table fill; deferring the table would
move that cost to the first lookups.

### After part 2 (10M x 100 B, one run each; `bench/results/2026-11-04-memory/final`)

| | peak RSS (anon / file) | after load | end RSS (anon / file) | C ops/s | C open-loop p50 / p99 | A ops/s |
|---|---|---|---|---|---|---|
| LogEt at item start | 1,053 (481 / 572) | 353 | 850 (530 / 319) | 1.90M | — | 94.5k |
| part 1 | 701 (130 / 570) | 195 | 490 (173 / 317) | 1.65M | — | 92.3k |
| part 2, mapped (default) | 557 (148 / 409) | 118 | 512 (189 / 323) | 1.75M | 6.2 µs / 0.05 ms | 117.6k |
| part 2, bounded 64 MiB | 410 (108 / 302), in C | 70 | 274 (179 / 94) | 1.65M | 6.7 µs / 3.6 ms | 106.0k |
| Pebble (defaults) | 737 (719 / 18) | 710 | 52 (34 / 18) | 0.20M | 14.6 µs / 4.5 ms | 137.6k |

| | open from checkpoint | 200k random reads after open | prefix100 c=8 | full scan (iterator) | RSS after full scan |
|---|---|---|---|---|---|
| before (HEAD at item start) | 3.40 s | 0.75 s | — | — | — |
| mapped | 1.97–2.19 s (3 runs) | 0.72–0.74 s | 22,280 scans/s | 590k keys/s | 308 MiB |
| bounded 64 MiB | 1.98–2.05 s | 0.81–0.98 s | 21,966 scans/s | 606k keys/s | 131 MiB |
| Pebble | < 0.2 s (design 24) | | 22,535 (design 22) | 7.20M keys/s (design 22) | 163 MiB |

A-phase throughput varies ±8–15% run to run; p99 of open-loop C varied
0.05–0.6 ms across mapped runs. Decision: **mapped stays the default**;
bounded is opt-in. It halves steady RSS for a ~6% read-throughput cost but
puts multi-millisecond stalls in the read tail (dropping a mapping takes
the process's mmap lock, which page faults of every reader also need), and
its bound is soft.

## What is left, and why not now

- **A hard bound.** Only a real `pread` + block cache read path gives one;
  it is the large change described above and would cost lookups a copy
  and a cache probe. Not done.
- **Steady RSS in mapped mode** stays ~10x Pebble's (index working set
  ~320 MiB, Go heap ~150 MiB). Bounded mode trades that for tail latency.
- **Open is ~10x Pebble's** (2.0 s vs < 0.2 s): Pebble opens without
  touching its data; LogEt copies the checkpoint's runs into run files,
  checks them and builds their tables. Storing tables in checkpoints
  (format change) or mapping checkpoint pieces directly would remove most
  of it.
- **Go heap.** Journal segment bookkeeping and segment-key strings are the
  largest live items; GC tuning (`GOMEMLIMIT`) is left to the application.
