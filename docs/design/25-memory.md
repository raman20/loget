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

## What is left, and why not now

- **Mapped index pages.** File-backed RSS is reclaimable by the kernel under
  pressure (it is page cache mapped into the process, not anonymous memory),
  so it does not cause OOM kills the way anon RSS does, but it does count
  toward RSS and cgroup memory. Matching Pebble's ~50 MiB would need the
  index read with `pread` into a bounded block cache instead of mmap: a large
  change to `internal/runfile` and the index read path, with a latency cost
  on every lookup. Not done; recorded on the ROADMAP.
- **Merge peak.** The A-phase file peak comes from merges touching input and
  output runs together. `MADV_DONTNEED` on consumed input ranges during a
  merge could lower it; not done yet.
- **Go heap.** Journal segment bookkeeping and segment-key strings are the
  largest live items; GC tuning (`GOMEMLIMIT`) is left to the application —
  the library does not set process-wide GC knobs.
- **Open time** (3.4 s vs <0.2 s) is not affected by these changes; it is
  dominated by loading checkpoint runs (design 24) and is a separate item.
