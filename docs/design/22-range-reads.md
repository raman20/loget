# Design 22: range reads and iterators (2026-10-31)

ROADMAP item 2. Status: implemented. Numbers marked "before" were
measured at 544c248 on the reference box (8 vCPU, 16 GiB, ext4) on
2026-10-31 at 23:53-23:58 IST. Numbers marked "after" come from the same
box at 00:14-00:25 IST on 2026-11-01 (see Benchmarks). Raw files are in
`bench/results/2026-10-31-range-reads/{before,after}/`.

## Problem

The only range read was `Tx.Scan(prefix)` / `ScanRecords(space, prefix)`:

- **Prefix only.** No `[start, end)` bounds, no reverse order and no limit,
  so there was no way to page through a range.
- **Fully materialized.** Under the view lock, `index.KV.Scan` collected
  every matching key and version into a slice. It kept a `done` map of
  every key seen and then sorted the result. `ScanRecords` opened every
  value into a second map, sorted the keys again and built a third slice.
  A scan of the whole space therefore needed all of its keys and values in
  RAM at once, whatever design 21 did for the index itself.
- **Coarse conflicts.** OCC added the whole prefix to the read set, even
  when the caller only used the first few keys.

Before, on the reference box (`loget-bench scan`; values read through
the same API; Pebble uses an iterator with bounds):

| 1M keys x 1 KiB | LogEt | Pebble |
|---|---|---|
| 100-key prefix scan, 1 client | 5,584 scans/s (p50 156 us) | 13,939 scans/s (p50 72 us) |
| 100-key prefix scan, 8 clients | 19,731 scans/s | 22,458 scans/s |
| full scan of 1M keys | 184k keys/s, RSS 470 -> **1,729 MiB**, heap peak 1,405 MiB | 1.45M keys/s, RSS 178 MiB flat |
| start key + 1..100 keys, reverse | not expressible | 22.6k / 13.2k scans/s |

| 10M keys x 100 B | LogEt | Pebble |
|---|---|---|
| 100-key prefix scan, 1 / 8 clients | 5,977 / 22,535 scans/s | 38,514 / 93,422 scans/s |
| full scan of 10M keys | 207k keys/s, 48 s, RSS 1,892 -> **5,595 MiB**, heap peak 4,793 MiB | 6.3M keys/s, RSS 183 MiB flat |

After `Compact` (1M, so that the keys are in the base): same shape, a
full scan peaked at 1,664 MiB.

## Requirements

1. Ordered forward and reverse range reads over a space, bounded by a
   prefix and/or `[start, end)`, with a limit.
2. Snapshot consistency: every key an iterator returns is from the
   transaction's snapshot `At()` (plus the transaction's own writes),
   even if commits, freezes, run merges or run-file unmaps happen between
   steps.
3. Memory bounded by a page, not by the size of the range, whether the
   index is on the heap or in run files (design 21).
4. Pagination that clients can use across requests (HTTP) without
   server-side cursor state.
5. Serializable read-write transactions: the read set covers exactly what
   the caller observed, including gaps (phantoms).
6. Do not break `Scan`, `ScanRecords` or `ReadPrefix`.

## Options considered

**A. A long-lived merged iterator holding the view's read lock** (the
LevelDB/Pebble shape, with the memtable and SST iterators kept open).
Simplest per step, but user code would run between steps while the lock
is held. That blocks every commit's apply (the writer takes the write
lock) for as long as the caller dawdles, and a caller that writes into
the same DB from inside the loop deadlocks. Rejected.

**B. A long-lived iterator that pins runs instead of the lock.** Hold
references to the hot set and the runs, then read them without the lock.
The hot set is a mutable map that changes under the writer, so it would
need a copy or a persistent structure. Pinned file runs would delay
unmapping, which would need reference counts on runs. This means more
machinery in the hottest code path. Rejected for now.

**C. Paged re-seek (chosen).** An index-level `KV.Range(space, lo, hi,
at, reverse, fn)` runs entirely under the read lock and stops when `fn`
returns false. The public `Iterator` fetches one page of keys and index
versions at a time (16 keys first, doubling to 1024) and releases the
lock. It then opens values one by one as the caller advances. The next
page re-seeks from just after the last key. This is snapshot-consistent
because the index is MVCC: every version at or below `At()` stays put,
and later commits only add versions above it. Merges and unmaps change
where a version lives, never whether it is visible at `At()`. The cost
is one seek per run per page: a binary search over block first keys and
at most one 16-entry block scan. That is amortised over up to 1024 keys.

## Design

### Index: `KV.Range`

- **Sources.** The sources are the hot set and every run, newest first:
  - Hot set: the sorted `keys` subslice inside `[lo, hi)`, merged on the
    fly with the in-range part of `recent`, sorted. `recent` holds at
    most 1024 keys, so this costs no allocation proportional to the hot
    set.
  - Forward run cursor: the existing block-index `seek` plus an upper
    bound check.
  - Reverse run cursor (new, `rCursor`): runs are front-coded forwards,
    so it decodes one block (up to 16 entries) into a small reusable
    buffer, walks it backwards, then loads the previous block via `bofs`.
    Seeking finds the last block whose first key is below `hi`.
- **Merge.** Each step takes the smallest key (the largest in reverse)
  over at most 25 sources with a linear scan. The first source, newest
  first, that has a version visible at `at` decides that key. A tombstone
  hides the key. Every source positioned on the key is advanced.
- **Lifetimes.** Values from file runs are copied with `run.own` (as in
  `Get`), so nothing returned aliases a mapping that a later write may
  unmap. The caller holds the read lock for the whole call, and run
  release needs the write lock.
- **`KV.Scan`** is now `Range` with `PrefixEnd(prefix)`: no `done` map, no
  sort.

### Transactions: `Tx.Iter`, `Tx.Range`

```go
type IterOptions struct {
    Prefix, Start, End string // keys with Prefix, Start <= k < End ("" = unbounded)
    Reverse bool
    Limit   int               // 0 = no limit
}
func (o IterOptions) After(key string) IterOptions // the next page
func (tx *Tx) Iter(o IterOptions) *Iterator         // Next / Key / Value / Err / All()
func (tx *Tx) Range(o IterOptions) ([]KV, error)    // collected; IterRecords / RangeRecords per space
```

- **Own writes.** `IterRecords` snapshots the transaction's own writes in
  range at creation, sorted. Next merges them over the index pages: a
  local put or delete shadows the index key. Writes made after the
  iterator was created are not visible to it. This matches Pebble's
  batch iterators.
- **Values.** Each value is opened (log read, decryption) only when Next
  reaches it. Shredded values are skipped, as in `Scan`.
- **Pagination.** `After(last)` sets `Start = last+"\x00"` (forward) or
  `End = last` (reverse). There is no server-side cursor, so a page token
  is just a key. A page read at a different snapshot sees that snapshot,
  as with any keyset pagination.
- **`ScanRecords`** is now `RangeRecords(space, {Prefix})`.

### OCC: conflict ranges

The read set now holds ranges, `keyRange{space, lo, hi, inf}`, instead of
prefixes (`ReadPrefix` adds `[p, PrefixEnd(p))`). An iterator in an
Update keeps one range up to date as it advances:

- **Forward:** `[lo, last+"\x00")`.
- **Reverse:** `[last, hi)`.
- **Exhausted:** the whole `[lo, hi)`. This happens once Next has
  returned false without an error.

The range therefore covers every key and every gap the caller has
observed, so an insert between two returned keys conflicts (no
phantoms). Keys past the point where the caller stopped do not conflict,
whether it stopped by a limit, a `break` or by abandoning the iterator.
The range is extended eagerly on each Next, so an abandoned iterator
needs no Close. `writer.conflict` tests each recent write against each
range, which costs the same as the prefix loop it replaces.

### HTTP

`GET /v1/kv?prefix=&start=&end=&reverse=1&limit=&after=` returns a page.
When a page is full, the response carries `Loget-Next-After` (the
query-escaped last key). Authorisation is still by `prefix`. Start and
end only narrow the range inside it, because `IterOptions` intersects
the bounds.

## Model and proofs

There is no new TLA+ spec. The concurrency argument is the existing MVCC
invariant (versions at or below an applied LSN are immutable) plus the
existing OCC spec, with "key in a read range" in place of "key under a
read prefix". Both are predicates on the write's key, so the
serializability argument does not change. The new code is sequential
ordering logic, which a reference ordered map checks better:

- `index.checkRange` (inside `checkKV`, so it runs in
  `TestKVRunsRefineMap`, `TestKVFilesRefineMap` and
  `TestKVSetLatestRefineMap`). It compares `Range` with a sorted map model
  for 81 bound pairs, both directions and 3 snapshots, also stopped half
  way. This runs while runs are frozen, merged, rebuilt, reloaded, kept
  in files and redacted by shreds. Mutation check: breaking the reverse
  cursor's block step or the snapshot test makes it fail in under 20
  cases.
- `FuzzKVRange`: arbitrary keys (any bytes), interleaved freezes and
  merges, arbitrary bounds.
- `TestIterRefinesOrderedMap` (rapid): committed history, a snapshot at
  a random LSN, uncommitted own writes and random options. It checks that
  `Range` and a full paginated walk both equal the reference, and that
  commits landing mid-iteration are invisible.
- `TestIterConflictModel` (rapid): T1 reads a random range with a random
  limit, T2 commits random writes, then T1 commits. T1 must conflict
  exactly when the model's covered range contains a T2 write.
  `TestIterConflictRanges` lists the edge cases by name.
- Crash: `TestCrashKill9` (heap and run-file index) now also walks every
  recovered group in reverse and checks it against `Scan`.
- Simulator: every recovery check also pages through the KV space in
  reverse with a seed-chosen page size and compares it with `Scan`.

## Benchmarks

After: 7179c12, 2026-11-01 00:14-00:25 IST, same box and command
(`bench/results/2026-10-31-range-reads/README.md`). Scans per second
(1 / 8 clients); keys/s for full scans.

| 1M keys x 1 KiB | LogEt before | LogEt after | Pebble after |
|---|---|---|---|
| prefix100 | 5,584 / 19,731 | 6,196 / 21,342 | 14,354 / 23,237 |
| range50 (start + 50) | n/a | 8,195 / 29,827 | 24,488 / 40,311 |
| reverse50 | n/a | 8,978 / 30,221 | 14,128 / 20,249 |
| full, slice API | 184k, RSS peak 1,729 MiB, heap 1,405 | 325k, RSS peak 1,570, heap 1,229 | 1.69M, 179 MiB |
| full, iterator | n/a | 450k, heap peak 138 MiB | 1.74M, 132 MiB |
| full, reverse iterator | n/a | 468k, RSS 491 MiB flat | 1.44M, 132 MiB |

| 10M keys x 100 B | LogEt before | LogEt after | Pebble after |
|---|---|---|---|
| prefix100 | 5,977 / 22,535 | 6,206 / 23,867 | 38,782 / 92,195 |
| range50 | n/a | 7,121 / 29,881 | 53,185 / 112,011 |
| reverse50 | n/a | 9,514 / 34,187 | 20,164 / 33,128 |
| full, slice API | 207k (48.4 s), RSS 1,892 -> 5,595 MiB, heap 4,793 | 440k (22.7 s), RSS 1,909 -> 3,833, heap 3,168 | 6.47M, 168 MiB |
| full, iterator | n/a | 498k (20.1 s), heap peak 1,258 MiB | 7.20M, 163 MiB |
| full, reverse iterator | n/a | 553k (18.1 s), RSS 2,006 MiB (load: 1,395) | 5.49M, 163 MiB |

Reading the numbers:

- **Memory.** An iterator's full scan no longer grows with the range:
  heap peak 138 MiB at 1M (heap after load 60 MiB live, 126 MiB GC goal)
  and 1,258 MiB at 10M (live 560-637, goal 1,127-1,280: the GC goal of
  the resident index, not scan state). The slice APIs (`Scan`,
  `ScanRecords`) still return the whole result by contract, so a full
  slice scan still peaks at gigabytes; use `Iter` for large ranges.
  `full-iter` RSS is inflated by the preceding slice scan (see README).
- **Throughput.** Full scans are 1.8-2.1x faster than before through the
  slice API and 2.4x through the iterator (no `done` map, no re-sorting,
  no second value map), but the iterator is still 3.9x (1M) and 14x
  (10M) slower than Pebble (reverse: 3.1x and 9.9x): each key costs a
  separate value open (see Limits). Short prefix scans gained 4-11 %;
  Pebble is 1.1-2.3x faster at 1M and 3.9-6.2x at 10M for prefix100,
  3.7-7.5x for range50 at 10M. LogEt reverse scans are as fast as
  forward ones; at c=8 they beat Pebble's reverse at 1M (30.2k vs
  20.2k) and match it at 10M (34.2k vs 33.1k).
- **Compaction and heap mode** (1M): after `Compact` prefix100
  6,567 / 21,350, full-iter 435k keys/s, heap peak 21 MiB; with
  `-index-in-memory` 5,631 / 21,224, full-iter 456k, heap peak 236 MiB.
- Before ran on a binary without the new phases, hence the n/a cells.

## Limits

- A page is fetched under the read lock: up to 1024 keys plus one block
  per run. With many runs, a page costs a seek per run, which is fine
  while there are about 25 runs at most (`maxRuns`).
- Values are opened one at a time. A full scan therefore costs one value
  read per key (a value-cache lookup or a `pread` of the log or base
  chunk). There is no readahead or batching of adjacent values yet, so
  LogEt's full-scan throughput stays well below Pebble's, whose values sit
  next to their keys in SST blocks.
- The iterator does not see writes the transaction makes after creating
  it.
- Derived indexes (`Tx.Index`) still have only their own read APIs. Range
  reads cover the record store (KV and any space).
