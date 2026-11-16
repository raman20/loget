# Design 29: scan read path: heap merge, batched value lookups, read-ahead (2026-11-16)

ROADMAP KV item 2 (full-scan gap against Pebble). Numbers:
[bench/results/2026-11-16-scan](../../bench/results/2026-11-16-scan/README.md).

## Where the time went (10M × 100 B, key-ordered load, full forward scan)

Profiled with `loget-bench scan -profile` (CPU and allocations per phase):
15% of CPU in `fmt.Sprintf` naming a segment per value, 11% in
`logState.locate` (lock, binary search and a heap-allocated defer, twice
per value), 27% in `KV.Range` comparing every source's key for every key,
10% in GC, and ~6 allocations per key against Pebble's one.

## Changes

1. **Value lookups without per-value work.** Object keys are built without
   `fmt`; `locateSpan` returns the LSN span that resolves to the same
   segment, and a page caches it, so a page of keys written together costs
   a few binary searches. Prefetch groups by a comparable object id and
   names the object once per ranged read.
2. **Heap merge in `KV.Range`.** Sources (hot set, then runs, newest
   first) sit in a binary heap ordered by key, ties newest first, so equal
   keys still resolve to the newest version visible at the read LSN, as
   before. Runs of a key-ordered load cover disjoint ranges: the top stays
   on top and a key costs one or two comparisons, not one per run. The
   key becomes a string only when returned.
3. **Reused iterator buffers.** Prefetch requests, groups and value slots
   live across pages (−34% bytes allocated per scan).
4. **One page of read-ahead.** Once an iterator has grown to full pages
   (1,024 keys; it has already returned ~1,000, so short and point-like
   reads are unaffected), it ranges the keys of page N+1 and starts its
   value reads in the background before the caller walks page N. Key
   decoding and value I/O overlap.
   - At most one page is in flight and it is bounded to 4 MiB of values
     (`readAheadMax`); a larger page is read when reached. An abandoned
     iterator wastes at most that.
   - `DB.Close` (and `Crash`) wait for read-ahead in flight and refuse new
     read-ahead, so no read reaches a closed store.
   - Only reads move: pages are still ranged in order under the view's
     read lock, at the transaction's LSN; values are still checked against
     their CRC, and anything not prefetched is read on use (which retries
     and reports errors). The read set of an `Update` still covers only
     the keys returned, so conflicts are unchanged.
   - Not used with `Limit` set (the caller said how much it wants).

## What does not change

No format, manifest or log change; the index is still a pure fold of the
log. None of the TLA+ models (`spec/`) describe the read path below a
snapshot read of immutable objects, so their invariants are unaffected.
Covered by: `TestObjectKeysMatchFmt`, `TestLocateSpanExact`, `FuzzKVRange`
and the index model tests (heap merge), `TestIterReadAhead` (both
directions, writes between steps), `TestIterReadAheadAbandoned` (no
goroutine after Close), `TestIterReadAheadFaulty` (random read failures:
a key carries its value or the scan reports an error).

## Cost

Read-ahead uses a second core: the 10M forward scan takes the same CPU
time (~3.9 s) in less wall time (2.0 s vs 3.1 s). Pebble does the same
scan in ~1.5 s of CPU on one core, so per key LogEt still spends ~2.5×
the CPU. The remainder is in key decoding (front-coded runs decoded per
entry, a full `Version` decoded per key), the value copy every returned
value gets (the caller owns it, unlike Pebble's borrowed slices), and the
ranged-read buffers.
