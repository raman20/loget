# Design 30: log-only values, pointer compaction, log GC (2026-11-16)

ROADMAP follow-on to design 28. Numbered 30 because design 29 is the
scan read path. Status: specified and model-checked
(`spec/LogEtLogGC.tla`); implementation is behind manifest format 5.

The north star is **single log, any index**: the log is the only durable
copy of user data. Every index is a disposable, rebuildable pointer
structure over log offsets. Compaction of an index rewrites pointers.
Garbage collection of the log is a separate, index-agnostic sweep.
A new index is `fold` of the retained log.

## What's wrong today

Until the first compaction, LogEt is already close to this. A value
larger than `InlineMax` (64 B) is stored once, in the group-commit
segment, and the KV index keeps a position `(segment, offset, length,
CRC-32C)` (`index.Version`, `Version.Ref()`). Compaction is what breaks
it (`writeBase` in `compact.go`):

1. It reads live values back out of those segments and PUTs them into a
   second object family, `base/<id>/…/*.val` chunks.
2. It commits a manifest that points the log at a KV record-store image
   and lists every covered segment as garbage.
3. After the sweep, the log prefix is gone. Reads, time travel, branches
   and any index not already folded into that base return `ErrCompacted`
   below the base LSN.

Design 28 only softened the copy (share chunks that are still ≥ ½ live).
The first compaction still copies every large value, and a new index
can no longer be built by folding the log. Compaction was doing three
jobs at once: shrink the KV pointer index, copy values so segments
could die, and erase history. Only the first is an index-local concern.

## The design

### Values

The durable copy of every value lives in the log segment that committed
it. New compactions never write `.val` chunks and never copy values
into a "survivor" segment. The KV index stores positions into segments
for values larger than `InlineMax`. Values at or below `InlineMax` may
still be inlined in the key run as a cache (they are also in the
segment; the inline bytes are rebuilt from the log).

Subject (encrypted) values stay in the original segment. Compaction
does not reseal them. Associated data already includes log id, LSN, op
index, space and key, which do not change. `Shred` is unchanged: it
deletes the DEK and leaves ciphertext in the segment.

### KV compaction (pointers only)

`Compact` rewrites the KV pointer index at the cut (the log head, or a
retention cut): one new base made of run pieces, same as design 24, but
every `Version.Ref()` still names the original segment (or, during
migration, a leftover format-4 chunk). Deletes, overwritten versions,
redacted puts and keys expired at the cut's commit timestamp are
dropped from the index, as today. Commits still pause briefly at the
commit step, as today.

A compact does **not** list covered segments as garbage and does **not**
make history `ErrCompacted`. The base is a shortcut for opening the KV
index (and the hash-chain head at the cut), not a replacement of the
log. `WithCompactCopyAll()` becomes a no-op on a format-5 database.

### The live-reference map

Log GC needs one shared answer to "which stored objects are still
pointed at?". It is recomputed at each sweep by walking every index and
every live `Snapshot` (the same shape as `shareChunks` today: one pass,
no I/O). Incremental refcounts are deferred.

An object (a segment, or a leftover `.val` chunk) is **live** if any of
the following holds:

- the KV index holds a `Version` whose position resolves to it;
- any derived index reports a reference to it (`index.Kind` gains a
  `Refs` hook, so topics, docs and vectors plug in when they land; on
  main, KV is the only built-in index);
- a live, not-aged `Snapshot` needs it (the snapshot's pointer state,
  plus any segment required to serve reads at that snapshot LSN);
- it is in the **recoverable tail**: a segment at or after the KV
  base's `Seq`. Open still replays those frames onto the pointer
  index. They stay until a later compact advances the base past them.

A background check is not a source of truth; a missed ref would be a
lost value. The sweep walks the live set it just computed, under the
same lock that snapshots and compact planning already share
(design 27).

### Log GC

A separate sweep, index-agnostic, deletes an object only when it is
not live and its grace period has passed (the same timing assumption
as today's garbage: readers holding an older manifest finish first).
Orphan pointer-base pieces of an older epoch, not named by the latest
manifest, are deleted as today.

This is "no live references," not design 28's `shareMin`. A segment
that still holds one live key is kept in full. Space amplification is
the dead bytes in segments that are still live. That is honest and
unbounded in the worst case (a large segment with a single never-
overwritten key). We do **not** copy stragglers into a new segment in
this design; if measurements in the implementation phase show a bad
trade-off, we stop and review rather than reintroduce a shadow store.
A later, explicit opt-in could write a new **log segment** of
stragglers and retarget pointers — still the log, not `.val`.

### Retention policy

Retention is how history (old versions, time travel below a cut) is
dropped. It is not a back door to delete a segment that still holds a
live latest value.

`GC` already has a retention window (design 08, `spec/LogEtGC.tla`).
Under this design a retention cut at LSN C means:

1. Snapshots below C must have aged out or been released (design 27's
   `SnapshotMaxAge` still bounds how long they pin).
2. Every index drops versions that are not visible at C (KV already
   does this when compacting at C).
3. Then the ordinary live-reference sweep may delete segments that
   nobody references.

`ErrCompacted` is redefined: it means "the segment for this LSN has
been deleted by log GC or retention," not "a KV compact has run."
`ViewAt`, `Rewind` and `CreateBranch` below a cut succeed if every
segment in the needed prefix is still retained, and fail with
`ErrCompacted` only when one is missing. This is an intentional
API change.

A convenience flag on `GC` / `Compact` can request "drop history at
this cut" (today's user-visible reclaim: old versions go away, live
values stay in their original segments). Without that flag, compact
only shrinks the pointer index.

### Manifest format 5

`manifestFormatLogOnly = 5`. Older binaries refuse it, as they did
format 4. A new database starts at 1 and bumps to 5 on the first
format-5 compact (same pattern as 2, 3 and 4).

`LogMeta` gains a retained-segment list for history below the KV
base: each entry is `{seq, first, n}` (the same facts as `segIdx`).
Value reads, `ViewAt` and a new index fold use it to find segments
that `recoverLog` must not treat as the writable prefix. After each
compact or log-GC sweep the list is rewritten: tail segments the new
base now covers move into it; unreferenced segments leave it and
appear in `Garbage` (kind `segs`, named by seq, not as a dense
`Below` range that would also catch live neighbours).

A new base has no `refs` and no `chunks`. Leftover format-4 `refs` on
an older base remain until that base is replaced; the replacement's
pointer run may still name those chunk objects (see migration).

### Recovery

Unchanged for the writable log: if a KV base exists, `nextSeq` starts
at `Base.Seq`, the pointer run is loaded, and only the tail is
replayed. `recoverLog` does not chain retained history segments into
the prefix (they sit below `Base.Seq` and stay out of `scanSegments`).

Value reads change: `logState.source` / `locate` must resolve a
position in a retained history segment, not only in the tail or in a
chunk. The retained-segment list is the index for that. A version
with `Chunk != 0` still goes through the chunk path.

Time travel and a new index fold walk retained history in LSN order,
then the tail. If a complete prefix is not present, they return
`ErrCompacted`. There may be no pointer index at an intermediate LSN
(only the KV image at the last compact, plus any earlier checkpoint
not yet swept). Folding from the retained log is correct and may be
slow; that is acceptable for time travel and for building an index
once.

### Migration from format 4

Old bases and `.val` chunks stay readable. The `Chunk != 0` path
remains until no live version names a chunk. New compactions do not
produce chunks.

If a format-4 compact has already swept the original segments, those
values have no log copy left. Pointers keep naming the chunks; log GC
deletes a chunk only when it is unreferenced, same rule as segments.
A new index cannot fold history that was already deleted — that
damage is done. Keys later overwritten are written to new segments
and then live only in the log. A database that still has its
pre-compact segments (grace not expired, or never swept) can, on the
first format-5 compact, retarget pointers at those segments and let
the chunks become unreferenced.

Golden compacted fixtures stay under `TestGoldenRead`.

### Snapshots

A `Snapshot` still pins under the design-27 mutex and still ages out
at `SnapshotMaxAge`. What it pins changes: it contributes its
referenced objects to the live set of **log GC**, and it no longer
has to hold back a pointer-only compact that does not delete
segments. A snapshot older than a *retention* cut still holds that
cut back, as today.

### Scan locality and a disposable scan run

After a format-4 compact, newly copied values sit in key order in 64
MiB chunks, which is why a full scan went from 1.54M to 2.56M keys/s
(design 28). Design 29 then cut per-value work, heap-merged range
sources and added page read-ahead; measure against that baseline, which
is still 3–5× slower than Pebble's 7.28M. Under this
design, values stay in commit order across many `.seg` objects, so
scans are expected to regress toward the pre-pack figure, or worse,
on randomly ordered writes. Sequential bulk ingest still lands
nearby keys nearby in a segment, and iterator prefetch
(`prefetch.go`: gap ≤ 4 KiB, span ≤ 1 MiB, 4-way parallel) still
coalesces those.

The fix is an **optional, disposable, key-ordered scan run**: a
derived structure of pointers (or packed copies in RAM / a local
file) that is not a source of truth and can be rebuilt by walking
the pointer index. It is specified here so the trade-off is
honest. It is not required to ship in the first implementation
step. Phase 4 measures forward and reverse scan keys/s against
main; if the regression is bad enough to sink the design, we stop
and review.

### Open-time checkSeg (independent, included here)

A segment-index checkpoint is already a bound on the prefix it
covers (`checkpoint.go`, `CkptSound` in `LogEtLog`). `recoverLog`
skips those GETs. `buildView`'s `eachOwnAfter` still `checkSeg`s
every earlier segment (`log.go`), which undoes the skip and is
expensive on S3. Design 25 kept the extra read as a local
corruption check.

Decision: do not `checkSeg` segments the checkpoint already
covered. Mid-log damage is found when the segment is next read, or
by `Verify`, which still reads everything. The trade-off is the one
`checkpoint.go` already documents.

## Protocol

1. Sweep due garbage (live-reference map, then delete what is unreferenced
   and past grace; orphan pointer-base pieces of older epochs).
2. For each log that needs a new pointer base: write run pieces
   (create-only), no value objects.
3. One create-only manifest: set `Base` (pointer run, format 5),
   refresh the retained-segment list, do not list those segments as
   garbage. List only the previous pointer base / replaced run pieces
   (and any history segments the live-reference map has already found
   unreferenced, if this compact requested a retention cut).
4. Reload the in-memory pointer index from the new base plus the
   tail. Commits resume.
5. Sweep again.

Safety, as in `LogEtCompact`, plus the new model `LogEtLogGC`:

- C1 (no loss): the manifest that stops naming a pointer base is
  committed only after the replacement is durable. Segments are not
  un-named by a pointer compact.
- C2 (fencing): create-only manifest `ID+1`, unchanged.
- C3 (garbage is final): a segment or chunk listed as garbage is
  unreferenced by the live map of that manifest and of later ones.
- C4 (readers / snapshots): grace, plus snapshots contributing to
  the live set, so a live snapshot never misses an object.
- C5 (no dangling): every ref from KV, derived indexes, live
  snapshots and the recoverable tail exists in the store.
- C6 (fold): a new index only ever observes retained segments that
  were stored at the time of the fold.

## Failure modes

- Crash before step 3: old manifest is intact; new run pieces are
  orphans of this epoch and are swept later. No value was moved.
- A zombie compact cannot commit (create-only). It may only leave
  orphans or delete objects already listed as garbage.
- A reader holding a manifest longer than the grace can get
  `ErrNotFound`, never wrong bytes (CRC-checked reads).
- A format-4 database whose segments were already swept cannot
  reconstruct a pre-compact log for a new index. Chunks remain until
  unreferenced.

## Tests

- TLA+ `spec/LogEtLogGC.tla`: CI bounds (MaxLSN=3, MaxBase=2, one leftover
  chunk, one snapshot, one reader) 773,363 / 124,028 distinct states,
  depth 20, no error. Mutants: history GC ignoring index and snapshot
  refs (`LogEtLogGC_sweepref.cfg`) violates `NoDangling` in 4 steps;
  a pointer compact that lists all covered segments as garbage
  (`LogEtLogGC_dropsegs.cfg`, the design-03 behaviour) violates it in
  6 steps. Fencing of the manifest itself stays in LogEtLog / LogEtCompact.
  LogEtChunks remains the model of format-4 chunk sharing, still needed
  to read leftover `.val` objects.
- Property and simulation: compact, overwrite, GC, crash; every
  Get equals a model map; a second index folded after compact sees
  every retained record.
- Crash: kill -9 during pointer compact and during log GC; every
  acked write is present; no dangling position.
- Migration: reopen a format-4 compacted fixture, compact again,
  read every key, GC unreferenced chunks, no new `.val` objects.
- `ErrCompacted` only when a needed segment is gone.
- Open does not GET checkpoint-covered segments (except the bind
  of the last one); `Verify` still does.

## Success metrics (phase 4, 10M keys, vs main)

First and repeat compact: time and bytes written, PUT/GET counts,
forward/reverse scan keys/s, open time, steady and peak memory. A
small test or demo that a new index can be folded over the retained
log after a compact. If first-compact I/O does not drop to roughly
the pointer-run size, or if scan/memory trade-offs are bad enough
to sink the design, stop and report.

## What we are not doing now

- Incremental / sharded pointer runs (design 24 option D) and
  storing hash slots in checkpoints (the open-time copy of run
  pieces). Separate from log-only values.
- A pread + block cache for key runs (design 25).
- Copying stragglers to bound space amplification.
- Building the disposable scan run.
- Changing group-commit, fencing, shred, or TTL's meaning of "now."

## Open questions

1. Worst-case space amplification (one live key pinning a large
   segment): measure at 10M with 1% churn and with a long-lived
   key left in the first segment. Decide then whether a later
   "rewrite stragglers as a new log segment" opt-in is needed.
2. Scan regression vs main's post-compact packed chunks: if it is
   severe, the disposable scan run may have to come with this
   change rather than after it.
3. `Refs` on `index.Kind`: the exact shape (object key vs
   `(log, seq)` vs a callback the engine walks). KV can be
   implemented first; the hook should exist before a second
   index lands.
4. Whether `Compact` without a retention flag should still bump
   format and rewrite the pointer base at the head by default
   (yes, unless we find a reason to split the API).
