# Design 27: per-key TTL and snapshot handles (2026-11-04)

ROADMAP KV-completion items 4 (TTL) and 5's open part (pinned snapshot
handles that compaction respects).

## Part a: TTL

### API

`Put(k, v, TTL(d))` and `Put(k, v, ExpiresAt(t))`, in transactions and in
`Batch`. Only puts into the KV space may expire; `d <= 0`, `t` at or before
1970, or an expiry on a delete or another space fail with `ErrBadValue`.
`Ingest` does not take expiries (its ops have no options; unchanged).
The HTTP server does not expose TTL yet.

### Expiry is in the log

- A put with an expiry is stored with kind `PUT|0x80` and an absolute
  expiry (i64 unix nanos) after its value (docs/formats.md).
- `TTL(d)` is resolved by the writer, once, when it assigns the frame's
  commit timestamp: expiry = commit TS + d (saturating). Replay reads the
  absolute value back; nothing is recomputed, so live state, replay,
  checkpoints and bases agree byte for byte.

### "Now" is never the wall clock at read time

A read at LSN L compares expiries with **the commit timestamp of the frame
at L** (`Tx.Now()`; the base's stored TS when L is the base LSN). A
`Snapshot` may instead be created with an explicit `Now`. A version is
visible iff its expiry is 0 or > now. Consequences:

- the same (LSN, key) read returns the same answer forever, on any replica,
  after a reopen, from a checkpoint or after compaction;
- a key does not disappear while the database is idle: it is seen as
  expired at the first commit whose timestamp reaches its expiry (or by a
  snapshot with an explicit later `Now`). An app that needs wall-clock
  expiry on an idle database can commit an empty marker or read with
  `Now`.

Cost: the head's TS is kept in memory (no I/O). `Tx.Now()` for an older LSN
reads one segment (cached per transaction) and is computed only if a
version with an expiry is found; reads of keys without expiries never ask.

### Transactions (OCC)

Expiry acts as a write at its time. A read-write transaction that saw a
key as live records the smallest expiry it relied on; validation at commit
fails (`ErrConflict`, retried by `Update`) if that expiry is at or below
the timestamp of the newest committed frame. The retry reads at a newer
LSN and sees the key as expired, so this cannot livelock. A transaction's
reads of its own uncommitted writes ignore expiry (the commit TS is not
known yet). Batches read nothing and never conflict.

### Iterators and scans

Iterators skip expired versions using the transaction's read time; the
read time is fetched before taking the index lock only when the index
holds some expiring version (`KV.MayExpire`), so iteration over data
without TTLs runs as before. An error fetching it ends the iterator with
that error.

### Compaction is the garbage collector

`writeBase` drops versions expired at the base's TS (the commit TS at the
cut, not the wall clock) and counts them in `CompactStats.Expired`; kept
versions carry their expiry into the base. The base's TS is the read time
at the base LSN, so reads at or above the cut are unchanged
(spec/LogEtTTL.tla; dropping at a later time breaks this, which TLC finds).
Nothing else deletes expired data: until a compaction, expired keys use
memory and disk as before.

### Index representation

`Version.Exp`; in memory an extra word only for versions with an expiry
(the 40-byte per-version struct is unchanged for keys without one).
Snapshots of the key index use tag `KV4` only when some version expires;
runs use flag `fExp`. Data without TTLs is written byte-identically to
before (golden tests unchanged). Older builds reject the new op kind,
`KV4` and `fExp`.

## Part b: Snapshot handles

```go
s, err := db.Snapshot(ctx, loget.SnapshotOptions{})  // or Branch.Snapshot, At, Now
defer s.Release()
err = s.View(ctx, func(tx *loget.Tx) error { ... })
```

- **Cheap**: creating one takes a mutex and inserts into a map (no I/O at
  the head; one segment read for an older `At` to find its time).
- **Stable**: every `View` reads the same LSN and read time.
- **Explicit release**: `Release` (idempotent) unpins; later views fail
  with `ErrSnapshotReleased`.
- **Bounded retention**: a snapshot pins compaction of every log in its
  branch's lineage (`CompactStats.Pinned` counts logs skipped) for at most
  `Options.SnapshotMaxAge` (default 10 min, measured with `Options.Clock`).
  After that it no longer pins and `View` returns `ErrSnapshotTooOld` (also
  returned at creation when `At` is below the compaction base or a planned
  cut). `View` re-checks after `fn`, so a compaction that passed an aged
  snapshot during `fn` is reported, never a silently mixed view.

Protocol (readview.go): creation checks the base and the per-log planned
cut and registers under one mutex; compaction planning, under the same
mutex, skips a log with a live young snapshot below the cut, otherwise
records the cut (monotone, kept even if the compaction fails).
spec/LogEtSnapshot.tla checks that a live snapshot is never below the
base or a cut being committed (151,967 distinct states); with check and
register split, or with creation ignoring the planned cut, TLC finds a
counterexample (spec/RESULTS.md).

Limits: aged snapshots stay in the registry (a few hundred bytes) until
released, so callers must release them. The clock is read by `plan` only
while some snapshot is live, so deterministic-clock tests are unchanged.

## Verification

| Check | Result |
|---|---|
| `TestTTLModel`: reference model (map + per-frame TS) at every LSN, live, after reopen, after compaction + reopen, checkpoints every 0 / 7 commits | equal; ≥ 50 expired and ≥ 50 unexpired reads asserted |
| `TestTTLConflict`, `TestTTLBasic` | expiry-at-commit conflict, retry sees it expired; option validation |
| index property test + `FuzzKVRange` with expiries vs map model | pass |
| simulator, 500 seeds (a third of puts expire; `tx.Now()` vs the head's TS) | pass; 2,888 expired / 8,209 expiring reads checked |
| `TestCrashKill9TTL` (`LOGET_CRASH_TTL`, 20 ms TTLs) | 25 kill -9 cycles; live TTL keys form a suffix, equal after a second open |
| snapshot tests (`TestSnapshotView`, `PinsCompaction`, `Race`) | pass; race run: 1,023 snapshots, 508 too old, 2 pinned compactions, 18 bases |
| fuzz 40 s each: DecodePayload, DecodeSegment, LoadKV, LoadKVRuns, KVRange (seeds with expiries) | no failures |
| TLC LogEtTTL (base+tail = replay; OCC rule sound) | 1,833,081 distinct states, no error; 2 mutants fail |
| TLC LogEtSnapshot | 151,967 distinct, no error; 2 mutants fail |

## Benchmarks (1M keys, medians of 5, bench/results/2026-11-04-ttl)

Keys without expiry, before (27e01a6) → after:

| Benchmark | before | after |
|---|---:|---:|
| KVBuild ns/key, heap B/key | 1,719, 49.21 | 1,685, 49.21 |
| KVGet ns/op | 225.5 | 225.4 |
| KVScan100 ns/op | 25,051 | 23,427 |
| KVSnapshotLoad ms, heap B/key | 229.2, 29.51 | 230.1, 29.51 |
| PutSerial mem / fs µs | 6.73 / 274.9 | 6.77 / 278.4 |
| PutParallel mem / fs µs | 3.44 / 9.34 | 3.26 / 8.65 |
| Get (DB) ns/op | 655 | 674 (+3%; spread 638–688 before, 648–777 after) |

Every key with an expiry: KVBuild 1,879 ns/key (+11%), **65.18 heap B/key
(+16 B)** after the fold; 37.51 B/key (+8 B) after loading a snapshot;
snapshot 22% larger (8 B per version). One box, one run of each set; the
DB `Get` difference is within the run-to-run spread seen in design 25.

## Not done

HTTP TTL; TTL on ingest; expiry on an idle database without a commit;
lineage (parent logs) not in the TLA model; snapshot handles over HTTP.
