# Roadmap

LogEt is **pre-1.0** and has no release yet. This roadmap is a statement of
intent, not a promise, and has no dates. Items move on evidence
(benchmarks, bugs, users) and must respect the
[design principles](design-principles.md).

## The plan in one line

**Make KV the best embedded KV store it can be, then add indexes one at a
time.** One log, different indexes over it; KV first. The order of the
indexes after KV is in [PIPELINE.md](PIPELINE.md).

## Where KV stands (honest gaps, by impact)

Compared with Pebble and RocksDB (LSM trees) and LMDB (a memory-mapped
B+tree). Numbers are from this repo's benchmarks on a shared 8-vCPU VM
([benchmarks.md](benchmarks.md), [STATUS.md](STATUS.md)); they are single
runs, not performance claims.

| # | gap | LogEt today | Pebble / RocksDB / LMDB | impact |
|---|---|---|---|---|
| 1 | **Key index in mapped run files (designs 21, 25)** | done: runs of ≥ 1 MiB live in unlinked, memory-mapped files; hot set, small runs and block offsets on the heap. 10M × 100 B YCSB C+A: peak RSS 557 MiB (Pebble 737), end 490 MiB mapped / 274 MiB with `IndexResidentBytes` (Pebble 52) | Pebble 737 MiB peak, 52 MiB steady at 10M | steady memory still ~5–10× Pebble (no pread + block cache) |
| 2 | **Iterators (designs 22, 28)** | done: `Tx.Iter` with prefix, `[start, end)`, reverse, limit, `After` pagination; values read in coalesced parallel batches per page (design 28). Full scan 10M × 100 B: 1.54M keys/s (606k before batching), 2.56M after compaction | Pebble 7.28M keys/s full scan; seek on a live iterator | scans 3–5× slower than Pebble; no seek on a live iterator |
| 3 | **Batches and bulk ingest (design 26)** | done: `Batch`/`Apply` (atomic, no OCC), sorted `Ingest` as one commit. 10M × 100 B fsync: ingest 1.45M keys/s / 172 MiB peak, batches of 1,000 335k keys/s | Pebble SST ingest 3.96M keys/s / 46 MiB; batches 2.09M keys/s | ingest 2.7× and batches 6× slower; commits wait during an ingest |
| 4 | **Compaction (designs 23, 28)** | whole-log fold into a base, streamed; value chunks ≥ ½ live are shared between bases instead of copied (design 28): 10M × 100 B with 1% churn 26–27 s → 6.7 s, 1,158 → 213 MiB written. The key run is still rewritten whole each time; peak RSS 1.4–1.9 GiB; commits pause 11–14 ms | Pebble/RocksDB: leveled, incremental, in the background without commit pauses. LMDB: copy-on-write with a free list | O(keys) work and memory per compaction; short commit pauses |
| 5 | **Open / recovery time (designs 24, 25)** | loads the newest index checkpoint (stored runs) and replays the tail: 10M keys 1.97–2.19 s (full replay 26–28 s); still O(index size) | Pebble opens in under 0.2 s | ~10× Pebble; grows with the index |
| 6 | **Snapshot lifetime** | time travel to any LSN and zero-copy branches (stronger than all three); snapshot handles pin compaction for up to `SnapshotMaxAge` (design 27), else history below each branch head is dropped (`410`) (checkpoints and bases are stored in ≤ 64 MiB pieces since design 24) | Pebble/RocksDB: in-memory snapshot handles pin versions until released; LMDB: a read txn pins pages | readers longer than `SnapshotMaxAge` get `ErrSnapshotTooOld` |
| 7 | **TTL** | per-key TTL, deterministic (design 27) | RocksDB: TTL DB / compaction filters; Pebble and LMDB: none built in | expired data is freed only by compaction; no HTTP TTL |
| 8 | **Compression** | key runs are front-coded; values are not compressed | per-block Snappy/Zstd/LZ4 (Pebble, RocksDB); LMDB none | larger disk and object-store footprint |
| 9 | **Point reads** | faster than Pebble in YCSB C while the index fits in RAM (1.83M vs 197k ops/s, 8 clients); the runs made the index `Get` 1.1–1.3× slower than the old map | block cache plus bloom filters | small today; will matter once item 1 moves the index to disk |

What LogEt already has that these engines don't: object storage with
create-only fencing, branches and time travel, crypto-shred with signed
certificates, and TLA+ specs plus a simulator for the log.

## Now: perfect KV, one measured item at a time

Each item gets a design note, a formal model where it applies, tests, a
before/after benchmark against Pebble on the same box, docs and a commit.
Nothing else is added to `main` meanwhile.

1. **Disk-backed key index**: done (design 21): runs in memory-mapped
   files, peak RSS at 10M keys 3,546 → 1,916 MiB (Pebble 740). Left:
   OCC window during bulk loads, open time (item 5), a bounded pread
   cache if page-cache residency proves a problem (design 21 option C).
2. **Range scans and iterators**: done (design 22): `Tx.Iter` with
   prefix/`[start, end)`, reverse, limits and `After` pagination, paged
   over the hot set and runs; HTTP pagination. Design 28 batches a page's
   value reads: 10M full scan 606k → 1.54M keys/s, still 3–5× slower
   than Pebble (7.28M). Left: seek on a live iterator, faster scans of
   randomly loaded values.
3. **Batch and transaction API**: done (design 26): `Batch`/`Apply` with
   size limits checked at add time, `POST /v1/batch`, and sorted bulk
   `Ingest` as one commit (10M keys: 1.45M keys/s, 172 MiB peak vs Pebble
   3.96M/s, 46 MiB). Left: stream pieces while building, let commits run
   during an ingest, batch throughput (335k vs Pebble 2.09M keys/s).
4. **TTL**: done (design 27): per-key expiry stored in the log, read
   time = commit timestamp at the read LSN, expiry as an OCC write,
   collected by compaction. Left: HTTP, ingest, expiry on an idle DB.
5. **Open and snapshots**: open without full replay: done (design 24):
   incremental index checkpoints of stored runs, base stored as run
   pieces (no 1 GiB / ~15M-key cap); 10M-key open 7.9 s (snapshot) / 5.5 s
   (base) → 3.4 / 3.3 s, peak RSS 3.2 / 2.2 GiB → 0.58 / 0.39 GiB. Pinned
   snapshot handles that compaction respects: done (design 27, bounded by
   `SnapshotMaxAge`). Left: checkpoints naming
   base pieces; incremental derived-index sections.
6. **Compaction**: incremental compaction without a commit pause. The
   three problems measured in design 21 (design 23): `writeBase` peaking
   at 6.3 GiB RSS at 10M keys: fixed, the base is streamed (peak 3.2 GiB,
   twice as fast); one `fdatasync` per deleted object on the journal
   (480k deletes = 60 s): fixed, deletes are batched in one journal
   record (1M compaction after A 57.3 → 6.8 s); the OCC recent-commit window as the
   largest heap user in bulk writes: fixed, the writer keeps only commits
   an open attempt can conflict with (heap after a 10M load 561 → 19 MiB).
   Final run (design 23 summary): 10M compaction peak 6.4 → 2.5 GiB RSS,
   1M compaction with ~455k deletes 62 → 6.9 s. Design 28: value chunks at
   least half live are shared between bases instead of copied (10M × 100 B,
   1% churn: 26–27 s → 6.7 s, 1,158 → 213 MiB written per compaction).
   Still open: the key run is rewritten whole (O(keys) per compaction),
   peak RSS not bounded per step (1.4–1.9 GiB at 10M), no merge of small
   chunks, short commit pause.

Also open for KV, no order yet: tests on AWS S3 and Cloudflare R2 (only
MinIO is tested; no real-world deployment yet), online journal shrink,
value compression. There is no replication or HA: one writer per
database (read replicas are listed under "Later").

## After KV

The indexes waiting on branches, in order: topics, documents,
cross-index transactions, vectors; then full-text and a lakehouse export.
Each is ported to the index framework ([index-authoring.md](index-authoring.md))
and must meet the same bar as KV before it merges. Details and entry
criteria: [PIPELINE.md](PIPELINE.md).

## Later, exploratory

- **Read replicas without Raft**: processes that tail the log from object
  storage and serve reads at a known LSN.
- **KMS clients and rekey**: ready-made `KeyProvider`s and a crash-safe
  rekey (dev key → real KEK).
- **Auth beyond tokens**: mTLS, per-branch rights.
- **SQL**: only outside the core, if at all.
- **Multi-writer**: only if a design keeps the single-log simplicity;
  sharding by database comes first.

## Good first issues

Well scoped, with clear tests. Open an issue before starting so work is
not duplicated.

1. `GET /v1/version` in the HTTP API, with a test in `server/server_test.go`.
2. `GET /v1/whoami` returning the calling token's name and scope.
3. `loget scan --keys-only` and `--limit`.
4. `loget log --follow` that tails new entries.
5. Better CLI errors for bad URLs (`s3://` without a bucket should name
   the problem and show an example).
6. Docs: a deployment guide for Cloudflare R2, with whether the S3 tests
   pass against it.
7. An opt-in benchmark of commit latency on real S3 (`LOGET_BENCH_S3`).
