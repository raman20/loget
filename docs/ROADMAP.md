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
| 1 | **Key index mostly in RAM → mapped run files (design 21)** | runs of ≥ 1 MiB live in unlinked, memory-mapped files (file-backed, reclaimable pages); the hot set, small runs and block offsets stay on the heap. 10M × 100 B YCSB: peak RSS 3,546 → 1,916 MiB, end 920 MiB of which 319 MiB file-backed; 1M × 1 KiB: 958 → 640 MiB peak. Open loads the newest index checkpoint (design 24): 3.4 s at 10M keys, 26–28 s replaying | Pebble 740 MiB peak, 53 MiB end at 10M; opens in under 0.2 s | design 25 (2026-11-04): OCC window bounded (item 6), 32 MiB value cache on local stores, merges drop consumed pages, open 3.4 → 2.0 s; 10M peak 557 MiB (Pebble 737), end 512 MiB mapped / 274 MiB with opt-in `IndexResidentBytes` (Pebble 52). Remaining: no hard bound (pread + block cache not built), ~150 MiB Go heap, open ~10× Pebble |
| 2 | **Range reads → iterators (design 22)** | `Tx.Iter` with prefix and `[start, end)` bounds, reverse, limit, `After` pagination (HTTP `after=` / `Loget-Next-After`), paged under the read lock so memory stays bounded; OCC conflicts cover only the returned range. Full iterator scan at 10M × 100 B: 498k keys/s, no growth over the resident index (before: 207k keys/s via the slice API, heap peak 4.8 GiB); 100-key prefix scan 6.2k/s at c=1 | Pebble 7.2M keys/s full scan, 39k/s prefix100 at c=1, flat 163 MiB RSS; a live iterator can `SeekGE`/`SeekLT` | one value open per key (no readahead), so scans are 4–15× slower than Pebble; no seek on a live iterator (open a new one with `Start`/`After`) |
| 3 | **Batches and bulk load** | every write is an OCC transaction (`Update`, auto-retry); one segment and one `fdatasync` per group commit; index ingest 2.0–2.9 µs/key | Pebble `Batch`/`IndexedBatch`, SST ingestion; RocksDB `WriteBatch`, `IngestExternalFile`; LMDB `MDB_APPEND` | no write-only fast path; no bulk import; no per-write durability choice |
| 4 | **Compaction** | whole-log: writes a base image of live data per log, then deletes garbage after a grace period. Commits pause 11–14 ms; branch operations wait for the whole run; journal file shrinks only offline | Pebble/RocksDB: leveled, incremental, in the background without commit pauses. LMDB: copy-on-write with a free list, no compaction | pauses and O(live data) rewrites per run |
| 5 | **Open / recovery time** | loads the newest index checkpoint (stored runs, written in the background every 512 MiB of segments and at close; design 24) and replays the tail: 10M keys 3.4 s / 580 MiB peak (full replay 26–28 s); still O(index size): pieces are copied and hash slots rebuilt | replay the WAL into a memtable only | open time grows with the index; after a crash, the tail since the last checkpoint is replayed |
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
   over the hot set and runs; HTTP pagination. Full iterator scans 2.4x
   faster than before with memory bounded by a page; still 4-15x slower
   than Pebble (one value open per key). Left: value readahead, seek on a
   live iterator.
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
   1M compaction with ~455k deletes 62 → 6.9 s. Still open: incremental compaction (whole-log rewrite; the base is
   stored in pieces since design 24, so the 1 GiB object limit is gone).

Also open for KV, no order yet: tests on AWS S3 and Cloudflare R2 (only
MinIO is tested), online journal shrink, value compression.

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
