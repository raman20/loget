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
| 1 | **Key index mostly in RAM → mapped run files (design 21)** | runs of ≥ 1 MiB live in unlinked, memory-mapped files (file-backed, reclaimable pages); the hot set, small runs and block offsets stay on the heap. 10M × 100 B YCSB: peak RSS 3,546 → 1,916 MiB, end 920 MiB of which 319 MiB file-backed; 1M × 1 KiB: 958 → 640 MiB peak. Open still rebuilds the index: ~33 s at 10M keys without a snapshot | Pebble 740 MiB peak, 53 MiB end at 10M; opens in under 0.2 s | remaining RAM users: OCC recent-commit window during bulk writes, value cache, Go GC headroom; open time |
| 2 | **Range reads → iterators (design 22)** | `Tx.Iter` with prefix and `[start, end)` bounds, reverse, limit, `After` pagination (HTTP `after=` / `Loget-Next-After`), paged under the read lock so memory stays bounded; OCC conflicts cover only the returned range. Full iterator scan at 10M × 100 B: 498k keys/s, no growth over the resident index (before: 207k keys/s via the slice API, heap peak 4.8 GiB); 100-key prefix scan 6.2k/s at c=1 | Pebble 7.2M keys/s full scan, 39k/s prefix100 at c=1, flat 163 MiB RSS; a live iterator can `SeekGE`/`SeekLT` | one value open per key (no readahead), so scans are 4–15× slower than Pebble; no seek on a live iterator (open a new one with `Start`/`After`) |
| 3 | **Batches and bulk load** | every write is an OCC transaction (`Update`, auto-retry); one segment and one `fdatasync` per group commit; index ingest 2.0–2.9 µs/key | Pebble `Batch`/`IndexedBatch`, SST ingestion; RocksDB `WriteBatch`, `IngestExternalFile`; LMDB `MDB_APPEND` | no write-only fast path; no bulk import; no per-write durability choice |
| 4 | **Compaction** | whole-log: writes a base image of live data per log, then deletes garbage after a grace period. Commits pause 11–14 ms; branch operations wait for the whole run; journal file shrinks only offline | Pebble/RocksDB: leveled, incremental, in the background without commit pauses. LMDB: copy-on-write with a free list, no compaction | pauses and O(live data) rewrites per run |
| 5 | **Open / recovery time** | restores the latest snapshot and replays the tail; without snapshots, replays every segment | replay the WAL into a memtable only | slow opens on big logs without `WithSnapshotEvery` |
| 6 | **Snapshot lifetime** | time travel to any LSN and zero-copy branches (stronger than all three); but compaction drops history below each branch head (`410`), and a checkpoint snapshot is one object (> 5 GiB fails on AWS S3) | Pebble/RocksDB: in-memory snapshot handles pin versions until released; LMDB: a read txn pins pages | long readers can lose history to compaction |
| 7 | **TTL** | none | RocksDB: TTL DB / compaction filters; Pebble and LMDB: none built in | expiry must be done by the app |
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
3. **Batch and transaction API**: write-only batches, bulk load, explicit
   transaction size limits.
4. **TTL**: per-key expiry that is still a pure function of the log
   (expiry by entry timestamp, never by wall clock at read time).
5. **Snapshots**: pinned snapshot handles that compaction respects;
   multi-part checkpoint snapshots for large indexes.
6. **Compaction**: incremental compaction without a commit pause. The
   three problems measured in design 21 (design 23): `writeBase` peaking
   at 6.3 GiB RSS at 10M keys: fixed, the base is streamed (peak 3.2 GiB,
   twice as fast); one `fdatasync` per deleted object on the journal
   (480k deletes = 60 s): fixed, deletes are batched in one journal
   record (1M compaction after A 57.3 → 6.8 s); the OCC recent-commit window as the
   largest heap user in bulk writes: fixed, the writer keeps only commits
   an open attempt can conflict with (heap after a 10M load 561 → 19 MiB).
   Still open: incremental compaction (one base object per log, journal
   limit 1 GiB).

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
