# Index pipeline

LogEt is one log with different indexes over it. Indexes reach `main` one
at a time, each finished before the next starts. "Finished" means the bar
below, not "has features".

## The bar for merging an index

1. Written against the index framework ([index-authoring.md](index-authoring.md)):
   no index-specific code in the core.
2. Passes the contract tests (fold equivalence, compaction equivalence,
   shred equivalence, canonical snapshots, a fuzzer on every decoder).
3. Driven by the simulator under crashes and faults, checked against a model.
4. A TLA+ spec where the index adds protocol (cursors, retention, waiters).
5. Benchmarked against the best-known engine for its job, on the same box,
   with the numbers and the losses in the docs.
6. Component review written (defects found, tests, verdict).
7. KV's own items on [ROADMAP.md](ROADMAP.md) are done first.

## Stages

| # | index | status | where the code is | entry criteria to start | compare against |
|---|---|---|---|---|---|
| 1 | **KV** (record store, MVCC, prefix scan, time travel) | **on `main`; KV roadmap items 1–4 done, gaps left (key-run rewrite per compaction, steady memory, open time, scans)** | `index/kv.go`, `index/kvrun.go` | started | Pebble, RocksDB, LMDB, bbolt, Badger |
| 2 | **Topics** (streams, consumer groups, retention, push) | parked | branch `index/topics` (old wiring and old package name) | KV items 1–6 done; framework supports per-index read-your-writes | NATS JetStream, Kafka/Redpanda |
| 3 | **Documents** (JSON, document field indexes) | parked | branch `index/docs` | topics merged; online index build without stalling writers | SQLite JSON, MongoDB (embedded comparisons only) |
| 4 | **Cross-index transactions** (consume + write + ack in one commit) | OCC core on `main`; cross-index APIs parked | branch `index/txn` | topics and documents merged | Postgres outbox pattern |
| 5 | **Vectors** (exact + HNSW) | parked | branch `index/vectors` | stage 4 done; graph saved without a crash cliff | flat scan, hnswlib, pgvector |
| 6 | **Full-text** | not started | none | stage 5 done; design note | Bleve, Tantivy, SQLite FTS5 |
| 7 | **Lakehouse export** (Parquet/Iceberg by LSN range) | not started | none | stage 6 done; design note | DuckDB reading exports |

Each parked branch carries a `docs/INDEX-BRANCH.md` listing its files,
tests, specs and the port plan. The code on those branches predates the
framework and the rename: it still uses its old package name and
hard-coded wiring in `view.go`, `snapshot.go` and `compact.go`. Porting
an index means rewriting that wiring as an `index.Kind`, as
[examples/counter](../examples/counter) shows.

## What carries over from the branches

The component reviews of topics, documents and vectors (defects, fuzzers,
mutation results, benchmarks) were done on freshDB and are in each
branch's `docs/components/`. Their findings stand; their numbers must be
remeasured after the port.
