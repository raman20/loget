# index/vectors: exact + HNSW approximate vector search

Status: **parked**. Main ships only KV.

## What belongs to this index (file list as of the import)

- Index: `lens/vector.go`, `lens/vector_test.go`, `lens/vector_fuzz_test.go`,
  `lens/hnsw.go`, `lens/hnsw_persist.go`, `lens/hnsw_*_test.go`. Records: `OpPut` / `OpDel`
  in spaces `vec:<collection>`; persisted graph images (`UHNSW1`).
- Engine: `VecTx` in `tx.go`; `ann.go` (graph persistence and restore);
  `EnableANN` and `ANNOptions` in `branch.go`; the `vec` field in `view.go`;
  the `vec:` synthetic entries in `compact.go`.
- Tests: `ann_test.go`, `ann_persist_test.go`, `vec_tx_test.go`,
  `vecsearch_test.go`, `vec_bench_test.go`.
- Server: `PUT|DELETE /v1/vectors/{c}/{id}`, `POST /v1/vectors/{c}/search`,
  `POST /v1/vectors/{c}/ann`.
- Specs: `spec/LogEtANN*`.
- Docs: `docs/components/vectors.md`, `docs/design/04-hnsw.md`,
  `docs/design/10-hnsw-persist.md`, `docs/design/19-vectors-next.md`.
- Review: component E (freshDB), done (commit 309aedc there). Duplicates now
  share a node: recall@10 on 35 directions × 5k vectors went from 0.12 to 1.00.

## How this branch relates to main

Terminology: main calls these *indexes* (one log, different indexes over
it). The code on this branch predates that and still says *lens*
(package `lens`, `lens/topic.go`, ...); the port renames it.

This branch is the full LogEt import (commit e5872c3, which is freshDB
5a08052 renamed) plus this note. On this branch the index is wired into
the core the old way: `view.go`, `snapshot.go` and `compact.go` have
fields hard-coded for it. Main replaced that wiring with the generic
index framework (`docs/index-authoring.md` on main). Nothing on this branch
has been ported yet.

Pipeline position and entry criteria: `docs/PIPELINE.md` on main.

## Port plan (do these in order when the index's turn in the pipeline comes)

1. Rebase the index onto main's `index.Index` + `index.Kind` interface. State
   must come only from `Apply` over log records, and the index must declare
   its spaces.
2. Make snapshot and restore go through `Kind.Restore` / `Snapshot`, not
   through fields in the core. Make the compaction base handle this
   index's records the generic way.
3. Add the read API on `Tx`. Record every read in the read set
   (`tx.ReadKey` / `tx.ReadPrefix`) so the core's OCC covers it.
4. Bring back the tests, simulator ops, server routes, specs and docs
   listed above. Change each one to match the new interface; don't copy
   any of them back as they are.
5. Run a component review on the index (same depth as the freshDB A–E
   reviews), with before/after measurements and the TLC results.
