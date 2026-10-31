# index/txn: cross-index transactions (snapshot of the import)

Status: **partly on main**. The OCC core is on main. The cross-index
parts are parked here.

Transactions can't be split out as a separate index. Optimistic
concurrency control (snapshot reads at a log position, read sets, a
conflict check in the single writer, then one atomic frame per commit)
lives in the core. Main keeps it and offers it to every index through the
read-set hooks (`tx.ReadKey` / `tx.ReadPrefix`).

## What this branch keeps that main dropped

- Atomic commits across indexes: consume a message, write KV and
  documents, and ack the group cursor in one transaction (`tx.go`
  `Group.Ack` / `Fetch` combined with `DocTx` and `VecTx`).
- The `POST /v1/tx` op kinds for topics, docs and vectors (`server/server.go`).
- The simulator ops for those indexes (`sim/sim.go`).
- Spec: `spec/LogEtTxn.tla` models OCC with the consume+ack workload.
  That spec is also on main, where it still checks the core OCC rules.

## Port plan

Each index brings back its own transaction API when its turn in the
pipeline comes. This branch is the reference for how those APIs combined.
Main's `POST /v1/tx` handles KV only until then.

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
