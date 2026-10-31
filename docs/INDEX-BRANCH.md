# index/topics: append-only streams, consumer groups, retention, push

Status: **parked**. Main ships only KV.

## What belongs to this index (file list as of the import)

- Index: `lens/topic.go`, `lens/topic_bench_test.go`, `lens/topic_fuzz_test.go`,
  `lens/topic_prop_test.go`. Record kind: `format.OpAppend` in spaces `topic:<name>`.
- Engine: the `TopicTx`, `Group` and `Message` APIs in `tx.go`; topic sections
  in `snapshot.go` and in the compaction base (`compact.go`: `TopicRetention`,
  `topicCuts`, base messages); `notify.go` (change notification / push).
- Tests: `topic_test.go`, `topic_bench_test.go`, `topic_retention_test.go`,
  `notify_test.go`, `server/push_test.go`, `server/topic_cap_test.go`.
- Server: `POST|GET /v1/topics/{t}`, `POST /v1/topics/{t}/groups/{g}/next`,
  `GET /v1/topics/{t}/stream` (SSE push).
- Specs: `spec/LogEtRetain*`, `spec/LogEtNotify*` (LogEtTxn on main covers OCC
  generically; its consume+ack workload came from this index).
- Docs: `docs/components/topics.md`, `docs/design/06-push-consumers.md`,
  `docs/design/08-gc-retention.md` (topic retention part).
- Review: component C (freshDB), done 2026-10-30.

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
