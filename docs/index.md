# LogEt documentation

LogEt is one log with different indexes over it; `main` ships the KV
index. It is pre-1.0 and has no release yet. These docs describe `main`.
Design notes and results dated before 2026-10-31 were written for freshDB,
LogEt's predecessor, and are kept as they were apart from the rename.

## Start here

| Doc | For |
|---|---|
| [README](../README.md) | what LogEt is, quickstart, API tour, guarantees, benchmark summary |
| [getting-started.md](getting-started.md) | KV from the CLI, server mode, Go, the agent demo |
| [ROADMAP.md](ROADMAP.md) | KV gaps vs Pebble/RocksDB/LMDB by impact, and the plan |
| [PIPELINE.md](PIPELINE.md) | which indexes come after KV, in what order, with entry criteria |
| [faq.md](faq.md) | production readiness, platforms, freshDB data, S3/R2, shred |

## Running it

| Doc | For |
|---|---|
| [operations.md](operations.md) | storage URLs and layouts, `loget migrate`, memory and cache sizing, server, backups, compaction |
| [security.md](security.md) | what is encrypted, shred limits, dev-mode keys, network exposure |
| [../SECURITY.md](../SECURITY.md) | how to report a vulnerability |

## How it works

| Doc | For |
|---|---|
| [architecture.md](architecture.md) | stores, manifests, fencing, writer and group commit, indexes, branches, shred |
| [formats.md](formats.md) | byte layouts: bucket keys, journal file, segments, payloads, manifests, snapshots |
| [compatibility.md](compatibility.md) | format version stamps, upgrade/downgrade policy and tested matrix, golden files |
| [index-authoring.md](index-authoring.md) | how to write an index: the contract, the counter example, the tests |
| [design/20-index-framework.md](design/20-index-framework.md) | design: the generic index framework and KV-only core |
| [design/21-disk-key-index.md](design/21-disk-key-index.md) | design: the KV key index in memory-mapped run files |
| [design/22-range-reads.md](design/22-range-reads.md) | design: range reads and iterators |
| [design/23-compaction-memory.md](design/23-compaction-memory.md) | design: streamed compaction bases, batched journal deletes, bounded OCC window |
| [design/24-open-checkpoints.md](design/24-open-checkpoints.md) | design: open from index checkpoints, bases as run pieces |
| [design/25-memory.md](design/25-memory.md) | design: memory at 10M keys, bounded index mode, open time |
| [design/26-batch-ingest.md](design/26-batch-ingest.md) | design: batches and sorted bulk ingest |
| [design/27-ttl-snapshots.md](design/27-ttl-snapshots.md) | design: deterministic per-key TTL, snapshot handles |
| [design/28-incremental-compaction.md](design/28-incremental-compaction.md) | design: value chunks shared across compactions, batched scan reads |
| [design/29-scan-path.md](design/29-scan-path.md) | design: scan read path (heap merge, batched value lookups, read-ahead) |
| [components/log.md](components/log.md), [components/kv.md](components/kv.md) | component reviews: the log, the KV index |
| [design-principles.md](design-principles.md) | why one log, and what we refuse to build |
| [design/01-journal-default.md](design/01-journal-default.md) | design: journal as the default local layout, migration |
| [design/02-read-path.md](design/02-read-path.md) | design: point reads, the off-heap value cache |
| [design/03-compaction.md](design/03-compaction.md) | design: compaction base images, garbage with a grace period |
| [design/05-auth-tls-keys.md](design/05-auth-tls-keys.md) | design: bearer-token auth, TLS, key providers |
| [design/07-memory.md](design/07-memory.md) | design: where RSS goes, packed versions, `GOMEMLIMIT` curve, fair Pebble comparison |
| [design/08-gc-retention.md](design/08-gc-retention.md) | design: GC of old logs, manifests and fences with a retention window; journal shrink |
| [design/09-journal-prealloc.md](design/09-journal-prealloc.md) | design: growing the journal file off the commit path |
| [design/11-auth-scopes.md](design/11-auth-scopes.md) | design: scoped tokens, rotation, certificate reload, stream/long-poll caps |
| [design/12-durability.md](design/12-durability.md) | design: size limits, journal corruption vs torn tail, repair opt-in, power-loss crash-point test |
| [design/13-format-versioning.md](design/13-format-versioning.md) | design: version stamps, `ErrVersion`, golden databases, compatibility policy |
| [design/14-observability.md](design/14-observability.md) | design: `/metrics`, `/readyz`, structured/access/slow logs, pprof, `DB.Stats` |
| [design/15-backup.md](design/15-backup.md) | design: online backup (delete hold), incremental copy, digest record, restore, PITR |
| [design/16-operational-safety.md](design/16-operational-safety.md) | design: graceful drain/shutdown, bind before open, per-request timeouts, key limits, lease records |
| [design/17-s3-correctness.md](design/17-s3-correctness.md) | design: S3 fault injection, request cost, `s3check` |
| [design/18-soak-chaos.md](design/18-soak-chaos.md) | design: the soak and chaos harness |
| [s3.md](s3.md) | running on S3: required guarantees, retries, cost per operation, `s3check`, tested-on matrix |
| [api-stability.md](api-stability.md) | semver, which surfaces are stable-track vs experimental, deprecation policy |
| [threat-model.md](threat-model.md) | assets, actors, threats and their status |
| [release-checklist.md](release-checklist.md) | gates before any release |

## Evidence and numbers

| Doc | For |
|---|---|
| [benchmarks.md](benchmarks.md) | every benchmark table, method, environment and caveat; "Changes since v0.1.0" |
| [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) | production-readiness audit: every known gap by severity, with status |
| [STATUS.md](STATUS.md) | finished work items with commit and measured result |
| [../spec/RESULTS.md](../spec/RESULTS.md) | TLA+ specs and TLC results |
| [../bench/](../bench/) | the `loget-bench` suite and raw results |

## Project

[ROADMAP](ROADMAP.md) · [PIPELINE](PIPELINE.md) · [CHANGELOG](../CHANGELOG.md) ·
[CONTRIBUTING](../CONTRIBUTING.md) · [GOVERNANCE](../GOVERNANCE.md) ·
[website](https://raman20.github.io/loget/) · [rebrand notes](rebrand/)
