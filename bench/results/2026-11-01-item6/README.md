# ROADMAP item 6: compaction memory, journal deletes, OCC window

Design: [docs/design/23-compaction-memory.md](../../../docs/design/23-compaction-memory.md).
Reference box (8 vCPU, 16 GiB, ext4), journal layout, fsync on, single
runs. Each directory was produced by `run.sh BIN NAME` with a
`loget-bench` built at the commit named below:

`ycsb -system loget -keys {10000000,1000000} -value 100 -workloads C,A
-clients 8 -warmup 1s -duration 10s -compact-between`

| directory | code | IST, 2026-11-01 |
|---|---|---|
| `before/` | 94b2362 + the 20 ms compaction peak sampler | 00:31-00:38 |
| `fix1-streaming-base/` | part 1 (streamed base) | 00:40-00:46 |

`compact after X` lines in the logs (and `"Kind": "compact"` entries in
the JSON) give each compaction's time, deleted objects and peak RSS, anon
and Go heap sampled every 20 ms.
