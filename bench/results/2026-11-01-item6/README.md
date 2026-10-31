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
| `fix2-batched-deletes/` | part 2 (batched journal deletes, garbage batches of 1024) | 01:08-01:12 |
| `fix3-occ-window/` | part 3 (OCC window bounded by open snapshots) | 01:54-01:58 |

`fix2-batched-deletes/sweep-bench.txt` has `BenchmarkCompactSweepPause`
before and after part 2 and the time one `Journal.DeleteBatch` holds the
journal's write lock by batch size.

`compact after X` lines in the logs (and `"Kind": "compact"` entries in
the JSON) give each compaction's time, deleted objects and peak RSS, anon
and Go heap sampled every 20 ms.

`fix3-occ-window/put-bench.txt` has `BenchmarkPutSerial` and
`BenchmarkPutParallel` at part 2's commit and with part 3.
