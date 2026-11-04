# Design 28 benchmarks (2026-11-04)

Box: shared Linux VM, local fs layout (no MinIO; the test MinIO is at
127.0.0.1:9100, :9000 refuses connections; not used here). Single runs.

## Repeated compaction, 10M keys x 100 B, 1% random overwrites between

`loget-bench recompact -keys 10000000 -value 100 -cycles 3 [-copyall]`.
`-copyall` = every value copied each time (the behaviour before design 28).

| cycle | copy-all ms | written MiB | peak RSS | shared ms | written MiB | shared chunks | peak RSS |
|---|---|---|---|---|---|---|---|
| 0 (first) | 26,546 | 1,157.7 | 1.46 GiB | 25,727 | 1,157.7 | 0 | 1.47 GiB |
| 1 | 27,294 | 1,157.7 | 1.59 GiB | 6,719 | 213.5 | 15 | 1.39 GiB |
| 2 | 80,691 (outlier) | 1,157.7 | 1.93 GiB | 6,749 | 213.5 | 16 | 1.77 GiB |
| 3 | 26,095 | 1,157.7 | 2.14 GiB | 6,781 | 213.4 | 17 | 1.85 GiB |

The 213 MiB still written per cycle is the key run (rewritten whole) plus
the overwritten values. Peak RSS is not reduced (the run is still built in
memory-mapped pages); it is not bounded per step.

## Full scans (design 28 part b, prefetch), `loget-bench scan`

| keys x value | phase | 3ac2e00 keys/s | 34027cd keys/s | Pebble keys/s |
|---|---|---|---|---|
| 10M x 100 B | full-iter | 606k | 1,535k | 7,278k |
| 10M x 100 B | full-reverse | 603k | 1,682k | 6,005k |
| 10M x 100 B | full-iter after compact | 528k | 2,560k | – |
| 1M x 1 KiB | full-iter | 468k | 889k | 1,525k |
| 1M x 1 KiB | full-iter after compact | 487k | 1,062k | – |

The "after" scan runs overlapped with a `go test ./...` run, so they are, if
anything, pessimistic. Keys were loaded in key order, so values are
contiguous; random-order loads gain less from coalescing.
