# after: design 21 run files (bench binary at 9c5f706 + local flags)

Same box as before/ (8 vCPU, 16 GiB, ext4), 2026-10-31 22:59-23:31 IST, one engine at a time.
`loget` = run files (default), `loget-index-in-memory` = same binary with the whole index on the heap.
Both include the GetRange fix (9c5f706); before/ (d013307) does not, so before -> after mixes
design 21 with that fix; files vs index-in-memory isolates design 21.

- 1m-*: YCSB 1M x 1 KiB, A,B,C x 1/8/64 clients. vc8-*: same with an 8 MiB value cache, A,C x 8.
- 10m-*: YCSB 10M x 100 B, C,A x 8. 10m-loget is the -keep rerun used for the opens
  (log-first-run: the identical first run, whose kept directory a later run reused).
- open-{1,2}-*: reopen of the 10M directory (no snapshot: full log replay), 200k uniform reads.
- 10m-base + open-base-*: same with Compact between workloads, then reopen from the base.
- ab-compact-*: 1M x 100 B, C,A with Compact between. ab-read-*: 1M x 1 KiB, C x 8, interleaved.
CPU/heap profiles are kept outside the repo.
