# Open time and peak RSS before/after design 24 (2026-11-01)

Box: 8 vCPU / 16 GiB shared VM, ext4, files/journal default layout, fsync on,
page cache warm (dropping it needs root). Single runs.

- `before/`: commit f63ec57 (+ the bench's `-snapshot-every` flag), 02:17–02:22 IST,
  script `open-before.sh` (modes replay, base, snap).
- `after/`: commit 677521b (+ the bench's `-checkpoint-bytes` flag), 03:23–03:29 IST,
  script `open-after.sh` (modes replay = `-checkpoint-bytes -1`, ckpt = defaults,
  base = compacted without checkpoints, baseckpt = compacted with the close checkpoint).

Each case: `loget-bench ycsb -keys N -value 100 -workloads C -keep` then
`loget-bench open` in a new process; `peak_rss` in `open-*/open-loget.json`
is that process's VmHWM. Summary table: docs/design/24-open-checkpoints.md.
