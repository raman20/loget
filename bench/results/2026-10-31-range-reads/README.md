# Range reads (design 22): before/after vs Pebble

Box: the reference box (8 vCPU, 16 GiB, ext4), 2026-10-31/11-01 IST.
`loget-bench scan` (bench/cmd/loget-bench/scan.go); scripts `before.sh`
and `after.sh` (paths are the local scratch layout).

- before: 23:53-23:58 IST at 544c248, binary built before the library
  change (no LogEt `[start, end)`/reverse, so range50/reverse50 and
  full-iter exist for Pebble only, and full-iter not at all).
- after: 00:14-00:25 IST at 7179c12. The 10M LogEt run was repeated
  alone (00:23-00:25) because a first run was killed by mistake and a
  second overlapped the Pebble run; only the clean run is kept here. The 10M Pebble run was also repeated
  alone (00:27 IST) for the same reason.

Phases: `prefix100` = 100-key prefix scan with values; `range50` = start
key + up to 50 keys; `reverse50` the same descending; `full` = whole space
through the slice API (`Tx.Scan`); `full-iter` / `full-reverse` = whole
space through `Tx.Iter`. RSS "peak" is sampled; Go does not return freed
heap at once, so `full-iter` (run right after `full`) inherits `full`'s
RSS. Use its heap peak, and `full-reverse` RSS, for the iterator.
`1m-inmem` = `-index-in-memory` (design 21 heap mode).
