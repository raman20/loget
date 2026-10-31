#!/bin/bash
# usage: open.sh BIN OUT [KEYS...]; per KEYS: load (C only, keep) -> open replay; load+compact (keep) -> open from base
B=$(readlink -f $1); T=bench/results/2026-11-01-open-ckpt/$2; mkdir -p $T; shift 2
for N in ${@:-1000000 10000000}; do
 for mode in ${MODES:-replay base}; do
  D=/workspace/i5data-$mode; rm -rf $D
  extra=""; [ $mode = base ] && extra="-compact-between"; [ $mode = snap ] && extra="-snapshot-every 2000"
  date +%T; $B ycsb -system loget -keys $N -value 100 -workloads C -clients 8 -warmup 1s -duration 5s -dir $D $extra -keep -out $T/ycsb-$N-$mode > $T/ycsb-$N-$mode.log 2>&1
  sync
  $B open -system loget -dir $D/ycsb-loget-true -keys $N -reads 200000 -out $T/open-$N-$mode > $T/open-$N-$mode.log 2>&1
  du -sh $D >> $T/open-$N-$mode.log; rm -rf $D
 done
done
date +%T; echo RUNDONE
