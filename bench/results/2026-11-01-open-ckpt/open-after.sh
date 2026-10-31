#!/bin/bash
# usage: open2.sh BIN OUT [KEYS...]; MODES: replay (no checkpoints), ckpt (defaults: background + close checkpoint),
# base (compacted, no checkpoints), baseckpt (compacted, defaults)
B=$(readlink -f $1); T=bench/results/2026-11-01-open-ckpt/$2; mkdir -p $T; shift 2
for N in ${@:-1000000 10000000}; do
 for mode in ${MODES:-replay ckpt base baseckpt}; do
  D=/workspace/i5data-$mode; rm -rf $D
  extra=""
  case $mode in replay) extra="-checkpoint-bytes -1";; base) extra="-compact-between -checkpoint-bytes -1";; baseckpt) extra="-compact-between";; esac
  date +%T; $B ycsb -system loget -keys $N -value 100 -workloads C -clients 8 -warmup 1s -duration 5s -dir $D $extra -keep -out $T/ycsb-$N-$mode > $T/ycsb-$N-$mode.log 2>&1
  sync
  find $D -path '*ckpt*' | wc -l >> $T/open-$N-$mode.log
  $B open -system loget -dir $D/ycsb-loget-true -keys $N -reads 200000 -out $T/open-$N-$mode >> $T/open-$N-$mode.log 2>&1
  du -sh $D >> $T/open-$N-$mode.log; rm -rf $D
 done
done
date +%T; echo RUNDONE
