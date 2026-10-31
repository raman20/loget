#!/bin/bash
set -x
B=/workspace/loget-tools/loget-bench-scan-before
O=/workspace/loget-tools/item2/before
mkdir -p $O
for s in loget pebble; do
  $B scan -system $s -keys 1000000 -out $O/1m 2>&1 | tee $O/1m-$s.log
done
$B scan -system loget -compact -keys 1000000 -out $O/1m-compact 2>&1 | tee $O/1m-compact.log
for s in loget pebble; do
  $B scan -system $s -keys 10000000 -value 100 -out $O/10m 2>&1 | tee $O/10m-$s.log
done
echo BEFOREDONE
