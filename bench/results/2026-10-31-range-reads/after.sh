#!/bin/bash
set -x
B=/workspace/loget-tools/loget-bench-scan-after
O=/workspace/loget-tools/item2/after
mkdir -p $O
for s in loget pebble; do
  $B scan -system $s -keys 1000000 -out $O/1m 2>&1 | tee $O/1m-$s.log
done
$B scan -system loget -compact -keys 1000000 -out $O/1m-compact 2>&1 | tee $O/1m-compact.log
$B scan -system loget -index-in-memory -keys 1000000 -out $O/1m-inmem 2>&1 | tee $O/1m-inmem.log
for s in loget pebble; do
  $B scan -system $s -keys 10000000 -value 100 -out $O/10m 2>&1 | tee $O/10m-$s.log
done
echo AFTERDONE
