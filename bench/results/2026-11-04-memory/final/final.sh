#!/bin/bash
# final.sh: design 25 final runs with bench-f (YCSB mapped/bounded, open x3 each, scan mapped/bounded)
cd /workspace/loget-tools/mem
./run.sh bench-f f-mapped
./run.sh bench-f f-bounded -index-resident 67108864
./mkopen.sh bench-f
for m in 0 67108864; do for i in 1 2 3; do ./bench-f open -system loget -dir /workspace/opendata/ycsb-loget-true -keys 10000000 -reads 200000 -index-resident $m -out f-open-$m >> f-open-$m.log 2>&1; done; done
rm -rf /workspace/opendata
for m in 0 67108864; do ./bench-f scan -system loget -keys 10000000 -value 100 -clients 8 -phases prefix100,full-iter -dir /workspace/scandata -index-resident $m -out f-scan-$m > f-scan-$m.log 2>&1; done
rm -rf /workspace/scandata
date +%T; echo FINALDONE
