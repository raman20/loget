#!/bin/bash
# usage: run.sh BIN OUTNAME ; 10M x 100B C,A compact-between, then 1M x 100B
B=$1; T=/workspace/loget-tools/item6/$2; D=/workspace/i6data
mkdir -p $T
date +%T; $B ycsb -system loget -keys 10000000 -value 100 -workloads C,A -clients 8 -warmup 1s -duration 10s -dir $D -compact-between -out $T/10m > $T/10m.log 2>&1; rm -rf $D
date +%T; $B ycsb -system loget -keys 1000000 -value 100 -workloads C,A -clients 8 -warmup 1s -duration 10s -dir $D -compact-between -out $T/1m > $T/1m.log 2>&1; rm -rf $D
date +%T; echo RUNDONE
