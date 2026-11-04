#!/bin/bash
# usage: run.sh BIN OUT [extra flags]; 10M x 100B C,A x8 (design 21 memory case)
B=$(readlink -f $1); T=/workspace/loget-tools/mem/$2; shift 2; D=/workspace/memdata; rm -rf $D; mkdir -p $T
date +%T; $B ycsb -system loget -keys 10000000 -value 100 -workloads C,A -clients 8 -warmup 1s -duration 10s -dir $D "$@" -out $T > $T/log 2>&1; rm -rf $D
date +%T; echo RUNDONE
