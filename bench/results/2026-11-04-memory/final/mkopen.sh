#!/bin/bash
# mkopen.sh BIN: keep a 10M x 100B checkpointed DB in /workspace/opendata (C 5s)
B=$(readlink -f $1); D=/workspace/opendata; rm -rf $D
date +%T; $B ycsb -system loget -keys 10000000 -value 100 -workloads C -clients 8 -warmup 1s -duration 5s -dir $D -keep -out /workspace/loget-tools/mem/mk > mk.log 2>&1; sync; date +%T; echo MKDONE
