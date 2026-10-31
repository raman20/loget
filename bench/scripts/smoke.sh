#!/usr/bin/env bash
# Tiny run of every suite (about 2 minutes). Checks the harness works and the
# correctness checks inside the benchmarks hold. The numbers mean nothing.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT:-$(mktemp -d)}
D=$(mktemp -d)
go build -o bin/loget-bench ./cmd/loget-bench
B=bin/loget-bench
$B calib -duration 1s -out "$OUT"
for sys in loget badger pebble bbolt sqlite; do
  $B ycsb -system $sys -keys 5000 -workloads A,C,F,D -clients 1,8 -warmup 200ms -duration 500ms -dir "$D" -out "$OUT"
done
$B txn -clients 8 -hot 1,64 -duration 500ms -dir "$D" -out "$OUT"
if [[ -n "${LOGET_BENCH_S3:-}" ]]; then
  $B s3 -batch 1,10 -clients 1,4 -segments 20 -duration 500ms -out "$OUT"
fi
$B report -dir "$OUT"
# the suites record their own correctness checks; fail if any is false
if grep -Eq '"(invariant_ok|exactly_once_check)": false' "$OUT"/*.json; then
  echo "correctness check failed in a benchmark" >&2; exit 1
fi
echo "smoke ok: $OUT"
