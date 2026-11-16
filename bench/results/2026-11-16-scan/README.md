# Scan read path vs Pebble (design 29, 2026-11-16)

`loget-bench scan -system {loget,pebble} -keys 10000000 -value 100
-phases full-iter,full-reverse -profile DIR`: 10M keys loaded in key
order (batches of 1,000), then one full forward and one full reverse
iterator scan, one client. Default options (journal layout, no fsync in
this suite). 8-CPU shared VM; single runs, expect ±10% noise. CPU is the
profile's sample total for the phase.

| step | commit | fwd keys/s | rev keys/s | fwd wall / CPU | load keys/s | scan RSS peak |
|---|---|---:|---:|---|---:|---:|
| before | 22de0f0 | 1,540,231 | 1,577,261 | 6.49 s / 7.85 s | 342k | 256 MiB |
| 1 value lookups (no fmt, cached segment spans) | 1a2917d | 2,621,016 | 3,242,842 | 3.82 s / 5.02 s | 319k | 260 MiB |
| 2 heap merge in KV.Range | 2abf0f8 | 3,455,639 | 3,255,049 | 2.89 s / 3.81 s | 358k | 258 MiB |
| 3 reused iterator buffers | 45c6d9f | 3,255,740 | 3,295,806 | 3.07 s / 3.89 s | 364k | 257 MiB |
| 4 one page of read-ahead | 5135489 | 5,046,354 | 5,098,787 | 1.98 s / 3.92 s | 358k | 261 MiB |
| 5 cheaper per-key decode (varint fast path, bound checks per block) | 90e9bda | 5,680,824 | 5,387,354 | 1.76 s / 3.41 s | 373k | 260 MiB |
| 5, rerun at the end (box shared with an S3-sim job) | 90e9bda | 5,346,067 | 5,845,299 | – | 371k | 261 MiB |
| Pebble v2 | – | 6,405,070 | 5,747,974 | 1.56 s / 1.49 s | – | 503 MiB |

Step 3 cut bytes allocated per forward scan from 4.18 GB to 2.75 GB with
no measurable throughput change. Step 4 keeps CPU time flat and uses a
second core for value reads.

Pebble rerun at the end: 7,269,514 fwd / 6,179,270 rev keys/s.

## Write, compaction, memory: before (22de0f0) vs after (90e9bda)

One run each, same box, `side/*.log`. The after binary also contains
99de183 (batched compaction value reads, not part of design 29), which
is what moved the first compaction.

| metric | before | after |
|---|---:|---:|
| batch puts, 10M x 100 B, fsync (`ingest -mode batch`) | 363,919 keys/s, peak 247 MiB | 370,206 keys/s, peak 247 MiB |
| recompact cycle 0 / 1 / 2 (ms) | 29,315 / 7,174 / 6,982 | 9,584 / 6,506 / 6,735 |
| recompact peak RSS cycle 0 / 1 / 2 | 1.46 / 1.40 / 1.80 GiB | 1.21 / 1.38 / 1.73 GiB |
| YCSB C / A, 8 clients, ops/s | 1,536,672 / 114,693 | 1,882,305 / 125,921 |
| RSS after C / after A | 414 / 510 MiB | 413 / 514 MiB |

Files: `<step>-scan-loget.json` / `.log` per step, `pebble-*` for Pebble.
