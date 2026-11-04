# Bulk ingest and batch puts vs Pebble (2026-11-04)

`loget-bench ingest -system {loget,pebble} -mode {bulk,batch} -n 10000000`
(bench/cmd/loget-bench/ingest.go): 10M sorted keys `key%012d`, 100-byte
values, fsync on, local disk (LogEt: default journal layout with run
files; Pebble v2.1.7 defaults). One process per row, so peak RSS is
VmHWM of that run. Time is from open to the data being durable and
readable (Pebble bulk: write one sstable + `DB.Ingest`). One run each on
an 8-CPU, 15 GiB VM; expect noise of ±10–20 %.

| system | mode | keys/s | time | peak RSS |
|---|---|---:|---:|---:|
| LogEt  | bulk (`Ingest`) | 1,446,309 | 6.91 s | 172 MiB |
| Pebble | bulk (sstable + `Ingest`) | 3,963,331 | 2.52 s | 46 MiB |
| LogEt  | batch of 1,000 (`Apply`) | 335,073 | 29.84 s | 249 MiB |
| Pebble | batch of 1,000 (`Commit(Sync)`) | 2,088,630 | 4.79 s | 35 MiB |

LogEt's bulk path is 4.4× faster than its own batches but 2.7× slower
than Pebble's, with 3.7× the peak RSS (one 64 MiB piece in flight plus
the store's copy of it). Before reading each piece's pages once and
dropping them (index/ingest.go `Finish`), the LogEt bulk peak was
1,124 MiB. Batch puts are 6.2× slower than Pebble's. Not tuned yet:
pieces are uploaded one at a time after the build, not streamed.
