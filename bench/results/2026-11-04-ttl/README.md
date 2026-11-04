# 2026-11-04 TTL and snapshots (design 27)

- base27-*: commit 27e01a6 (before design 27)
- loget-*: design 27, keys without expiry
- ttl-index.txt: design 27, `LOGET_KV_EXP=1` (every key has an expiry)

`go test -run '^$' -bench 'KVBuild|KVGet|KVScan100|KVSnapshotLoad' -count 5 ./index`
and `-bench 'PutSerial$|PutParallel$|Get$' -count 5 .`; 1M keys; same box,
runs back to back. Tables in docs/design/27-ttl-snapshots.md.
