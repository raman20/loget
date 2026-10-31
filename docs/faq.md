# FAQ

**Is LogEt production-ready?**
No. It is pre-1.0 and has no release yet. Its correctness core is
model-checked and simulation-tested, but it has no production users,
compaction pauses commits, and the KV index keeps every key in RAM. See
the README's [status section](../README.md#status) and
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md).

**What does "LogEt" mean?**
Danish and Norwegian for "the log"; "et" also means "one". One log, with
different indexes over it.

**Where did topics, documents and vectors go?**
LogEt's predecessor, freshDB, had them all in one engine. LogEt finishes
KV first and adds the others back one at a time through the index
framework, each meeting the same testing and benchmarking bar. The code
waits on the `index/topics`, `index/docs`, `index/vectors` and
`index/txn` branches; the order is in [PIPELINE.md](PIPELINE.md).

**Why not just Postgres?**
Postgres is a great default, and if it works for you, keep it. LogEt is for
when you want what a log gives you natively: branches, rewind, time travel,
and per-subject deletion that also covers history and every index. And you want them on object storage, from one
binary, without running a server cluster.

**How is this different from SlateDB?**
SlateDB is an embedded LSM KV store on object storage, written in Rust. It
inspired LogEt's fencing protocol. LogEt is pure Go, keeps every index on
one log with branches, time travel and crypto-shredding, and is far less
mature. SlateDB is the better KV store on object storage today; LogEt's
KV gaps are listed in [ROADMAP.md](ROADMAP.md).

**Which platforms are supported?**
Linux is the primary platform: all tests, the simulator and the crash tests run
there. In CI the unit tests also run on macOS. Windows binaries build but
are untested: Windows
can't fsync a directory, so the durability of a new file's directory entry
after power loss is not verified there. Windows uses the files layout; the
journal layout is Unix-only.

**Can LogEt open a freshDB database?**
No. Every on-storage magic string changed at the rename
([rebrand notes](rebrand/README.md)) and there is no converter.

**How do I move a files-layout directory to the journal layout?**
Stop every process using it, then run `loget migrate --db DIR`. It copies
and verifies every object and leaves the original as `DIR.bak-files`. If
it is interrupted, run it again and it finishes the job.
`loget migrate --to files` converts back.

**Can two processes write to the same database?**
Only one at a time. A new opener fences the old one, which then gets
`ErrFenced`. For many writers, use many databases (for example one per tenant
or agent), or run `loget serve` and send writes through it.

**What happens if my process crashes mid-commit?**
Acked transactions survive. A transaction that wasn't acked either fully
appears after recovery or doesn't appear at all. This is checked by the
LogEtLog spec, the simulator, the kill -9 crash test and, for the journal
store, a power-loss test that enumerates torn and reordered writes
(docs/design/12-durability.md). Damaged data (a bit flip, a bad sector) is
reported as `ErrCorrupt`, never silently skipped; `--repair-tail` keeps the
valid prefix of a damaged final segment if you cannot restore a backup.

**What exactly does shred guarantee?**
After `Shred(s)` returns, LogEt will not return any value written with
subject `s`. That holds on every branch and at every LSN (time travel),
because the only key that decrypts those values has been deleted. The log
bytes stay, so the hash chain still verifies. You get a signed certificate.
See *Guarantees* in the README for the exact limits: keys and unsubjected
values are not covered, and the dev-mode KEK caveat applies.

**Does shred work with S3 versioning or backups?**
Ciphertext in old object versions is useless without the DEK. **But if your
bucket keeps old versions of `keys/dek/*`, or a backup copied them, the DEK
still exists there.** Expire noncurrent versions of `keys/`, and back up with
that in mind. KMS-held keys are on the roadmap.

**How much memory does it need?**
Every key and version is indexed in RAM (about 30 B per key of heap after
a snapshot load at 10M keys, ~60 B while ingesting). KV values over 64 B
are read from the log through an off-heap cache (`WithValueCacheBytes`;
default 1/16 of the memory limit, 16–256 MiB). The 1M × 1 KiB YCSB run
peaked at 1.55 GiB, measured before the compressed key index; Pebble
held ~0.09 GiB after its load. Old versions stay indexed until `loget
compact` or `loget gc`. A disk-backed key index is roadmap item 1. See
[operations.md](operations.md#memory-and-cache-sizing).

**How fast is it?**
See [benchmarks.md](benchmarks.md) and the summary in the README. They are
single runs on one shared VM on local disk and are not a performance claim. On S3, commit latency is dominated by the PUT round
trip. Group commit amortises it across concurrent transactions.

**Which object stores work?**
Anything with create-only conditional writes (`If-None-Match: *`). **Only
MinIO is tested** (in CI, on every push). AWS S3 has supported `If-None-Match`
on PUT since August 2024, and Cloudflare R2
[documents atomic `If-None-Match: *`](https://developers.cloudflare.com/r2/reference/consistency/).
Both should work, but we have not tested them yet; reports are welcome. Stores
without create-only semantics cannot be opened for writing.

**What is an index here?**
A pure, deterministic fold over the log. Everything durable is in the log;
an index holds only state it can rebuild from it, so it gets transactions,
branches, time travel, compaction and shred from the core. KV is the
built-in one. [How to write an index](index-authoring.md).
