# How to write an index

LogEt is one log with different indexes over it. An **index** is a pure,
deterministic fold over the log's records: it reads committed entries in
log order and keeps whatever state answers its queries fast. KV is the
built-in index. Every other index is written against the same small
framework, and this guide shows how, using the example in
[`examples/counter`](../examples/counter) (about 250 lines plus tests).

`main` ships KV only. Topics, documents, vectors and cross-index
transactions wait on `index/*` branches until KV is done
([PIPELINE.md](PIPELINE.md)). This guide is how they will come back.

## The pieces

| piece | where | what it is |
|---|---|---|
| `index.Kind` | `index/kind.go` | a registration: `Name`, `Version`, `Spaces` (owned key prefixes, the index's declared record types), `New`, `Restore` |
| `index.Index` | `index/index.go` | the fold: `Name`, `Apply(entry, env)`, `DropSubject(s)`, `Digest()` |
| `index.Snapshotter` | `index/kind.go` | optional `Snapshot() []byte`; with `Kind.Restore` it lets open skip the replay |
| record API | `records.go` | `Tx.PutRecord`, `DeleteRecord`, `GetRecord`, `ScanRecords` on owned spaces |
| read-set hooks | `records.go` | `Tx.ReadKey`, `Tx.ReadPrefix`: tell OCC what an answer depended on |
| read access | `records.go` | `Tx.Index(name, fn)` runs `fn` on the branch's index under its read lock |
| registry | `index.Registry` | built by `loget.WithIndex(kind)`; rejects duplicate names and overlapping spaces |

Mapping to the usual vocabulary: *Apply* is `Index.Apply`; *Snapshot* is
`Snapshotter.Snapshot`; *Restore* is `Kind.Restore`; *Checkpoint* is the
engine writing every index's snapshot section next to the log position
it covers (with every index checkpoint: `WithCheckpointBytes`,
`WithSnapshotEvery`, and at close), so open restores the
section and replays only the tail.

There are two shapes:

- **Record store** (`New == nil`): no state of its own. Its records are
  the state and live in the engine's MVCC record store. Reads are
  `GetRecord` / `ScanRecords`. KV is one (`Spaces: ["kv"]`).
- **Derived index** (`New != nil`): folds its spaces' records into its
  own structure, served through `Tx.Index`. The counter is one.

## The contract

1. **Pure and deterministic.** `Apply` may use only the entry and `env`.
   No clocks (use `e.TS`), randomness, goroutines, files or network, and
   no map iteration order that leaks into state. Indexes inside this
   repo are held to an import allow-list by `index/purity_test.go`.
2. **State depends only on live records.** Compaction keeps just the
   latest put of each `(space, key)` and feeds it back as synthetic
   entries. So the state must be a function of the live records, not of
   update order or overwritten values. `TestIndexContract` checks this.
3. **Shred equivalence.** `DropSubject(s)` must leave the index exactly as
   if every op of `s` had been redacted from the start. `env.Open(e, i)`
   returning `ok == false` means the subject is shredded: treat the put as
   a delete.
4. **Skip what isn't yours.** `Apply` sees every op of an entry, other
   indexes' spaces and empty slots (`Kind 0`) of a compaction base
   included. Ignore them.
5. **Canonical bytes.** `Snapshot` and `Digest` must give equal bytes for
   equal states (sort keys; fixed encodings). `Restore` must accept only
   bytes `Snapshot` could write (the counter's fuzzer found a
   non-canonical varint that broke this) and must never panic.
6. **Version on change.** Bump `Kind.Version` when the fold or snapshot
   format changes. Snapshot sections are named `name@version`; another
   version's section is ignored and the index is rebuilt by replay.
7. **Declare reads.** A read API used inside `Update` must call
   `ReadKey` / `ReadPrefix` for every record its answer depends on, or
   OCC cannot see the conflict. `counter.Count` declares the whole
   counter. Inside an `Update`, `Tx.Index` reads the branch head, not the
   transaction's own pending writes.
8. **Copy what you keep.** Op values and `env.Open` results may share
   buffers with the engine. Treat them as read-only and copy before
   keeping one (the counter keeps `string(p)`).

## Walkthrough: the counter

A counter groups items under a name and gives each a value (poll
`lunch`, voter `ana`, choice `tacos`). Its query is "how many items hold
value v", answered without a scan.

```go
db, _ := loget.Open(ctx, url, loget.WithIndex(counter.Kind()))
db.Update(ctx, func(tx *loget.Tx) error {
    return counter.Set(tx, "lunch", "ana", "tacos", loget.Subject("ana"))
})
db.View(ctx, func(tx *loget.Tx) error {
    n, _ := counter.Count(tx, "lunch", "tacos") // 1
    return nil
})
```

1. **Kind.** `Name "counter"`, `Spaces ["ctr:"]`, `New`, `Restore`.
2. **Writes** are records: `Set` is `tx.PutRecord("ctr:"+name, item, val)`.
   Transactions, branches, time travel, compaction and shredding come
   from the engine for free.
3. **State.** `items` (space → key → value, subject) and `counts`
   (space → value → n). `Apply` moves an item's count when its value
   changes; a shredded put is a delete.
4. **Reads.** `Count` calls `tx.ReadPrefix("ctr:"+name, "")` and then
   `tx.Index("counter", ...)`.
5. **Snapshot** encodes only `items`, sorted; `counts` is rebuilt by
   `restore`, which rejects unsorted, duplicate or non-canonical input.

## Tests every index needs

The counter's tests (`examples/counter/counter_test.go`) are the
template:

| test | checks |
|---|---|
| `TestCounts` | the read API on a small workload |
| `TestNotRegistered` | writes into an unregistered space fail; `Tx.Index` on an unknown name fails |
| `TestDeclaredReadsKeepInvariant` | a concurrent change to a declared read makes the commit retry |
| `TestTimeTravelAndBranches` | `ViewAt` and branches see their own state |
| `TestShred` | `DropSubject` equals a fold of the redacted log |
| `TestIndexContract` | fold(full log) == fold(compacted log), across 20 seeds, and after reopen |
| `TestSnapshotRestoreAndVersionBump` | open restores the section; a version bump forces replay |
| `TestSnapshotEncoding`, `FuzzRestore` | canonical bytes; `Restore` never panics and round-trips |

Also add the index to the simulator (`sim/`) if it should be checked
under crashes and faults; the counter is.

## Checklist

- [ ] `Kind` with a unique name and non-overlapping `Spaces`
- [ ] `Apply` skips foreign spaces and Kind 0; shredded puts are deletes
- [ ] State depends only on live records (`TestIndexContract` passes)
- [ ] `DropSubject` == fold(redacted)
- [ ] Canonical `Snapshot`, strict `Restore`, a fuzzer on `Restore`
- [ ] Read APIs declare their reads
- [ ] `Version` bumped on any fold or format change
- [ ] `go test ./...` and the simulator pass

## Values that live in the log

An index may keep a *position* instead of a value, as the KV index does
for values over `InlineMax`: the position (segment, offset, length, CRC)
is a pure function of the log, so the fold stays deterministic. Reading
the bytes back is the engine's job, not the index's (`Apply` does no
I/O).

Design background: [design/20-index-framework.md](design/20-index-framework.md).
