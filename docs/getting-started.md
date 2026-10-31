# Getting started

This walks through the KV index from the CLI, then the same engine from
Go. `main` ships KV only; other indexes come one at a time
([PIPELINE.md](PIPELINE.md)), and you can write your own
([index-authoring.md](index-authoring.md)). All commands and outputs below
are from a real run of `main` on Linux (LSNs depend on what you ran
before; hashes differ every run).

## Install

There is no release yet. Install `main`:

```sh
go install github.com/raman20/loget/cmd/loget@main     # needs Go 1.27+
loget version
```

or build from a clone:

```sh
git clone https://github.com/raman20/loget && cd loget
go build ./cmd/loget && ./loget version
```

## Where the data goes

Every command takes `--db URL`, or reads `LOGET_URL`, and defaults to
`./loget-data`. A new local directory gets the journal layout on
Linux/macOS/BSD (one file, `journal.fj`). See [operations.md](operations.md)
for every URL form.

```console
$ export LOGET_URL=./gs
```

Without `--addr` each CLI call opens the database itself (embedded mode),
which takes the write lease. That is fine for trying things out; for
anything long-running, use `loget serve` (below).

## KV

```console
$ loget put config/model small
{"lsn":1}
$ loget put config/temp 0.2
{"lsn":2}
$ loget scan config/
[{"key":"config/model","value":"small"},{"key":"config/temp","value":"0.2"}]
$ loget del config/temp
{"lsn":3}
$ loget scan config/
[{"key":"config/model","value":"small"}]
$ loget get config/temp --at 2              # time travel: as of LSN 2
0.2
$ loget put user/alice/email alice@example.com --subject alice
{"lsn":4}
```

`--subject alice` encrypts the value under Alice's own key so it can be
shredded later (below).

## Transactions

`tx` applies a batch of `put` / `del` ops atomically (one LSN):

```console
$ loget tx '[{"op":"put","key":"stock/apple","value":"8"},{"op":"put","key":"order/2","value":"apple"},{"op":"del","key":"config/model"}]'
{"lsn":5,"ops":3}
```

From Go, `db.Update` runs a function as a serializable transaction: reads
are validated at commit, and on a conflict the function is retried.

## Forget a subject

```console
$ loget shred alice                          # prints a signed certificate
{ "certificate": { ... "dek_deleted": true, ... "shred_lsn": 6, ... }, "valid_signature": true }
$ loget get user/alice/email
error (404): loget: not found
$ loget verify
{ ..., "ok": true, ... }
```

`loget log [--from LSN]` prints the raw log. Subject-tagged values show
only as `<encrypted N bytes, sha256 …>`, and after a shred nothing can
decrypt them.

## Branches, rewind, promote

```console
$ loget branch create try --at 2             # fork main as of LSN 2; copies nothing
{"heads":{"main":6,"try":2},"ok":true}
$ loget get config/temp --branch try
0.2
$ loget branch list                          # refs, heads and the reflog
$ loget rewind main 1                        # main back to LSN 1; old head kept in the reflog
{"heads":{"main":1,"try":2},"ok":true}
```

`branch promote SRC DST` points `DST` at `SRC`'s log; it only succeeds as a
fast-forward (`409 promote is not a fast-forward` otherwise).

## Server mode

```sh
loget serve --db ./gs --listen 127.0.0.1:7070 &
export LOGET_ADDR=http://127.0.0.1:7070       # the CLI now talks to the server
loget put k v
curl -s localhost:7070/v1/kv/k                  # v
curl -s -X PUT localhost:7070/v1/kv/k2 -d v2    # {"lsn":...}
```

The endpoint list is in the [README](../README.md#go-api). The server listens
on `127.0.0.1` by default. To expose it, add `--token` (or `LOGET_TOKEN`)
and `--tls-cert` / `--tls-key`; see [operations](operations.md).

## From Go

```sh
go get github.com/raman20/loget@main
```

[`examples/tour/main.go`](../examples/tour/main.go) uses KV, a scan, the
example counter index, time travel, branches, rewind, shred and verify on
`mem://`. From a clone:
`go run ./examples/tour`.

```go
db, err := loget.Open(ctx, "./data")       // or "s3://bucket/prefix?endpoint=...", "mem://x"
if err != nil { ... }
defer db.Close()
db.Put(ctx, "config/model", []byte("small"))
```

Options: `WithIndex` (register an index), `WithValueCacheBytes`, `WithInlineMax`, `WithKEK`,
`WithSnapshotEvery`, `WithLogf` (see [operations.md](operations.md) and
[pkg.go.dev](https://pkg.go.dev/github.com/raman20/loget)).

## The agent demo

[`examples/agent-demo.sh`](../examples/agent-demo.sh) runs a server and walks
an AI agent through memory, tasks committed atomically with their
results, undo, a branch plan and forgetting. It needs `jq` and `curl`:

```sh
go build -o /tmp/loget ./cmd/loget && LOGET=/tmp/loget ./examples/agent-demo.sh
```

It writes `./agent-demo-data` in the current directory.
