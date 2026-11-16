# Remaining reproducers on design/log-only

WIP after the fork/rewind/parent-ref engine fixes. Do not treat these as
accepted failures: they should go green without weakening the tests.

## TestPropCompactEqualsUncompacted

After GC with KeepCommits=1, `dump b3@1` returns ErrCompacted
(`the requested history was deleted by log GC: LSN 1`).

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep1-b3-at-1.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep1-b3-at-1.fail .`
- Seed: `-rapid.seed=9964188859791950934`

## sim seed 24

`recovered()`: `main starts from base L1@2 that no compaction made`.
The manifest has a pointer base the simulator did not record, typically
after a compact that faulted and a later rewind.

Replay: `go test ./sim -run TestSimulation -seed=24 -steps=60`
