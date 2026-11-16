# Reproducers on design/log-only

These failfiles are regression fixtures. They should stay green.

## TestPropCompactEqualsUncompacted keep1-b3-at-1

After GC with KeepCommits=1, a branch forked at the parent head still
needs ViewAt below its own pointer base (`dump b3@1`). Fixed by capping
the parent when a child's keep window extends below ParentLSN, plus
addLiveWindow so the window pins ancestor segments if the cut cannot
be rewritten.

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep1-b3-at-1.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep1-b3-at-1.fail .`

## sim seed 24

`recovered()` used to require a prefix recorded at compact time. After
rewind the floor can be an ancestor base the simulator never recorded
under that key (`main starts from base L1@2 that no compaction made`).
`learnPrefix` now trims a later recorded prefix of the same lineage,
or rebuilds from retained entries.

Replay: `go test ./sim -run TestSimulation -seed=24 -steps=60`

## TestPropCompactEqualsUncompacted keep3-b16-at-5 (still red)

After GC with KeepCommits=3, branch b16 at LSN 5 is missing subject key
k0="0x" (k1 is present). Same family as fork + inlined subject AAD:
the un-GC'd reference still has k0; the post-GC fold does not.

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep3-b16-at-5.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep3-b16-at-5.fail .`
- Seed: `-rapid.seed=8114144812621859952`
