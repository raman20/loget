# Reproducers on design/log-only

These failfiles are regression fixtures. They should stay green.

## keep1-b3-at-1 and keep3-b16-at-5

Historical shrinks of TestPropCompactEqualsUncompacted. After
self-describing subject ciphertext (`Version.Log`) they are no longer
valid rapid programs; the missing-key / AAD class they captured is
fixed. Kept here (not under testdata/rapid/) so rapid will not auto-load
them and poison a later run.

- `testdata/repro/keep1-b3-at-1.fail`
- `testdata/repro/keep3-b16-at-5.fail`

## keep2-b2-at-2

After GC keep=2, branch b2 at LSN 2 (the fork point) was ErrCompacted
because gcPlan skipped capping the parent when `cp == ParentLSN` and
the child's pointer base sat above the fork. The cap now applies in
that case (keep=0 still does not cap a based child's parent at the
fork, so retired parents can drop).

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep2-b2-at-2.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep2-b2-at-2.fail .`

## keep0-reopen-lsn3 (fixed)

After GC keep=0, OpenStore used to fail with `history was deleted by
log GC: LSN 3`. gcReach now pins needed(manifest) — the segments
Open/recover/ViewAt(head) will read, including an unbased child's
fold prefix when the parent base sits after the fork. chainAt walks
ancestor pointer bases so a branch at an ancestor's cut can reopen
after a later compact of the child. TestOpenReadsOnlyNeeded fails if
Open/ViewAt(head) GETs a log/*.seg outside needed ∪ Snapshots ∪
retention windows.

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep0-reopen-lsn3.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep0-reopen-lsn3.fail .`
- Seed: `-rapid.seed=1188524893587870226`
