# Reproducers on design/log-only

These failfiles are regression fixtures. They should stay green.

## keep1-b3-at-1 and keep3-b16-at-5

Historical shrinks of TestPropCompactEqualsUncompacted. After
self-describing subject ciphertext (`Version.Log`) they are no longer
valid rapid programs; the missing-key / AAD class they captured is
fixed. Keep them only as a record.

## keep2-b2-at-2

After GC keep=2, branch b2 at LSN 2 (the fork point) was ErrCompacted
because gcPlan skipped capping the parent when `cp == ParentLSN` and
the child's pointer base sat above the fork. The cap now applies in
that case (keep=0 still does not cap a based child's parent at the
fork, so retired parents can drop).

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep2-b2-at-2.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep2-b2-at-2.fail .`

## keep0-reopen-lsn3 (still red)

TestPropCompactEqualsUncompacted: after GC keep=0, a later OpenStore
fails with `history was deleted by log GC: LSN 3`. Unit tests and a
200-seed sim sweep are green; this is the remaining prop shrink.

- Failfile: `testdata/rapid/TestPropCompactEqualsUncompacted/keep0-reopen-lsn3.fail`
- Replay: `go test -run TestPropCompactEqualsUncompacted -rapid.failfile=testdata/rapid/TestPropCompactEqualsUncompacted/keep0-reopen-lsn3.fail .`
- Seed: `-rapid.seed=1188524893587870226`
