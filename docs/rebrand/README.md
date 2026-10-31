# Rebrand: freshDB → LogEt (2026-10-31)

LogEt evolved from freshDB (github.com/raman20/freshdb, last commit
5a08052), which began as a rewrite of the goli prototype. LogEt started
a new git history at the rename. The import commit is the freshDB tree at
5a08052 with the names changed. Branch `archive/freshdb-final` keeps that
whole tree, with every index in it.

Name choice: [name-check.md](name-check.md).

## On-disk format: magic strings changed

LogEt can't read freshDB data, on purpose. Every magic string changed,
but each version digit stayed the same, so the version checks work as
before:

| object | freshDB | LogEt |
|---|---|---|
| log segment (`format.SegMagic`) | `FRDBSEG1` | `LGETSEG1` |
| checkpoint `.idx` | `FRDBIDX1` | `LGETIDX1` |
| compaction base `.base` | `FRDBBAS1` | `LGETBAS1` |
| base value chunk `.val` | `FRDBVAL1` | `LGETVAL1` |
| snapshot `.snap` | `FRSNAP2\n` | `LGSNAP2\n` |
| local journal | `FRESHJ1\0` | `LOGETJ1\0` |
| ANN graph image (index/vectors branch only) | `FHNSW1\0\0` | `LHNSW1\0\0` |

Also renamed: Go module `github.com/raman20/freshdb` →
`github.com/raman20/loget`, binary `freshdb` → `loget`, env vars
`FRESHDB_*` → `LOGET_*`, metrics `freshdb_*` → `loget_*`, and the CI
bucket `freshdb-ci` → `loget-ci`. The golden database
`testdata/golden/v0.1.0` (written by freshDB v0.1.0) was dropped, and
the `main` goldens were written again with the new magic strings.

## History kept as is, and spec names

- `bench/results/**`: past measurements of freshDB. They keep their
  file names and contents as history.
- TLA+ modules were renamed `Fresh*` → `LogEt*` (`LogEtLog`,
  `LogEtTxn`, ...). The models are unchanged; the state counts in
  `spec/RESULTS.md` were measured under the old names.
