# Compatibility: format versions, upgrades and downgrades

loget is pre-1.0. This page says what is checked today, not what is
promised for 1.0. See [design 13](design/13-format-versioning.md) for the
reasoning and [formats.md](formats.md) for the byte layouts.

## Version stamps

`loget version` prints the formats the binary reads and writes:

```
formats: manifest 1-2, segment 1, base 1, journal 1-2, snapshot 2 (KV2), checkpoint 1
```

| object | stamp | unknown version |
|---|---|---|
| manifest (`.mf`) | JSON `"format"`: 1, or 2 once compacted | open fails, `ErrVersion` |
| segment (`.seg`) | magic `LGETSEG1` + u16 version | open fails, `ErrVersion` |
| base (`.base`) | magic `LGETBAS1` (last byte = version) | open fails, `ErrVersion` |
| journal file (`journal.fj`) | magic `LOGETJ2\0` for new files (byte 6 = version; 2 adds batch delete records, design 23). A version 1 file is read and kept at version 1 (deletes stay one record each), so the build that made it can still open it | open fails, `ErrVersion` (a pre-design-23 build refuses a version 2 file) |
| snapshot (`.snap`) | magic `LGSNAP2\n` (v2, bound to its log and hash chain); `KV2` tag inside, then one section per derived index named `name@version` | ignored, rebuilt from the log (cache); a section of another index version is ignored and that index replays |
| segment-index checkpoint (`.idx`) | magic `LGETIDX1` (last byte = version) | ignored, the segments are scanned (cache). Older builds ignore `.idx` objects; their GC leaves them behind (a few bytes each) |
| value chunk (`.val`) | magic `LGETVAL1`, not checked | gated by the manifest format (each value has its own CRC) |
| keys (`keys/…`), shred certificates, token file | none | gated by the manifest format |

`ErrVersion` (`errors.Is(err, loget.ErrVersion)`) means "written by a
newer loget": the message names the object and says to upgrade the
binary. A refused open changes nothing on storage (tested for the
manifest and the journal; a segment or base is only read).

The manifest format is the database-level version: a change to any
unversioned object must bump it, so an older binary refuses the whole
database rather than misreading one object.

## Policy (pre-1.0)

- **Upgrades**: a new binary reads every format any earlier release
  wrote. This is tested by golden databases (below) for every released
  version, read in both local layouts.
- **No silent rewrites**: opening a database never rewrites existing
  objects in a newer format. A newer format is written only by an
  operation that needs it (today: `compact` writes manifest format 2).
- **Downgrades**: an older binary refuses formats it does not know (see
  the matrix). Downgrading is not supported once a newer format has been
  written; keep a backup taken before upgrading if you might go back.
- **Format changes** come with a version bump, a CHANGELOG entry under
  "Changed (breaking)" if older binaries can no longer read the result,
  and a new golden directory. Minor versions before 1.0 may change formats;
  1.0 will freeze this policy (docs/PRODUCTION_READINESS.md S6.4).

## Tested matrix

No LogEt version has been released, so there is nothing older to read
yet. LogEt does **not** read freshDB data: every magic string changed at
the rename ([rebrand notes](rebrand/README.md)), and freshDB's golden
`v0.1.0` database was dropped with it. A freshDB binary does not
recognise LogEt objects either.

| written by → read by | main |
|---|---|
| main, uncompacted (files and journal layouts) | yes (golden `main`) |
| main, compacted (manifest 2) | yes (golden `main-compacted`) |

The first LogEt release adds its own golden directory and a column here.

## Golden files

`testdata/golden/<version>/db` holds a database written by that version's
code with one fixed workload (KV with inline and out-of-line values,
deletes, records of a second record-store index, a shredded subject, a
branch), and `dump.txt` what that version read back.

- `TestGoldenRead` opens a copy of each one (files layout, and journal
  after `store.Migrate`) and must read exactly `dump.txt`.
- `TestGoldenBytes` rebuilds `main` and `main-compacted` with fixed clock
  and randomness and requires every object to be byte-identical, so an
  encoder change cannot alter the on-storage format without a failing test.
- An intended format change: bump the stamp, copy the current `main*`
  directories to `testdata/golden/<last release or commit>` so they stay
  under `TestGoldenRead`, then `LOGET_UPDATE_GOLDEN=1 go test -run
  TestGoldenBytes .`.
