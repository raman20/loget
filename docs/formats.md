# On-storage formats

Describes `main` (unreleased). The layouts are freshDB's with new magic
strings ([rebrand notes](rebrand/README.md)); LogEt does not read
freshDB data.

> **Pre-1.0: these formats may change between minor versions.** Readers refuse
> unknown versions with `ErrVersion` instead of guessing. A version bump will
> always come with a CHANGELOG entry, and a migration path once there are
> real users. Version stamps, the upgrade/downgrade policy and the golden
> files that pin these bytes: [compatibility.md](compatibility.md).

All integers are little-endian. "uvarint" is Go's `encoding/binary` unsigned
varint, and the decoder only accepts the minimal encoding. That makes every
payload's bytes canonical, which the hash chain relies on.

## Bucket layout

```
<prefix>/
  manifest/<id:%020d>.mf             JSON manifest, create-only, dense ids from 1
  log/<logID>/<seq:%020d>.seg        segment = one group commit (or a fence), create-only, dense seq from 1
  snap/<logID>/<lsn:%020d>.snap      index snapshot written before design 24 (read only; cache, never truth)
  ckpt/<logID>/<lsn:%020d>-e<epoch>-<nonce>.ckpt              index checkpoint descriptor (cache, never truth)
  ckpt/<logID>/<lsn:%020d>-e<epoch>-<nonce>.r<i>.<p:%06d>.run piece p of a stored record-store run
  log/<logID>/<seq>.idx                 segment-index checkpoint covering segments up to <seq> (cache, never
                                        truth; create-only; deleted with the segment by GC)
  log/<logID>/<seq>-e<epoch>-<nonce>.ingest.<p:%06d>  piece p of a bulk-ingest run committed by segment <seq>'s
                                        INGEST op (create-only; deleted with the segment by GC; orphans
                                        of an older epoch deleted by the orphan sweep)
  log/<logID>/<seq>.repair              repair note of a damaged segment kept by --repair-tail:
                                        "loget-repair v1 seq= frames= valid= size= sha256=" (create-only;
                                        the segment stays as is; deleted with the segment by GC)
  quarantine/<logID>/<seq>-<epoch>.seg  written by versions before the repair note (copy of a repaired tail)
  base/<logID>/<lsn:%020d>-e<epoch>-<nonce>.base         compaction base image (before design 24; read only)
  base/<logID>/<lsn:%020d>-e<epoch>-<nonce>.<p:%06d>.run  piece p of a compaction base run (design 24)
  base/<logID>/<lsn:%020d>-e<epoch>-<nonce>.<n:%06d>.val value chunk of that base, ≤ 64 MiB
  keys/_kek                          dev-mode key-encryption key (see SECURITY.md)
  keys/_sign                         ed25519 seed for shred certificates (dev mode)
  keys/dek/<subjectHash>             per-subject DEK, AES-GCM-wrapped by the KEK
  keys/shredded/<subjectHash>        shred marker (create-only)
  keys/certs/<subjectHash>-<lsn>.json  signed shred certificate
```

`subjectHash` is a truncated SHA-256 of the subject id (`SubjectHash` in
`keyring.go`). Raw subject ids never reach storage.

## Local directory layouts

A local directory stores the objects above in one of two layouts. The
objects and their bytes are the same in both.
- **files**: one file per object at its key path. It is the default on
  Windows.
  - Writes go to `.tmp/`, are fsynced, renamed into place, and then the
    directory is fsynced.
- **journal** (`journal.fj`): the default for new directories on Unix.
  - 32-byte header: magic `LOGETJ2\0` (`LOGETJ1\0` for files made
    before design 23, still read and written as version 1), a 16-byte
    random salt, and the CRC-32C of the first 24 bytes.
  - Then records. Each record has a 12-byte header: crc u32, kind u8
    (1 put, 2 delete, 3 dead, 4 batch delete), a pad byte, klen u16 and
    dlen u32. The key and data follow. A batch delete's key is
    `\0dels` and its data lists the deleted keys, each as u16 length +
    key (at most 1 MiB of keys per record; version 2 files only).
  - The record CRC is CRC-32C over the salt, the record's own offset, the
    header, the key and the data. A dead record's CRC covers only its
    header and key.
  - A torn tail is never read as data. On open, a record failing its CRC
    is a torn append if nothing valid follows (its bytes are zeroed), an
    interrupted delete if its delete record follows (the delete is
    finished), and otherwise corruption (`store.ErrCorrupt`, file left
    unchanged). The salt keeps bytes from an older file at the same offset
    from passing the CRC.
  - Deleting an object appends a delete record. The put record is then
    marked dead and its data is hole-punched (or zeroed).
    `store.DeleteBatch` (compaction's garbage sweep) appends one batch
    delete record for many objects, syncs, erases every put, and syncs
    again: two syncs per batch instead of two per object. One CRC covers
    the whole batch, so a torn batch is a torn tail (nothing of it
    applies); a damaged put followed by a batch listing its key is an
    interrupted delete and is finished on open.
  - The file grows in 8 MiB zero-filled, fsynced chunks.
  - Writers take an exclusive `flock` and readers a shared one.

`store.DetectLayout`:
- a directory holding `journal.fj` is journal;
- any other content (except an empty `.tmp/`) makes it files;
- an empty or missing directory gets the platform default.

`loget migrate --db DIR [--to journal|files]` converts between the
layouts offline, with a backup. It is crash-safe; see
[operations.md](operations.md#migrating-between-layouts),
[design 01](design/01-journal-default.md) and
[`spec/LogEtMigrate.tla`](../spec/LogEtMigrate.tla).

## Value positions (in memory, not on storage)

KV values larger than `InlineMax` (default 64 B) are not kept in RAM. The
KV index stores a position into the immutable segment that holds them
(segment, offset within the segment, length, CRC-32C of the stored bytes)
and reads them back with a ranged read. The position is derived from the
log, so this changes no on-storage bytes; it is why KV snapshots record the
inline limit (below).

## Segment (`.seg`)

```
header (32 bytes)
  0  [8]  magic "LGETSEG1"
  8  u16  version = 1
  10 u16  reserved (0)
  12 u64  epoch     writer epoch that created this segment (fencing)
  20 u64  seq       sequence number within the log
  28 u32  CRC32C(header[0:28])
frames...
  u32  payload length (≤ 64 MiB)
  u32  CRC32C(payload)
  payload
```

A fence segment has a header and no frames.

### Frame payload (one committed transaction)

```
u64       LSN
i64       commit timestamp (unix nanos, from the DB clock; part of the log so indexes stay deterministic)
[32]      prev  = hash-chain head before this frame
uvarint   number of ops
op × n:
  u8      kind   1=PUT 2=DEL 3=APPEND (reserved for the topics index; main never writes it) 4=SHRED
                 5=INGEST (alone in its frame; space "kv", key = the run's object key,
                 value = JSON {meta, sha256}: design 26)
  uvarint+bytes  subject   ("" = not subject-scoped; else subjectHash)
  uvarint+bytes  space     "kv", or a space owned by a registered index (index.Kind.Spaces)
  uvarint+bytes  key
  uvarint+bytes  value     plaintext, or nonce‖AES-GCM ciphertext if subject != ""
```

**Hash chain:** `head' = SHA-256(head ‖ SHA-256(payload))`, computed over the
stored bytes (so ciphertext, for subject data). A branch's chain continues
from its parent's head at the fork LSN. Shredding deletes keys, not bytes, so
the chain still verifies after a shred.

**Encryption:** values with a subject are sealed with AES-256-GCM under the
subject's DEK. The additional data binds the ciphertext to its place:
`logID‖0‖LSN‖opIndex‖space‖0‖key`. A ciphertext copied anywhere else fails
authentication.

## Manifest (`.mf`)

```json
{
  "format": 1,
  "id": 7,
  "epoch": 4,
  "db_id": "…",
  "next_log": 3,
  "logs": {
    "L1": {"id": "L1"},
    "L2": {"id": "L2", "parent": "L1", "parent_lsn": 10, "snap_lsn": 0, "snap_key": ""}
  },
  "refs": {"main": "L2", "exp": "L1"},
  "reflog": [{"name": "main", "from": "L1", "to": "L2", "at": 10, "why": "rewind"}]
}
```

After a compaction the manifest has `"format": 2`, each
compacted log has a `"base": {"lsn", "seq", "key", "chunks", "chain", "ts"}`
(the full state at `lsn`; segments before `seq` are no longer needed, nor
is the parent), and a `"garbage"` list of
`{"kind": "segs"|"snaps"|"base", "log", "below", "key", "chunks", "after"}`
entries deleted once the clock passes `after`. A base object is the magic
`LGETBAS1`, the LSN (u64), the length of the record image (u64), the
image (KV snapshot encoding of every record store space, values pointing
into the chunks), and a CRC-32C; each value keeps its own CRC. Chunks
start with `LGETVAL1`. Derived indexes are rebuilt from the base's
records at open.
See [design/03-compaction.md](design/03-compaction.md).

Since design 24 compaction writes `"format": 3` and the base as one
record-store run in pieces instead of a `.base` object: `"base"` gains
`"run": {"len", "n", "vers", "piece", "sums"}` (run bytes, keys, versions,
bytes per piece, one CRC-32C per piece) and the pieces are
`<key>.<p:%06d>.run` (run piece layout below). Bases of format 2 are still
read. A log's newest index checkpoint is `"ckpt": {"lsn", "floor", "key",
"runs": [{"key", "pieces"}]}` (any format; older binaries ignore the field);
garbage kinds `"ckpt"` (a descriptor) and `"run"` (a run's `chunks` pieces)
are added. See [design/24-open-checkpoints.md](design/24-open-checkpoints.md).

The manifest with the highest id is current. Creating `id+1` is how a writer
takes the lease and how branch operations commit; `ErrExists` means another
writer got there first, and this one is fenced.

## Segment-index checkpoint (`.idx`)

Key `log/<log>/<seq %020d>.idx`, where `seq` is the last segment it
covers. Written with each snapshot. Version 1:

```
8B   magic "LGETIDX1"
u8   len(log id), then the log id
u64  first segment sequence number of the log (1, or the compaction base's)
32B  hash-chain head the covered frames extend (the log's base)
u64  epoch of segment seq
i64  timestamp of the last covered frame
32B  hash-chain head after the last covered frame
u32  count (>= 1)
count x { u64 seq | u64 first LSN | u64 frame count (>= 1) }   ascending seq, contiguous LSNs
u32  CRC32C(all of the above)
```

Recovery lists the log's directory, tries the newest three checkpoints at
or above the log's first segment, and uses the first that (1) decodes,
(2) names this log, its first segment and base chain, and starts at the
LSN the log continues from, and (3) matches segment `seq` exactly: frame
count, first LSN, chain head after it, last timestamp and header epoch.
It then reads segments `seq+1` on as usual. Anything else is ignored with
a log line. Segments a checkpoint covers are not re-read at open: their
frames are checked (CRC, sequence, first LSN) when read, and `verify`
scans everything and warns about any checkpoint that differs from the
segments.

## Index checkpoint (`.ckpt`) and run pieces (`.run`)

Design 24. Descriptor key `ckpt/<log>/<lsn %020d>-e<epoch>-<nonce hex>.ckpt`.
Version 1, little endian:

```
8B   magic "LGCKPT1\n"
u64  LSN covered
u64  floor: compaction base LSN of the lineage the record store was built on
32B  hash-chain head after that LSN
u8   len(log id), then the log id
u32  record store inline limit
u32  run count, then per run (newest first):
       u16 len(key) | key | u64 bytes | u64 keys | u64 versions
       | u32 bytes per piece | u32 piece count | u32 CRC32C per piece
u32  section count
section × count { u8 len(name) | name "<index>@<version>" | u64 len | bytes }   sorted by name
u32  CRC32C(all of the above)
```

A run's bytes are the record store's run entries as kept in its run file
(design 21), split into pieces `<run key>.<p %06d>.run` of the stated size
(the last may be shorter); a piece has no header, its CRC is in the
descriptor (or, for a base, in the manifest). A run keeps the key it was
first stored under, so a descriptor may name runs stored by an earlier
checkpoint. Open uses a checkpoint only if the manifest's `ckpt` names it,
the descriptor's checksum holds, it names this log, its LSN and floor match
the manifest and the log's own base, its chain head equals the log's at
that LSN, its inline limit is the current one, and every piece has its
length and CRC; each run's entries are then walked and its hash slots
rebuilt. Anything else logs a line, counts in
`CheckpointFallbacks`, and the log is replayed (from an older snapshot or
the base when there is one).

## Snapshot (`.snap`)

Written before design 24; still read (a checkpoint replaces it at the
next checkpoint). Key `snap/<log>/<lsn %020d>.snap`. Version 2:

```
8B   magic "LGSNAP2\n"
u64  LSN covered
32B  hash-chain head after that LSN
u8   len(log id), then the log id
u64  len(kv)
kv index snapshot bytes (all record-store spaces)
u32  section count
section × count { u8 len(name) | name "<index>@<version>" | u64 len | bytes }   sorted by name
u32  CRC32C(all of the above)
```

Open uses a snapshot only if its checksum holds, its log id and LSN match
its key and the log, the LSN lies between the log's compaction floor and
its head, and the hash-chain head equals the log's own at that LSN. So the
bytes of another log's snapshot, or of a snapshot from a different history
under the same name, are never loaded. Anything else (missing, damaged,
mismatched) is ignored with a log line and the index is rebuilt from the
log. A section whose name has no registered index at that version is ignored
and that index is rebuilt from the log.

The KV part starts with the tag `KV2` and records the inline limit it was
written with. A `KV1` snapshot or one written with a different
limit is ignored the same way, and the log is replayed.
