# Design 28: shared value chunks across compactions, batched scan reads (2026-11-04)

ROADMAP KV item 6 (compaction) and the full-scan gap of item 2.
Numbers: [bench/results/2026-11-04-compaction-scan](../../bench/results/2026-11-04-compaction-scan/README.md).

## Part a: compaction no longer rewrites every value

Before: each compaction read every live value and wrote it again into new
chunks (`<base>.<n>.val`), then the whole old base became garbage.

Now a base lists its value chunks by reference: `"refs": [{"key", "n",
"size"}]` (chunk number c in a version means `refs[c-1]`). A compaction:

1. counts, per chunk of the log's current base, the bytes of values still
   live at the cut (one pass over the index, no reads);
2. keeps every chunk with live bytes ≥ ½ of its size (`shareMin`): versions
   in it are renumbered, not read or uploaded;
3. copies values from other chunks, from segments and all subject
   (encrypted) values into new chunks, as before;
4. commits garbage = old base's run pieces and `.base` (kind `base`, no
   chunks) plus a kind `vals` entry naming the old chunks the new base does
   **not** refer to.

Space held by dead values in shared chunks is bounded by 2× live value
bytes. The orphan sweep treats every chunk named by a current base or by
`vals` garbage as live, whatever epoch wrote it. Bases record their own
chunk sizes (`chunk_sizes`), so a base written before this change (no
sizes) is copied once and shared from then on. `WithCompactCopyAll()`
turns sharing off.

Format: `chunk_sizes` is ignored by older builds; a manifest whose base
has `refs` is format 4, refused by older builds (they would misread chunk
numbers). Golden `3ac2e00-compacted` keeps the previous format under
`TestGoldenRead`.

Verified: TLA+ `spec/LogEtChunks.tla` (crashes before commit, sweeps
under grace, orphan sweep, readers of older manifests; 2.16M distinct
states, no error); mutants "garbage does not subtract shared chunks" and
"orphan sweep keeps only chunks the current base wrote" both violate
`NoDangling` in 6 steps. `TestCompactSharesChunks` (8 compactions with 4 KiB
chunks, model check, no stray or missing chunk objects, reopen in a new
epoch + compaction, Verify) on mem/fs/journal; `TestCrashKill9CompactShared`
(25 kill -9 cycles with 2 KiB chunks, all acked writes present). Value
cache keys for referenced chunks are by object, so cached values survive a
compaction that shares their chunk.

Result at 10M × 100 B with 1% churn: 26–27 s → 6.7 s per compaction,
1,158 → 213 MiB written.

### Limits (honest)

- The key run is still rewritten whole each compaction (~200 MiB at 10M
  keys); compaction work is still O(keys), only value I/O became
  O(changed values). Not "incremental compaction" in the LSM sense.
- Peak RSS per compaction is not reduced or bounded (1.4–1.9 GiB at 10M).
- Each compaction adds a small chunk of new values; small chunks that stay
  mostly live accumulate (no merge of small chunks yet).
- Commits still pause briefly at the commit step, as before.
- On S3 the saving is upload bytes; not measured on S3 here.

## Part b: batched value reads in iterators

An iterator page (`prefetch.go`) groups its values by object, sorts by
offset and reads runs of nearby values with one ranged read (gap ≤ 4 KiB,
span ≤ 1 MiB, values ≤ 64 KiB), up to 4 reads in parallel; each value's
CRC is checked and any failure falls back to the per-value path (so
corruption still returns `ErrCorrupt`). 10M × 100 B full iterator scan:
606k → 1.54M keys/s (2.56M after compaction); Pebble 7.28M. Still ~3–5×
slower than Pebble. Keys loaded in random order would coalesce less.
`Snapshot` creation and `View` retry when a compaction swaps the view.
