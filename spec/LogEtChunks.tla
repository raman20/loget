---------------------------- MODULE LogEtChunks ----------------------------
(* Value chunks shared between bases (design 28). A compaction keeps some
   chunks of the current base, uploads fresh ones, then commits a manifest
   whose base refers to both and whose garbage lists the old base's chunks
   the new base no longer refers to. Sweeps delete due garbage; the orphan
   sweep deletes chunk objects nothing lists (a crashed compaction's
   uploads). Readers holding an older manifest finish within the grace,
   modelled as: garbage is not deleted while a reader holds it.
   Checked: no object a manifest or reader refers to is ever deleted.
   Mutants: garbage not subtracting shared chunks (SubtractShared=FALSE);
   orphan sweep counting only chunks the current base wrote (OrphanByOwner). *)
EXTENDS Naturals, FiniteSets

CONSTANTS Chunks, MaxCompact, SubtractShared, OrphanByOwner

VARIABLES
  stored,    \* chunk objects in the store
  owner,     \* owner[c]: number of the compaction that wrote c (0: none)
  base,      \* chunks the manifest's base refers to
  bno,       \* number of the compaction that wrote the current base
  garbage,   \* set of chunk sets listed as garbage
  pending,   \* chunks uploaded by an in-flight compaction (not committed)
  keep,      \* chunks of base the in-flight compaction shares
  n,         \* compactions started
  readers    \* chunk sets held by readers of older manifests

vars == <<stored, owner, base, bno, garbage, pending, keep, n, readers>>

Init ==
  /\ stored = {} /\ owner = [c \in Chunks |-> 0] /\ base = {} /\ bno = 0
  /\ garbage = {} /\ pending = {} /\ keep = {} /\ n = 0 /\ readers = {}

\* Start a compaction: choose chunks to share, upload fresh ones.
Start(k, f) ==
  /\ n < MaxCompact /\ pending = {} /\ keep = {}
  /\ f # {}
  /\ \A c \in f : owner[c] = 0
  /\ k \subseteq base
  /\ n' = n + 1
  /\ stored' = stored \cup f
  /\ owner' = [c \in Chunks |-> IF c \in f THEN n + 1 ELSE owner[c]]
  /\ pending' = f /\ keep' = k
  /\ UNCHANGED <<base, bno, garbage, readers>>

Commit ==
  /\ pending # {}
  /\ base' = keep \cup pending
  /\ bno' = n
  /\ garbage' = garbage \cup
       {IF SubtractShared THEN base \ (keep \cup pending) ELSE base}
  /\ pending' = {} /\ keep' = {}
  /\ UNCHANGED <<stored, owner, n, readers>>

\* The process dies before the commit: its uploads are orphans.
Crash ==
  /\ pending # {}
  /\ pending' = {} /\ keep' = {}
  /\ UNCHANGED <<stored, owner, base, bno, garbage, n, readers>>

StartRead == /\ base # {} /\ readers' = readers \cup {base}
             /\ UNCHANGED <<stored, owner, base, bno, garbage, pending, keep, n>>
EndRead(r) == /\ r \in readers /\ readers' = readers \ {r}
              /\ UNCHANGED <<stored, owner, base, bno, garbage, pending, keep, n>>

\* Grace: due garbage no reader holds any more.
Sweep(g) ==
  /\ g \in garbage
  /\ \A r \in readers : g \cap r = {}
  /\ stored' = stored \ g
  /\ garbage' = garbage \ {g}
  /\ UNCHANGED <<owner, base, bno, pending, keep, n, readers>>

listed == IF OrphanByOwner THEN {c \in base : owner[c] = bno} ELSE base
\* Orphans: stored chunks no base or garbage lists, not ours in flight.
SweepOrphan(c) ==
  /\ c \in stored \ (listed \cup UNION garbage \cup pending)
  /\ \A r \in readers : c \notin r
  /\ stored' = stored \ {c}
  /\ UNCHANGED <<owner, base, bno, garbage, pending, keep, n, readers>>

Next ==
  \/ \E k \in SUBSET base, f \in SUBSET Chunks : Start(k, f)
  \/ Commit \/ Crash \/ StartRead
  \/ \E r \in readers : EndRead(r)
  \/ \E g \in garbage : Sweep(g)
  \/ \E c \in stored : SweepOrphan(c)

Spec == Init /\ [][Next]_vars

TypeOK == stored \subseteq Chunks /\ base \subseteq Chunks /\ pending \subseteq Chunks

\* Nothing the manifest's base, a reader or an in-flight compaction
\* refers to is missing from the store.
NoDangling ==
  /\ base \subseteq stored
  /\ \A r \in readers : r \subseteq stored
  /\ (keep \cup pending) \subseteq stored

=============================================================================
