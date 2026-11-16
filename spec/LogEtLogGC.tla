---------------------------- MODULE LogEtLogGC ----------------------------
(* Log-only values and index-agnostic log GC (design 30).

   Segments hold values. A KV compact writes only a pointer base and
   never uploads a value object. Segments below the KV base (history)
   are deleted only when no index and no live snapshot references
   them, and they are not in the recoverable tail. Leftover format-4
   chunks are GC'd by the same rule; no new chunks appear.
   A second index may fold any currently retained segment.
   A compact does not list history segments as garbage.
   Crashes before the manifest commit leave orphan pointer bases.
   Grace: a listed object is not deleted while a reader holds it.

   Mutants: SweepReferenced (GC ignores index and snapshot refs);
   CompactDropsSegs (a pointer compact lists all covered segments
   as garbage, the design-03 behaviour). *)
EXTENDS Naturals, FiniteSets

CONSTANTS MaxLSN, MaxBase, MaxSnaps, MaxReaders, Chunks, SweepReferenced, CompactDropsSegs

Segs == {<<"seg", s>> : s \in 1..MaxLSN}
Bases == {<<"base", b>> : b \in 1..MaxBase}
Values == Segs \cup Chunks
Seg(n) == <<"seg", n>>
Base(n) == <<"base", n>>

VARIABLES
  stored,     \* value objects and pointer bases in the store
  head,       \* last written segment number (0 = none)
  tailFrom,   \* segment numbers tailFrom..head are the recoverable tail
  kv,         \* live KV pointer refs (segments and leftover chunks)
  idx,        \* live refs of a second index folded over the log
  snaps,      \* live snapshot ref-sets (value objects only)
  base,       \* current pointer-base id (0 = none)
  pending,    \* pointer-base id being written (0 = none)
  nbase,      \* pointer bases started
  garbage,    \* sets of objects listed as garbage
  readers     \* value-object sets held by readers of older manifests

vars == <<stored, head, tailFrom, kv, idx, snaps, base, pending, nbase,
          garbage, readers>>

Tail == {Seg(s) : s \in {i \in 1..MaxLSN : i >= tailFrom /\ i <= head}}

\* Objects the live map must keep: indexes, snapshots, the tail.
Live == kv \cup idx \cup UNION snaps \cup Tail

Init ==
  /\ stored = Chunks          \* leftover format-4 chunks, if any
  /\ head = 0 /\ tailFrom = 1
  /\ kv = Chunks /\ idx = {} /\ snaps = {}
  /\ base = 0 /\ pending = 0 /\ nbase = 0
  /\ garbage = {} /\ readers = {}

\* Append a segment and point KV at it. Overwrite: optionally drop
\* one existing KV ref (an older version of some key).
Write ==
  /\ pending = 0 /\ head < MaxLSN
  /\ head' = head + 1
  /\ stored' = stored \cup {Seg(head + 1)}
  /\ \E drop \in SUBSET kv :
       /\ Cardinality(drop) <= 1
       /\ kv' = (kv \ drop) \cup {Seg(head + 1)}
  /\ UNCHANGED <<tailFrom, idx, snaps, base, pending, nbase, garbage, readers>>

\* A second index folds one retained segment (the log, not a chunk).
Fold ==
  /\ \E s \in (stored \cap Segs) :
       /\ s \notin idx
       /\ idx' = idx \cup {s}
  /\ UNCHANGED <<stored, head, tailFrom, kv, snaps, base, pending, nbase,
                 garbage, readers>>

\* The second index drops a ref (retention, or it no longer needs it).
IdxDrop ==
  /\ \E s \in idx : idx' = idx \ {s}
  /\ UNCHANGED <<stored, head, tailFrom, kv, snaps, base, pending, nbase,
                 garbage, readers>>

SnapStart ==
  /\ Cardinality(snaps) < MaxSnaps
  /\ Live \cap Values # {}
  /\ snaps' = snaps \cup {Live \cap Values}
  /\ UNCHANGED <<stored, head, tailFrom, kv, idx, base, pending, nbase,
                 garbage, readers>>

SnapEnd ==
  /\ \E r \in snaps : snaps' = snaps \ {r}
  /\ UNCHANGED <<stored, head, tailFrom, kv, idx, base, pending, nbase,
                 garbage, readers>>

\* Write a pointer base (run pieces). No value object is created.
\* Allowed even when the tail is empty (tailFrom = head+1): a later
\* retention cut moves the tail back into retained history.
StartCompact ==
  /\ pending = 0 /\ nbase < MaxBase /\ head > 0
  /\ nbase' = nbase + 1
  /\ pending' = nbase + 1
  /\ stored' = stored \cup {Base(nbase + 1)}
  /\ UNCHANGED <<head, tailFrom, kv, idx, snaps, base, garbage, readers>>

\* Manifest commit: a pointer base is installed and the recoverable
\* tail starts at newTail. newTail = head+1 is a pointer compact at
\* the head (history is retained, not in Tail, and SweepHist may drop
\* unreferenced history). newTail < tailFrom is a retention cut that
\* extends the tail back into history: every segment in the keep
\* window is again Tail, so SweepHist cannot delete it. This is the
\* KeepCommits/KeepFor cut the first model omitted (a cut only in the
\* writable tail left in-window history unswept-as-Tail and GC'd).
Commit ==
  /\ pending # 0
  /\ \E newTail \in 1..(head + 1) :
       /\ \A i \in newTail..head : Seg(i) \in stored
       /\ base' = pending
       /\ tailFrom' = newTail
       /\ garbage' = garbage \cup
            (IF base # 0 THEN {{Base(base)}} ELSE {}) \cup
            (IF CompactDropsSegs
             THEN {stored \cap Segs}
             ELSE {})
       /\ pending' = 0
       /\ UNCHANGED <<stored, head, kv, idx, snaps, nbase, readers>>

\* Crash before the commit: the pointer base is an orphan.
Crash ==
  /\ pending # 0
  /\ pending' = 0
  /\ UNCHANGED <<stored, head, tailFrom, kv, idx, snaps, base, nbase,
                 garbage, readers>>

StartRead ==
  /\ Cardinality(readers) < MaxReaders
  /\ readers' = readers \cup {Live \cap Values}
  /\ UNCHANGED <<stored, head, tailFrom, kv, idx, snaps, base, pending,
                 nbase, garbage>>

EndRead ==
  /\ \E r \in readers : readers' = readers \ {r}
  /\ UNCHANGED <<stored, head, tailFrom, kv, idx, snaps, base, pending,
                 nbase, garbage>>

held == UNION readers

Sweep(g) ==
  /\ g \in garbage
  /\ g \cap held = {}
  /\ stored' = stored \ g
  /\ garbage' = garbage \ {g}
  /\ UNCHANGED <<head, tailFrom, kv, idx, snaps, base, pending, nbase,
                 readers>>

\* Log GC of one history value object with no live refs.
\* SweepReferenced skips the Live check (mutant).
SweepHist(o) ==
  /\ o \in stored \cap Values
  /\ o \notin Tail
  /\ o \notin held
  /\ SweepReferenced \/ o \notin Live
  /\ stored' = stored \ {o}
  /\ UNCHANGED <<head, tailFrom, kv, idx, snaps, base, pending, nbase,
                 garbage, readers>>

\* Orphan pointer bases: stored, not current, not in flight, not listed.
SweepOrphan(b) ==
  /\ b \in 1..MaxBase
  /\ Base(b) \in stored
  /\ b # base /\ b # pending
  /\ Base(b) \notin UNION garbage
  /\ stored' = stored \ {Base(b)}
  /\ UNCHANGED <<head, tailFrom, kv, idx, snaps, base, pending, nbase,
                 garbage, readers>>

Next ==
  \/ Write \/ Fold \/ IdxDrop
  \/ SnapStart \/ SnapEnd
  \/ StartCompact \/ Commit \/ Crash
  \/ StartRead \/ EndRead
  \/ \E g \in garbage : Sweep(g)
  \/ \E o \in stored : SweepHist(o)
  \/ \E b \in 1..MaxBase : SweepOrphan(b)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ stored \subseteq (Values \cup Bases)
  /\ head \in 0..MaxLSN
  /\ tailFrom \in 1..(MaxLSN + 1)
  /\ kv \subseteq Values /\ idx \subseteq Segs
  /\ snaps \subseteq SUBSET Values
  /\ base \in 0..MaxBase
  /\ pending \in 0..MaxBase
  /\ nbase \in 0..MaxBase
  /\ readers \subseteq SUBSET Values

\* C5: every live ref exists in the store. This is also
\* SnapshotValid and "no GC of a referenced object".
NoDangling ==
  /\ kv \subseteq stored
  /\ idx \subseteq stored
  /\ \A r \in snaps : r \subseteq stored
  /\ \A r \in readers : r \subseteq stored
  /\ Tail \subseteq stored
  /\ (pending # 0 => Base(pending) \in stored)
  /\ (base # 0 => Base(base) \in stored)

\* The recoverable tail is never history-GC'd.
TailPinned == Tail \subseteq stored

\* New compactions do not create chunks; only leftover Init chunks exist.
NoNewChunks == stored \cap Chunks \subseteq Chunks

\* Indexes and snapshots point at value objects, never at a pointer base.
ValuesOnly ==
  /\ kv \subseteq Values
  /\ idx \subseteq Segs
  /\ \A r \in snaps : r \subseteq Values

=============================================================================
