---------------------------- MODULE LogEtLogGCFork ----------------------------
(* Parent/child lineages, rewind, and child refs into parent segments
   (design 30). Companion to LogEtLogGC.

   One parent log P and at most one child C, forked or rewound at
   ForkLSN. LSNs 1..head on each log are one frame (and one segment)
   each. Two keys: k (KV space) and d (record space). A live value is
   a ref to the segment where it was written.

   A pointer compact at a cut stores the fold of the lineage at that
   cut (all spaces). ViewAt(cut) is that image. History GC may delete
   a parent segment only when neither P's latest nor C's latest (nor
   a pointer base, nor the recoverable tail) refers to it.

   Mutants:
   - SweepIgnoresChild: parent history GC uses only P's latest refs.
     After a rewind, a child's record that still lives in a parent
     segment is deleted; a later open/ViewAt cannot resolve it
     (sim seed 12: "LSN N of ctr:c/i1 not in the log").
   - BaseFromHead: a retention cut below the head writes the head
     (latest) image as the pointer base. Record keys deleted between
     the cut and the head are then missing from ViewAt(cut)
     (TestPropCompactEqualsUncompacted: t:d keys gone at the cut,
     KV still matches).
   - SweepOmitsOpenPrefix: sweep uses only live refs and the tail,
     not the continuous fold prefix Open/recover will read. After
     parent compact-at-head and a sweep of an overwritten parent
     segment, an unbased child forked earlier cannot be opened
     (keep0-reopen-lsn3: "history was deleted by log GC: LSN 3").

   Correctness: NoDangling, CutEqualsFold, ChildReadable,
   OpenReadsOnlyLive (every segment Open/recover reads is stored). *)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS MaxLSN, SweepIgnoresChild, BaseFromHead, SweepOmitsOpenPrefix

Keys == {"k", "d"}   \* k = KV, d = record store
PSeg(n) == <<"p", n>>
CSeg(n) == <<"c", n>>
PSegs == {PSeg(i) : i \in 1..MaxLSN}
CSegs == {CSeg(i) : i \in 1..MaxLSN}
Segs  == PSegs \cup CSegs

None == <<>>   \* no value

VARIABLES
  stored,    \* segments still in the store
  pHead,     \* parent head LSN
  cHead,     \* child head LSN (0 = no child)
  fork,      \* child ParentLSN (0 = no child)
  pWrites,   \* Seq of [lsn, key, del]
  cWrites,   \* child's own writes (lsn > fork)
  pBase,     \* parent pointer-base LSN (0 = none)
  cBase,     \* child pointer-base LSN
  pImage,    \* stored fold at pBase: key -> [lsn, loc] or None
  cImage,    \* stored fold at cBase
  pTail,     \* parent tailFrom (first recoverable tail LSN)
  cTail

vars == <<stored, pHead, cHead, fork, pWrites, cWrites,
          pBase, cBase, pImage, cImage, pTail, cTail>>

Init ==
  /\ stored = {}
  /\ pHead = 0 /\ cHead = 0 /\ fork = 0
  /\ pWrites = <<>> /\ cWrites = <<>>
  /\ pBase = 0 /\ cBase = 0
  /\ pImage = [k \in Keys |-> None]
  /\ cImage = [k \in Keys |-> None]
  /\ pTail = 1 /\ cTail = 1

\* The visible write log of a lineage at LSN L: parent writes with
\* lsn <= min(L, fork) (or all parent writes with lsn <= L if no child
\* / asking about P), plus the child's own writes with lsn <= L.
pLog == pWrites
cLog == IF fork = 0 THEN <<>> ELSE
          SelectSeq(pWrites, LAMBDA w: w.lsn <= fork) \o cWrites

WritesUpTo(log, L) == SelectSeq(log, LAMBDA w: w.lsn <= L)

\* Latest value of key at or before L in a write log, or None.
RECURSIVE Fold(_, _, _)
Fold(log, key, L) ==
  LET ws == WritesUpTo(log, L)
  IN IF ws = <<>> THEN None
     ELSE LET w == ws[Len(ws)]
          IN IF w.key # key
             THEN Fold(SubSeq(ws, 1, Len(ws)-1), key, L)
             ELSE IF w.del THEN None
                  ELSE [lsn |-> w.lsn,
                        loc |-> IF w.who = "p" THEN PSeg(w.lsn) ELSE CSeg(w.lsn)]

PFold(key, L) == Fold(pLog, key, L)
CFold(key, L) == Fold(cLog, key, L)

\* Locations named by a fold image (function key -> None or [lsn,loc]).
RefsOf(img) ==
  {img[k].loc : k \in {x \in Keys : img[x] # None}}

ImageOf(log, L) == [k \in Keys |-> Fold(log, k, L)]

PLive == RefsOf(ImageOf(pLog, pHead)) \cup
         (IF pBase # 0 THEN RefsOf(pImage) ELSE {})
CLive == IF fork = 0 THEN {} ELSE
           RefsOf(ImageOf(cLog, cHead)) \cup
           (IF cBase # 0 THEN RefsOf(cImage) ELSE {})

PTailSegs == {PSeg(i) : i \in {n \in 1..MaxLSN : n >= pTail /\ n <= pHead}}
CTailSegs == IF fork = 0 THEN {} ELSE
               {CSeg(i) : i \in {n \in 1..MaxLSN : n > fork /\ n >= cTail /\ n <= cHead}}

\* Segments Open / recover / ViewAt(head) read for the parent:
\* the pointer-base image's refs, plus the continuous tail after the
\* base (or every parent segment, if there is no base).
OpenP ==
  IF pHead = 0 THEN {}
  ELSE IF pBase # 0
       THEN RefsOf(pImage) \cup
            {PSeg(i) : i \in {n \in 1..MaxLSN : n > pBase /\ n <= pHead}}
       ELSE {PSeg(i) : i \in 1..pHead}

\* Inherited prefix an unbased child must replay at its head (the fork).
\* A parent base at or below the fork can start the fold; a later parent
\* base (compact-at-head after the fork) cannot, so the child needs
\* every parent segment 1..fork (keep0-reopen-lsn3).
InheritC ==
  IF fork = 0 \/ (cBase # 0 /\ cBase >= fork) THEN {}
  ELSE IF pBase # 0 /\ pBase <= fork
       THEN RefsOf(pImage) \cup
            {PSeg(i) : i \in {n \in 1..MaxLSN : n > pBase /\ n <= fork}}
       ELSE {PSeg(i) : i \in 1..fork}

OpenC ==
  IF fork = 0 THEN {}
  ELSE InheritC \cup
       (IF cBase # 0 THEN RefsOf(cImage) ELSE {}) \cup
       RefsOf(ImageOf(cLog, cHead)) \cup
       {CSeg(i) : i \in {n \in 1..MaxLSN :
                          n > fork /\ n <= cHead /\ (cBase = 0 \/ n > cBase)}}

\* needed(manifest) at each log's head. Snapshots are not in this model;
\* retention windows are PTailSegs / CTailSegs (a cut below the head
\* extends the tail back so ViewAt in the window stays foldable).
OpenSegs == OpenP \cup OpenC

\* Correct live set: exactly what Open will read, plus the retention
\* window and the current images' refs (already in OpenSegs when the
\* window is the head). Go gcReach is this set plus live Snapshots.
Live == OpenSegs \cup PLive \cup CLive \cup PTailSegs \cup CTailSegs

\* Buggy live sets.
PSweepLive ==
  IF SweepIgnoresChild
  THEN PLive \cup PTailSegs
  ELSE IF SweepOmitsOpenPrefix
  THEN PLive \cup CLive \cup PTailSegs \cup CTailSegs
  ELSE Live

WriteP(key, del) ==
  /\ pHead < MaxLSN
  /\ pHead' = pHead + 1
  /\ stored' = stored \cup {PSeg(pHead + 1)}
  /\ pWrites' = Append(pWrites,
       [who |-> "p", lsn |-> pHead + 1, key |-> key, del |-> del])
  /\ UNCHANGED <<cHead, fork, cWrites, pBase, cBase, pImage, cImage, pTail, cTail>>

WriteC(key, del) ==
  /\ fork # 0 /\ cHead < MaxLSN
  /\ cHead' = cHead + 1
  /\ stored' = stored \cup {CSeg(cHead + 1)}
  /\ cWrites' = Append(cWrites,
       [who |-> "c", lsn |-> cHead + 1, key |-> key, del |-> del])
  /\ UNCHANGED <<pHead, fork, pWrites, pBase, cBase, pImage, cImage, pTail, cTail>>

\* Fork or rewind: C becomes a child of P at F, with no own frames.
\* Rewind of an existing child is the same: drop C's own segs from
\* the live lineage (they may stay stored until swept).
ForkAt(F) ==
  /\ pHead > 0 /\ F \in 1..pHead
  /\ RefsOf(ImageOf(pLog, F)) \subseteq stored
  /\ \/ /\ pBase # 0 /\ F >= pBase
         /\ \A i \in (pBase + 1)..F : PSeg(i) \in stored
      \/ \A i \in 1..F : PSeg(i) \in stored
  /\ fork' = F
  /\ cHead' = F
  /\ cWrites' = <<>>
  /\ cBase' = 0
  /\ cImage' = [k \in Keys |-> None]
  /\ cTail' = F + 1
  /\ UNCHANGED <<stored, pHead, pWrites, pBase, pImage, pTail>>

\* Pointer compact of P at cut. cut = pHead is compact-at-head;
\* cut < pHead is a retention cut. BaseFromHead stores the head
\* image even when the cut is below the head.
CanFoldP(cut) ==
  /\ RefsOf(ImageOf(pLog, cut)) \subseteq stored
  /\ \/ /\ pBase # 0 /\ cut >= pBase
         /\ \A i \in (pBase + 1)..cut : PSeg(i) \in stored
      \/ \A i \in 1..cut : PSeg(i) \in stored

CanFoldC(cut) ==
  /\ fork # 0
  /\ RefsOf(ImageOf(cLog, cut)) \subseteq stored
  /\ \/ /\ cBase # 0 /\ cut >= cBase
         /\ \A i \in {n \in (cBase + 1)..cut : n > fork} : CSeg(i) \in stored
         /\ (cBase > fork \/ CanFoldP(fork))
      \/ /\ CanFoldP(fork)
         /\ \A i \in {n \in (fork + 1)..cut : n <= cHead} : CSeg(i) \in stored

CompactP(cut) ==
  /\ pHead > 0 /\ cut \in 1..pHead
  /\ CanFoldP(cut)
  /\ \A i \in (cut + 1)..pHead : PSeg(i) \in stored
  \* Replacing P's base must leave C openable. An unbased child cannot
  \* use a parent base above the fork, so 1..fork must still be stored
  \* whenever this compact would move pBase past the fork.
  /\ (fork = 0 \/ cBase # 0 \/ cut <= fork \/
        \A i \in 1..fork : PSeg(i) \in stored)
  /\ pBase' = cut
  /\ pImage' = IF BaseFromHead /\ cut < pHead
               THEN ImageOf(pLog, pHead)
               ELSE ImageOf(pLog, cut)
  \* Pointer compact at head: tail starts after the head.
  \* Retention cut: tail extends back so cut+1..head stays pinned.
  /\ pTail' = IF cut = pHead THEN pHead + 1 ELSE cut + 1
  /\ UNCHANGED <<stored, pHead, cHead, fork, pWrites, cWrites,
                 cBase, cImage, cTail>>

CompactC(cut) ==
  \* A child with no own frames (cHead = fork) can still take a pointer
  \* base at the fork, so a later parent compact-at-head can drop 1..fork.
  /\ fork # 0 /\ cHead >= fork /\ cut \in fork..cHead
  /\ CanFoldC(cut)
  /\ \A i \in {n \in (cut + 1)..cHead : n > fork} : CSeg(i) \in stored
  /\ cBase' = cut
  /\ cImage' = IF BaseFromHead /\ cut < cHead
               THEN ImageOf(cLog, cHead)
               ELSE ImageOf(cLog, cut)
  /\ cTail' = IF cut = cHead THEN cHead + 1 ELSE cut + 1
  /\ UNCHANGED <<stored, pHead, cHead, fork, pWrites, cWrites,
                 pBase, pImage, pTail>>

SweepP(n) ==
  /\ n \in 1..pHead
  /\ PSeg(n) \in stored
  /\ PSeg(n) \notin PTailSegs
  /\ PSeg(n) \notin PSweepLive
  /\ stored' = stored \ {PSeg(n)}
  /\ UNCHANGED <<pHead, cHead, fork, pWrites, cWrites,
                 pBase, cBase, pImage, cImage, pTail, cTail>>

SweepC(n) ==
  /\ fork # 0 /\ n \in (fork + 1)..cHead
  /\ CSeg(n) \in stored
  /\ CSeg(n) \notin CTailSegs
  /\ CSeg(n) \notin Live
  /\ stored' = stored \ {CSeg(n)}
  /\ UNCHANGED <<pHead, cHead, fork, pWrites, cWrites,
                 pBase, cBase, pImage, cImage, pTail, cTail>>

\* Successful open/recover: every segment it would read is still stored.
\* The interesting check is the invariant OpenReadsOnlyLive; this action
\* is enabled only in good states (it does not add behaviour).
Recover ==
  /\ pHead > 0
  /\ OpenSegs \subseteq stored
  /\ UNCHANGED vars

Next ==
  \/ \E k \in Keys : \/ WriteP(k, FALSE) \/ WriteP(k, TRUE)
  \/ \E k \in Keys : \/ WriteC(k, FALSE) \/ WriteC(k, TRUE)
  \/ \E F \in 1..MaxLSN : ForkAt(F)
  \/ \E cut \in 1..MaxLSN : CompactP(cut) \/ CompactC(cut)
  \/ \E n \in 1..MaxLSN : SweepP(n) \/ SweepC(n)
  \/ Recover

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ stored \subseteq Segs
  /\ pHead \in 0..MaxLSN
  /\ cHead \in 0..MaxLSN
  /\ fork \in 0..MaxLSN
  /\ pBase \in 0..MaxLSN
  /\ cBase \in 0..MaxLSN
  /\ pTail \in 1..(MaxLSN + 1)
  /\ cTail \in 1..(MaxLSN + 1)

\* Every live ref, tail segment, and pointer-base ref exists.
NoDangling ==
  /\ PLive \subseteq stored
  /\ CLive \subseteq stored
  /\ PTailSegs \subseteq stored
  /\ CTailSegs \subseteq stored

\* A stored pointer base equals the lineage fold at its cut (both keys).
\* This is the prop-test failure when BaseFromHead is true: the base
\* at the cut is the later head image, so a record deleted after the
\* cut is missing from ViewAt(cut).
CutEqualsFold ==
  /\ (pBase # 0 => pImage = ImageOf(pLog, pBase))
  /\ (cBase # 0 => cImage = ImageOf(cLog, cBase))

\* After a fork or rewind, the child's visible state at the fork
\* is exactly the parent's fold there; any later child ref is stored.
ChildReadable ==
  fork # 0 =>
    /\ (\A k \in Keys : CFold(k, fork) = PFold(k, fork))
    /\ CLive \subseteq stored

\* Every segment Open/recover/ViewAt(head) will read is still stored.
\* Violated by SweepOmitsOpenPrefix (keep0-reopen-lsn3).
OpenReadsOnlyLive == OpenSegs \subseteq stored
=============================================================================
