---------------------------- MODULE LogEtIngest ----------------------------
(* Bulk ingest (docs/design/26-batch-ingest.md, part 2), as in ingest.go:
   an ingest stores its run's pieces beside the segment its commit will
   take (log/<log>/<seq>-e<epoch>-<nonce>.ingest.<i>), then commits one
   frame naming them by PutIfAbsent of that segment. GC deletes segments
   below a base together with their ingest objects; the orphan sweep of a
   newer epoch deletes ingest objects of an older epoch that no frame
   names, but only below its own next segment (a segment it saw), since
   the older process may still be running and commit at that segment.

   Processes open with a fresh epoch (lease takeover), commit plain
   frames or ingests, abort ingests, crash, GC and sweep, all
   interleaved. Invariants: a committed frame's run is intact (all pieces
   present) while its segment is live; nothing is visible but whole
   committed ingests (the frame is the commit point).

   Flags (a FALSE value is a known-bad variant TLC must reject):
     PiecesFirst: the frame is committed only after every piece is stored.
     SweepBelowNext: the sweep keeps objects at or above its next segment. *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Procs, MaxSeq, NP, MaxEpoch, PiecesFirst, SweepBelowNext

Seqs == 1..MaxSeq
Pieces == 1..NP
Idle == [s |-> 0, n |-> 0] \* no ingest in progress
NoSeg == {<<"none", 0, 0>>} \* an absent segment (no frame names this id)

VARIABLES
  epoch,  \* last epoch handed out
  wep,    \* wep[p]: p's epoch, 0 if not open (crashed or fenced)
  next,   \* next[p]: the segment p's next commit takes
  seg,    \* seg[s]: NoSeg, or the set of ingest ids its frame names
  objs,   \* stored ingest pieces <<id, i>>, id = <<p, ep, s>>
  ing,    \* ing[p]: p's ingest in progress, or Idle: [s, n]
  gcBelow \* segments below this were deleted by GC (covered by a base)

vars == <<epoch, wep, next, seg, objs, ing, gcBelow>>

Ids == Procs \X (1..MaxEpoch) \X Seqs

TypeOK ==
  /\ epoch \in 0..MaxEpoch
  /\ wep \in [Procs -> 0..MaxEpoch]
  /\ next \in [Procs -> 1..MaxSeq+1]
  /\ seg \in [Seqs -> SUBSET (Ids \cup NoSeg)]
  /\ objs \subseteq Ids \X Pieces
  /\ gcBelow \in 1..MaxSeq+1

Init ==
  /\ epoch = 0
  /\ wep = [p \in Procs |-> 0]
  /\ next = [p \in Procs |-> 1]
  /\ seg = [s \in Seqs |-> NoSeg]
  /\ objs = {}
  /\ ing = [p \in Procs |-> Idle]
  /\ gcBelow = 1

\* first segment at or after gcBelow that is absent (the open's scan)
Scan == IF \E s \in Seqs : s >= gcBelow /\ seg[s] = NoSeg
        THEN CHOOSE s \in Seqs : s >= gcBelow /\ seg[s] = NoSeg /\ \A t \in gcBelow..s-1 : seg[t] # NoSeg
        ELSE MaxSeq + 1

\* Open for write fences the log: it puts an empty segment (header with
\* its epoch) at the first absent slot, so a writer of an older epoch
\* cannot commit there or later (log.go, fenceLog).
Open(p) ==
  /\ wep[p] = 0 /\ epoch < MaxEpoch /\ Scan <= MaxSeq
  /\ epoch' = epoch + 1
  /\ wep' = [wep EXCEPT ![p] = epoch + 1]
  /\ seg' = [seg EXCEPT ![Scan] = {}]
  /\ next' = [next EXCEPT ![p] = Scan + 1]
  /\ ing' = [ing EXCEPT ![p] = Idle]
  /\ UNCHANGED <<objs, gcBelow>>

\* PutIfAbsent of segment next[p]; losing the race fences p. A slot GC
\* deleted is never written again: every writer's next is above it (the
\* fence), and a fenced writer's late put is the grace/lease matter of
\* LogEtGC and LogEtCkpt, not modelled here.
PutSeg(p, names) ==
  LET s == next[p] IN
  IF seg[s] = NoSeg /\ s >= gcBelow
  THEN /\ seg' = [seg EXCEPT ![s] = names]
       /\ next' = [next EXCEPT ![p] = s + 1]
       /\ wep' = wep
  ELSE /\ wep' = [wep EXCEPT ![p] = 0]
       /\ UNCHANGED <<seg, next>>

Commit(p) ==
  /\ wep[p] > 0 /\ ing[p] = Idle /\ next[p] <= MaxSeq
  /\ PutSeg(p, {})
  /\ UNCHANGED <<epoch, objs, ing, gcBelow>>

StartIngest(p) ==
  /\ wep[p] > 0 /\ ing[p] = Idle /\ next[p] <= MaxSeq
  /\ ing' = [ing EXCEPT ![p] = [s |-> next[p], n |-> 0]]
  /\ UNCHANGED <<epoch, wep, next, seg, objs, gcBelow>>

PutPiece(p) ==
  /\ wep[p] > 0 /\ ing[p] # Idle /\ ing[p].n < NP
  /\ objs' = objs \cup {<<<<p, wep[p], ing[p].s>>, ing[p].n + 1>>}
  /\ ing' = [ing EXCEPT ![p].n = @ + 1]
  /\ UNCHANGED <<epoch, wep, next, seg, gcBelow>>

CommitIngest(p) ==
  /\ wep[p] > 0 /\ ing[p] # Idle
  /\ PiecesFirst => ing[p].n = NP
  /\ PutSeg(p, {<<p, wep[p], ing[p].s>>})
  /\ ing' = [ing EXCEPT ![p] = Idle]
  /\ UNCHANGED <<epoch, objs, gcBelow>>

AbortIngest(p) == \* error or cancel: the pieces stay (best-effort delete not modelled)
  /\ ing[p] # Idle
  /\ ing' = [ing EXCEPT ![p] = Idle]
  /\ UNCHANGED <<epoch, wep, next, seg, objs, gcBelow>>

Crash(p) ==
  /\ wep[p] > 0
  /\ wep' = [wep EXCEPT ![p] = 0]
  /\ ing' = [ing EXCEPT ![p] = Idle]
  /\ UNCHANGED <<epoch, next, seg, objs, gcBelow>>

\* compaction made a base covering segments below b; GC deletes them and
\* the ingest objects stored beside them
GC(p) ==
  /\ wep[p] > 0
  /\ \E b \in gcBelow+1..next[p] :
       /\ \A s \in gcBelow..b-1 : seg[s] # NoSeg
       /\ gcBelow' = b
       /\ seg' = [s \in Seqs |-> IF s < b THEN NoSeg ELSE seg[s]]
       /\ objs' = {o \in objs : o[1][3] >= b}
  /\ UNCHANGED <<epoch, wep, next, ing>>

\* sweepIngests: older-epoch objects that their segment does not name
Orphan(p, o) ==
  LET id == o[1] IN
  /\ id[2] < wep[p]
  /\ SweepBelowNext => id[3] < next[p]
  /\ id \notin seg[id[3]]

Sweep(p) ==
  /\ wep[p] > 0
  /\ \E o \in objs : Orphan(p, o)
  /\ objs' = {o \in objs : ~Orphan(p, o)}
  /\ UNCHANGED <<epoch, wep, next, seg, ing, gcBelow>>

Next == \E p \in Procs :
  \/ Open(p) \/ Commit(p) \/ StartIngest(p) \/ PutPiece(p)
  \/ CommitIngest(p) \/ AbortIngest(p) \/ Crash(p) \/ GC(p) \/ Sweep(p)

Spec == Init /\ [][Next]_vars

\* every live committed ingest frame can be replayed: all its pieces exist
IngestIntact ==
  \A s \in Seqs : seg[s] # NoSeg =>
    \A id \in seg[s] : \A i \in Pieces : <<id, i>> \in objs

\* a frame names only ingests of the process and segment that made it
FrameOK == \A s \in Seqs : seg[s] # NoSeg => \A id \in seg[s] : id[3] = s

Sym == Permutations(Procs)
=============================================================================
