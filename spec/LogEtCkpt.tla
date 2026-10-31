------------------------------ MODULE LogEtCkpt ------------------------------
(* Incremental index checkpoints of one log (docs/design/24-open-checkpoints.md).
   The in-memory record store of a writer is a set of immutable runs, each
   holding the versions of a contiguous LSN interval, plus a hot set (the
   interval after the newest run). A checkpoint at LSN L is a descriptor
   object naming run objects (the runs, each PUT once and shared by later
   checkpoints) and the hot interval (inline), built on the base at K.
   The manifest points at the newest checkpoint; open loads it if every
   object it names exists and its base is the log's base, and replays the
   segments after L; otherwise it replays everything after the base.
   Compaction (LogEtCompact.tla, here in two steps) moves the base to the
   head and drops history; runs and descriptors no longer referenced
   become garbage, deleted after the grace period; objects written by a
   crashed or fenced writer are swept as orphans.
   Properties: recovery from the latest manifest folds every LSN after
   the base exactly once (= a full replay); the live checkpoint's objects
   are never deleted; garbage is final; readers under the grace guard
   never miss an object.
   Mutants (cfg constants): BaseCheck = FALSE (a checkpoint is not bound
   to its base), OrphanEpoch = FALSE (orphan sweeps ignore the writer
   epoch), GraceGuard = FALSE, FenceCheck = FALSE. *)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Procs, MaxLSN, MaxEpoch, MaxMans, GraceGuard, FenceCheck, BaseCheck, OrphanEpoch

Seg(n) == <<"seg", n, 0, 0>>
Base(k, e) == <<"base", k, 0, e>>
Run(lo, hi, e) == <<"run", lo, hi, e>>
Desc(l, k, e) == <<"desc", l, k, e>> \* named by its base too (TLC: a checkpoint at the
                                     \* same LSN after a compaction reused a garbage name)
NoCk == [l |-> 0, k |-> 0, d |-> <<"none", 0, 0, 0>>, runs |-> {}, hlo |-> 1, ok |-> FALSE]
NoName == <<"none", 0, 0, 0>>
Range(a, b) == a..b

VARIABLES objs, head, mans, ep, cm, pc, mem, pend, kb, rm, rneed, rpc
vars == <<objs, head, mans, ep, cm, pc, mem, pend, kb, rm, rneed, rpc>>

Last == mans[Len(mans)]
MaxEp == Last.e
CkObjs(c) == IF c.ok THEN {c.d} \cup {r.nm : r \in c.runs} ELSE {}
BaseObj(m) == IF m.bk = 0 THEN {} ELSE {Base(m.bk, m.be)}
Refs(m) == BaseObj(m) \cup {Seg(n) : n \in m.bk+1..head} \cup CkObjs(m.ck)
FirstListed(g) == CHOOSE i \in 1..Len(mans) : g \in mans[i].garbage /\ \A j \in 1..i-1 : g \notin mans[j].garbage

\* The LSNs a set of runs plus a hot interval hold, as a multiset-free
\* check: runs pairwise disjoint, the hot interval after all of them.
Cover(runs) == UNION {Range(r.lo, r.hi) : r \in runs}
Disjoint(runs) == \A r1, r2 \in runs : r1 # r2 => Range(r1.lo, r1.hi) \cap Range(r2.lo, r2.hi) = {}

\* What open folds from manifest m: the checkpoint (if usable) and then
\* the segments after it, or the segments after the base. Returns the set
\* of LSNs folded and whether some LSN would be folded twice.
UseCk(m) == m.ck.ok /\ CkObjs(m.ck) \subseteq objs /\ (BaseCheck => m.ck.k = m.bk)
Folded(m) == IF UseCk(m) THEN Cover(m.ck.runs) \cup Range(m.ck.hlo, m.ck.l) \cup Range(m.ck.l + 1, head)
             ELSE Range(m.bk + 1, head)
Twice(m) == UseCk(m) /\ (~Disjoint(m.ck.runs) \/ Cover(m.ck.runs) \cap Range(m.ck.hlo, head) # {})
\* History below the base is gone: a fold holding an LSN <= base differs from replay.
RecoverOK(m) == Folded(m) = Range(m.bk + 1, head) /\ ~Twice(m)

EmptyMem(k, h) == [b |-> k, runs |-> {}, hlo |-> k + 1, vh |-> h]

Init ==
  /\ objs = {} /\ head = 0
  /\ mans = << [e |-> 0, bk |-> 0, be |-> 0, ck |-> NoCk, garbage |-> {}] >>
  /\ ep = [p \in Procs |-> 0] /\ cm = [p \in Procs |-> 0]
  /\ pc = [p \in Procs |-> "down"]
  /\ mem = [p \in Procs |-> EmptyMem(0, 0)]
  /\ pend = [p \in Procs |-> NoCk]
  /\ kb = [p \in Procs |-> 0]
  /\ rm = 0 /\ rneed = {} /\ rpc = "idle"

\* Open for write: fence with a new manifest, load the checkpoint if
\* usable (else replay from the base); the replayed tail is the hot set.
Open(p) ==
  /\ pc[p] = "down" /\ MaxEp < MaxEpoch
  /\ mans' = Append(mans, [Last EXCEPT !.e = MaxEp + 1])
  /\ ep' = [ep EXCEPT ![p] = MaxEp + 1]
  /\ cm' = [cm EXCEPT ![p] = Len(mans) + 1]
  /\ pc' = [pc EXCEPT ![p] = "idle"]
  /\ mem' = [mem EXCEPT ![p] = IF UseCk(Last)
                THEN [b |-> Last.bk, runs |-> Last.ck.runs, hlo |-> Last.ck.hlo, vh |-> head]
                ELSE EmptyMem(Last.bk, head)]
  /\ pend' = [pend EXCEPT ![p] = NoCk]
  /\ UNCHANGED <<objs, head, kb, rm, rneed, rpc>>

\* Commit a frame (a fenced writer's segment put fails).
Write(p) ==
  /\ pc[p] = "idle" /\ ep[p] = MaxEp /\ head < MaxLSN /\ mem[p].vh = head
  /\ head' = head + 1 /\ objs' = objs \cup {Seg(head + 1)}
  /\ mem' = [mem EXCEPT ![p].vh = head + 1]
  /\ UNCHANGED <<mans, ep, cm, pc, pend, kb, rm, rneed, rpc>>

\* The hot set becomes a new (unpersisted) run.
Freeze(p) ==
  /\ pc[p] \in {"idle", "ckpt"} /\ mem[p].hlo <= mem[p].vh
  /\ mem' = [mem EXCEPT ![p].runs = @ \cup {[lo |-> mem[p].hlo, hi |-> mem[p].vh, nm |-> NoName]},
                         ![p].hlo = mem[p].vh + 1]
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, pend, kb, rm, rneed, rpc>>

\* Two adjacent runs merge into a new (unpersisted) run.
Merge(p) ==
  /\ pc[p] \in {"idle", "ckpt"}
  /\ \E r1, r2 \in mem[p].runs :
       /\ r1.hi + 1 = r2.lo
       /\ mem' = [mem EXCEPT ![p].runs = (@ \ {r1, r2}) \cup {[lo |-> r1.lo, hi |-> r2.hi, nm |-> NoName]}]
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, pend, kb, rm, rneed, rpc>>

\* Checkpoint step 1: PUT one run that has no object yet.
PutRun(p) ==
  /\ pc[p] = "idle"
  /\ \E r \in mem[p].runs :
       /\ r.nm = NoName
       /\ objs' = objs \cup {Run(r.lo, r.hi, ep[p])}
       /\ mem' = [mem EXCEPT ![p].runs = (@ \ {r}) \cup {[r EXCEPT !.nm = Run(r.lo, r.hi, ep[p])]}]
  /\ UNCHANGED <<head, mans, ep, cm, pc, pend, kb, rm, rneed, rpc>>

\* Checkpoint step 2: every run has an object: PUT the descriptor (runs,
\* hot set inline, base K, LSN L).
PutDesc(p) ==
  /\ pc[p] = "idle" /\ \A r \in mem[p].runs : r.nm # NoName
  /\ mem[p].vh > mans[cm[p]].ck.l
  /\ objs' = objs \cup {Desc(mem[p].vh, mem[p].b, ep[p])}
  /\ pend' = [pend EXCEPT ![p] = [l |-> mem[p].vh, k |-> mem[p].b, d |-> Desc(mem[p].vh, mem[p].b, ep[p]),
                                  runs |-> mem[p].runs, hlo |-> mem[p].hlo, ok |-> TRUE]]
  /\ pc' = [pc EXCEPT ![p] = "ckpt"]
  /\ UNCHANGED <<head, mans, ep, cm, mem, kb, rm, rneed, rpc>>

\* Checkpoint step 3: create-only manifest commit naming the descriptor;
\* the previous checkpoint's objects it no longer names become garbage.
\* Refused if the log's base moved past the checkpoint's.
CommitCk(p) ==
  /\ pc[p] = "ckpt"
  /\ pc' = [pc EXCEPT ![p] = "idle"] /\ pend' = [pend EXCEPT ![p] = NoCk]
  /\ LET m == mans[cm[p]] IN
     IF (~FenceCheck \/ cm[p] = Len(mans)) /\ (BaseCheck => pend[p].k = m.bk) /\ pend[p].l > m.ck.l
     THEN /\ mans' = Append(mans, [m EXCEPT !.e = ep[p], !.ck = pend[p],
                         !.garbage = (m.garbage \cap objs) \cup (CkObjs(m.ck) \ CkObjs(pend[p]))])
          /\ cm' = [cm EXCEPT ![p] = Len(mans) + 1]
     ELSE UNCHANGED <<mans, cm>>
  /\ UNCHANGED <<objs, head, ep, mem, kb, rm, rneed, rpc>>

\* Compaction step 1: write the base at the view's head.
WriteBase(p) ==
  /\ pc[p] = "idle" /\ mem[p].vh > mans[cm[p]].bk
  /\ objs' = objs \cup {Base(mem[p].vh, ep[p])}
  /\ kb' = [kb EXCEPT ![p] = mem[p].vh]
  /\ pc' = [pc EXCEPT ![p] = "based"]
  /\ UNCHANGED <<head, mans, ep, cm, mem, pend, rm, rneed, rpc>>

\* Compaction step 2: commit the base; the checkpoint (built on the old
\* base) is dropped with its objects; the view reloads from the base.
CommitBase(p) ==
  /\ pc[p] = "based"
  /\ pc' = [pc EXCEPT ![p] = "idle"]
  /\ IF ~FenceCheck \/ cm[p] = Len(mans)
     THEN LET m == mans[cm[p]]
              oldb == BaseObj(m)
              keep == ~BaseCheck /\ m.ck.ok
          IN /\ mans' = Append(mans, [e |-> ep[p], bk |-> kb[p], be |-> ep[p],
                          ck |-> IF keep THEN m.ck ELSE NoCk,
                          garbage |-> (m.garbage \cap objs) \cup oldb \cup {Seg(n) : n \in m.bk+1..kb[p]}
                                      \cup (IF keep THEN {} ELSE CkObjs(m.ck))])
             /\ cm' = [cm EXCEPT ![p] = Len(mans) + 1]
             /\ mem' = [mem EXCEPT ![p] = EmptyMem(kb[p], kb[p])]
     ELSE UNCHANGED <<mans, cm, mem>>
  /\ UNCHANGED <<objs, head, ep, pend, kb, rm, rneed, rpc>>

\* Delete garbage listed in the manifest p holds, after the grace.
Sweep(p) ==
  /\ pc[p] = "idle"
  /\ \E g \in mans[cm[p]].garbage \cap objs :
       /\ GraceGuard => ~(rpc = "reading" /\ rm < FirstListed(g))
       /\ objs' = objs \ {g}
  /\ UNCHANGED <<head, mans, ep, cm, pc, mem, pend, kb, rm, rneed, rpc>>

\* Delete a run or descriptor of an older epoch that the manifest p holds
\* does not name (left by a crashed or fenced writer's checkpoint).
SweepOrphan(p) ==
  /\ pc[p] = "idle"
  /\ \E o \in objs :
       /\ o[1] \in {"run", "desc"}
       /\ (OrphanEpoch => o[4] < ep[p])
       /\ o \notin CkObjs(mans[cm[p]].ck)
       /\ o \notin {r.nm : r \in mem[p].runs}
       /\ GraceGuard => ~(rpc = "reading" /\ o \in rneed)
       /\ objs' = objs \ {o}
  /\ UNCHANGED <<head, mans, ep, cm, pc, mem, pend, kb, rm, rneed, rpc>>

Crash(p) ==
  /\ pc[p] # "down"
  /\ pc' = [pc EXCEPT ![p] = "down"] /\ ep' = [ep EXCEPT ![p] = 0]
  /\ mem' = [mem EXCEPT ![p] = EmptyMem(0, 0)] /\ pend' = [pend EXCEPT ![p] = NoCk]
  /\ UNCHANGED <<objs, head, mans, cm, kb, rm, rneed, rpc>>

\* A reader (read-only open) reads a manifest, then what it names.
RStart ==
  /\ rpc \in {"idle", "done"} /\ rm < Len(mans)
  /\ rm' = Len(mans) /\ rneed' = Refs(Last) /\ rpc' = "reading"
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, mem, pend, kb>>

RRead ==
  /\ rpc = "reading"
  /\ IF rneed = {} THEN rpc' = "done" /\ UNCHANGED rneed
     ELSE \E o \in rneed :
            IF o \in objs THEN rneed' = rneed \ {o} /\ UNCHANGED rpc
            ELSE rpc' = "failed" /\ UNCHANGED rneed
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, mem, pend, kb, rm>>

Next == \/ \E p \in Procs : Open(p) \/ Write(p) \/ Freeze(p) \/ Merge(p) \/ PutRun(p) \/ PutDesc(p)
                            \/ CommitCk(p) \/ WriteBase(p) \/ CommitBase(p) \/ Sweep(p) \/ SweepOrphan(p) \/ Crash(p)
        \/ RStart \/ RRead
Spec == Init /\ [][Next]_vars

\* Bounds for TLC: at most MaxMans manifests; the two processes are interchangeable.
Bounded == Len(mans) <= MaxMans
Sym == Permutations(Procs)

TypeOK ==
  /\ head \in 0..MaxLSN
  /\ \A p \in Procs : pc[p] \in {"down", "idle", "ckpt", "based"}
  /\ rpc \in {"idle", "reading", "done", "failed"}

\* K1: open from the latest manifest folds exactly what a full replay folds.
CkptEqReplay == RecoverOK(Last)
\* K2: the latest checkpoint's objects exist (GC never breaks it: open
\* would still be correct, by falling back, but slow).
CkptIntact == CkObjs(Last.ck) \subseteq objs
\* K3: the live writer's in-memory view is exactly the fold after its base.
ViewOK == \A p \in Procs : (pc[p] # "down" /\ ep[p] = MaxEp) =>
            (Disjoint(mem[p].runs) /\ Cover(mem[p].runs) \cup Range(mem[p].hlo, mem[p].vh) = Range(mem[p].b + 1, mem[p].vh)
             /\ Cover(mem[p].runs) \cap Range(mem[p].hlo, mem[p].vh) = {})
NoLoss == BaseObj(Last) \cup {Seg(n) : n \in Last.bk+1..head} \subseteq objs
EpochMonotone == \A i \in 1..Len(mans)-1 : mans[i].e <= mans[i+1].e
GarbageFinal == \A i \in 1..Len(mans) : \A j \in i..Len(mans) : mans[i].garbage \cap Refs(mans[j]) = {}
ReaderOK == rpc # "failed"
=============================================================================
