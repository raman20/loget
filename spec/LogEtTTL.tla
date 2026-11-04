------------------------------- MODULE LogEtTTL -------------------------------
(***************************************************************************)
(* Per-key expiry as part of the log fold (docs/design/27-ttl-snapshots.md) *)
(*                                                                         *)
(* Frames carry a commit timestamp (non-decreasing, as the writer assigns  *)
(* max(previous, clock)); a put may carry an absolute expiry. A read at    *)
(* LSN L uses the read time TS(L), the commit time of frame L: a version   *)
(* is visible iff it is the newest write of its key at or below L, is a   *)
(* put, and its expiry is 0 or > TS(L). Compaction writes a base at a cut  *)
(* C: the newest put of each key at or below C, minus those expired at     *)
(* TS(C), with their expiries kept; the base's TS is TS(C).                *)
(*                                                                         *)
(* Checked, over every log of bounded length and every cut:               *)
(*  - BaseEqReplay: reads at any L >= C from the base plus the frames      *)
(*    after it equal reads from the whole log (checkpoint/compaction vs    *)
(*    replay agree; no read uses a wall clock).                            *)
(*  - OCCSound: a transaction that read key k at snapshot L and commits    *)
(*    after head H passes validation (no write to k in (L, H], and no      *)
(*    expiry it relied on at or below TS(H)) only if a read of k at H      *)
(*    returns what it read: expiry acts as a write at its time.            *)
(* With DropAtRead = TRUE the base drops keys expired at the wall-clock    *)
(* "now" of compaction instead of TS(C) (modelled as any time >= TS(C)):   *)
(* TLC finds BaseEqReplay violated. With CheckExp = FALSE validation       *)
(* ignores expiries: OCCSound is violated.                                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Keys, MaxLen, MaxTS, DropAtRead, CheckExp

VARIABLES log, cut, base, bts

vars == <<log, cut, base, bts>>

None == [lsn |-> 0, del |-> TRUE, exp |-> 0]
Frame == [ts : 0..MaxTS, key : Keys, del : BOOLEAN, exp : 0..MaxTS]

TS(L) == IF L = 0 THEN 0 ELSE log[L].ts

\* newest write of k in frames lo+1..hi, or None
Newest(k, lo, hi) ==
  LET is == {i \in (lo + 1)..hi : log[i].key = k}
  IN IF is = {} THEN None
     ELSE LET i == CHOOSE i \in is : \A j \in is : j <= i
          IN [lsn |-> i, del |-> log[i].del, exp |-> log[i].exp]

Live(v, now) == ~v.del /\ (v.exp = 0 \/ v.exp > now)

\* what a read of k at L returns: the LSN of the visible put, or 0
Read(k, L) == LET v == Newest(k, 0, L) IN IF Live(v, TS(L)) THEN v.lsn ELSE 0

ReadBase(k, L) ==
  LET v == Newest(k, cut, L)
      w == IF v.lsn # 0 THEN v ELSE base[k]
  IN IF Live(w, TS(L)) THEN w.lsn ELSE 0

Init == log = << >> /\ cut = 0 /\ base = [k \in Keys |-> None] /\ bts = 0

AppendFrame ==
  /\ Len(log) < MaxLen
  /\ \E f \in Frame :
       /\ f.ts >= TS(Len(log))
       /\ (f.del => f.exp = 0)
       /\ log' = Append(log, f)
  /\ UNCHANGED <<cut, base, bts>>

Compact ==
  /\ \E c \in (cut + 1)..Len(log), now \in 0..MaxTS :
       /\ now >= TS(c) /\ (~DropAtRead => now = TS(c))
       /\ cut' = c /\ bts' = TS(c)
       /\ base' = [k \in Keys |->
             LET v == Newest(k, cut, c)
                 w == IF v.lsn # 0 THEN v ELSE base[k]
             IN IF Live(w, now) THEN w ELSE None]
  /\ UNCHANGED log

Next == AppendFrame \/ Compact

Spec == Init /\ [][Next]_vars

TypeOK == log \in Seq(Frame) /\ cut \in 0..MaxLen

BaseEqReplay ==
  \A L \in cut..Len(log), k \in Keys : ReadBase(k, L) = Read(k, L)

\* a transaction's read of k at L, validated against head H
Conflict(k, L, H) ==
  \/ \E i \in (L + 1)..H : log[i].key = k
  \/ (CheckExp /\ LET v == Newest(k, 0, L) IN
        Live(v, TS(L)) /\ v.exp # 0 /\ v.exp <= TS(H))

OCCSound ==
  \A L \in 0..Len(log), H \in 0..Len(log), k \in Keys :
     (L <= H /\ ~Conflict(k, L, H)) => Read(k, H) = Read(k, L)
=============================================================================
