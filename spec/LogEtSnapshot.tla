---------------------------- MODULE LogEtSnapshot ----------------------------
(***************************************************************************)
(* Snapshot retention (docs/design/27-ttl-snapshots.md, part b).            *)
(*                                                                         *)
(* A Snapshot pins an LSN of a log. Compaction moves the log's base        *)
(* (floor) up to a cut; history below the floor is gone. The protocol in   *)
(* readview.go: snapshot creation (check floor and the planned cut, then   *)
(* register) and compaction planning (scan live snapshots, then record the *)
(* cut as planned) each run under one mutex. A snapshot older than the max *)
(* age stops pinning (Age); reads through it then fail ErrSnapshotTooOld.  *)
(* Writers append concurrently; compaction plans, then commits the new     *)
(* base or fails (the planned cut stays recorded: it is monotone).         *)
(*                                                                         *)
(* Safety: every live (registered, not aged, not released) snapshot is at  *)
(* or above the floor and above any cut being committed, so its reads are *)
(* always served. With Atomic = FALSE creation checks and registers in two *)
(* steps (no mutex), and with CheckCut = FALSE creation ignores the       *)
(* planned cut: TLC finds a lost snapshot in both.                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Snaps, MaxLSN, Atomic, CheckCut

VARIABLES head, floor, cutting, st, at, cstate, ccut

vars == <<head, floor, cutting, st, at, cstate, ccut>>

States == {"none", "checked", "live", "aged", "released"}

TypeOK ==
  /\ head \in 0..MaxLSN /\ floor \in 0..MaxLSN /\ cutting \in 0..MaxLSN
  /\ st \in [Snaps -> States] /\ at \in [Snaps -> 0..MaxLSN]
  /\ cstate \in {"idle", "planned"} /\ ccut \in 0..MaxLSN

Init ==
  /\ head = 0 /\ floor = 0 /\ cutting = 0
  /\ st = [s \in Snaps |-> "none"] /\ at = [s \in Snaps |-> 0]
  /\ cstate = "idle" /\ ccut = 0

Write == head < MaxLSN /\ head' = head + 1 /\ UNCHANGED <<floor, cutting, st, at, cstate, ccut>>

\* the check: at between the floor (and any planned cut) and the head
OkAt(a) == a >= floor /\ (CheckCut => a >= cutting) /\ a <= head

CreateAtomic(s) ==
  /\ Atomic /\ st[s] = "none"
  /\ \E a \in 0..head : OkAt(a) /\ at' = [at EXCEPT ![s] = a]
  /\ st' = [st EXCEPT ![s] = "live"]
  /\ UNCHANGED <<head, floor, cutting, cstate, ccut>>

CreateCheck(s) ==
  /\ ~Atomic /\ st[s] = "none"
  /\ \E a \in 0..head : OkAt(a) /\ at' = [at EXCEPT ![s] = a]
  /\ st' = [st EXCEPT ![s] = "checked"]
  /\ UNCHANGED <<head, floor, cutting, cstate, ccut>>

CreateReg(s) ==
  /\ st[s] = "checked"
  /\ st' = [st EXCEPT ![s] = "live"]
  /\ UNCHANGED <<head, floor, cutting, at, cstate, ccut>>

Pins(c) == \E s \in Snaps : st[s] = "live" /\ at[s] < c

Plan ==
  /\ cstate = "idle"
  /\ \E c \in (floor + 1)..head :
       /\ ~Pins(c)
       /\ ccut' = c
       /\ cutting' = IF c > cutting THEN c ELSE cutting
  /\ cstate' = "planned"
  /\ UNCHANGED <<head, floor, st, at>>

Commit ==
  /\ cstate = "planned"
  /\ floor' = ccut /\ cstate' = "idle"
  /\ UNCHANGED <<head, cutting, st, at, ccut>>

Fail ==
  /\ cstate = "planned" /\ cstate' = "idle"
  /\ UNCHANGED <<head, floor, cutting, st, at, ccut>>

Age(s) == st[s] = "live" /\ st' = [st EXCEPT ![s] = "aged"] /\ UNCHANGED <<head, floor, cutting, at, cstate, ccut>>

Release(s) ==
  /\ st[s] \in {"live", "aged"} /\ st' = [st EXCEPT ![s] = "released"]
  /\ UNCHANGED <<head, floor, cutting, at, cstate, ccut>>

Next ==
  \/ Write \/ Plan \/ Commit \/ Fail
  \/ \E s \in Snaps : CreateAtomic(s) \/ CreateCheck(s) \/ CreateReg(s) \/ Age(s) \/ Release(s)

Spec == Init /\ [][Next]_vars

\* A live snapshot is readable now and after the compaction in progress.
SnapshotReadable ==
  \A s \in Snaps : st[s] = "live" =>
     /\ at[s] >= floor
     /\ (cstate = "planned" => at[s] >= ccut)

FloorMonotone == [][floor' >= floor]_vars
=============================================================================
