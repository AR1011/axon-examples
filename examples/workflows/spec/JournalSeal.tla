---------------------------- MODULE JournalSeal ----------------------------
(***************************************************************************)
(* The journal's seal, recovery and carry (src/journal/core.rs).           *)
(*                                                                         *)
(* Each replica holds, per writer epoch, a prefix of that writer's         *)
(* segment: a length, since a record is named by its address               *)
(* <<epoch, seq>> and one writer writes a segment. A writer seals the      *)
(* replicas at its epoch; a replica raises its fence and answers with its  *)
(* segments' lengths once the fence is synced. On a quorum of answers the  *)
(* writer counts the newest segment to the longest answer, follows each    *)
(* Own.prev back, reads every record it counts from a sealed replica or a  *)
(* carry, and writes Own { prev, carry } first in its own segment, carry   *)
(* holding the newest segment's records that fewer than a majority of the  *)
(* answers hold. A record is acked once a majority has synced it. A        *)
(* replica crash keeps what was synced, the fence too.                     *)
(*                                                                         *)
(* Invariants: AckedKept (an owner opens with every record of an older    *)
(* owner that a majority synced before), NoBroken (an owner reads every record it          *)
(* counts), NoLateWrite (I1: a replica takes no record of an epoch below   *)
(* a seal it answered).                                                    *)
(* Mutation weakens one rule, and each spec/mutations/*.cfg must fail.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Replicas, Writers, MaxLen, MaxCrashes, Mutation

Epochs == 1..Writers
Majority == Cardinality(Replicas) \div 2 + 1
\* Answers a writer opens on, and replicas a record is acked on.
SealQuorum == IF Mutation = "ReadOneReplica" THEN 1 ELSE Majority
AckQuorum == IF Mutation = "AckOnOne" THEN 1 ELSE Majority
None == <<0, 0>>
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES answers  \* answers[e]: replicas that answered e's seal
\* The highest seal r answered, for NoLateWrite.
Answered(r) == LET sealed == {e \in Epochs : r \in answers[e]} IN IF sealed = {} THEN 0 ELSE Max(sealed)

VARIABLES
    log,         \* log[r][e]: records of segment e on replica r
    synced,      \* synced[r][e]: of those, synced
    fence,       \* fence[r]: the replica's fence
    fsynced,     \* fsynced[r]: the fence a crash keeps
    pending,     \* pending[r]: seals waiting for the fence to sync
    phase,       \* phase[e]: "sealing" or "open"
    lens,        \* lens[e][r][s]: segment s's length in r's answer to e
    own,         \* own[e]: e's Own record: prev and carry
    written,     \* written[e]: records e appended, Own first
    committed,   \* committed[e]: records of older owners on an ack quorum when e opened
    journal,     \* journal[e]: the addresses e opened with
    late, broken, crashes

vars == <<log, synced, fence, fsynced, pending, phase, answers, lens, own, written,
          committed, journal, late, broken, crashes>>

Zeros == [s \in Epochs |-> 0]

Init ==
    /\ log = [r \in Replicas |-> Zeros]
    /\ synced = [r \in Replicas |-> Zeros]
    /\ fence = [r \in Replicas |-> 0]
    /\ fsynced = [r \in Replicas |-> 0]
    /\ pending = [r \in Replicas |-> {}]
    /\ phase = [e \in Epochs |-> "sealing"]
    /\ answers = [e \in Epochs |-> {}]
    /\ lens = [e \in Epochs |-> [r \in Replicas |-> Zeros]]
    /\ own = [e \in Epochs |-> [prev |-> None, carry |-> {}]]
    /\ written = [e \in Epochs |-> 0]
    /\ committed = [e \in Epochs |-> {}]
    /\ journal = [e \in Epochs |-> {}]
    /\ late = FALSE
    /\ broken = FALSE
    /\ crashes = 0

\* r's answer to e's seal: the length of each segment below e.
Answer(e, r) ==
    /\ answers' = [answers EXCEPT ![e] = @ \cup {r}]
    /\ lens' = [lens EXCEPT ![e][r] = [s \in Epochs |-> IF s < e THEN log[r][s] ELSE 0]]

\* The replica raises its fence; the answer waits for the fence to sync, unless NoFenceSync.
Seal(e, r) ==
    /\ phase[e] = "sealing"
    /\ r \notin answers[e]
    /\ e > fence[r]
    /\ fence' = [fence EXCEPT ![r] = e]
    /\ IF Mutation = "NoFenceSync"
          THEN Answer(e, r) /\ UNCHANGED pending
          ELSE pending' = [pending EXCEPT ![r] = @ \cup {e}] /\ UNCHANGED <<answers, lens>>
    /\ UNCHANGED <<log, synced, fsynced, phase, own, written, committed, journal, late, broken, crashes>>

Reply(e, r) ==
    /\ e \in pending[r]
    /\ fsynced[r] >= e
    /\ pending' = [pending EXCEPT ![r] = @ \ {e}]
    /\ Answer(e, r)
    /\ UNCHANGED <<log, synced, fence, fsynced, phase, own, written, committed, journal, late, broken, crashes>>

\* The records of the chain from <<s, n>> back, and whether S's replicas and the carries hold them all.
RECURSIVE Walk(_, _, _)
Walk(at, pool, S) ==
    LET s == at[1]
        recs == {<<s, k>> : k \in 1..at[2]}
        readable == \A a \in recs : a \in pool \/ \E r \in S : log[r][s] >= a[2]
    IN IF ~readable THEN [ok |-> FALSE, recs |-> {}]
       ELSE IF own[s].prev = None THEN [ok |-> TRUE, recs |-> recs]
       ELSE LET rest == Walk(own[s].prev, pool \cup own[s].carry, S)
            IN [ok |-> rest.ok, recs |-> recs \cup rest.recs]

Open(e, S) ==
    /\ phase[e] = "sealing"
    /\ S \subseteq answers[e]
    /\ Cardinality(S) >= SealQuorum
    /\ LET held == {s \in Epochs : s < e /\ \E r \in S : lens[e][r][s] > 0}
       IN IF held = {}
          THEN /\ own' = [own EXCEPT ![e] = [prev |-> None, carry |-> {}]]
               /\ journal' = [journal EXCEPT ![e] = {}]
               /\ UNCHANGED broken
          ELSE LET newest == Max(held)
                   len(r) == lens[e][r][newest]
                   longest == Max({len(r) : r \in S})
                   onMajority == Max({k \in 0..MaxLen : k = 0 \/ Cardinality({r \in S : len(r) >= k}) >= Majority})
                   carry == IF Mutation = "NoCarry" THEN {} ELSE {<<newest, k>> : k \in onMajority + 1..longest}
                   walk == Walk(<<newest, longest>>, {}, S)
               IN /\ own' = [own EXCEPT ![e] = [prev |-> <<newest, longest>>, carry |-> carry]]
                  /\ journal' = [journal EXCEPT ![e] = walk.recs]
                  /\ broken' = (broken \/ ~walk.ok)
    /\ phase' = [phase EXCEPT ![e] = "open"]
    /\ written' = [written EXCEPT ![e] = 1]
    /\ committed' = [committed EXCEPT ![e] = {<<s, k>> \in Epochs \X (1..MaxLen) :
           s < e /\ Cardinality({r \in Replicas : synced[r][s] >= k}) >= AckQuorum}]
    \* Its other answers are no use now.
    /\ pending' = [r \in Replicas |-> pending[r] \ {e}]
    /\ lens' = [lens EXCEPT ![e] = [r \in Replicas |-> Zeros]]
    /\ UNCHANGED <<log, synced, fence, fsynced, answers, late, crashes>>

Append(e) ==
    /\ phase[e] = "open"
    /\ written[e] < MaxLen
    /\ written' = [written EXCEPT ![e] = @ + 1]
    /\ UNCHANGED <<log, synced, fence, fsynced, pending, phase, answers, lens, own, committed,
                   journal, late, broken, crashes>>

\* The next record of e's segment reaches r, which takes it at or above its fence (and raises the fence to it).
Replicate(e, r) ==
    /\ phase[e] = "open"
    /\ log[r][e] < written[e]
    /\ e >= fence[r]
    /\ log' = [log EXCEPT ![r][e] = @ + 1]
    /\ fence' = [fence EXCEPT ![r] = e]
    /\ late' = (late \/ e < Answered(r))
    /\ UNCHANGED <<synced, fsynced, pending, phase, answers, lens, own, written, committed,
                   journal, broken, crashes>>

\* One group commit: every record and the fence.
Sync(r) ==
    /\ synced[r] # log[r] \/ fsynced[r] # fence[r]
    /\ synced' = [synced EXCEPT ![r] = log[r]]
    /\ fsynced' = [fsynced EXCEPT ![r] = fence[r]]
    /\ UNCHANGED <<log, fence, pending, phase, answers, lens, own, written, committed, journal,
                   late, broken, crashes>>

Crash(r) ==
    /\ crashes < MaxCrashes
    /\ log' = [log EXCEPT ![r] = synced[r]]
    /\ fence' = [fence EXCEPT ![r] = fsynced[r]]
    /\ pending' = [pending EXCEPT ![r] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<synced, fsynced, phase, answers, lens, own, written, committed, journal,
                   late, broken>>

Next ==
    \/ \E e \in Epochs, r \in Replicas : Seal(e, r) \/ Reply(e, r) \/ Replicate(e, r)
    \/ \E e \in Epochs, S \in SUBSET Replicas : Open(e, S)
    \/ \E e \in Epochs : Append(e)
    \/ \E r \in Replicas : Sync(r) \/ Crash(r)

Spec == Init /\ [][Next]_vars

\* A record is acked once an ack quorum has synced it, and every owner after that opens with it.
AckedKept == \A e \in Epochs : phase[e] = "open" => committed[e] \subseteq journal[e]
NoBroken == ~broken
NoLateWrite == ~late

Symmetry == Permutations(Replicas)
=============================================================================
