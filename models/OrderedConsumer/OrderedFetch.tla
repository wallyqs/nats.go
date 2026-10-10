----------------------------- MODULE OrderedFetch -----------------------------
(***************************************************************************)
(* The ordered consumer's Fetch / FetchBytes / FetchNoWait / Next path     *)
(* (jetstream/ordered.go:414-535) over pullConsumer.fetch                  *)
(* (jetstream/pull.go:933-1036).                                           *)
(*                                                                         *)
(* One user goroutine issues calls one after another (each call returns   *)
(* before the next starts, as the API requires); every call starts a      *)
(* fetch goroutine (pull.go:966-1034) that keeps delivering into its batch *)
(* after the call returned. The user reads every batch completely. The    *)
(* cursor handling is the subject: Fetch copies the previous result's last *)
(* stream sequence into the cursor before resetting (ordered.go:420-427);  *)
(* FetchNoWait does neither that nor record its own result (495-513).     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, TLC

CONSTANTS
    N,          \* messages in the stream
    B,          \* batch size of every call
    MaxCalls,   \* calls the user makes
    Kinds       \* subset of {"Fetch", "FetchNoWait"} the user may call

VARIABLES
    lock,       \* c.Mutex holder: "none", "user" (inside a call) or "leaked"
    curS,       \* c.cursor.streamSeq
    rf,         \* c.runningFetch: [set, done, sseq, g] (g = its fetch goroutine)
    fg,         \* fetch goroutines: g -> [nS (next stream seq), got, done]
    nG,
    upc,        \* user pc: "idle", "f1" (Fetch waiting for c.Mutex), "nw" (FetchNoWait), "reset", "blocked"
    kind,       \* kind of the call in progress
    calls,
    obs         \* stream seqs read from the batches, in order

vars == <<lock, curS, rf, fg, nG, upc, kind, calls, obs>>

Init ==
    /\ lock = "none" /\ curS = 0
    /\ rf = [set |-> FALSE, done |-> TRUE, sseq |-> 0, g |-> 0]
    /\ fg = [g \in 1..MaxCalls |-> [nS |-> 0, got |-> 0, done |-> TRUE]]
    /\ nG = 0 /\ upc = "idle" /\ kind = "none" /\ calls = 0 /\ obs = <<>>

Call(k) ==
    /\ upc = "idle" /\ calls < MaxCalls /\ k \in Kinds
    /\ calls' = calls + 1 /\ kind' = k
    /\ upc' = IF k = "Fetch" THEN "f1" ELSE "nw"
    /\ UNCHANGED <<lock, curS, rf, fg, nG, obs>>

\* Fetch / FetchBytes up to the reset (ordered.go:415-434)
F1 ==
    /\ upc = "f1"
    /\ IF lock /= "none" THEN upc' = "blocked" /\ UNCHANGED <<lock, curS>>   \* c.Lock() blocks forever
       ELSE IF rf.set /\ ~rf.done
          THEN lock' = "leaked" /\ upc' = "idle" /\ UNCHANGED curS   \* 421-423: returns, c.Mutex still held
          ELSE /\ curS' = IF rf.set /\ rf.sseq /= 0 THEN rf.sseq ELSE curS   \* 424-426
               /\ upc' = "reset" /\ UNCHANGED lock
    /\ UNCHANGED <<rf, fg, nG, kind, calls, obs>>

\* FetchNoWait up to the reset (ordered.go:496-508): no lock, no cursor update
NW ==
    /\ upc = "nw"
    /\ IF rf.set /\ ~rf.done
          THEN upc' = "idle"                                     \* 499-501
          ELSE upc' = "reset"
    /\ UNCHANGED <<lock, curS, rf, fg, nG, kind, calls, obs>>

\* reset() takes c.Mutex (ordered.go:553) and creates a consumer starting at
\* curS + 1; the fetch then starts its goroutine. Fetch records the result
\* as runningFetch (443); FetchNoWait returns it untracked (512).
Reset ==
    /\ upc = "reset"
    /\ IF lock /= "none" THEN upc' = "blocked" /\ UNCHANGED <<rf, fg, nG>>
       ELSE /\ nG' = nG + 1
            /\ fg' = [fg EXCEPT ![nG + 1] = [nS |-> curS + 1, got |-> 0, done |-> FALSE]]
            /\ rf' = IF kind = "Fetch" THEN [set |-> TRUE, done |-> FALSE, sseq |-> 0, g |-> nG + 1]
                     ELSE rf
            /\ upc' = "idle"
    /\ UNCHANGED <<lock, curS, kind, calls, obs>>

\* the fetch goroutine receives the next message (pull.go:971-1013)
Deliver(g) ==
    LET f == fg[g] IN
    /\ g \in 1..nG /\ ~f.done /\ f.nS <= N
    /\ obs' = Append(obs, f.nS)
    /\ fg' = [fg EXCEPT ![g] = [nS |-> f.nS + 1, got |-> f.got + 1, done |-> f.got + 1 = B]]
    /\ rf' = IF rf.set /\ rf.g = g
                THEN [rf EXCEPT !.sseq = f.nS, !.done = (f.got + 1 = B)] ELSE rf
    /\ UNCHANGED <<lock, curS, nG, upc, kind, calls>>

\* ... or the request ends (expiry / no messages): done (pull.go:976-987, 1021-1025)
Finish(g) ==
    /\ g \in 1..nG /\ ~fg[g].done
    /\ fg' = [fg EXCEPT ![g].done = TRUE]
    /\ rf' = IF rf.set /\ rf.g = g THEN [rf EXCEPT !.done = TRUE] ELSE rf
    /\ UNCHANGED <<lock, curS, nG, upc, kind, calls, obs>>

Next ==
    \/ \E k \in Kinds : Call(k)
    \/ F1 \/ NW \/ Reset
    \/ \E g \in 1..MaxCalls : Deliver(g) \/ Finish(g)

Spec == Init /\ [][Next]_vars /\ WF_vars(F1 \/ NW \/ Reset)

\* every message reaches the user once and in stream order
InOrder == \A j \in 1..Len(obs) : obs[j] = j
\* a call that returned left c.Mutex free
LockFree == upc = "idle" => lock = "none"
\* no call blocks forever
CallsReturn == upc /= "blocked"
=============================================================================
