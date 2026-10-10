---------------------------- MODULE StatusListener ----------------------------
(***************************************************************************)
(* A connection status listener: nc.StatusChanged(statuses...) returns a  *)
(* buffered channel (cap 10, nats.go:6787) registered for each status;    *)
(* changeConnStatus -> sendStatusEvent (nats.go:6833-6851) runs under      *)
(* nc.mu for every status change; RemoveStatusListener (nats.go:6799).     *)
(*                                                                         *)
(* sendStatusEvent first probes "is the channel closed?" with             *)
(*     select { case <-ch: delete(listeners[s], ch); continue; default: }  *)
(* which is a receive: on an open channel holding an undelivered event it  *)
(* takes that event and unregisters the listener for s.                    *)
(*                                                                         *)
(* The user goroutine reads the channel at arbitrary times, may close it   *)
(* itself (the repo's own tests do `defer close(statusCh)`), or calls      *)
(* RemoveStatusListener. The connection emits an arbitrary bounded         *)
(* sequence of status changes.                                             *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS
    Cap,          \* channel capacity (10 in the code)
    Statuses,     \* statuses the listener asked for
    Emittable,    \* statuses the connection may change to
    MaxEvents,    \* status changes explored
    FixProbe,     \* CONN-1 fix: never receive in sendStatusEvent (send with recover instead)
    FixRemove     \* CONN-2 fix: RemoveStatusListener always closes (once)

VARIABLES
    buf,        \* channel buffer
    chClosed,   \* channel closed
    reg,        \* statuses the channel is registered for
    emitted,    \* events offered to this listener (status in Statuses at emit time)
    got,        \* events the user received
    stolen,     \* events taken off the channel by the library
    removed,    \* RemoveStatusListener called
    userClosed, \* the user closed the channel
    nev

vars == <<buf, chClosed, reg, emitted, got, stolen, removed, userClosed, nev>>

Init ==
    /\ buf = <<>>
    /\ chClosed = FALSE
    /\ reg = Statuses
    /\ emitted = <<>>
    /\ got = <<>>
    /\ stolen = 0
    /\ removed = FALSE
    /\ userClosed = FALSE
    /\ nev = 0

\* changeConnStatus(s) -> sendStatusEvent(s), lock held.
Emit(s) ==
    /\ nev < MaxEvents
    /\ nev' = nev + 1
    /\ emitted' = IF s \in Statuses /\ ~removed /\ ~userClosed THEN Append(emitted, s) ELSE emitted
    /\ IF s \notin reg
       THEN UNCHANGED <<buf, reg, stolen>>
       ELSE IF FixProbe
            THEN IF chClosed
                 THEN /\ reg' = reg \ {s}          \* send panics, recovered: drop
                      /\ UNCHANGED <<buf, stolen>>
                 ELSE /\ buf' = IF Len(buf) < Cap THEN Append(buf, s) ELSE buf
                      /\ UNCHANGED <<reg, stolen>>
            ELSE IF chClosed \/ buf # <<>>
                 THEN \* case <-ch: receives (an event, or the zero value of a
                      \* closed channel) and unregisters
                      /\ reg' = reg \ {s}
                      /\ buf' = IF buf # <<>> THEN Tail(buf) ELSE buf
                      /\ stolen' = IF buf # <<>> THEN stolen + 1 ELSE stolen
                 ELSE /\ buf' = IF Len(buf) < Cap THEN Append(buf, s) ELSE buf
                      /\ UNCHANGED <<reg, stolen>>
    /\ UNCHANGED <<chClosed, got, removed, userClosed>>

Recv ==
    /\ buf # <<>>
    /\ got' = Append(got, Head(buf))
    /\ buf' = Tail(buf)
    /\ UNCHANGED <<chClosed, reg, emitted, stolen, removed, userClosed, nev>>

UserCloses ==
    /\ ~chClosed /\ ~removed /\ ~userClosed
    /\ chClosed' = TRUE
    /\ userClosed' = TRUE
    /\ UNCHANGED <<buf, reg, emitted, got, stolen, removed, nev>>

\* RemoveStatusListener: select { case <-ch: default: close(ch) }; then
\* unregister everywhere.
Remove ==
    /\ ~removed /\ ~userClosed
    /\ removed' = TRUE
    /\ reg' = {}
    /\ IF FixRemove
       THEN /\ chClosed' = TRUE
            /\ UNCHANGED <<buf, stolen>>
       ELSE IF buf # <<>> \/ chClosed
            THEN /\ buf' = IF buf # <<>> THEN Tail(buf) ELSE buf
                 /\ stolen' = IF buf # <<>> THEN stolen + 1 ELSE stolen
                 /\ UNCHANGED chClosed
            ELSE /\ chClosed' = TRUE
                 /\ UNCHANGED <<buf, stolen>>
    /\ UNCHANGED <<emitted, got, userClosed, nev>>

Next ==
    \/ \E s \in Emittable : Emit(s)
    \/ Recv \/ UserCloses \/ Remove

Spec == Init /\ [][Next]_vars

\* The library never takes an event off the user's channel.
NoEventStolen == stolen = 0

\* Until the user removes or closes it, the listener stays registered for
\* every status it asked for.
StaysRegistered == (~removed /\ ~userClosed) => reg = Statuses

\* After RemoveStatusListener the channel is closed (its doc comment), so a
\* `for range ch` over it ends.
RemoveCloses == removed => chClosed

\* With room in the channel, the user sees every offered event, in order:
\* what was received plus what is buffered is exactly what was offered.
\* (Checked only for runs that never filled the channel.)
NoLossBelowCap == (Len(emitted) <= Cap) => (got \o buf = emitted \/ removed \/ userClosed)

=============================================================================
