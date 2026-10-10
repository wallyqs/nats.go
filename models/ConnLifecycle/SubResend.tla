------------------------------ MODULE SubResend ------------------------------
(***************************************************************************)
(* One subscription of a reconnecting nats.go connection and the server's *)
(* view of it: AutoUnsubscribe(max), Unsubscribe, Subscription.Drain with  *)
(* its checkDrained goroutine, delivery through waitForMsgs (async) or     *)
(* NextMsg (sync), and resendSubscriptions on reconnect.                   *)
(*                                                                         *)
(*   client side                                server side (per conn)     *)
(*   nc.subs[sid] present  subIn                srvSub   interest exists   *)
(*   sub.closed            closed               srvMax   UNSUB max         *)
(*   sub.max / delivered   max / delivered      srvNm    msgs sent (nm)    *)
(*   pMsgs                 pendQ (+1 inCb)                                 *)
(*                                                                         *)
(* Client->server protocol (SUB / UNSUB [max]) is a FIFO c2s that the      *)
(* server consumes; MSGs for the sid are a count s2c. A disconnect loses   *)
(* both and the server's subscription; reconnect writes what               *)
(* resendSubscriptions (nats.go:6141-6176) writes.                         *)
(*                                                                         *)
(* Each user call is one action (it runs under nc.mu / sub.mu in the       *)
(* code). checkDrained's nc.Flush() is over-approximated: it may return at *)
(* any time (success, timeout, or error on disconnect).                    *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS
    SubType,        \* "async" (Subscribe) | "sync" (SubscribeSync)
    MaxPub,         \* messages published on the subject
    MaxDisc,        \* disconnects
    AutoMax,        \* values AutoUnsubscribe may be called with ({} = never)
    AllowUnsub,     \* user may call Unsubscribe
    AllowDrain,     \* user may call Drain
    FixDrainResend, \* CONN-3: resendSubscriptions skips draining subscriptions
    FixAutoBase,    \* CONN-4: count AutoUnsubscribe's max against what the server has sent
                    \* on this connection (received since the last resend), not delivered
    FixRemoveOnce   \* CONN-10: removeSub / checkDrained do nothing for a closed sub

VARIABLES
    status,     \* "CONNECTED" | "RECONNECTING"
    c2s,        \* protocol in flight to the server
    srvSub, srvMax, srvNm,
    s2c,        \* MSGs in flight to the client
    subIn,      \* sid in nc.subs
    closed,     \* sub.closed
    max,        \* sub.max
    delivered,  \* sub.delivered
    rcvd,       \* messages processMsg accepted for the sub (FixAutoBase's counter)
    base,       \* rcvd when the subscription was last (re)sent (FixAutoBase)
    pendQ,      \* messages queued in the client, not yet handed to the user
    inCb,       \* async: waitForMsgs is inside the callback (pMsgs still counts it)
    wfmDone,    \* async: waitForMsgs returned
    draining,   \* sub.draining
    cd,         \* checkDrained: "none" | "flush" | "loop" | "done"
    handlerCalls, \* closed handler (SetClosedHandler) invocations
    userGot,    \* messages handed to the user (callback / NextMsg)
    gotAtAuto,  \* userGot when AutoUnsubscribe was called (-1: not called)
    pubs, discs, unsubCalled, drainCalled

vars == <<status, c2s, srvSub, srvMax, srvNm, s2c, subIn, closed, max, delivered,
          rcvd, base, pendQ, inCb, wfmDone, draining, cd, handlerCalls, userGot,
          gotAtAuto, pubs, discs, unsubCalled, drainCalled>>

SUB == [k |-> "SUB", n |-> 0]
UNSUB(n) == [k |-> "UNSUB", n |-> n]

Init ==
    /\ status = "CONNECTED"
    /\ c2s = <<SUB>>            \* subscribeLocked appended SUB (nats.go:5221)
    /\ srvSub = FALSE /\ srvMax = 0 /\ srvNm = 0
    /\ s2c = 0
    /\ subIn = TRUE /\ closed = FALSE
    /\ max = 0 /\ delivered = 0 /\ rcvd = 0 /\ base = 0
    /\ pendQ = 0 /\ inCb = FALSE /\ wfmDone = FALSE
    /\ draining = FALSE /\ cd = "none"
    /\ handlerCalls = 0 /\ userGot = 0 /\ gotAtAuto = -1
    /\ pubs = 0 /\ discs = 0 /\ unsubCalled = FALSE /\ drainCalled = FALSE

Srv == <<srvSub, srvMax, srvNm>>
User == <<userGot, gotAtAuto, pubs, discs, unsubCalled, drainCalled>>

\* removeSub, nats.go:5237-5280: drop from nc.subs, close; the closed handler
\* runs here for non-async subs (async ones get it when waitForMsgs exits).
RemoveSub ==
    IF FixRemoveOnce /\ closed
    THEN UNCHANGED <<subIn, closed, handlerCalls>>
    ELSE /\ subIn' = FALSE
         /\ closed' = TRUE
         /\ handlerCalls' = IF SubType = "sync" THEN handlerCalls + 1 ELSE handlerCalls

\* Append a protocol op unless reconnecting (nats.go:5599 / 5222).
Send(op) == c2s' = IF status = "RECONNECTING" THEN c2s ELSE Append(c2s, op)

(***************************************************************************)
(* Server                                                                  *)
(***************************************************************************)
\* processSub / processUnsub (server/client.go processUnsub: max > nm sets
\* the max, otherwise unsubscribes).
SrvProcess ==
    /\ status = "CONNECTED"
    /\ c2s # <<>>
    /\ c2s' = Tail(c2s)
    /\ LET op == Head(c2s) IN
       IF op.k = "SUB"
       THEN /\ srvSub' = TRUE /\ srvMax' = 0 /\ srvNm' = 0
       ELSE IF srvSub /\ op.n > 0 /\ op.n > srvNm
            THEN /\ srvMax' = op.n /\ UNCHANGED <<srvSub, srvNm>>
            ELSE /\ srvSub' = FALSE /\ srvMax' = 0 /\ UNCHANGED srvNm
    /\ UNCHANGED <<status, s2c, subIn, closed, max, delivered, rcvd, base, pendQ, inCb,
                   wfmDone, draining, cd, handlerCalls, User>>

\* Another client publishes on the subject; the server delivers to the sid
\* if it has interest and auto-unsubscribes at max.
Publish ==
    /\ pubs < MaxPub
    /\ pubs' = pubs + 1
    /\ IF status = "CONNECTED" /\ srvSub
       THEN /\ s2c' = s2c + 1
            /\ srvNm' = srvNm + 1
            /\ IF srvMax > 0 /\ srvNm + 1 >= srvMax
               THEN srvSub' = FALSE /\ srvMax' = 0
               ELSE UNCHANGED <<srvSub, srvMax>>
       ELSE UNCHANGED <<s2c, Srv>>
    /\ UNCHANGED <<status, c2s, subIn, closed, max, delivered, rcvd, base, pendQ, inCb,
                   wfmDone, draining, cd, handlerCalls, userGot, gotAtAuto, discs,
                   unsubCalled, drainCalled>>

(***************************************************************************)
(* Client: readLoop -> processMsg, nats.go:3888-4130                       *)
(***************************************************************************)
MsgArrive ==
    /\ status = "CONNECTED"
    /\ s2c > 0
    /\ s2c' = s2c - 1
    /\ IF subIn /\ ~closed
       THEN /\ pendQ' = pendQ + 1
            /\ rcvd' = rcvd + 1
       ELSE UNCHANGED <<pendQ, rcvd, base>>
    /\ UNCHANGED <<status, c2s, Srv, subIn, closed, max, delivered, base, inCb, wfmDone,
                   draining, cd, handlerCalls, User>>

(***************************************************************************)
(* Delivery                                                                *)
(***************************************************************************)
\* waitForMsgs pops a message and calls the callback if delivered <= max
\* (nats.go:3806-3853).
AsyncCbStart ==
    /\ SubType = "async"
    /\ ~closed /\ ~inCb /\ pendQ > 0
    /\ pendQ' = pendQ - 1
    /\ inCb' = TRUE
    /\ delivered' = delivered + 1
    /\ userGot' = IF max = 0 \/ delivered + 1 <= max THEN userGot + 1 ELSE userGot
    /\ UNCHANGED <<status, c2s, Srv, s2c, subIn, closed, max, rcvd, base, wfmDone, draining,
                   cd, handlerCalls, gotAtAuto, pubs, discs, unsubCalled, drainCalled>>

\* Callback returned: if delivered >= max, removeSub and exit.
AsyncCbEnd ==
    /\ inCb
    /\ inCb' = FALSE
    /\ IF max > 0 /\ delivered >= max
       THEN RemoveSub
       ELSE UNCHANGED <<subIn, closed, handlerCalls>>
    /\ UNCHANGED <<status, c2s, Srv, s2c, max, delivered, rcvd, base, pendQ, wfmDone,
                   draining, cd, User>>

\* waitForMsgs sees closed and returns, calling the closed handler.
AsyncExit ==
    /\ SubType = "async"
    /\ closed /\ ~inCb /\ ~wfmDone
    /\ wfmDone' = TRUE
    /\ handlerCalls' = handlerCalls + 1
    /\ UNCHANGED <<status, c2s, Srv, s2c, subIn, closed, max, delivered, rcvd, base, pendQ,
                   inCb, draining, cd, User>>

\* NextMsg -> processNextMsgDelivered (nats.go:5805-5845).
SyncNext ==
    /\ SubType = "sync"
    /\ ~closed /\ pendQ > 0
    /\ pendQ' = pendQ - 1
    /\ delivered' = delivered + 1
    /\ userGot' = IF max > 0 /\ delivered + 1 > max THEN userGot ELSE userGot + 1
    /\ IF max > 0 /\ delivered + 1 = max
       THEN RemoveSub
       ELSE UNCHANGED <<subIn, closed, handlerCalls>>
    /\ UNCHANGED <<status, c2s, Srv, s2c, max, rcvd, base, inCb, wfmDone, draining, cd,
                   gotAtAuto, pubs, discs, unsubCalled, drainCalled>>

(***************************************************************************)
(* User calls -> nc.unsubscribe(sub, max, drainMode), nats.go:5551-5620    *)
(***************************************************************************)
AutoUnsubscribe(n) ==
    /\ gotAtAuto = -1
    /\ ~closed                                   \* nats.go:5536
    /\ gotAtAuto' = userGot
    /\ max' = n
    \* As is: UNSUB sid n if delivered < n, else UNSUB sid and removeSub. The
    \* server counts n from its SUB on *this* connection.
    /\ LET srvN == IF FixAutoBase THEN (IF rcvd < n THEN n - base ELSE 0)
                                   ELSE (IF delivered < n THEN n ELSE 0)
       IN IF ~subIn
          THEN UNCHANGED <<c2s, subIn, closed, handlerCalls>>
          ELSE /\ IF delivered >= n THEN RemoveSub
                  ELSE UNCHANGED <<subIn, closed, handlerCalls>>
               /\ Send(UNSUB(srvN))
    /\ UNCHANGED <<status, Srv, s2c, delivered, rcvd, base, pendQ, inCb, wfmDone, draining,
                   cd, userGot, pubs, discs, unsubCalled, drainCalled>>

Unsubscribe ==
    /\ AllowUnsub
    /\ ~unsubCalled
    /\ ~closed                                   \* nats.go:5454 ErrBadSubscription
    /\ unsubCalled' = TRUE
    /\ IF subIn
       THEN /\ RemoveSub
            /\ Send(UNSUB(0))
       ELSE UNCHANGED <<c2s, subIn, closed, handlerCalls>>
    /\ UNCHANGED <<status, Srv, s2c, max, delivered, rcvd, base, pendQ, inCb, wfmDone,
                   draining, cd, userGot, gotAtAuto, pubs, discs, drainCalled>>

Drain ==
    /\ AllowDrain
    /\ ~drainCalled
    /\ drainCalled' = TRUE
    /\ IF subIn
       THEN /\ draining' = TRUE
            /\ cd' = "flush"                     \* go nc.checkDrained(sub)
            /\ Send(UNSUB(0))
       ELSE UNCHANGED <<c2s, draining, cd>>
    /\ UNCHANGED <<status, Srv, s2c, subIn, closed, max, delivered, rcvd, base, pendQ, inCb,
                   wfmDone, handlerCalls, userGot, gotAtAuto, pubs, discs, unsubCalled>>

(***************************************************************************)
(* checkDrained, nats.go:5468-5520                                         *)
(***************************************************************************)
CDFlushReturns ==
    /\ cd = "flush"
    /\ cd' = "loop"
    /\ UNCHANGED <<status, c2s, Srv, s2c, subIn, closed, max, delivered, rcvd, base, pendQ,
                   inCb, wfmDone, draining, handlerCalls, User>>

\* if conn == nil || closed || pMsgs == 0 { removeSub; return } -- even when
\* the subscription is already closed.
CDRemove ==
    /\ cd = "loop"
    /\ closed \/ (pendQ = 0 /\ ~inCb)
    /\ cd' = "done"
    /\ draining' = FALSE
    /\ RemoveSub
    /\ UNCHANGED <<status, c2s, Srv, s2c, max, delivered, rcvd, base, pendQ, inCb, wfmDone, User>>

(***************************************************************************)
(* Connection                                                              *)
(***************************************************************************)
Disconnect ==
    /\ status = "CONNECTED"
    /\ discs < MaxDisc
    /\ discs' = discs + 1
    /\ status' = "RECONNECTING"
    /\ c2s' = <<>>
    /\ s2c' = 0
    /\ srvSub' = FALSE /\ srvMax' = 0 /\ srvNm' = 0
    /\ UNCHANGED <<subIn, closed, max, delivered, rcvd, base, pendQ, inCb, wfmDone, draining,
                   cd, handlerCalls, userGot, gotAtAuto, pubs, unsubCalled, drainCalled>>

\* resendSubscriptions (nats.go:6141-6176) on the new connection.
Resend ==
    IF ~subIn \/ (FixDrainResend /\ draining) THEN <<>>
    ELSE IF max > 0
         THEN LET got == IF FixAutoBase THEN rcvd ELSE delivered
                  adj == IF got < max THEN max - got ELSE 0
              IN IF adj = 0 THEN <<UNSUB(0)>> ELSE <<SUB, UNSUB(adj)>>
         ELSE <<SUB>>

Reconnect ==
    /\ status = "RECONNECTING"
    /\ status' = "CONNECTED"
    /\ c2s' = Resend
    /\ base' = IF FixAutoBase THEN rcvd ELSE base
    /\ UNCHANGED <<Srv, s2c, subIn, closed, max, delivered, rcvd, base, pendQ, inCb, wfmDone,
                   draining, cd, handlerCalls, User>>

Next ==
    \/ SrvProcess \/ Publish \/ MsgArrive
    \/ AsyncCbStart \/ AsyncCbEnd \/ AsyncExit \/ SyncNext
    \/ \E n \in AutoMax : AutoUnsubscribe(n)
    \/ Unsubscribe \/ Drain
    \/ CDFlushReturns \/ CDRemove
    \/ Disconnect \/ Reconnect

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
\* AutoUnsubscribe(n): at most n messages reach the user (counting those that
\* already had when it was called), across reconnects.
MaxRespected ==
    gotAtAuto >= 0 => userGot <= IF gotAtAuto > max THEN gotAtAuto ELSE max

\* Once the client has removed the subscription and the server has caught
\* up with everything the client sent, the server holds no interest for it.
NoLeakedInterest ==
    (status = "CONNECTED" /\ c2s = <<>> /\ ~subIn) => ~srvSub

\* The closed handler runs at most once.
ClosedHandlerOnce == handlerCalls <= 1

=============================================================================
