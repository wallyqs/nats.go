---------------------------- MODULE PullConsume ----------------------------
(***************************************************************************)
(* Model of jetstream/pull.go pullConsumer.Consume() as of the base commit *)
(* of this branch (upstream nats.go main after #2175).                     *)
(*                                                                         *)
(* Goroutines, one PlusCal process each:                                   *)
(*   h    the nats.go async-subscription goroutine (waitForMsgs,           *)
(*        nats.go:3786) running internalHandler (pull.go:245-302) and,     *)
(*        on exit, the subscription closed handler (pull.go:313-324)       *)
(*   s    the status goroutine started by Consume (pull.go:348-428)        *)
(*   p    pullMessages (pull.go:1074-1098) + cleanup (pull.go:1120-1150)   *)
(*   t    the heartbeat timer, time.AfterFunc of scheduleHeartbeatCheck    *)
(*        (pull.go:1106-1118): arming, firing, and the fired goroutines    *)
(*   u    the user: Stop (pull.go:816), Drain (pull.go:827), Closed        *)
(*        (pull.go:838)                                                    *)
(*   lib  nats.go drain bookkeeping (checkDrained, nats.go:5468)           *)
(*   env  server + network: deliveries, 408 expiries, idle heartbeats,     *)
(*        disconnect / reconnect / close, sendStatusEvent (nats.go:6833)   *)
(*                                                                         *)
(* Channels keep their real capacities: fetchNext 1 (pull.go:235), errs 10 *)
(* (pull.go:233), StatusChanged 10 (nats.go:6788).  sub.Lock is `lock`.    *)
(*                                                                         *)
(* The constant Fixes switches on the proposed fixes from the README, one  *)
(* name per finding, so that every *_bug.cfg differs from the passing cfg  *)
(* by exactly one fix.                                                     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxMsgs,            \* consumeOpts.MaxMessages
    Thresh,             \* consumeOpts.ThresholdMessages = ceil(MaxMsgs/2) (pull.go:1234)
    StopAfter,          \* consumeOpts.StopAfter, 0 = unset
    StreamMsgs,         \* messages the stream can deliver over the whole behavior
    MaxReconnects,      \* bound on disconnect/reconnect cycles
    MaxExpires,         \* bound on server-side request expirations (408)
    MaxHBMsgs,          \* bound on idle heartbeats sent by the server
    MaxHBFires,         \* bound on heartbeat timer firings
    ErrsCap,            \* cap(sub.errs) = 10
    StatusCap,          \* cap(StatusChanged chan) = 10
    UserOps,            \* subset of {"stop", "drain", "closed"}
    AllowClose,         \* nc.Close() may happen
    ReqsOnDisconnect,   \* "keep" | "drop" | "either": waiting pulls at the server after a disconnect
    LateHeartbeats,     \* the heartbeat timer may fire while the server still holds a
                        \* waiting pull (heartbeats delayed > 2x by the network)
    FastPull,           \* the server never answers a pull (message or 408) before
                        \* pullMessages has run the statement after pull() returned  \* a disconnect may make the server forget waiting pulls
    Fixes               \* subset of {"orphan", "steal", "stopafter", "pendingreset", "fip",
                        \*            "clamp", "drainrace", "closedch"}

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

InitBatch == IF StopAfter > 0 THEN Min(MaxMsgs, StopAfter) ELSE MaxMsgs   \* pull.go:329-332

(* --algorithm PullConsume {
variables
    \* --- connection and server ---
    conn = "up",             \* up | down (RECONNECTING) | closed
    reconnects = 0, expires = 0, hbMsgs = 0, hbFires = 0,
    reqs = << InitBatch >>,  \* waiting pull requests at the server: messages left, FIFO
    rbuf = << >>,            \* pull requests published while reconnecting (nats.go pending buffer)
    avail = StreamMsgs,      \* messages the stream can still deliver
    \* --- nats.go async subscription on the consume inbox ---
    subState = "active",     \* active | drainflush | draining | closed
    subQ = << >>,            \* pending list (pHead..pTail), unbounded in nats.go
    inCb = FALSE,            \* a message is being delivered (pMsgs > 0 until loop top)
    \* --- StatusChanged listener (nats.go:6782) ---
    regs = {"RECONNECTING", "CONNECTED", "CLOSED"},  \* statuses this chan is registered for
    statusCh = << >>, statusClosed = FALSE,
    \* --- pullSubscription ---
    lock = "none",
    fetchNext = << >>, errs = << >>,
    done = FALSE, closed = 0, draining = 0, fip = 0,
    pending = IF "pendingreset" \in Fixes THEN InitBatch ELSE MaxMsgs,  \* resetPendingMsgs, pull.go:328
    delivered = 0,
    hbArmed = TRUE, hbClosed = FALSE, hbFiring = 0,
    closedCh = "nil",        \* sub.closedCh: nil | ch
    chRan = FALSE,           \* the subscription closed handler has run
    userCh = "none",         \* the channel returned by Closed(): none | open | closed
    \* --- observation only ---
    handlerCalls = 0, maxBatch = InitBatch, badBatch = FALSE,
    userStop = FALSE, userDrain = FALSE, drainDowngraded = FALSE,
    drainLost = FALSE;       \* a message buffered when Drain was called never reached the handler

define {
    \* With FastPull the newest pull, just published by pullMessages (now at
    \* p_fip, fetchInProgress still 1), gets no answer before p_fip runs.
    PullSettled == ~(FastPull /\ pc["p"] = "p_fip" /\ Len(reqs) = 1)
    \* Without late heartbeats the 2x heartbeat timer (>= 1s) only runs out
    \* when no heartbeat can come: no pull waiting at the server or on its
    \* way there (fetchNext, pull in progress, reconnect buffer), or no
    \* connection / no interest), and only while the consuming goroutine is
    \* idle (it re-arms the timer on every message it processes).
    HBCanExpire == \/ LateHeartbeats
                   \/ /\ pc["h"] = "h_wait"
                      /\ \/ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
                         \/ conn # "up"
                         \/ subState \notin {"active", "drainflush"}
    Accepting == conn = "up" /\ subState \in {"active", "drainflush"}
    ConnBatch == IF "stopafter" \in Fixes /\ StopAfter > 0
                 THEN Min(MaxMsgs, StopAfter - delivered)
                 ELSE MaxMsgs                                   \* pull.go:375
    NoHBBatch == IF StopAfter > 0 THEN Min(MaxMsgs, StopAfter - delivered)
                 ELSE MaxMsgs                                   \* pull.go:397-400
    \* value resetPendingMsgs leaves behind (pull.go:439); with the
    \* pendingreset fix it is the batch that was actually requested
    ResetTo(b) == IF "pendingreset" \in Fixes THEN b ELSE MaxMsgs
    CanSendFetch == Len(fetchNext) < 1
    \* with the orphan fix every fetchNext send is select{fetchNext<-; <-done}
    FetchSendDone == "orphan" \in Fixes /\ done
}

macro SendStatus(s) {   \* sendStatusEvent, nats.go:6833-6851
    if (s \in regs) {
        if (statusClosed) {
            regs := regs \ {s};
        } else if (Len(statusCh) > 0 /\ "steal" \notin Fixes) {
            \* `case <-ch:` meant as a closed-channel probe receives a
            \* buffered event instead: the event is lost, the listener is
            \* unregistered for s, and s is not sent (nats.go:6837-6841)
            statusCh := Tail(statusCh);
            regs := regs \ {s};
        } else if (Len(statusCh) < StatusCap) {
            statusCh := Append(statusCh, s);
        }
    }
}

macro DoStop() {        \* pull.go:816-821
    if (closed = 0) { closed := 1; done := TRUE; }
}

procedure CheckPending()   \* pull.go:461-504, called with sub.Lock held
variables batch = 0;
{
cp_eval:
    \* With the "fip" fix (PULL-10) the check no longer waits for
    \* fetchInProgress to clear, and the send never blocks: when fetchNext
    \* already holds a request, this one is skipped.
    if (pending < Thresh /\ (fip = 0 \/ "fip" \in Fixes)) {
        batch := IF StopAfter > 0
                 THEN Min(MaxMsgs - pending, StopAfter - delivered - pending)
                 ELSE MaxMsgs - pending;
        if (batch > 0) {
cp_send:
            await CanSendFetch \/ FetchSendDone \/ "fip" \in Fixes;            \* pull.go:488
            if (CanSendFetch) {
                fetchNext := Append(fetchNext, batch);
                pending := IF "pendingreset" \in Fixes THEN pending + batch ELSE MaxMsgs;  \* pull.go:500
            } else if ("fip" \notin Fixes) {
                pending := IF "pendingreset" \in Fixes THEN pending + batch ELSE MaxMsgs;
            };
        };
    };
cp_ret:
    return;
}

fair process (Handler = "h")
variables item = "none", left = 0;
{
h_wait:
    while (TRUE) {
        await subState = "closed" \/ Len(subQ) > 0;          \* nats.go:3806
        if (subState = "closed") {                           \* nats.go:3836-3838
            if (Len(subQ) > 0 /\ userDrain /\ ~userStop /\ conn # "closed") { drainLost := TRUE; };
            goto h_ch;
        } else {
            item := Head(subQ).t; left := Head(subQ).n;
            subQ := Tail(subQ); inCb := TRUE;
            hbArmed := FALSE;                                \* pull.go:246-248
        };
h_disp:
        if (item = "hb") {                                   \* pull.go:250-255
            if (~hbClosed) { hbArmed := TRUE; };
            inCb := FALSE;
        } else if (item = "msg") {
            if ("stopafter" \in Fixes /\ ((closed = 1 /\ draining = 0) \/ (StopAfter > 0 /\ delivered >= StopAfter))) {
                inCb := FALSE;                               \* fix: stopped, or StopAfter reached: no callback
            } else {
h_user:
                handlerCalls := handlerCalls + 1;            \* pull.go:289
h_lk1:
                await lock = "none"; lock := "h";            \* pull.go:290
                pending := IF "clamp" \in Fixes THEN Max(0, pending - 1)
                           ELSE pending - 1;                 \* pull.go:291, 446
                delivered := delivered + 1;                  \* pull.go:292
                call CheckPending();                         \* pull.go:293
h_rs1:
                if (~hbClosed) { hbArmed := TRUE; };         \* pull.go:294-296
                lock := "none";                              \* pull.go:297
h_sa:
                if (StopAfter > 0 /\ StopAfter = delivered) { DoStop(); };  \* pull.go:299-301
                inCb := FALSE;
            };
        } else {                                             \* 408 Request Timeout
h_lk2:
            await lock = "none"; lock := "h";                \* pull.go:262
            pending := Max(0, pending - left);               \* pull.go:768-775
            call CheckPending();                             \* pull.go:265
h_rs2:
            if (~hbClosed) { hbArmed := TRUE; };             \* pull.go:266-268
            lock := "none";                                  \* pull.go:270
            inCb := FALSE;
        };
    };
h_ch:                                                        \* closed handler, pull.go:315-316
    draining := 0; chRan := TRUE;
h_chlk:
    await lock = "none"; lock := "h";                        \* pull.go:317
    if (closedCh = "ch") { userCh := "closed"; closedCh := "nil"; };  \* pull.go:318-321
h_chul:
    lock := "none";                                          \* pull.go:322
}

fair process (Status = "s")
variables isConn = TRUE, st = "none", e = "none", nb = 0;
{
s_top:
    while (TRUE) {
        if (closed = 1) { goto s_exit; };                    \* pull.go:351
s_sel:
        either {                                             \* pull.go:355
            await Len(statusCh) > 0;
            st := Head(statusCh); statusCh := Tail(statusCh);
            if (st = "RECONNECTING") {                       \* pull.go:360-364
                hbArmed := FALSE; isConn := FALSE;
            } else if (st = "CONNECTED") {
s_clk:
                await lock = "none"; lock := "s";            \* pull.go:366
                if (~isConn) {
                    isConn := TRUE; nb := ConnBatch;         \* pull.go:367-375
                    if (nb > 0) {
s_csend:
                        await CanSendFetch \/ FetchSendDone; \* pull.go:373
                        if (CanSendFetch) { fetchNext := Append(fetchNext, nb); };
                        if (~hbClosed) { hbArmed := TRUE; }; \* pull.go:384-386
                        pending := ResetTo(nb);              \* pull.go:387
                    };
                };
s_cul:
                lock := "none";                              \* pull.go:389
            } else {
s_clsend:
                await Len(errs) < ErrsCap;                   \* pull.go:392, self-send
                errs := Append(errs, "closed");
            };
        } or {
            await statusClosed /\ Len(statusCh) = 0;         \* !ok -> continue, pull.go:356-358
        } or {
            await Len(errs) > 0;                             \* pull.go:394
            e := Head(errs); errs := Tail(errs);
s_elk:
            await lock = "none"; lock := "s";                \* pull.go:395
            if (e = "nohb" /\ ("stopafter" \notin Fixes \/ NoHBBatch > 0)) {
                nb := NoHBBatch;                             \* pull.go:397-400
s_esend:
                await CanSendFetch \/ FetchSendDone;         \* pull.go:401
                if (CanSendFetch) { fetchNext := Append(fetchNext, nb); };
                if (~hbClosed) { hbArmed := TRUE; };         \* pull.go:412-414
                pending := ResetTo(nb);                      \* pull.go:415
            };
s_eul:
            lock := "none";                                  \* pull.go:417
s_eh:                                                        \* ErrHandler, pull.go:418-423
            if (e = "closed") { DoStop(); };
        } or {
            await done;                                      \* pull.go:424-425
            goto s_exit;
        };
    };
s_exit:
    skip;
}

fair process (Puller = "p")
variables req = 0;
{
p_sel:
    while (TRUE) {
        either {
            await Len(fetchNext) > 0;                        \* pull.go:1077
            req := Head(fetchNext); fetchNext := Tail(fetchNext);
            fip := 1;                                        \* pull.go:1078
p_pull:                                                      \* pull(), pull.go:1154-1173
            if (closed = 1) {                                \* pull.go:1157, 1081-1083
                goto p_cl1;
            } else if (req < 1) {                            \* pull.go:1160
                badBatch := TRUE;
                goto p_err;
            } else if (conn = "up") {
                reqs := Append(reqs, req); maxBatch := Max(maxBatch, req);
            } else if (conn = "down") {                      \* buffered while reconnecting
                rbuf := Append(rbuf, req); maxBatch := Max(maxBatch, req);
            } else {                                         \* ErrConnectionClosed
                goto p_err;
            };
p_fip:
            fip := 0;                                        \* pull.go:1092
        } or {
            await done;                                      \* pull.go:1093-1095
            goto p_cl1;
        };
    };
p_err:                                                       \* pull.go:1087-1090
    either { await Len(errs) < ErrsCap; errs := Append(errs, "pullerr"); }
    or     { await done; };
p_fip2:
    fip := 0;
    goto p_sel;
p_cl1:                                                       \* cleanup, pull.go:1132-1138
    \* RemoveStatusListener, nats.go:6799-6814: consumes one buffered value
    \* instead of closing the channel when one is buffered
    if (Len(statusCh) > 0) { statusCh := Tail(statusCh); } else { statusClosed := TRUE; };
    regs := {};
    hbClosed := TRUE; hbArmed := FALSE;                      \* hbMonitor.Close
p_cl2:                                                       \* pull.go:1139-1149
    if (subState = "closed") {
        closed := 1;
    } else if (draining = 1) {
        subState := "drainflush"; closed := 1;
    } else {
        subState := "closed"; closed := 1;
        if (userDrain /\ ~userStop) { drainDowngraded := TRUE; };
    };
}

fair process (Timer = "t")
{
t_loop:
    while (TRUE) {
        either {                                             \* AfterFunc fires, pull.go:1111
            await hbArmed /\ hbFires < MaxHBFires /\ HBCanExpire;
            hbArmed := FALSE; hbFiring := hbFiring + 1; hbFires := hbFires + 1;
        } or {                                               \* pull.go:1112-1114
            await hbFiring > 0 /\ Len(errs) < ErrsCap;
            errs := Append(errs, "nohb"); hbFiring := hbFiring - 1;
        } or {                                               \* pull.go:1115
            await hbFiring > 0 /\ done;
            hbFiring := hbFiring - 1;
        };
    };
}

fair process (User = "u")
{
u_main:
    either {
        await "stop" \in UserOps;                            \* pull.go:816-821
        userStop := TRUE;
        DoStop();
    } or {
        await "drain" \in UserOps;                           \* pull.go:827-833
        if (closed = 0) {
            closed := 1; userDrain := TRUE;
            if ("drainrace" \in Fixes) { draining := 1; done := TRUE; };
        } else {
            goto u_closed;
        };
u_drain2:
        if ("drainrace" \notin Fixes) { draining := 1; done := TRUE; };
    } or {
        skip;
    };
u_closed:
    if ("closed" \in UserOps) {
u_clk:
        await lock = "none"; lock := "u";                    \* pull.go:839
        if (IF "closedch" \in Fixes THEN chRan ELSE subState = "closed") {   \* pull.go:846-849
            userCh := "closed"; closedCh := "nil";
        } else {
            userCh := "open"; closedCh := "ch";              \* pull.go:841-845
        };
u_clul:
        lock := "none";
    };
}

fair process (Lib = "lib")
{
l_loop:
    while (TRUE) {
        either {                                             \* checkDrained: Flush, nats.go:5478
            await subState = "drainflush";
            subState := "draining";
        } or {                                               \* pMsgs == 0 -> removeSub, nats.go:5490-5493
            await subState = "draining" /\ Len(subQ) = 0 /\ ~inCb;
            subState := "closed";
        };
    };
}

process (Env = "env")
{
e_loop:
    while (TRUE) {
        either {        \* server delivers one message for the oldest waiting pull
            await Accepting /\ Len(reqs) > 0 /\ avail > 0 /\ PullSettled;
            subQ := Append(subQ, [t |-> "msg", n |-> 0]);
            avail := avail - 1;
            reqs := IF Head(reqs) = 1 THEN Tail(reqs) ELSE << Head(reqs) - 1 >> \o Tail(reqs);
        } or {          \* oldest pull expires: 408 with Nats-Pending-Messages
            await Len(reqs) > 0 /\ expires < MaxExpires /\ PullSettled;
            if (Accepting) { subQ := Append(subQ, [t |-> "408", n |-> Head(reqs)]); };
            reqs := Tail(reqs);
            expires := expires + 1;
        } or {          \* idle heartbeat (100) for a waiting pull
            await Accepting /\ Len(reqs) > 0 /\ hbMsgs < MaxHBMsgs;
            subQ := Append(subQ, [t |-> "hb", n |-> 0]);
            hbMsgs := hbMsgs + 1;
        } or {          \* connection lost, nats.go enters RECONNECTING
            await conn = "up" /\ reconnects < MaxReconnects;
            conn := "down";
            SendStatus("RECONNECTING");
            if (ReqsOnDisconnect = "drop") { reqs := << >>; }
            else if (ReqsOnDisconnect = "either") { either { reqs := << >>; } or { skip; }; };
        } or {          \* reconnected: pending buffer flushed, CONNECTED
            await conn = "down";
            conn := "up"; reconnects := reconnects + 1;
            reqs := reqs \o rbuf; rbuf := << >>;
            SendStatus("CONNECTED");
        } or {          \* nc.Close(), nats.go:6204: subs marked closed, then CLOSED
            await AllowClose /\ conn # "closed";
            conn := "closed"; rbuf := << >>; reqs := << >>;
            subState := "closed";
            SendStatus("CLOSED");
        };
    };
}
} *)
\* BEGIN TRANSLATION
VARIABLES pc, conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, avail, 
          subState, subQ, inCb, regs, statusCh, statusClosed, lock, fetchNext, 
          errs, done, closed, draining, fip, pending, delivered, hbArmed, 
          hbClosed, hbFiring, closedCh, chRan, userCh, handlerCalls, maxBatch, 
          badBatch, userStop, userDrain, drainDowngraded, drainLost, stack

(* define statement *)
PullSettled == ~(FastPull /\ pc["p"] = "p_fip" /\ Len(reqs) = 1)





HBCanExpire == \/ LateHeartbeats
               \/ /\ pc["h"] = "h_wait"
                  /\ \/ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
                     \/ conn # "up"
                     \/ subState \notin {"active", "drainflush"}
Accepting == conn = "up" /\ subState \in {"active", "drainflush"}
ConnBatch == IF "stopafter" \in Fixes /\ StopAfter > 0
             THEN Min(MaxMsgs, StopAfter - delivered)
             ELSE MaxMsgs
NoHBBatch == IF StopAfter > 0 THEN Min(MaxMsgs, StopAfter - delivered)
             ELSE MaxMsgs


ResetTo(b) == IF "pendingreset" \in Fixes THEN b ELSE MaxMsgs
CanSendFetch == Len(fetchNext) < 1

FetchSendDone == "orphan" \in Fixes /\ done

VARIABLES batch, item, left, isConn, st, e, nb, req

vars == << pc, conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, avail, 
           subState, subQ, inCb, regs, statusCh, statusClosed, lock, 
           fetchNext, errs, done, closed, draining, fip, pending, delivered, 
           hbArmed, hbClosed, hbFiring, closedCh, chRan, userCh, handlerCalls, 
           maxBatch, badBatch, userStop, userDrain, drainDowngraded, 
           drainLost, stack, batch, item, left, isConn, st, e, nb, req >>

ProcSet == {"h"} \cup {"s"} \cup {"p"} \cup {"t"} \cup {"u"} \cup {"lib"} \cup {"env"}

Init == (* Global variables *)
        /\ conn = "up"
        /\ reconnects = 0
        /\ expires = 0
        /\ hbMsgs = 0
        /\ hbFires = 0
        /\ reqs = << InitBatch >>
        /\ rbuf = << >>
        /\ avail = StreamMsgs
        /\ subState = "active"
        /\ subQ = << >>
        /\ inCb = FALSE
        /\ regs = {"RECONNECTING", "CONNECTED", "CLOSED"}
        /\ statusCh = << >>
        /\ statusClosed = FALSE
        /\ lock = "none"
        /\ fetchNext = << >>
        /\ errs = << >>
        /\ done = FALSE
        /\ closed = 0
        /\ draining = 0
        /\ fip = 0
        /\ pending = (IF "pendingreset" \in Fixes THEN InitBatch ELSE MaxMsgs)
        /\ delivered = 0
        /\ hbArmed = TRUE
        /\ hbClosed = FALSE
        /\ hbFiring = 0
        /\ closedCh = "nil"
        /\ chRan = FALSE
        /\ userCh = "none"
        /\ handlerCalls = 0
        /\ maxBatch = InitBatch
        /\ badBatch = FALSE
        /\ userStop = FALSE
        /\ userDrain = FALSE
        /\ drainDowngraded = FALSE
        /\ drainLost = FALSE
        (* Procedure CheckPending *)
        /\ batch = [ self \in ProcSet |-> 0]
        (* Process Handler *)
        /\ item = "none"
        /\ left = 0
        (* Process Status *)
        /\ isConn = TRUE
        /\ st = "none"
        /\ e = "none"
        /\ nb = 0
        (* Process Puller *)
        /\ req = 0
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self = "h" -> "h_wait"
                                        [] self = "s" -> "s_top"
                                        [] self = "p" -> "p_sel"
                                        [] self = "t" -> "t_loop"
                                        [] self = "u" -> "u_main"
                                        [] self = "lib" -> "l_loop"
                                        [] self = "env" -> "e_loop"]

cp_eval(self) == /\ pc[self] = "cp_eval"
                 /\ IF pending < Thresh /\ (fip = 0 \/ "fip" \in Fixes)
                       THEN /\ batch' = [batch EXCEPT ![self] = IF StopAfter > 0
                                                                THEN Min(MaxMsgs - pending, StopAfter - delivered - pending)
                                                                ELSE MaxMsgs - pending]
                            /\ IF batch'[self] > 0
                                  THEN /\ pc' = [pc EXCEPT ![self] = "cp_send"]
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "cp_ret"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "cp_ret"]
                            /\ batch' = batch
                 /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, 
                                 reqs, rbuf, avail, subState, subQ, inCb, regs, 
                                 statusCh, statusClosed, lock, fetchNext, errs, 
                                 done, closed, draining, fip, pending, 
                                 delivered, hbArmed, hbClosed, hbFiring, 
                                 closedCh, chRan, userCh, handlerCalls, 
                                 maxBatch, badBatch, userStop, userDrain, 
                                 drainDowngraded, drainLost, stack, item, left, 
                                 isConn, st, e, nb, req >>

cp_send(self) == /\ pc[self] = "cp_send"
                 /\ CanSendFetch \/ FetchSendDone \/ "fip" \in Fixes
                 /\ IF CanSendFetch
                       THEN /\ fetchNext' = Append(fetchNext, batch[self])
                            /\ pending' = (IF "pendingreset" \in Fixes THEN pending + batch[self] ELSE MaxMsgs)
                       ELSE /\ IF "fip" \notin Fixes
                                  THEN /\ pending' = (IF "pendingreset" \in Fixes THEN pending + batch[self] ELSE MaxMsgs)
                                  ELSE /\ TRUE
                                       /\ UNCHANGED pending
                            /\ UNCHANGED fetchNext
                 /\ pc' = [pc EXCEPT ![self] = "cp_ret"]
                 /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, 
                                 reqs, rbuf, avail, subState, subQ, inCb, regs, 
                                 statusCh, statusClosed, lock, errs, done, 
                                 closed, draining, fip, delivered, hbArmed, 
                                 hbClosed, hbFiring, closedCh, chRan, userCh, 
                                 handlerCalls, maxBatch, badBatch, userStop, 
                                 userDrain, drainDowngraded, drainLost, stack, 
                                 batch, item, left, isConn, st, e, nb, req >>

cp_ret(self) == /\ pc[self] = "cp_ret"
                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                /\ batch' = [batch EXCEPT ![self] = Head(stack[self]).batch]
                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, 
                                reqs, rbuf, avail, subState, subQ, inCb, regs, 
                                statusCh, statusClosed, lock, fetchNext, errs, 
                                done, closed, draining, fip, pending, 
                                delivered, hbArmed, hbClosed, hbFiring, 
                                closedCh, chRan, userCh, handlerCalls, 
                                maxBatch, badBatch, userStop, userDrain, 
                                drainDowngraded, drainLost, item, left, isConn, 
                                st, e, nb, req >>

CheckPending(self) == cp_eval(self) \/ cp_send(self) \/ cp_ret(self)

h_wait == /\ pc["h"] = "h_wait"
          /\ subState = "closed" \/ Len(subQ) > 0
          /\ IF subState = "closed"
                THEN /\ IF Len(subQ) > 0 /\ userDrain /\ ~userStop /\ conn # "closed"
                           THEN /\ drainLost' = TRUE
                           ELSE /\ TRUE
                                /\ UNCHANGED drainLost
                     /\ pc' = [pc EXCEPT !["h"] = "h_ch"]
                     /\ UNCHANGED << subQ, inCb, hbArmed, item, left >>
                ELSE /\ item' = Head(subQ).t
                     /\ left' = Head(subQ).n
                     /\ subQ' = Tail(subQ)
                     /\ inCb' = TRUE
                     /\ hbArmed' = FALSE
                     /\ pc' = [pc EXCEPT !["h"] = "h_disp"]
                     /\ UNCHANGED drainLost
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, regs, statusCh, statusClosed, 
                          lock, fetchNext, errs, done, closed, draining, fip, 
                          pending, delivered, hbClosed, hbFiring, closedCh, 
                          chRan, userCh, handlerCalls, maxBatch, badBatch, 
                          userStop, userDrain, drainDowngraded, stack, batch, 
                          isConn, st, e, nb, req >>

h_disp == /\ pc["h"] = "h_disp"
          /\ IF item = "hb"
                THEN /\ IF ~hbClosed
                           THEN /\ hbArmed' = TRUE
                           ELSE /\ TRUE
                                /\ UNCHANGED hbArmed
                     /\ inCb' = FALSE
                     /\ pc' = [pc EXCEPT !["h"] = "h_wait"]
                ELSE /\ IF item = "msg"
                           THEN /\ IF "stopafter" \in Fixes /\ ((closed = 1 /\ draining = 0) \/ (StopAfter > 0 /\ delivered >= StopAfter))
                                      THEN /\ inCb' = FALSE
                                           /\ pc' = [pc EXCEPT !["h"] = "h_wait"]
                                      ELSE /\ pc' = [pc EXCEPT !["h"] = "h_user"]
                                           /\ inCb' = inCb
                           ELSE /\ pc' = [pc EXCEPT !["h"] = "h_lk2"]
                                /\ inCb' = inCb
                     /\ UNCHANGED hbArmed
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

h_lk2 == /\ pc["h"] = "h_lk2"
         /\ lock = "none"
         /\ lock' = "h"
         /\ pending' = Max(0, pending - left)
         /\ stack' = [stack EXCEPT !["h"] = << [ procedure |->  "CheckPending",
                                                 pc        |->  "h_rs2",
                                                 batch     |->  batch["h"] ] >>
                                             \o stack["h"]]
         /\ batch' = [batch EXCEPT !["h"] = 0]
         /\ pc' = [pc EXCEPT !["h"] = "cp_eval"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, delivered, hbArmed, hbClosed, hbFiring, closedCh, 
                         chRan, userCh, handlerCalls, maxBatch, badBatch, 
                         userStop, userDrain, drainDowngraded, drainLost, item, 
                         left, isConn, st, e, nb, req >>

h_rs2 == /\ pc["h"] = "h_rs2"
         /\ IF ~hbClosed
               THEN /\ hbArmed' = TRUE
               ELSE /\ TRUE
                    /\ UNCHANGED hbArmed
         /\ lock' = "none"
         /\ inCb' = FALSE
         /\ pc' = [pc EXCEPT !["h"] = "h_wait"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbClosed, hbFiring, closedCh, 
                         chRan, userCh, handlerCalls, maxBatch, badBatch, 
                         userStop, userDrain, drainDowngraded, drainLost, 
                         stack, batch, item, left, isConn, st, e, nb, req >>

h_user == /\ pc["h"] = "h_user"
          /\ handlerCalls' = handlerCalls + 1
          /\ pc' = [pc EXCEPT !["h"] = "h_lk1"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, maxBatch, 
                          badBatch, userStop, userDrain, drainDowngraded, 
                          drainLost, stack, batch, item, left, isConn, st, e, 
                          nb, req >>

h_lk1 == /\ pc["h"] = "h_lk1"
         /\ lock = "none"
         /\ lock' = "h"
         /\ pending' = (IF "clamp" \in Fixes THEN Max(0, pending - 1)
                        ELSE pending - 1)
         /\ delivered' = delivered + 1
         /\ stack' = [stack EXCEPT !["h"] = << [ procedure |->  "CheckPending",
                                                 pc        |->  "h_rs1",
                                                 batch     |->  batch["h"] ] >>
                                             \o stack["h"]]
         /\ batch' = [batch EXCEPT !["h"] = 0]
         /\ pc' = [pc EXCEPT !["h"] = "cp_eval"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, hbArmed, hbClosed, hbFiring, closedCh, chRan, 
                         userCh, handlerCalls, maxBatch, badBatch, userStop, 
                         userDrain, drainDowngraded, drainLost, item, left, 
                         isConn, st, e, nb, req >>

h_rs1 == /\ pc["h"] = "h_rs1"
         /\ IF ~hbClosed
               THEN /\ hbArmed' = TRUE
               ELSE /\ TRUE
                    /\ UNCHANGED hbArmed
         /\ lock' = "none"
         /\ pc' = [pc EXCEPT !["h"] = "h_sa"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbClosed, hbFiring, closedCh, 
                         chRan, userCh, handlerCalls, maxBatch, badBatch, 
                         userStop, userDrain, drainDowngraded, drainLost, 
                         stack, batch, item, left, isConn, st, e, nb, req >>

h_sa == /\ pc["h"] = "h_sa"
        /\ IF StopAfter > 0 /\ StopAfter = delivered
              THEN /\ IF closed = 0
                         THEN /\ closed' = 1
                              /\ done' = TRUE
                         ELSE /\ TRUE
                              /\ UNCHANGED << done, closed >>
              ELSE /\ TRUE
                   /\ UNCHANGED << done, closed >>
        /\ inCb' = FALSE
        /\ pc' = [pc EXCEPT !["h"] = "h_wait"]
        /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, 
                        avail, subState, subQ, regs, statusCh, statusClosed, 
                        lock, fetchNext, errs, draining, fip, pending, 
                        delivered, hbArmed, hbClosed, hbFiring, closedCh, 
                        chRan, userCh, handlerCalls, maxBatch, badBatch, 
                        userStop, userDrain, drainDowngraded, drainLost, stack, 
                        batch, item, left, isConn, st, e, nb, req >>

h_ch == /\ pc["h"] = "h_ch"
        /\ draining' = 0
        /\ chRan' = TRUE
        /\ pc' = [pc EXCEPT !["h"] = "h_chlk"]
        /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, 
                        avail, subState, subQ, inCb, regs, statusCh, 
                        statusClosed, lock, fetchNext, errs, done, closed, fip, 
                        pending, delivered, hbArmed, hbClosed, hbFiring, 
                        closedCh, userCh, handlerCalls, maxBatch, badBatch, 
                        userStop, userDrain, drainDowngraded, drainLost, stack, 
                        batch, item, left, isConn, st, e, nb, req >>

h_chlk == /\ pc["h"] = "h_chlk"
          /\ lock = "none"
          /\ lock' = "h"
          /\ IF closedCh = "ch"
                THEN /\ userCh' = "closed"
                     /\ closedCh' = "nil"
                ELSE /\ TRUE
                     /\ UNCHANGED << closedCh, userCh >>
          /\ pc' = [pc EXCEPT !["h"] = "h_chul"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, chRan, handlerCalls, maxBatch, badBatch, 
                          userStop, userDrain, drainDowngraded, drainLost, 
                          stack, batch, item, left, isConn, st, e, nb, req >>

h_chul == /\ pc["h"] = "h_chul"
          /\ lock' = "none"
          /\ pc' = [pc EXCEPT !["h"] = "Done"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

Handler == h_wait \/ h_disp \/ h_lk2 \/ h_rs2 \/ h_user \/ h_lk1 \/ h_rs1
              \/ h_sa \/ h_ch \/ h_chlk \/ h_chul

s_top == /\ pc["s"] = "s_top"
         /\ IF closed = 1
               THEN /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
               ELSE /\ pc' = [pc EXCEPT !["s"] = "s_sel"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, lock, fetchNext, errs, done, closed, 
                         draining, fip, pending, delivered, hbArmed, hbClosed, 
                         hbFiring, closedCh, chRan, userCh, handlerCalls, 
                         maxBatch, badBatch, userStop, userDrain, 
                         drainDowngraded, drainLost, stack, batch, item, left, 
                         isConn, st, e, nb, req >>

s_sel == /\ pc["s"] = "s_sel"
         /\ \/ /\ Len(statusCh) > 0
               /\ st' = Head(statusCh)
               /\ statusCh' = Tail(statusCh)
               /\ IF st' = "RECONNECTING"
                     THEN /\ hbArmed' = FALSE
                          /\ isConn' = FALSE
                          /\ pc' = [pc EXCEPT !["s"] = "s_top"]
                     ELSE /\ IF st' = "CONNECTED"
                                THEN /\ pc' = [pc EXCEPT !["s"] = "s_clk"]
                                ELSE /\ pc' = [pc EXCEPT !["s"] = "s_clsend"]
                          /\ UNCHANGED << hbArmed, isConn >>
               /\ UNCHANGED <<errs, e>>
            \/ /\ statusClosed /\ Len(statusCh) = 0
               /\ pc' = [pc EXCEPT !["s"] = "s_top"]
               /\ UNCHANGED <<statusCh, errs, hbArmed, isConn, st, e>>
            \/ /\ Len(errs) > 0
               /\ e' = Head(errs)
               /\ errs' = Tail(errs)
               /\ pc' = [pc EXCEPT !["s"] = "s_elk"]
               /\ UNCHANGED <<statusCh, hbArmed, isConn, st>>
            \/ /\ done
               /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
               /\ UNCHANGED <<statusCh, errs, hbArmed, isConn, st, e>>
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusClosed, 
                         lock, fetchNext, done, closed, draining, fip, pending, 
                         delivered, hbClosed, hbFiring, closedCh, chRan, 
                         userCh, handlerCalls, maxBatch, badBatch, userStop, 
                         userDrain, drainDowngraded, drainLost, stack, batch, 
                         item, left, nb, req >>

s_clk == /\ pc["s"] = "s_clk"
         /\ lock = "none"
         /\ lock' = "s"
         /\ IF ~isConn
               THEN /\ isConn' = TRUE
                    /\ nb' = ConnBatch
                    /\ IF nb' > 0
                          THEN /\ pc' = [pc EXCEPT !["s"] = "s_csend"]
                          ELSE /\ pc' = [pc EXCEPT !["s"] = "s_cul"]
               ELSE /\ pc' = [pc EXCEPT !["s"] = "s_cul"]
                    /\ UNCHANGED << isConn, nb >>
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, st, e, req >>

s_csend == /\ pc["s"] = "s_csend"
           /\ CanSendFetch \/ FetchSendDone
           /\ IF CanSendFetch
                 THEN /\ fetchNext' = Append(fetchNext, nb)
                 ELSE /\ TRUE
                      /\ UNCHANGED fetchNext
           /\ IF ~hbClosed
                 THEN /\ hbArmed' = TRUE
                 ELSE /\ TRUE
                      /\ UNCHANGED hbArmed
           /\ pending' = ResetTo(nb)
           /\ pc' = [pc EXCEPT !["s"] = "s_cul"]
           /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                           rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                           statusClosed, lock, errs, done, closed, draining, 
                           fip, delivered, hbClosed, hbFiring, closedCh, chRan, 
                           userCh, handlerCalls, maxBatch, badBatch, userStop, 
                           userDrain, drainDowngraded, drainLost, stack, batch, 
                           item, left, isConn, st, e, nb, req >>

s_cul == /\ pc["s"] = "s_cul"
         /\ lock' = "none"
         /\ pc' = [pc EXCEPT !["s"] = "s_top"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, isConn, st, e, 
                         nb, req >>

s_clsend == /\ pc["s"] = "s_clsend"
            /\ Len(errs) < ErrsCap
            /\ errs' = Append(errs, "closed")
            /\ pc' = [pc EXCEPT !["s"] = "s_top"]
            /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                            rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                            statusClosed, lock, fetchNext, done, closed, 
                            draining, fip, pending, delivered, hbArmed, 
                            hbClosed, hbFiring, closedCh, chRan, userCh, 
                            handlerCalls, maxBatch, badBatch, userStop, 
                            userDrain, drainDowngraded, drainLost, stack, 
                            batch, item, left, isConn, st, e, nb, req >>

s_elk == /\ pc["s"] = "s_elk"
         /\ lock = "none"
         /\ lock' = "s"
         /\ IF e = "nohb" /\ ("stopafter" \notin Fixes \/ NoHBBatch > 0)
               THEN /\ nb' = NoHBBatch
                    /\ pc' = [pc EXCEPT !["s"] = "s_esend"]
               ELSE /\ pc' = [pc EXCEPT !["s"] = "s_eul"]
                    /\ nb' = nb
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, isConn, st, e, 
                         req >>

s_esend == /\ pc["s"] = "s_esend"
           /\ CanSendFetch \/ FetchSendDone
           /\ IF CanSendFetch
                 THEN /\ fetchNext' = Append(fetchNext, nb)
                 ELSE /\ TRUE
                      /\ UNCHANGED fetchNext
           /\ IF ~hbClosed
                 THEN /\ hbArmed' = TRUE
                 ELSE /\ TRUE
                      /\ UNCHANGED hbArmed
           /\ pending' = ResetTo(nb)
           /\ pc' = [pc EXCEPT !["s"] = "s_eul"]
           /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                           rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                           statusClosed, lock, errs, done, closed, draining, 
                           fip, delivered, hbClosed, hbFiring, closedCh, chRan, 
                           userCh, handlerCalls, maxBatch, badBatch, userStop, 
                           userDrain, drainDowngraded, drainLost, stack, batch, 
                           item, left, isConn, st, e, nb, req >>

s_eul == /\ pc["s"] = "s_eul"
         /\ lock' = "none"
         /\ pc' = [pc EXCEPT !["s"] = "s_eh"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, isConn, st, e, 
                         nb, req >>

s_eh == /\ pc["s"] = "s_eh"
        /\ IF e = "closed"
              THEN /\ IF closed = 0
                         THEN /\ closed' = 1
                              /\ done' = TRUE
                         ELSE /\ TRUE
                              /\ UNCHANGED << done, closed >>
              ELSE /\ TRUE
                   /\ UNCHANGED << done, closed >>
        /\ pc' = [pc EXCEPT !["s"] = "s_top"]
        /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, 
                        avail, subState, subQ, inCb, regs, statusCh, 
                        statusClosed, lock, fetchNext, errs, draining, fip, 
                        pending, delivered, hbArmed, hbClosed, hbFiring, 
                        closedCh, chRan, userCh, handlerCalls, maxBatch, 
                        badBatch, userStop, userDrain, drainDowngraded, 
                        drainLost, stack, batch, item, left, isConn, st, e, nb, 
                        req >>

s_exit == /\ pc["s"] = "s_exit"
          /\ TRUE
          /\ pc' = [pc EXCEPT !["s"] = "Done"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

Status == s_top \/ s_sel \/ s_clk \/ s_csend \/ s_cul \/ s_clsend \/ s_elk
             \/ s_esend \/ s_eul \/ s_eh \/ s_exit

p_sel == /\ pc["p"] = "p_sel"
         /\ \/ /\ Len(fetchNext) > 0
               /\ req' = Head(fetchNext)
               /\ fetchNext' = Tail(fetchNext)
               /\ fip' = 1
               /\ pc' = [pc EXCEPT !["p"] = "p_pull"]
            \/ /\ done
               /\ pc' = [pc EXCEPT !["p"] = "p_cl1"]
               /\ UNCHANGED <<fetchNext, fip, req>>
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, lock, errs, done, closed, draining, 
                         pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, isConn, st, e, 
                         nb >>

p_pull == /\ pc["p"] = "p_pull"
          /\ IF closed = 1
                THEN /\ pc' = [pc EXCEPT !["p"] = "p_cl1"]
                     /\ UNCHANGED << reqs, rbuf, maxBatch, badBatch >>
                ELSE /\ IF req < 1
                           THEN /\ badBatch' = TRUE
                                /\ pc' = [pc EXCEPT !["p"] = "p_err"]
                                /\ UNCHANGED << reqs, rbuf, maxBatch >>
                           ELSE /\ IF conn = "up"
                                      THEN /\ reqs' = Append(reqs, req)
                                           /\ maxBatch' = Max(maxBatch, req)
                                           /\ pc' = [pc EXCEPT !["p"] = "p_fip"]
                                           /\ rbuf' = rbuf
                                      ELSE /\ IF conn = "down"
                                                 THEN /\ rbuf' = Append(rbuf, req)
                                                      /\ maxBatch' = Max(maxBatch, req)
                                                      /\ pc' = [pc EXCEPT !["p"] = "p_fip"]
                                                 ELSE /\ pc' = [pc EXCEPT !["p"] = "p_err"]
                                                      /\ UNCHANGED << rbuf, 
                                                                      maxBatch >>
                                           /\ reqs' = reqs
                                /\ UNCHANGED badBatch
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, avail, 
                          subState, subQ, inCb, regs, statusCh, statusClosed, 
                          lock, fetchNext, errs, done, closed, draining, fip, 
                          pending, delivered, hbArmed, hbClosed, hbFiring, 
                          closedCh, chRan, userCh, handlerCalls, userStop, 
                          userDrain, drainDowngraded, drainLost, stack, batch, 
                          item, left, isConn, st, e, nb, req >>

p_fip == /\ pc["p"] = "p_fip"
         /\ fip' = 0
         /\ pc' = [pc EXCEPT !["p"] = "p_sel"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, lock, fetchNext, errs, done, closed, 
                         draining, pending, delivered, hbArmed, hbClosed, 
                         hbFiring, closedCh, chRan, userCh, handlerCalls, 
                         maxBatch, badBatch, userStop, userDrain, 
                         drainDowngraded, drainLost, stack, batch, item, left, 
                         isConn, st, e, nb, req >>

p_err == /\ pc["p"] = "p_err"
         /\ \/ /\ Len(errs) < ErrsCap
               /\ errs' = Append(errs, "pullerr")
            \/ /\ done
               /\ errs' = errs
         /\ pc' = [pc EXCEPT !["p"] = "p_fip2"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, lock, fetchNext, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         closedCh, chRan, userCh, handlerCalls, maxBatch, 
                         badBatch, userStop, userDrain, drainDowngraded, 
                         drainLost, stack, batch, item, left, isConn, st, e, 
                         nb, req >>

p_fip2 == /\ pc["p"] = "p_fip2"
          /\ fip' = 0
          /\ pc' = [pc EXCEPT !["p"] = "p_sel"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

p_cl1 == /\ pc["p"] = "p_cl1"
         /\ IF Len(statusCh) > 0
               THEN /\ statusCh' = Tail(statusCh)
                    /\ UNCHANGED statusClosed
               ELSE /\ statusClosed' = TRUE
                    /\ UNCHANGED statusCh
         /\ regs' = {}
         /\ hbClosed' = TRUE
         /\ hbArmed' = FALSE
         /\ pc' = [pc EXCEPT !["p"] = "p_cl2"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, lock, fetchNext, 
                         errs, done, closed, draining, fip, pending, delivered, 
                         hbFiring, closedCh, chRan, userCh, handlerCalls, 
                         maxBatch, badBatch, userStop, userDrain, 
                         drainDowngraded, drainLost, stack, batch, item, left, 
                         isConn, st, e, nb, req >>

p_cl2 == /\ pc["p"] = "p_cl2"
         /\ IF subState = "closed"
               THEN /\ closed' = 1
                    /\ UNCHANGED << subState, drainDowngraded >>
               ELSE /\ IF draining = 1
                          THEN /\ subState' = "drainflush"
                               /\ closed' = 1
                               /\ UNCHANGED drainDowngraded
                          ELSE /\ subState' = "closed"
                               /\ closed' = 1
                               /\ IF userDrain /\ ~userStop
                                     THEN /\ drainDowngraded' = TRUE
                                     ELSE /\ TRUE
                                          /\ UNCHANGED drainDowngraded
         /\ pc' = [pc EXCEPT !["p"] = "Done"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subQ, inCb, regs, statusCh, statusClosed, 
                         lock, fetchNext, errs, done, draining, fip, pending, 
                         delivered, hbArmed, hbClosed, hbFiring, closedCh, 
                         chRan, userCh, handlerCalls, maxBatch, badBatch, 
                         userStop, userDrain, drainLost, stack, batch, item, 
                         left, isConn, st, e, nb, req >>

Puller == p_sel \/ p_pull \/ p_fip \/ p_err \/ p_fip2 \/ p_cl1 \/ p_cl2

t_loop == /\ pc["t"] = "t_loop"
          /\ \/ /\ hbArmed /\ hbFires < MaxHBFires /\ HBCanExpire
                /\ hbArmed' = FALSE
                /\ hbFiring' = hbFiring + 1
                /\ hbFires' = hbFires + 1
                /\ errs' = errs
             \/ /\ hbFiring > 0 /\ Len(errs) < ErrsCap
                /\ errs' = Append(errs, "nohb")
                /\ hbFiring' = hbFiring - 1
                /\ UNCHANGED <<hbFires, hbArmed>>
             \/ /\ hbFiring > 0 /\ done
                /\ hbFiring' = hbFiring - 1
                /\ UNCHANGED <<hbFires, errs, hbArmed>>
          /\ pc' = [pc EXCEPT !["t"] = "t_loop"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, reqs, rbuf, avail, 
                          subState, subQ, inCb, regs, statusCh, statusClosed, 
                          lock, fetchNext, done, closed, draining, fip, 
                          pending, delivered, hbClosed, closedCh, chRan, 
                          userCh, handlerCalls, maxBatch, badBatch, userStop, 
                          userDrain, drainDowngraded, drainLost, stack, batch, 
                          item, left, isConn, st, e, nb, req >>

Timer == t_loop

u_main == /\ pc["u"] = "u_main"
          /\ \/ /\ "stop" \in UserOps
                /\ userStop' = TRUE
                /\ IF closed = 0
                      THEN /\ closed' = 1
                           /\ done' = TRUE
                      ELSE /\ TRUE
                           /\ UNCHANGED << done, closed >>
                /\ pc' = [pc EXCEPT !["u"] = "u_closed"]
                /\ UNCHANGED <<draining, userDrain>>
             \/ /\ "drain" \in UserOps
                /\ IF closed = 0
                      THEN /\ closed' = 1
                           /\ userDrain' = TRUE
                           /\ IF "drainrace" \in Fixes
                                 THEN /\ draining' = 1
                                      /\ done' = TRUE
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << done, draining >>
                           /\ pc' = [pc EXCEPT !["u"] = "u_drain2"]
                      ELSE /\ pc' = [pc EXCEPT !["u"] = "u_closed"]
                           /\ UNCHANGED << done, closed, draining, userDrain >>
                /\ UNCHANGED userStop
             \/ /\ TRUE
                /\ pc' = [pc EXCEPT !["u"] = "u_closed"]
                /\ UNCHANGED <<done, closed, draining, userStop, userDrain>>
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, fip, pending, 
                          delivered, hbArmed, hbClosed, hbFiring, closedCh, 
                          chRan, userCh, handlerCalls, maxBatch, badBatch, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

u_drain2 == /\ pc["u"] = "u_drain2"
            /\ IF "drainrace" \notin Fixes
                  THEN /\ draining' = 1
                       /\ done' = TRUE
                  ELSE /\ TRUE
                       /\ UNCHANGED << done, draining >>
            /\ pc' = [pc EXCEPT !["u"] = "u_closed"]
            /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                            rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                            statusClosed, lock, fetchNext, errs, closed, fip, 
                            pending, delivered, hbArmed, hbClosed, hbFiring, 
                            closedCh, chRan, userCh, handlerCalls, maxBatch, 
                            badBatch, userStop, userDrain, drainDowngraded, 
                            drainLost, stack, batch, item, left, isConn, st, e, 
                            nb, req >>

u_closed == /\ pc["u"] = "u_closed"
            /\ IF "closed" \in UserOps
                  THEN /\ pc' = [pc EXCEPT !["u"] = "u_clk"]
                  ELSE /\ pc' = [pc EXCEPT !["u"] = "Done"]
            /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                            rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                            statusClosed, lock, fetchNext, errs, done, closed, 
                            draining, fip, pending, delivered, hbArmed, 
                            hbClosed, hbFiring, closedCh, chRan, userCh, 
                            handlerCalls, maxBatch, badBatch, userStop, 
                            userDrain, drainDowngraded, drainLost, stack, 
                            batch, item, left, isConn, st, e, nb, req >>

u_clk == /\ pc["u"] = "u_clk"
         /\ lock = "none"
         /\ lock' = "u"
         /\ IF IF "closedch" \in Fixes THEN chRan ELSE subState = "closed"
               THEN /\ userCh' = "closed"
                    /\ closedCh' = "nil"
               ELSE /\ userCh' = "open"
                    /\ closedCh' = "ch"
         /\ pc' = [pc EXCEPT !["u"] = "u_clul"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                         statusClosed, fetchNext, errs, done, closed, draining, 
                         fip, pending, delivered, hbArmed, hbClosed, hbFiring, 
                         chRan, handlerCalls, maxBatch, badBatch, userStop, 
                         userDrain, drainDowngraded, drainLost, stack, batch, 
                         item, left, isConn, st, e, nb, req >>

u_clul == /\ pc["u"] = "u_clul"
          /\ lock' = "none"
          /\ pc' = [pc EXCEPT !["u"] = "Done"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, subQ, inCb, regs, statusCh, 
                          statusClosed, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

User == u_main \/ u_drain2 \/ u_closed \/ u_clk \/ u_clul

l_loop == /\ pc["lib"] = "l_loop"
          /\ \/ /\ subState = "drainflush"
                /\ subState' = "draining"
             \/ /\ subState = "draining" /\ Len(subQ) = 0 /\ ~inCb
                /\ subState' = "closed"
          /\ pc' = [pc EXCEPT !["lib"] = "l_loop"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subQ, inCb, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbClosed, 
                          hbFiring, closedCh, chRan, userCh, handlerCalls, 
                          maxBatch, badBatch, userStop, userDrain, 
                          drainDowngraded, drainLost, stack, batch, item, left, 
                          isConn, st, e, nb, req >>

Lib == l_loop

e_loop == /\ pc["env"] = "e_loop"
          /\ \/ /\ Accepting /\ Len(reqs) > 0 /\ avail > 0 /\ PullSettled
                /\ subQ' = Append(subQ, [t |-> "msg", n |-> 0])
                /\ avail' = avail - 1
                /\ reqs' = (IF Head(reqs) = 1 THEN Tail(reqs) ELSE << Head(reqs) - 1 >> \o Tail(reqs))
                /\ UNCHANGED <<conn, reconnects, expires, hbMsgs, rbuf, subState, regs, statusCh>>
             \/ /\ Len(reqs) > 0 /\ expires < MaxExpires /\ PullSettled
                /\ IF Accepting
                      THEN /\ subQ' = Append(subQ, [t |-> "408", n |-> Head(reqs)])
                      ELSE /\ TRUE
                           /\ subQ' = subQ
                /\ reqs' = Tail(reqs)
                /\ expires' = expires + 1
                /\ UNCHANGED <<conn, reconnects, hbMsgs, rbuf, avail, subState, regs, statusCh>>
             \/ /\ Accepting /\ Len(reqs) > 0 /\ hbMsgs < MaxHBMsgs
                /\ subQ' = Append(subQ, [t |-> "hb", n |-> 0])
                /\ hbMsgs' = hbMsgs + 1
                /\ UNCHANGED <<conn, reconnects, expires, reqs, rbuf, avail, subState, regs, statusCh>>
             \/ /\ conn = "up" /\ reconnects < MaxReconnects
                /\ conn' = "down"
                /\ IF "RECONNECTING" \in regs
                      THEN /\ IF statusClosed
                                 THEN /\ regs' = regs \ {"RECONNECTING"}
                                      /\ UNCHANGED statusCh
                                 ELSE /\ IF Len(statusCh) > 0 /\ "steal" \notin Fixes
                                            THEN /\ statusCh' = Tail(statusCh)
                                                 /\ regs' = regs \ {"RECONNECTING"}
                                            ELSE /\ IF Len(statusCh) < StatusCap
                                                       THEN /\ statusCh' = Append(statusCh, "RECONNECTING")
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED statusCh
                                                 /\ regs' = regs
                      ELSE /\ TRUE
                           /\ UNCHANGED << regs, statusCh >>
                /\ IF ReqsOnDisconnect = "drop"
                      THEN /\ reqs' = << >>
                      ELSE /\ IF ReqsOnDisconnect = "either"
                                 THEN /\ \/ /\ reqs' = << >>
                                         \/ /\ TRUE
                                            /\ reqs' = reqs
                                 ELSE /\ TRUE
                                      /\ reqs' = reqs
                /\ UNCHANGED <<reconnects, expires, hbMsgs, rbuf, avail, subState, subQ>>
             \/ /\ conn = "down"
                /\ conn' = "up"
                /\ reconnects' = reconnects + 1
                /\ reqs' = reqs \o rbuf
                /\ rbuf' = << >>
                /\ IF "CONNECTED" \in regs
                      THEN /\ IF statusClosed
                                 THEN /\ regs' = regs \ {"CONNECTED"}
                                      /\ UNCHANGED statusCh
                                 ELSE /\ IF Len(statusCh) > 0 /\ "steal" \notin Fixes
                                            THEN /\ statusCh' = Tail(statusCh)
                                                 /\ regs' = regs \ {"CONNECTED"}
                                            ELSE /\ IF Len(statusCh) < StatusCap
                                                       THEN /\ statusCh' = Append(statusCh, "CONNECTED")
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED statusCh
                                                 /\ regs' = regs
                      ELSE /\ TRUE
                           /\ UNCHANGED << regs, statusCh >>
                /\ UNCHANGED <<expires, hbMsgs, avail, subState, subQ>>
             \/ /\ AllowClose /\ conn # "closed"
                /\ conn' = "closed"
                /\ rbuf' = << >>
                /\ reqs' = << >>
                /\ subState' = "closed"
                /\ IF "CLOSED" \in regs
                      THEN /\ IF statusClosed
                                 THEN /\ regs' = regs \ {"CLOSED"}
                                      /\ UNCHANGED statusCh
                                 ELSE /\ IF Len(statusCh) > 0 /\ "steal" \notin Fixes
                                            THEN /\ statusCh' = Tail(statusCh)
                                                 /\ regs' = regs \ {"CLOSED"}
                                            ELSE /\ IF Len(statusCh) < StatusCap
                                                       THEN /\ statusCh' = Append(statusCh, "CLOSED")
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED statusCh
                                                 /\ regs' = regs
                      ELSE /\ TRUE
                           /\ UNCHANGED << regs, statusCh >>
                /\ UNCHANGED <<reconnects, expires, hbMsgs, avail, subQ>>
          /\ pc' = [pc EXCEPT !["env"] = "e_loop"]
          /\ UNCHANGED << hbFires, inCb, statusClosed, lock, fetchNext, errs, 
                          done, closed, draining, fip, pending, delivered, 
                          hbArmed, hbClosed, hbFiring, closedCh, chRan, userCh, 
                          handlerCalls, maxBatch, badBatch, userStop, 
                          userDrain, drainDowngraded, drainLost, stack, batch, 
                          item, left, isConn, st, e, nb, req >>

Env == e_loop

Next == Handler \/ Status \/ Puller \/ Timer \/ User \/ Lib \/ Env
           \/ (\E self \in ProcSet: CheckPending(self))

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Handler) /\ WF_vars(CheckPending("h"))
        /\ WF_vars(Status)
        /\ WF_vars(Puller)
        /\ WF_vars(Timer)
        /\ WF_vars(User)
        /\ WF_vars(Lib)

\* END TRANSLATION

-----------------------------------------------------------------------------
TypeOK ==
    /\ conn \in {"up", "down", "closed"}
    /\ subState \in {"active", "drainflush", "draining", "closed"}
    /\ lock \in {"none", "h", "s", "u"}
    /\ Len(fetchNext) <= 1
    /\ Len(errs) <= ErrsCap
    /\ Len(statusCh) <= StatusCap

\* The user handler is never invoked more than StopAfter times
\* (StopAfter doc, jetstream_options.go:426-428).
StopAfterRespected == StopAfter = 0 \/ handlerCalls <= StopAfter

\* No pull request asks for more than MaxMessages.
BatchWithinMax == maxBatch <= MaxMsgs

\* Nothing reads fetchNext once pullMessages has returned, so a goroutine
\* parked on a full fetchNext at that point never wakes up, and it is
\* holding sub.Lock (pull.go:262/290, 366, 395).
FetchSenders == { x \in {"h", "s"} : pc[x] \in {"cp_send", "s_csend", "s_esend"} }
NoOrphanedFetchSend ==
    ~(pc["p"] = "Done" /\ ~CanSendFetch /\ ~FetchSendDone /\ FetchSenders # {})

\* The status goroutine is the only reader of errs, so its own blocking
\* send on a full errs (pull.go:392) never completes.
NoStuckSelfSend == ~(pc["s"] = "s_clsend" /\ Len(errs) = ErrsCap)

\* No pull request with a batch < 1 (rejected by pull(), pull.go:1160, and
\* reported to the user as an error).
NoInvalidPull == ~badBatch

\* Closed() is closed only once processing is complete: no callback is
\* running (ConsumeContext.Closed doc, pull.go:835-837).
ClosedMeansDone == userCh = "closed" => ~inCb

\* Drain must not degrade into Unsubscribe (buffered messages discarded),
\* and every message buffered by the time of Drain reaches the handler.
DrainNotDowngraded == ~drainDowngraded
DrainProcessesBuffered == ~drainLost

\* Nothing can move any more (all environment budgets spent, every goroutine
\* blocked or finished). Once Stop/Drain was called every goroutine must
\* have returned and a Closed() channel handed out must be closed.
Quiescent == ~ENABLED Next
AllExited == /\ pc["h"] = "Done" /\ pc["s"] = "Done" /\ pc["p"] = "Done"
             /\ hbFiring = 0 /\ pc["u"] = "Done"
NoDeadlock == (Quiescent /\ done) => (AllExited /\ userCh # "open")

\* The consumer is idle on a healthy connection with no pull request
\* anywhere (server, reconnect buffer, fetchNext, in flight) and nothing
\* queued that could lead to one, although it has not been stopped and the
\* stream has messages it still wants.
Idle ==
    /\ conn = "up" /\ subState = "active" /\ closed = 0
    /\ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
    /\ subQ = << >> /\ ~inCb /\ errs = << >> /\ statusCh = << >>
    /\ hbFiring = 0 /\ lock = "none"
    /\ pc["p"] = "p_sel" /\ pc["s"] \in {"s_top", "s_sel"} /\ pc["h"] = "h_wait"
    /\ avail > 0 /\ (StopAfter = 0 \/ delivered < StopAfter)
\* Only the heartbeat timer can get it going again (after 2*heartbeat, with
\* a spurious ErrNoHeartbeat reported to the user).
NoSilentStall == ~Idle
\* Not even the heartbeat timer is armed: stuck for good.
NoPermanentStall == ~(Idle /\ ~hbArmed)
=============================================================================
