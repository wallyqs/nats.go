---------------------------- MODULE PullMessages ----------------------------
(***************************************************************************)
(* Model of jetstream/pull.go pullConsumer.Messages() / MessagesContext    *)
(* Next / Stop / Drain.                                                    *)
(*                                                                         *)
(* Goroutines:                                                             *)
(*   n    the user goroutine calling Next (pull.go:624-746) up to MaxNext  *)
(*        times; Next holds sub.Lock for its whole duration (pull.go:648)  *)
(*   s    the status goroutine (pull.go:572-609)                           *)
(*   p    pullMessages + cleanup (pull.go:1074-1150)                       *)
(*   t    Next's heartbeat timer (pull.go:659-664, 1106-1118)              *)
(*   u    the user calling Stop or Drain from another goroutine            *)
(*   lib  nats.go: closed handler of the ChanSubscription (pull.go:554-565,*)
(*        run by removeSub, nats.go:5263-5268) and checkDrained            *)
(*   env  server + network                                                 *)
(*                                                                         *)
(* msgs is the ChanSubscribe channel, capacity MaxMessages (pull.go:534):  *)
(* nats.go never blocks on it, a message that does not fit is dropped as a *)
(* slow consumer (nats.go:4013-4022).                                      *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxMsgs,            \* consumeOpts.MaxMessages = cap(msgs)
    Thresh,             \* ThresholdMessages
    StopAfter,          \* 0 = unset
    StreamMsgs,
    MaxNext,            \* how many times the user calls Next
    MaxReconnects, MaxExpires, MaxHBMsgs, MaxHBFires,
    ErrsCap, StatusCap,
    UserOps,            \* subset of {"stop", "drain"}
    NextTimeout,        \* Next is called with NextMaxWait (may time out)
    ReportMissingHB,    \* WithMessagesErrOnMissingHeartbeat (default TRUE)
    ReqsOnDisconnect,   \* "keep" | "drop" | "either": waiting pulls at the server after a disconnect
    LateHeartbeats,     \* the heartbeat timer may fire while the server still holds a
                        \* waiting pull (heartbeats delayed > 2x by the network)
    FastPull,           \* the server never answers a pull (message or 408) before
                        \* pullMessages has run the statement after pull() returned
    Fixes               \* subset of {"orphan", "steal", "pendingreset", "clamp", "drainrace", "fip",
                        \*            "buffered", "unbounded"}

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

(* --algorithm PullMessages {
variables
    conn = "up", reconnects = 0, expires = 0, hbMsgs = 0, hbFires = 0,
    reqs = << >>, rbuf = << >>, avail = StreamMsgs,
    subState = "active",       \* active | drainflush | closed  (ChanSubscription)
    msgs = << >>, msgsClosed = FALSE,
    regs = {"RECONNECTING", "CONNECTED"},          \* pull.go:546
    statusCh = << >>, statusClosed = FALSE,
    lock = "none",
    fetchNext = << >>, errs = << >>,
    done = FALSE, closed = 0, draining = 0, fip = 0,
    pending = 0, delivered = 0,                     \* zero values: no initial pull
    hbArmed = FALSE, hbFiring = 0,
    \* --- observation ---
    returned = 0,              \* user messages returned by Next
    dropped = 0,               \* user messages dropped because msgs was full
    droppedStatus = 0,         \* 408s / heartbeats dropped because msgs was full
    maxBatch = 0,
    userDrain = FALSE, userStop = FALSE,
    lastResult = "none";       \* what the last Next returned

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
                   \/ /\ pc["n"] = "n_sel"
                      /\ \/ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
                         \/ conn # "up"
                         \/ subState \notin {"active", "drainflush"}
    Accepting == conn = "up" /\ subState = "active"
    CanSendFetch == Len(fetchNext) < 1
    FetchSendDone == "orphan" \in Fixes /\ done
    \* deliver into the chan subscription, or drop (slow consumer)
    \* what Next resets pending.msgCount to on a missed heartbeat or a
    \* reconnect: 0 (pull.go:712, 729), or with the "buffered" fix the
    \* number of channel slots already taken (len(s.msgs))
    ResetPending == IF "buffered" \in Fixes THEN Len(msgs) ELSE 0
}

macro SendStatus(s) {   \* sendStatusEvent, nats.go:6833-6851
    if (s \in regs) {
        if (statusClosed) {
            regs := regs \ {s};
        } else if (Len(statusCh) > 0 /\ "steal" \notin Fixes) {
            statusCh := Tail(statusCh);
            regs := regs \ {s};
        } else if (Len(statusCh) < StatusCap) {
            statusCh := Append(statusCh, s);
        }
    }
}

macro EnvDeliver(item) {   \* processMsg for a ChanSubscription, nats.go:4013-4022
    if (Len(msgs) < MaxMsgs \/ "unbounded" \in Fixes) { msgs := Append(msgs, item); }
    else if (item.t = "msg") { dropped := dropped + 1; }
    else { droppedStatus := droppedStatus + 1; };
}

macro DoStop() {
    if (closed = 0) { closed := 1; done := TRUE; }
}

procedure CheckPending()   \* pull.go:461-504
variables batch = 0;
{
cp_eval:
    \* With the "fip" fix (PULL-10) the check no longer waits for
    \* fetchInProgress to clear, and the send never blocks: when fetchNext
    \* already holds a request, this one is skipped.
    if (pending < Thresh /\ (fip = 0 \/ "fip" \in Fixes)) {
        batch := LET b == IF StopAfter > 0
                          THEN Min(MaxMsgs - pending, StopAfter - delivered - pending)
                          ELSE MaxMsgs - pending
                 IN IF "clamp" \in Fixes THEN Min(b, MaxMsgs) ELSE b;
        if (batch > 0) {
cp_send:
            await CanSendFetch \/ FetchSendDone \/ "fip" \in Fixes;
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

fair process (Nexter = "n")
variables calls = 0, m = "none", e = "none";
{
n_call:
    while (calls < MaxNext) {
        calls := calls + 1;
n_lock:
        await lock = "none";                                \* pull.go:648
        if (closed = 1 /\ draining = 0) {                   \* pull.go:650-658
            lastResult := "closed";                         \* (lock taken and released)
            goto n_call;
        } else {
            lock := "n";
        };
n_hb:
        if (StopAfter > 0 /\ delivered >= StopAfter) {      \* pull.go:666-669
            DoStop(); lastResult := "closed";
            lock := "none";
            goto n_call;
        } else {
            hbArmed := TRUE;                                \* scheduleHeartbeatCheck, pull.go:659
        };
n_loop:
        while (TRUE) {
            call CheckPending();                            \* pull.go:672
n_sel:
            either {                                        \* pull.go:674
                await Len(msgs) > 0 \/ msgsClosed;
                if (Len(msgs) = 0) {                        \* closed and drained, pull.go:675-688
                    if (closed = 0) { closed := 1; done := TRUE; };   \* s.Stop()
                    draining := 0;
                    lastResult := "closed";
                    hbArmed := FALSE; lock := "none";
                    goto n_call;
                } else {
                    m := Head(msgs); msgs := Tail(msgs);
                    if (m.t = "msg") {             \* pull.go:704-709
                        pending := IF "clamp" \in Fixes THEN Max(0, pending - 1) ELSE pending - 1;
                        delivered := delivered + 1;
                        returned := returned + 1;
                        lastResult := "msg";
                        hbArmed := FALSE; lock := "none";   \* deferred hbMonitor.Stop, Unlock
                        goto n_call;
                    } else if (m.t = "408") {      \* handleStatusMsg, pull.go:768-781
                        pending := Max(0, pending - m.n);
                        hbArmed := TRUE;                    \* pull.go:689-691
                    } else {
                        hbArmed := TRUE;                    \* pull.go:689-691
                    };
                };
            } or {
                await Len(errs) > 0;                        \* pull.go:710
                e := Head(errs); errs := Tail(errs);
                if (e = "nohb") {                           \* pull.go:711-720
                    pending := ResetPending;
                    if (ReportMissingHB) {
                        lastResult := "nohb";
                        hbArmed := FALSE; lock := "none";
                        goto n_call;
                    } else {
                        hbArmed := TRUE;
                    };
                } else if (e = "connected") {               \* pull.go:721-734
                    pending := ResetPending; hbArmed := TRUE;
                } else if (e = "disconnected") {            \* pull.go:735-739
                    hbArmed := FALSE;
                };
            } or {
                await NextTimeout;                          \* pull.go:740-741
                lastResult := "timeout";
                hbArmed := FALSE; lock := "none";
                goto n_call;
            };
        };
    };
}

fair process (Status = "s")
variables isConn = TRUE;
{
s_sel:
    while (TRUE) {
        either {                                            \* pull.go:582-586
            await Len(statusCh) > 0 \/ statusClosed;
            if (Len(statusCh) = 0) {                        \* closed: return
                goto s_exit;
            } else if (Head(statusCh) = "RECONNECTING") {
                statusCh := Tail(statusCh);
                isConn := FALSE;
s_dsend:
                either { await Len(errs) < ErrsCap; errs := Append(errs, "disconnected"); }
                or     { await done; goto s_exit; };
            } else if (~isConn) {                           \* CONNECTED, pull.go:594-603
                statusCh := Tail(statusCh);
                isConn := TRUE;
s_csend:
                either { await Len(errs) < ErrsCap; errs := Append(errs, "connected"); }
                or     { await done; goto s_exit; };
            } else {
                statusCh := Tail(statusCh);                 \* CONNECTED while connected
            };
        } or {
            await done; goto s_exit;                        \* pull.go:605-606
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
            await Len(fetchNext) > 0;
            req := Head(fetchNext); fetchNext := Tail(fetchNext);
            fip := 1;
p_pull:
            if (closed = 1) {
                goto p_cl1;
            } else if (req < 1) {
                goto p_err;
            } else if (conn = "up") {
                reqs := Append(reqs, req); maxBatch := Max(maxBatch, req);
            } else if (conn = "down") {
                rbuf := Append(rbuf, req); maxBatch := Max(maxBatch, req);
            } else {
                goto p_err;
            };
p_fip:
            fip := 0;
        } or {
            await done;
            goto p_cl1;
        };
    };
p_err:
    either { await Len(errs) < ErrsCap; errs := Append(errs, "pullerr"); }
    or     { await done; };
p_fip2:
    fip := 0;
    goto p_sel;
p_cl1:                                                      \* pull.go:1132-1138
    if (Len(statusCh) > 0) { statusCh := Tail(statusCh); } else { statusClosed := TRUE; };
    regs := {};
p_cl2:                                                      \* pull.go:1139-1149
    if (subState = "closed") {
        closed := 1;
    } else if (draining = 1) {
        subState := "drainflush"; closed := 1;
    } else {
        \* Unsubscribe: removeSub runs the closed handler, which closes msgs
        subState := "closed"; msgsClosed := TRUE; closed := 1;
    };
}

fair process (Timer = "t")
{
t_loop:
    while (TRUE) {
        either {
            await hbArmed /\ hbFires < MaxHBFires /\ HBCanExpire;
            hbArmed := FALSE; hbFiring := hbFiring + 1; hbFires := hbFires + 1;
        } or {
            await hbFiring > 0 /\ Len(errs) < ErrsCap;
            errs := Append(errs, "nohb"); hbFiring := hbFiring - 1;
        } or {
            await hbFiring > 0 /\ done;
            hbFiring := hbFiring - 1;
        };
    };
}

fair process (User = "u")
{
u_main:
    either {
        await "stop" \in UserOps;
        userStop := TRUE;
        DoStop();
    } or {
        await "drain" \in UserOps;
        if (closed = 0) {
            closed := 1; userDrain := TRUE;
            if ("drainrace" \in Fixes) { draining := 1; done := TRUE; };
        } else {
            goto u_done;
        };
u_drain2:
        if ("drainrace" \notin Fixes) { draining := 1; done := TRUE; };
    } or {
        skip;
    };
u_done:
    skip;
}

fair process (Lib = "lib")
{
l_loop:
    while (TRUE) {
        \* checkDrained for a ChanSubscription: Flush, then pMsgs (not
        \* tracked for channels) is 0: removeSub -> closed handler -> close(msgs)
        await subState = "drainflush";
        subState := "closed"; msgsClosed := TRUE;
    };
}

process (Env = "env")
{
e_loop:
    while (TRUE) {
        either {
            await Accepting /\ Len(reqs) > 0 /\ avail > 0 /\ PullSettled;
            EnvDeliver([t |-> "msg", n |-> 0]);
            avail := avail - 1;
            reqs := IF Head(reqs) = 1 THEN Tail(reqs) ELSE << Head(reqs) - 1 >> \o Tail(reqs);
        } or {
            await Len(reqs) > 0 /\ expires < MaxExpires /\ PullSettled;
            if (Accepting) { EnvDeliver([t |-> "408", n |-> Head(reqs)]); };
            reqs := Tail(reqs);
            expires := expires + 1;
        } or {
            await Accepting /\ Len(reqs) > 0 /\ hbMsgs < MaxHBMsgs;
            EnvDeliver([t |-> "hb", n |-> 0]);
            hbMsgs := hbMsgs + 1;
        } or {
            await conn = "up" /\ reconnects < MaxReconnects;
            conn := "down";
            SendStatus("RECONNECTING");
            if (ReqsOnDisconnect = "drop") { reqs := << >>; }
            else if (ReqsOnDisconnect = "either") { either { reqs := << >>; } or { skip; }; };
        } or {
            await conn = "down";
            conn := "up"; reconnects := reconnects + 1;
            reqs := reqs \o rbuf; rbuf := << >>;
            SendStatus("CONNECTED");
        };
    };
}
} *)
\* BEGIN TRANSLATION
VARIABLES pc, conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, avail, 
          subState, msgs, msgsClosed, regs, statusCh, statusClosed, lock, 
          fetchNext, errs, done, closed, draining, fip, pending, delivered, 
          hbArmed, hbFiring, returned, dropped, droppedStatus, maxBatch, 
          userDrain, userStop, lastResult, stack

(* define statement *)
PullSettled == ~(FastPull /\ pc["p"] = "p_fip" /\ Len(reqs) = 1)





HBCanExpire == \/ LateHeartbeats
               \/ /\ pc["n"] = "n_sel"
                  /\ \/ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
                     \/ conn # "up"
                     \/ subState \notin {"active", "drainflush"}
Accepting == conn = "up" /\ subState = "active"
CanSendFetch == Len(fetchNext) < 1
FetchSendDone == "orphan" \in Fixes /\ done




ResetPending == IF "buffered" \in Fixes THEN Len(msgs) ELSE 0

VARIABLES batch, calls, m, e, isConn, req

vars == << pc, conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, avail, 
           subState, msgs, msgsClosed, regs, statusCh, statusClosed, lock, 
           fetchNext, errs, done, closed, draining, fip, pending, delivered, 
           hbArmed, hbFiring, returned, dropped, droppedStatus, maxBatch, 
           userDrain, userStop, lastResult, stack, batch, calls, m, e, isConn, 
           req >>

ProcSet == {"n"} \cup {"s"} \cup {"p"} \cup {"t"} \cup {"u"} \cup {"lib"} \cup {"env"}

Init == (* Global variables *)
        /\ conn = "up"
        /\ reconnects = 0
        /\ expires = 0
        /\ hbMsgs = 0
        /\ hbFires = 0
        /\ reqs = << >>
        /\ rbuf = << >>
        /\ avail = StreamMsgs
        /\ subState = "active"
        /\ msgs = << >>
        /\ msgsClosed = FALSE
        /\ regs = {"RECONNECTING", "CONNECTED"}
        /\ statusCh = << >>
        /\ statusClosed = FALSE
        /\ lock = "none"
        /\ fetchNext = << >>
        /\ errs = << >>
        /\ done = FALSE
        /\ closed = 0
        /\ draining = 0
        /\ fip = 0
        /\ pending = 0
        /\ delivered = 0
        /\ hbArmed = FALSE
        /\ hbFiring = 0
        /\ returned = 0
        /\ dropped = 0
        /\ droppedStatus = 0
        /\ maxBatch = 0
        /\ userDrain = FALSE
        /\ userStop = FALSE
        /\ lastResult = "none"
        (* Procedure CheckPending *)
        /\ batch = [ self \in ProcSet |-> 0]
        (* Process Nexter *)
        /\ calls = 0
        /\ m = "none"
        /\ e = "none"
        (* Process Status *)
        /\ isConn = TRUE
        (* Process Puller *)
        /\ req = 0
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self = "n" -> "n_call"
                                        [] self = "s" -> "s_sel"
                                        [] self = "p" -> "p_sel"
                                        [] self = "t" -> "t_loop"
                                        [] self = "u" -> "u_main"
                                        [] self = "lib" -> "l_loop"
                                        [] self = "env" -> "e_loop"]

cp_eval(self) == /\ pc[self] = "cp_eval"
                 /\ IF pending < Thresh /\ (fip = 0 \/ "fip" \in Fixes)
                       THEN /\ batch' = [batch EXCEPT ![self] = LET b == IF StopAfter > 0
                                                                         THEN Min(MaxMsgs - pending, StopAfter - delivered - pending)
                                                                         ELSE MaxMsgs - pending
                                                                IN IF "clamp" \in Fixes THEN Min(b, MaxMsgs) ELSE b]
                            /\ IF batch'[self] > 0
                                  THEN /\ pc' = [pc EXCEPT ![self] = "cp_send"]
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "cp_ret"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "cp_ret"]
                            /\ batch' = batch
                 /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, 
                                 reqs, rbuf, avail, subState, msgs, msgsClosed, 
                                 regs, statusCh, statusClosed, lock, fetchNext, 
                                 errs, done, closed, draining, fip, pending, 
                                 delivered, hbArmed, hbFiring, returned, 
                                 dropped, droppedStatus, maxBatch, userDrain, 
                                 userStop, lastResult, stack, calls, m, e, 
                                 isConn, req >>

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
                                 reqs, rbuf, avail, subState, msgs, msgsClosed, 
                                 regs, statusCh, statusClosed, lock, errs, 
                                 done, closed, draining, fip, delivered, 
                                 hbArmed, hbFiring, returned, dropped, 
                                 droppedStatus, maxBatch, userDrain, userStop, 
                                 lastResult, stack, batch, calls, m, e, isConn, 
                                 req >>

cp_ret(self) == /\ pc[self] = "cp_ret"
                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                /\ batch' = [batch EXCEPT ![self] = Head(stack[self]).batch]
                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, 
                                reqs, rbuf, avail, subState, msgs, msgsClosed, 
                                regs, statusCh, statusClosed, lock, fetchNext, 
                                errs, done, closed, draining, fip, pending, 
                                delivered, hbArmed, hbFiring, returned, 
                                dropped, droppedStatus, maxBatch, userDrain, 
                                userStop, lastResult, calls, m, e, isConn, req >>

CheckPending(self) == cp_eval(self) \/ cp_send(self) \/ cp_ret(self)

n_call == /\ pc["n"] = "n_call"
          /\ IF calls < MaxNext
                THEN /\ calls' = calls + 1
                     /\ pc' = [pc EXCEPT !["n"] = "n_lock"]
                ELSE /\ pc' = [pc EXCEPT !["n"] = "Done"]
                     /\ calls' = calls
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, done, 
                          closed, draining, fip, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, lastResult, stack, batch, m, e, 
                          isConn, req >>

n_lock == /\ pc["n"] = "n_lock"
          /\ lock = "none"
          /\ IF closed = 1 /\ draining = 0
                THEN /\ lastResult' = "closed"
                     /\ pc' = [pc EXCEPT !["n"] = "n_call"]
                     /\ lock' = lock
                ELSE /\ lock' = "n"
                     /\ pc' = [pc EXCEPT !["n"] = "n_hb"]
                     /\ UNCHANGED lastResult
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, fetchNext, errs, done, 
                          closed, draining, fip, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, stack, batch, calls, m, e, 
                          isConn, req >>

n_hb == /\ pc["n"] = "n_hb"
        /\ IF StopAfter > 0 /\ delivered >= StopAfter
              THEN /\ IF closed = 0
                         THEN /\ closed' = 1
                              /\ done' = TRUE
                         ELSE /\ TRUE
                              /\ UNCHANGED << done, closed >>
                   /\ lastResult' = "closed"
                   /\ lock' = "none"
                   /\ pc' = [pc EXCEPT !["n"] = "n_call"]
                   /\ UNCHANGED hbArmed
              ELSE /\ hbArmed' = TRUE
                   /\ pc' = [pc EXCEPT !["n"] = "n_loop"]
                   /\ UNCHANGED << lock, done, closed, lastResult >>
        /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, rbuf, 
                        avail, subState, msgs, msgsClosed, regs, statusCh, 
                        statusClosed, fetchNext, errs, draining, fip, pending, 
                        delivered, hbFiring, returned, dropped, droppedStatus, 
                        maxBatch, userDrain, userStop, stack, batch, calls, m, 
                        e, isConn, req >>

n_loop == /\ pc["n"] = "n_loop"
          /\ stack' = [stack EXCEPT !["n"] = << [ procedure |->  "CheckPending",
                                                  pc        |->  "n_sel",
                                                  batch     |->  batch["n"] ] >>
                                              \o stack["n"]]
          /\ batch' = [batch EXCEPT !["n"] = 0]
          /\ pc' = [pc EXCEPT !["n"] = "cp_eval"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, done, 
                          closed, draining, fip, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, lastResult, calls, m, e, isConn, 
                          req >>

n_sel == /\ pc["n"] = "n_sel"
         /\ \/ /\ Len(msgs) > 0 \/ msgsClosed
               /\ IF Len(msgs) = 0
                     THEN /\ IF closed = 0
                                THEN /\ closed' = 1
                                     /\ done' = TRUE
                                ELSE /\ TRUE
                                     /\ UNCHANGED << done, closed >>
                          /\ draining' = 0
                          /\ lastResult' = "closed"
                          /\ hbArmed' = FALSE
                          /\ lock' = "none"
                          /\ pc' = [pc EXCEPT !["n"] = "n_call"]
                          /\ UNCHANGED << msgs, pending, delivered, returned, 
                                          m >>
                     ELSE /\ m' = Head(msgs)
                          /\ msgs' = Tail(msgs)
                          /\ IF m'.t = "msg"
                                THEN /\ pending' = (IF "clamp" \in Fixes THEN Max(0, pending - 1) ELSE pending - 1)
                                     /\ delivered' = delivered + 1
                                     /\ returned' = returned + 1
                                     /\ lastResult' = "msg"
                                     /\ hbArmed' = FALSE
                                     /\ lock' = "none"
                                     /\ pc' = [pc EXCEPT !["n"] = "n_call"]
                                ELSE /\ IF m'.t = "408"
                                           THEN /\ pending' = Max(0, pending - m'.n)
                                                /\ hbArmed' = TRUE
                                           ELSE /\ hbArmed' = TRUE
                                                /\ UNCHANGED pending
                                     /\ pc' = [pc EXCEPT !["n"] = "n_loop"]
                                     /\ UNCHANGED << lock, delivered, returned, 
                                                     lastResult >>
                          /\ UNCHANGED << done, closed, draining >>
               /\ UNCHANGED <<errs, e>>
            \/ /\ Len(errs) > 0
               /\ e' = Head(errs)
               /\ errs' = Tail(errs)
               /\ IF e' = "nohb"
                     THEN /\ pending' = ResetPending
                          /\ IF ReportMissingHB
                                THEN /\ lastResult' = "nohb"
                                     /\ hbArmed' = FALSE
                                     /\ lock' = "none"
                                     /\ pc' = [pc EXCEPT !["n"] = "n_call"]
                                ELSE /\ hbArmed' = TRUE
                                     /\ pc' = [pc EXCEPT !["n"] = "n_loop"]
                                     /\ UNCHANGED << lock, lastResult >>
                     ELSE /\ IF e' = "connected"
                                THEN /\ pending' = ResetPending
                                     /\ hbArmed' = TRUE
                                ELSE /\ IF e' = "disconnected"
                                           THEN /\ hbArmed' = FALSE
                                           ELSE /\ TRUE
                                                /\ UNCHANGED hbArmed
                                     /\ UNCHANGED pending
                          /\ pc' = [pc EXCEPT !["n"] = "n_loop"]
                          /\ UNCHANGED << lock, lastResult >>
               /\ UNCHANGED <<msgs, done, closed, draining, delivered, returned, m>>
            \/ /\ NextTimeout
               /\ lastResult' = "timeout"
               /\ hbArmed' = FALSE
               /\ lock' = "none"
               /\ pc' = [pc EXCEPT !["n"] = "n_call"]
               /\ UNCHANGED <<msgs, errs, done, closed, draining, pending, delivered, returned, m, e>>
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, msgsClosed, regs, statusCh, 
                         statusClosed, fetchNext, fip, hbFiring, dropped, 
                         droppedStatus, maxBatch, userDrain, userStop, stack, 
                         batch, calls, isConn, req >>

Nexter == n_call \/ n_lock \/ n_hb \/ n_loop \/ n_sel

s_sel == /\ pc["s"] = "s_sel"
         /\ \/ /\ Len(statusCh) > 0 \/ statusClosed
               /\ IF Len(statusCh) = 0
                     THEN /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
                          /\ UNCHANGED << statusCh, isConn >>
                     ELSE /\ IF Head(statusCh) = "RECONNECTING"
                                THEN /\ statusCh' = Tail(statusCh)
                                     /\ isConn' = FALSE
                                     /\ pc' = [pc EXCEPT !["s"] = "s_dsend"]
                                ELSE /\ IF ~isConn
                                           THEN /\ statusCh' = Tail(statusCh)
                                                /\ isConn' = TRUE
                                                /\ pc' = [pc EXCEPT !["s"] = "s_csend"]
                                           ELSE /\ statusCh' = Tail(statusCh)
                                                /\ pc' = [pc EXCEPT !["s"] = "s_sel"]
                                                /\ UNCHANGED isConn
            \/ /\ done
               /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
               /\ UNCHANGED <<statusCh, isConn>>
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, msgs, msgsClosed, regs, 
                         statusClosed, lock, fetchNext, errs, done, closed, 
                         draining, fip, pending, delivered, hbArmed, hbFiring, 
                         returned, dropped, droppedStatus, maxBatch, userDrain, 
                         userStop, lastResult, stack, batch, calls, m, e, req >>

s_dsend == /\ pc["s"] = "s_dsend"
           /\ \/ /\ Len(errs) < ErrsCap
                 /\ errs' = Append(errs, "disconnected")
                 /\ pc' = [pc EXCEPT !["s"] = "s_sel"]
              \/ /\ done
                 /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
                 /\ errs' = errs
           /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                           rbuf, avail, subState, msgs, msgsClosed, regs, 
                           statusCh, statusClosed, lock, fetchNext, done, 
                           closed, draining, fip, pending, delivered, hbArmed, 
                           hbFiring, returned, dropped, droppedStatus, 
                           maxBatch, userDrain, userStop, lastResult, stack, 
                           batch, calls, m, e, isConn, req >>

s_csend == /\ pc["s"] = "s_csend"
           /\ \/ /\ Len(errs) < ErrsCap
                 /\ errs' = Append(errs, "connected")
                 /\ pc' = [pc EXCEPT !["s"] = "s_sel"]
              \/ /\ done
                 /\ pc' = [pc EXCEPT !["s"] = "s_exit"]
                 /\ errs' = errs
           /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                           rbuf, avail, subState, msgs, msgsClosed, regs, 
                           statusCh, statusClosed, lock, fetchNext, done, 
                           closed, draining, fip, pending, delivered, hbArmed, 
                           hbFiring, returned, dropped, droppedStatus, 
                           maxBatch, userDrain, userStop, lastResult, stack, 
                           batch, calls, m, e, isConn, req >>

s_exit == /\ pc["s"] = "s_exit"
          /\ TRUE
          /\ pc' = [pc EXCEPT !["s"] = "Done"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, done, 
                          closed, draining, fip, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, lastResult, stack, batch, calls, 
                          m, e, isConn, req >>

Status == s_sel \/ s_dsend \/ s_csend \/ s_exit

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
                         rbuf, avail, subState, msgs, msgsClosed, regs, 
                         statusCh, statusClosed, lock, errs, done, closed, 
                         draining, pending, delivered, hbArmed, hbFiring, 
                         returned, dropped, droppedStatus, maxBatch, userDrain, 
                         userStop, lastResult, stack, batch, calls, m, e, 
                         isConn >>

p_pull == /\ pc["p"] = "p_pull"
          /\ IF closed = 1
                THEN /\ pc' = [pc EXCEPT !["p"] = "p_cl1"]
                     /\ UNCHANGED << reqs, rbuf, maxBatch >>
                ELSE /\ IF req < 1
                           THEN /\ pc' = [pc EXCEPT !["p"] = "p_err"]
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
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, avail, 
                          subState, msgs, msgsClosed, regs, statusCh, 
                          statusClosed, lock, fetchNext, errs, done, closed, 
                          draining, fip, pending, delivered, hbArmed, hbFiring, 
                          returned, dropped, droppedStatus, userDrain, 
                          userStop, lastResult, stack, batch, calls, m, e, 
                          isConn, req >>

p_fip == /\ pc["p"] = "p_fip"
         /\ fip' = 0
         /\ pc' = [pc EXCEPT !["p"] = "p_sel"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, msgs, msgsClosed, regs, 
                         statusCh, statusClosed, lock, fetchNext, errs, done, 
                         closed, draining, pending, delivered, hbArmed, 
                         hbFiring, returned, dropped, droppedStatus, maxBatch, 
                         userDrain, userStop, lastResult, stack, batch, calls, 
                         m, e, isConn, req >>

p_err == /\ pc["p"] = "p_err"
         /\ \/ /\ Len(errs) < ErrsCap
               /\ errs' = Append(errs, "pullerr")
            \/ /\ done
               /\ errs' = errs
         /\ pc' = [pc EXCEPT !["p"] = "p_fip2"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, msgs, msgsClosed, regs, 
                         statusCh, statusClosed, lock, fetchNext, done, closed, 
                         draining, fip, pending, delivered, hbArmed, hbFiring, 
                         returned, dropped, droppedStatus, maxBatch, userDrain, 
                         userStop, lastResult, stack, batch, calls, m, e, 
                         isConn, req >>

p_fip2 == /\ pc["p"] = "p_fip2"
          /\ fip' = 0
          /\ pc' = [pc EXCEPT !["p"] = "p_sel"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, done, 
                          closed, draining, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, lastResult, stack, batch, calls, 
                          m, e, isConn, req >>

p_cl1 == /\ pc["p"] = "p_cl1"
         /\ IF Len(statusCh) > 0
               THEN /\ statusCh' = Tail(statusCh)
                    /\ UNCHANGED statusClosed
               ELSE /\ statusClosed' = TRUE
                    /\ UNCHANGED statusCh
         /\ regs' = {}
         /\ pc' = [pc EXCEPT !["p"] = "p_cl2"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, subState, msgs, msgsClosed, lock, 
                         fetchNext, errs, done, closed, draining, fip, pending, 
                         delivered, hbArmed, hbFiring, returned, dropped, 
                         droppedStatus, maxBatch, userDrain, userStop, 
                         lastResult, stack, batch, calls, m, e, isConn, req >>

p_cl2 == /\ pc["p"] = "p_cl2"
         /\ IF subState = "closed"
               THEN /\ closed' = 1
                    /\ UNCHANGED << subState, msgsClosed >>
               ELSE /\ IF draining = 1
                          THEN /\ subState' = "drainflush"
                               /\ closed' = 1
                               /\ UNCHANGED msgsClosed
                          ELSE /\ subState' = "closed"
                               /\ msgsClosed' = TRUE
                               /\ closed' = 1
         /\ pc' = [pc EXCEPT !["p"] = "Done"]
         /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                         rbuf, avail, msgs, regs, statusCh, statusClosed, lock, 
                         fetchNext, errs, done, draining, fip, pending, 
                         delivered, hbArmed, hbFiring, returned, dropped, 
                         droppedStatus, maxBatch, userDrain, userStop, 
                         lastResult, stack, batch, calls, m, e, isConn, req >>

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
                          subState, msgs, msgsClosed, regs, statusCh, 
                          statusClosed, lock, fetchNext, done, closed, 
                          draining, fip, pending, delivered, returned, dropped, 
                          droppedStatus, maxBatch, userDrain, userStop, 
                          lastResult, stack, batch, calls, m, e, isConn, req >>

Timer == t_loop

u_main == /\ pc["u"] = "u_main"
          /\ \/ /\ "stop" \in UserOps
                /\ userStop' = TRUE
                /\ IF closed = 0
                      THEN /\ closed' = 1
                           /\ done' = TRUE
                      ELSE /\ TRUE
                           /\ UNCHANGED << done, closed >>
                /\ pc' = [pc EXCEPT !["u"] = "u_done"]
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
                      ELSE /\ pc' = [pc EXCEPT !["u"] = "u_done"]
                           /\ UNCHANGED << done, closed, draining, userDrain >>
                /\ UNCHANGED userStop
             \/ /\ TRUE
                /\ pc' = [pc EXCEPT !["u"] = "u_done"]
                /\ UNCHANGED <<done, closed, draining, userDrain, userStop>>
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, fip, 
                          pending, delivered, hbArmed, hbFiring, returned, 
                          dropped, droppedStatus, maxBatch, lastResult, stack, 
                          batch, calls, m, e, isConn, req >>

u_drain2 == /\ pc["u"] = "u_drain2"
            /\ IF "drainrace" \notin Fixes
                  THEN /\ draining' = 1
                       /\ done' = TRUE
                  ELSE /\ TRUE
                       /\ UNCHANGED << done, draining >>
            /\ pc' = [pc EXCEPT !["u"] = "u_done"]
            /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                            rbuf, avail, subState, msgs, msgsClosed, regs, 
                            statusCh, statusClosed, lock, fetchNext, errs, 
                            closed, fip, pending, delivered, hbArmed, hbFiring, 
                            returned, dropped, droppedStatus, maxBatch, 
                            userDrain, userStop, lastResult, stack, batch, 
                            calls, m, e, isConn, req >>

u_done == /\ pc["u"] = "u_done"
          /\ TRUE
          /\ pc' = [pc EXCEPT !["u"] = "Done"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, subState, msgs, msgsClosed, regs, 
                          statusCh, statusClosed, lock, fetchNext, errs, done, 
                          closed, draining, fip, pending, delivered, hbArmed, 
                          hbFiring, returned, dropped, droppedStatus, maxBatch, 
                          userDrain, userStop, lastResult, stack, batch, calls, 
                          m, e, isConn, req >>

User == u_main \/ u_drain2 \/ u_done

l_loop == /\ pc["lib"] = "l_loop"
          /\ subState = "drainflush"
          /\ subState' = "closed"
          /\ msgsClosed' = TRUE
          /\ pc' = [pc EXCEPT !["lib"] = "l_loop"]
          /\ UNCHANGED << conn, reconnects, expires, hbMsgs, hbFires, reqs, 
                          rbuf, avail, msgs, regs, statusCh, statusClosed, 
                          lock, fetchNext, errs, done, closed, draining, fip, 
                          pending, delivered, hbArmed, hbFiring, returned, 
                          dropped, droppedStatus, maxBatch, userDrain, 
                          userStop, lastResult, stack, batch, calls, m, e, 
                          isConn, req >>

Lib == l_loop

e_loop == /\ pc["env"] = "e_loop"
          /\ \/ /\ Accepting /\ Len(reqs) > 0 /\ avail > 0 /\ PullSettled
                /\ IF Len(msgs) < MaxMsgs \/ "unbounded" \in Fixes
                      THEN /\ msgs' = Append(msgs, ([t |-> "msg", n |-> 0]))
                           /\ UNCHANGED << dropped, droppedStatus >>
                      ELSE /\ IF ([t |-> "msg", n |-> 0]).t = "msg"
                                 THEN /\ dropped' = dropped + 1
                                      /\ UNCHANGED droppedStatus
                                 ELSE /\ droppedStatus' = droppedStatus + 1
                                      /\ UNCHANGED dropped
                           /\ msgs' = msgs
                /\ avail' = avail - 1
                /\ reqs' = (IF Head(reqs) = 1 THEN Tail(reqs) ELSE << Head(reqs) - 1 >> \o Tail(reqs))
                /\ UNCHANGED <<conn, reconnects, expires, hbMsgs, rbuf, regs, statusCh>>
             \/ /\ Len(reqs) > 0 /\ expires < MaxExpires /\ PullSettled
                /\ IF Accepting
                      THEN /\ IF Len(msgs) < MaxMsgs \/ "unbounded" \in Fixes
                                 THEN /\ msgs' = Append(msgs, ([t |-> "408", n |-> Head(reqs)]))
                                      /\ UNCHANGED << dropped, droppedStatus >>
                                 ELSE /\ IF ([t |-> "408", n |-> Head(reqs)]).t = "msg"
                                            THEN /\ dropped' = dropped + 1
                                                 /\ UNCHANGED droppedStatus
                                            ELSE /\ droppedStatus' = droppedStatus + 1
                                                 /\ UNCHANGED dropped
                                      /\ msgs' = msgs
                      ELSE /\ TRUE
                           /\ UNCHANGED << msgs, dropped, droppedStatus >>
                /\ reqs' = Tail(reqs)
                /\ expires' = expires + 1
                /\ UNCHANGED <<conn, reconnects, hbMsgs, rbuf, avail, regs, statusCh>>
             \/ /\ Accepting /\ Len(reqs) > 0 /\ hbMsgs < MaxHBMsgs
                /\ IF Len(msgs) < MaxMsgs \/ "unbounded" \in Fixes
                      THEN /\ msgs' = Append(msgs, ([t |-> "hb", n |-> 0]))
                           /\ UNCHANGED << dropped, droppedStatus >>
                      ELSE /\ IF ([t |-> "hb", n |-> 0]).t = "msg"
                                 THEN /\ dropped' = dropped + 1
                                      /\ UNCHANGED droppedStatus
                                 ELSE /\ droppedStatus' = droppedStatus + 1
                                      /\ UNCHANGED dropped
                           /\ msgs' = msgs
                /\ hbMsgs' = hbMsgs + 1
                /\ UNCHANGED <<conn, reconnects, expires, reqs, rbuf, avail, regs, statusCh>>
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
                /\ UNCHANGED <<reconnects, expires, hbMsgs, rbuf, avail, msgs, dropped, droppedStatus>>
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
                /\ UNCHANGED <<expires, hbMsgs, avail, msgs, dropped, droppedStatus>>
          /\ pc' = [pc EXCEPT !["env"] = "e_loop"]
          /\ UNCHANGED << hbFires, subState, msgsClosed, statusClosed, lock, 
                          fetchNext, errs, done, closed, draining, fip, 
                          pending, delivered, hbArmed, hbFiring, returned, 
                          maxBatch, userDrain, userStop, lastResult, stack, 
                          batch, calls, m, e, isConn, req >>

Env == e_loop

Next == Nexter \/ Status \/ Puller \/ Timer \/ User \/ Lib \/ Env
           \/ (\E self \in ProcSet: CheckPending(self))

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Nexter) /\ WF_vars(CheckPending("n"))
        /\ WF_vars(Status)
        /\ WF_vars(Puller)
        /\ WF_vars(Timer)
        /\ WF_vars(User)
        /\ WF_vars(Lib)

\* END TRANSLATION

-----------------------------------------------------------------------------
TypeOK ==
    /\ Len(fetchNext) <= 1 /\ Len(errs) <= ErrsCap
    /\ ("unbounded" \in Fixes \/ Len(msgs) <= MaxMsgs)
    /\ lock \in {"none", "n"}

\* No message is dropped because the client asked for more than it can buffer.
NoSlowConsumerDrop == dropped = 0
\* ... nor a 408 (which carries pending-message accounting) / heartbeat.
NoStatusDrop == droppedStatus = 0

BatchWithinMax == maxBatch <= MaxMsgs

StopAfterRespected == StopAfter = 0 \/ returned <= StopAfter

\* After Drain, Next reports the iterator closed only once every buffered
\* message was returned (the closed branch of the select only fires on an
\* empty, closed channel; the Stop branch of Next must not fire on Drain).
DrainKeepsBuffered == (userDrain /\ ~userStop /\ lastResult = "closed") => msgs = << >>

FetchSenders == IF pc["n"] = "cp_send" THEN {"n"} ELSE {}
NoOrphanedFetchSend ==
    ~(pc["p"] = "Done" /\ ~CanSendFetch /\ ~FetchSendDone /\ FetchSenders # {})

\* Next is parked in its select with no pull request anywhere and nothing
\* buffered or queued that could produce one: only Next's heartbeat timer
\* (ErrNoHeartbeat returned to the caller) or its own timeout ends it.
Waiting ==
    /\ pc["n"] = "n_sel" /\ conn = "up" /\ subState = "active" /\ closed = 0
    /\ reqs = << >> /\ rbuf = << >> /\ fetchNext = << >> /\ fip = 0
    /\ msgs = << >> /\ errs = << >> /\ statusCh = << >> /\ hbFiring = 0
    /\ pc["p"] = "p_sel" /\ pc["s"] = "s_sel"
    /\ avail > 0 /\ (StopAfter = 0 \/ delivered < StopAfter)
NoSilentStall == ~Waiting
\* ... and not even the heartbeat timer is armed: without a timeout or
\* context, Next blocks forever on a healthy connection.
NoPermanentStall == ~(Waiting /\ ~hbArmed /\ ~NextTimeout)

Quiescent == ~ENABLED Next
NoDeadlock == (Quiescent /\ done) =>
    (pc["p"] = "Done" /\ pc["s"] = "Done" /\ hbFiring = 0 /\ lock = "none")
=============================================================================
