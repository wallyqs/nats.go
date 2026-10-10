---------------------------- MODULE OrderedConsumer ----------------------------
(***************************************************************************)
(* The ordered consumer's Consume path (jetstream/ordered.go) on top of    *)
(* pull subscriptions (jetstream/pull.go), at the granularity of           *)
(* goroutines, the ordered consumer's mutex (c.Mutex), its channels and    *)
(* atomics. Line numbers refer to the checkout this model was written      *)
(* against (upstream main at 31a3ee4).                                     *)
(*                                                                         *)
(* Goroutines:                                                             *)
(*   rg        the reset goroutine started by Consume (ordered.go:162-200) *)
(*   usr       the user calling ConsumeContext.Stop (ordered.go:358-368)   *)
(*   d_i       the dispatcher of inner subscription i: nats.go             *)
(*             waitForMsgs -> pull.go:245-302 -> ordered.go:119-153        *)
(*   s_i       the status goroutine of inner subscription i                *)
(*             (pull.go:348-428)                                           *)
(*   cleanup_i pullMessages' cleanup of inner subscription i               *)
(*             (pull.go:1093-1094, 1120-1150)                              *)
(* plus the environment: the server-side consumers, the connection, the   *)
(* heartbeat timers and the async DeleteConsumer of reset                 *)
(* (ordered.go:563-567).                                                   *)
(*                                                                         *)
(* The stream holds messages 1..N, all matching the filter; a server-side  *)
(* consumer delivers (consumer seq, stream seq) pairs from its start       *)
(* sequence. Every consumer seq and stream seq is therefore observable.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    N,               \* messages in the stream
    SA,              \* StopAfter given to Consume, 0 = not set
    MaxSubs,         \* size of the inner subscription table (checked by BoundOK)
    MaxGaps,         \* messages lost between server and handler
    MaxHb,           \* spurious missed heartbeats (timer fires with server healthy)
    MaxExtDel,       \* external deletions of the current server-side consumer
    MaxDisc,         \* disconnect / reconnect cycles
    AllowClose,      \* the connection may be closed for good
    MaxCreateFail,   \* consumer create may fail with an API error while connected
    MaxReset,        \* OrderedConsumerConfig.MaxResetAttempts, 0 = unlimited
    UserStops,       \* the user calls Stop() on the ordered ConsumeContext
    UserErrHandler,  \* a ConsumeErrHandler is installed
    UserStopOn       \* errors on which that handler calls Stop() on the
                     \* ordered ConsumeContext returned by Consume

ASSUME N \in Nat /\ SA \in Nat /\ MaxSubs \in Nat \ {0}

\* Error values flowing into orderedConsumer.errHandler (ordered.go:204).
\*   "hb"         ErrNoHeartbeat                (pull.go:1113)
\*   "mismatch"   errOrderedSequenceMismatch    (ordered.go:137)
\*   "deleted"    ErrConsumerDeleted (409)      (pull.go:280)
\*   "connected"  errConnected                  (pull.go:370)
\*   "noresp"     nats.ErrNoResponders (503)    (pull.go:273)
\*   "closed"     ErrConnectionClosed           (pull.go:392)
\*   "createfail" error returned by reset()     (ordered.go:177)
\*   "invalid"    ErrInvalidOption from the inner Consume (ordered.go:184)
ResetTriggers == {"hb", "mismatch", "deleted", "connected", "noresp"}

NoMsg == [k |-> "none", d |-> 0, s |-> 0]
NullSub == [cons |-> 0, ser |-> 0, lim |-> 0, idel |-> 0, closed |-> TRUE,
            unsub |-> TRUE, q |-> <<>>, dpc |-> "idle", dmsg |-> NoMsg,
            dlast |-> FALSE, derr |-> "none", spc |-> "exit", ev |-> <<>>,
            errs |-> <<>>, isConn |-> TRUE, serr |-> "none", hb |-> FALSE,
            born |-> FALSE, pl |-> FALSE]

VARIABLES
    \* orderedConsumer fields
    lock,       \* holder of c.Mutex, or "none"
    serial,     \* c.serial
    curD,       \* c.cursor.deliverSeq
    curS,       \* c.cursor.streamSeq
    delivered,  \* c.deliveredMessages
    rip,        \* c.resetInProgress
    doReset,    \* items in c.doReset (buffer 1)
    oclosed,    \* orderedSubscription.closed
    odone,      \* orderedSubscription.done is closed
    curSub,     \* c.currentSub (index into subs)
    curCons,    \* c.currentConsumer (index into cons)
    \* environment
    cons,       \* server-side consumers: [exists, nD (next consumer seq), nS (next stream seq)]
    nCons,
    pendDel,    \* consumers with an in-flight DeleteConsumer from reset()
    conn,       \* "up", "down" (RECONNECTING) or "closed"
    \* inner pull subscriptions and their goroutines
    subs,
    nSubs,
    rg,         \* reset goroutine: [pc, snap, isa, att, nw]
    usr,        \* user Stop goroutine pc
    \* observation
    obs,        \* stream seqs handed to the user handler, in order of entry
    active,     \* inner subs whose user handler is running
    stopRet,    \* the user's Stop() has returned
    late,       \* a handler was entered for a message dequeued after Stop() returned
    lateNew,    \* ... for a message of an inner sub created after Stop() returned
    userErrs,   \* errors passed to the user's ConsumeErrHandler
    \* fault budgets
    gaps, hbs, extDels, discs, cfails

cvars == <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, odone, curSub, curCons>>
evars == <<cons, nCons, pendDel, conn>>
gvars == <<subs, nSubs, rg, usr>>
ovars == <<obs, active, stopRet, late, lateNew, userErrs>>
bvars == <<gaps, hbs, extDels, discs, cfails>>
vars == <<cvars, evars, gvars, ovars, bvars>>

MaxCons == MaxSubs + 1
Target == IF SA > 0 /\ SA < N THEN SA ELSE N
Free == lock = "none"

\* ---------------------------------------------------------------------------
\* errHandler (ordered.go:204-233). It takes c.Mutex and, while holding it,
\* calls the user's ConsumeErrHandler for everything but sequence mismatches
\* and reconnect notifications (ordered.go:208-210).
UserSees(e) == UserErrHandler /\ e \notin {"mismatch", "connected"}
\* The user's handler calls Stop() on the ordered ConsumeContext. Stop's CAS
\* (ordered.go:359) succeeds when nothing closed it before, and it then
\* blocks on c.Mutex (ordered.go:362), which this goroutine holds.
SelfStop(e) == UserSees(e) /\ e \in UserStopOn /\ ~oclosed
\* reset trigger (ordered.go:221-231)
Trig(e, ser) == e \in ResetTriggers /\ ser = serial /\ rip = 0

AddErr(e) == IF UserSees(e) THEN userErrs \cup {e} ELSE userErrs

\* Stop on an inner pull subscription (pull.go:816-821).
StopInner(sf, i) == IF i = 0 THEN sf ELSE [sf EXCEPT ![i].closed = TRUE]

NewSub(k, ser, lim, q0, born) ==
    [cons |-> k, ser |-> ser, lim |-> lim, idel |-> 0, closed |-> FALSE,
     unsub |-> FALSE, q |-> q0, dpc |-> "idle", dmsg |-> NoMsg,
     dlast |-> FALSE, derr |-> "none", spc |-> "loop", ev |-> <<>>,
     errs |-> <<>>, isConn |-> TRUE, serr |-> "none",
     hb |-> TRUE,           \* scheduleHeartbeatCheck arms the timer (pull.go:240)
     born |-> born,
     pl |-> FALSE]          \* current message was popped after Stop() returned

\* ---------------------------------------------------------------------------
Init ==
    \* OrderedConsumer() created consumer serial 1 (jetstream.go:917-929);
    \* Consume() reuses it (ordered.go:85) and starts inner sub 1 with
    \* StopAfter(SA) (ordered.go:107-113,156-160).
    /\ lock = "none" /\ serial = 1 /\ curD = 0 /\ curS = 0 /\ delivered = 0
    /\ rip = 0 /\ doReset = 0 /\ oclosed = FALSE /\ odone = FALSE
    /\ curSub = 1 /\ curCons = 1
    /\ cons = [k \in 1..MaxCons |-> IF k = 1 THEN [exists |-> TRUE, nD |-> 1, nS |-> 1]
                                    ELSE [exists |-> FALSE, nD |-> 1, nS |-> 1]]
    /\ nCons = 1 /\ pendDel = {} /\ conn = "up"
    /\ subs = [i \in 1..MaxSubs |-> IF i = 1 THEN NewSub(1, 1, SA, <<>>, FALSE) ELSE NullSub]
    /\ nSubs = 1
    /\ rg = [pc |-> "sel", snap |-> 0, isa |-> 0, att |-> 0, nw |-> 0]
    /\ usr = "idle"
    /\ obs = <<>> /\ active = {} /\ stopRet = FALSE /\ late = FALSE
    /\ lateNew = FALSE /\ userErrs = {}
    /\ gaps = 0 /\ hbs = 0 /\ extDels = 0 /\ discs = 0 /\ cfails = 0

\* ===========================================================================
\* Dispatcher of inner subscription i
\* ===========================================================================
SetD(i, f) == subs' = [subs EXCEPT ![i] = f]

\* nats.go waitForMsgs pops the next message unless the subscription was
\* removed (nats.go:3806-3849). pull.go:246-247 stops the heartbeat timer.
DPop(i) ==
    LET s == subs[i]  it == Head(s.q) IN
    /\ s.dpc = "idle" /\ s.q /= <<>> /\ ~s.unsub
    /\ CASE it.k = "msg" ->
              SetD(i, [s EXCEPT !.q = Tail(s.q), !.dmsg = it, !.dpc = "h1", !.hb = FALSE,
                                !.pl = stopRet])
         [] it.k = "409" ->
              \* terminal status (pull.go:262-283): ignored once closed (276)
              SetD(i, [s EXCEPT !.q = Tail(s.q), !.hb = FALSE,
                         !.dpc = IF s.closed THEN "idle" ELSE "eh",
                         !.derr = "deleted"])
         [] it.k = "503" ->
              \* non-terminal: checkPending + timer reset, then ErrHandler (264-274)
              SetD(i, [s EXCEPT !.q = Tail(s.q), !.hb = TRUE, !.dpc = "eh",
                         !.derr = "noresp"])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

\* ordered internalHandler under c.Mutex (ordered.go:121-148)
DH1(i) ==
    LET s == subs[i]  m == s.dmsg IN
    /\ s.dpc = "h1" /\ Free
    /\ IF s.ser /= serial THEN                      \* ordered.go:123
          /\ SetD(i, [s EXCEPT !.dpc = "hpost"])
          /\ UNCHANGED <<curD, curS, delivered>>
       ELSE IF m.d /= curD + 1 THEN                 \* ordered.go:135-138
          /\ SetD(i, [s EXCEPT !.dpc = "eh", !.derr = "mismatch"])
          /\ UNCHANGED <<curD, curS, delivered>>
       ELSE IF SA > 0 /\ delivered >= SA THEN       \* ordered.go:140-143
          /\ SetD(i, [s EXCEPT !.dpc = "hpost"])
          /\ UNCHANGED <<curD, curS, delivered>>
       ELSE                                         \* ordered.go:144-148
          /\ curD' = m.d /\ curS' = m.s /\ delivered' = delivered + 1
          /\ SetD(i, [s EXCEPT !.dpc = "hrun", !.dlast = (SA > 0 /\ delivered + 1 = SA)])
    /\ UNCHANGED <<lock, serial, rip, doReset, oclosed, odone, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

\* handler(msg) is entered after c.Unlock() (ordered.go:148-149)
DRun(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "hrun"
    /\ obs' = Append(obs, s.dmsg.s)
    /\ active' = active \cup {i}
    /\ late' = (late \/ s.pl)
    /\ lateNew' = (lateNew \/ (stopRet /\ s.born))
    /\ SetD(i, [s EXCEPT !.dpc = "hend"])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, stopRet, userErrs, bvars>>

\* the user handler returns; the last message stops the ordered sub (150-152)
DEnd(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "hend"
    /\ active' = active \ {i}
    /\ SetD(i, [s EXCEPT !.dpc = IF s.dlast THEN "st1" ELSE "hpost"])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, obs, stopRet, late, lateNew, userErrs, bvars>>

\* orderedSubscription.Stop, CAS (ordered.go:359)
DSt1(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "st1"
    /\ IF oclosed THEN SetD(i, [s EXCEPT !.dpc = "hpost"]) /\ UNCHANGED oclosed
       ELSE oclosed' = TRUE /\ SetD(i, [s EXCEPT !.dpc = "st2"])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

\* orderedSubscription.Stop under c.Mutex (ordered.go:362-367)
DSt2(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "st2" /\ Free
    /\ odone' = TRUE
    /\ subs' = [StopInner(subs, curSub) EXCEPT ![i].dpc = "hpost"]
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

\* pull.go:290-301: delivered++ and StopAfter on the inner subscription;
\* the heartbeat timer is re-armed unless cleanup closed it (pull.go:793-799).
DPost(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "hpost"
    /\ SetD(i, [s EXCEPT !.dpc = "idle", !.idel = s.idel + 1, !.hb = ~s.unsub,
                         !.closed = s.closed \/ (s.lim > 0 /\ s.idel + 1 = s.lim)])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

DNext(e) == CASE e = "deleted" -> "ehstop"   \* pull.go:282 sub.Stop()
              [] e = "mismatch" -> "hpost"   \* back in pull.go:290
              [] OTHER -> "idle"

\* errHandler called from the dispatcher (ordered.go:137 / pull.go:273,280)
DEh(i) ==
    LET s == subs[i]  e == s.derr IN
    /\ s.dpc = "eh" /\ Free
    /\ userErrs' = AddErr(e)
    /\ IF SelfStop(e) THEN
          /\ oclosed' = TRUE /\ lock' = <<"d", i>>
          /\ SetD(i, [s EXCEPT !.dpc = "dead"])
          /\ UNCHANGED <<rip, doReset>>
       ELSE IF Trig(e, s.ser) THEN
          /\ rip' = 1 /\ UNCHANGED oclosed
          /\ IF doReset = 0
                THEN doReset' = 1 /\ UNCHANGED lock /\ SetD(i, [s EXCEPT !.dpc = DNext(e)])
                ELSE UNCHANGED doReset /\ lock' = <<"d", i>> /\ SetD(i, [s EXCEPT !.dpc = "ehblk"])
       ELSE UNCHANGED <<rip, doReset, lock, oclosed>> /\ SetD(i, [s EXCEPT !.dpc = DNext(e)])
    /\ UNCHANGED <<serial, curD, curS, delivered, odone, curSub, curCons,
                   evars, nSubs, rg, usr, obs, active, stopRet, late, lateNew, bvars>>

\* blocked sending to c.doReset while holding c.Mutex (ordered.go:229)
DEhBlk(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "ehblk" /\ doReset = 0
    /\ doReset' = 1 /\ lock' = "none"
    /\ SetD(i, [s EXCEPT !.dpc = DNext(s.derr)])
    /\ UNCHANGED <<serial, curD, curS, delivered, rip, oclosed, odone, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

DEhStop(i) ==
    LET s == subs[i] IN
    /\ s.dpc = "ehstop"
    /\ SetD(i, [s EXCEPT !.dpc = "idle", !.closed = TRUE])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

Dispatcher(i) ==
    DPop(i) \/ DH1(i) \/ DRun(i) \/ DEnd(i) \/ DSt1(i) \/ DSt2(i) \/ DPost(i)
    \/ DEh(i) \/ DEhBlk(i) \/ DEhStop(i)

\* ===========================================================================
\* Status goroutine of inner subscription i (pull.go:348-428)
\* ===========================================================================
SExit(i) ==
    /\ subs[i].spc = "loop" /\ subs[i].closed          \* pull.go:351 / 424
    /\ SetD(i, [subs[i] EXCEPT !.spc = "exit"])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

SEv(i) ==
    LET s == subs[i]  e == Head(s.ev) IN
    /\ s.spc = "loop" /\ ~s.closed /\ s.ev /= <<>>
    /\ CASE e = "RECONNECTING" ->                       \* pull.go:360-364
              SetD(i, [s EXCEPT !.ev = Tail(s.ev), !.hb = FALSE, !.isConn = FALSE])
         [] e = "CONNECTED" /\ ~s.isConn ->             \* pull.go:365-388
              SetD(i, [s EXCEPT !.ev = Tail(s.ev), !.isConn = TRUE, !.hb = TRUE,
                                !.errs = Append(s.errs, "connected")])
         [] e = "CONNECTED" /\ s.isConn ->
              SetD(i, [s EXCEPT !.ev = Tail(s.ev)])
         [] e = "CLOSED" ->                             \* pull.go:391-392
              SetD(i, [s EXCEPT !.ev = Tail(s.ev), !.errs = Append(s.errs, "closed")])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

\* pull.go:394-419: a missed heartbeat re-pulls and re-arms the timer, then
\* every error goes to the (ordered) ErrHandler.
SErr(i) ==
    LET s == subs[i]  e == Head(s.errs) IN
    /\ s.spc = "loop" /\ ~s.closed /\ s.errs /= <<>>
    /\ SetD(i, [s EXCEPT !.errs = Tail(s.errs), !.spc = "eh", !.serr = e,
                         !.hb = IF e = "hb" THEN ~s.unsub ELSE s.hb])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

SEh(i) ==
    LET s == subs[i]  e == s.serr IN
    /\ s.spc = "eh" /\ Free
    /\ userErrs' = AddErr(e)
    /\ IF SelfStop(e) THEN
          /\ oclosed' = TRUE /\ lock' = <<"s", i>>
          /\ SetD(i, [s EXCEPT !.spc = "dead"])
          /\ UNCHANGED <<rip, doReset>>
       ELSE IF e = "closed" THEN
          \* ordered.go:211-216: unlock, then orderedSubscription.Stop()
          /\ SetD(i, [s EXCEPT !.spc = "cst1"])
          /\ UNCHANGED <<rip, doReset, lock, oclosed>>
       ELSE IF Trig(e, s.ser) THEN
          /\ rip' = 1 /\ UNCHANGED oclosed
          /\ IF doReset = 0
                THEN doReset' = 1 /\ UNCHANGED lock /\ SetD(i, [s EXCEPT !.spc = "post"])
                ELSE UNCHANGED doReset /\ lock' = <<"s", i>> /\ SetD(i, [s EXCEPT !.spc = "ehblk"])
       ELSE UNCHANGED <<rip, doReset, lock, oclosed>> /\ SetD(i, [s EXCEPT !.spc = "post"])
    /\ UNCHANGED <<serial, curD, curS, delivered, odone, curSub, curCons,
                   evars, nSubs, rg, usr, obs, active, stopRet, late, lateNew, bvars>>

SEhBlk(i) ==
    LET s == subs[i] IN
    /\ s.spc = "ehblk" /\ doReset = 0
    /\ doReset' = 1 /\ lock' = "none"
    /\ SetD(i, [s EXCEPT !.spc = "post"])
    /\ UNCHANGED <<serial, curD, curS, delivered, rip, oclosed, odone, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

SCst1(i) ==   \* orderedSubscription.Stop CAS (ordered.go:359)
    LET s == subs[i] IN
    /\ s.spc = "cst1"
    /\ IF oclosed THEN SetD(i, [s EXCEPT !.spc = "post"]) /\ UNCHANGED oclosed
       ELSE oclosed' = TRUE /\ SetD(i, [s EXCEPT !.spc = "cst2"])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

SCst2(i) ==
    LET s == subs[i] IN
    /\ s.spc = "cst2" /\ Free
    /\ odone' = TRUE
    /\ subs' = [StopInner(subs, curSub) EXCEPT ![i].spc = "post"]
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, curSub, curCons,
                   evars, nSubs, rg, usr, ovars, bvars>>

SPost(i) ==   \* pull.go:421-423
    LET s == subs[i] IN
    /\ s.spc = "post"
    /\ SetD(i, [s EXCEPT !.spc = "loop", !.closed = s.closed \/ s.serr = "closed"])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

Status(i) ==
    SExit(i) \/ SEv(i) \/ SErr(i) \/ SEh(i) \/ SEhBlk(i) \/ SCst1(i) \/ SCst2(i) \/ SPost(i)

\* pullMessages sees done and runs cleanup: status listener removed, heartbeat
\* monitor closed, subscription unsubscribed (pull.go:1093-1094, 1120-1150).
Cleanup(i) ==
    /\ subs[i].closed /\ ~subs[i].unsub
    /\ SetD(i, [subs[i] EXCEPT !.unsub = TRUE, !.hb = FALSE])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars, bvars>>

\* ===========================================================================
\* Reset goroutine (ordered.go:162-200) and reset() (ordered.go:552-601)
\* ===========================================================================
SetRG(f) == rg' = f

RGSel ==
    /\ rg.pc = "sel"
    /\ \/ doReset = 1 /\ doReset' = 0 /\ SetRG([rg EXCEPT !.pc = "chk"]) /\ UNCHANGED odone
       \/ odone /\ SetRG([rg EXCEPT !.pc = "dn"]) /\ UNCHANGED doReset
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, oclosed, odone, curSub, curCons,
                   evars, subs, nSubs, usr, ovars, bvars>>

RGChk ==   \* ordered.go:166-172
    /\ rg.pc = "chk" /\ Free
    /\ SetRG([rg EXCEPT !.pc = IF SA > 0 /\ delivered >= SA THEN "sa1" ELSE "r1"])
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, ovars, bvars>>

RGR1 ==    \* reset(): lock, stop current sub, async delete, serial++ (552-571, 604)
    /\ rg.pc = "r1" /\ Free
    /\ lock' = "rg"
    /\ subs' = StopInner(subs, curSub)
    /\ pendDel' = pendDel \cup {curCons}
    /\ curD' = 0 /\ serial' = serial + 1
    /\ SetRG([rg EXCEPT !.pc = "r2", !.att = 0])
    /\ UNCHANGED <<curS, delivered, rip, doReset, oclosed, odone, curSub, curCons,
                   cons, nCons, conn, nSubs, usr, ovars, bvars>>

RGR2 ==    \* closed check of every attempt (ordered.go:584-587)
    /\ rg.pc = "r2"
    /\ IF oclosed
          THEN lock' = "none" /\ rip' = 0 /\ SetRG([rg EXCEPT !.pc = "sel"])   \* 174-176
          ELSE UNCHANGED <<lock, rip>> /\ SetRG([rg EXCEPT !.pc = "r3"])
    /\ UNCHANGED <<serial, curD, curS, delivered, doReset, oclosed, odone, curSub, curCons,
                   evars, subs, nSubs, usr, ovars, bvars>>

\* CreateOrUpdateConsumer (ordered.go:588-594), still holding c.Mutex;
\* retryWithBackoff (ordered.go:753-812) retries until MaxReset attempts.
RGR3 ==
    /\ rg.pc = "r3"
    /\ \/ /\ conn = "up" /\ nCons < MaxCons
          /\ nCons' = nCons + 1
          /\ cons' = [cons EXCEPT ![nCons + 1] = [exists |-> TRUE, nD |-> 1, nS |-> curS + 1]]
          /\ curCons' = nCons + 1
          /\ lock' = "none" /\ rip' = 0
          /\ SetRG([rg EXCEPT !.pc = "opts"])
          /\ UNCHANGED cfails
       \/ /\ \/ conn /= "up"
             \/ conn = "up" /\ cfails < MaxCreateFail
          /\ cfails' = IF conn = "up" THEN cfails + 1 ELSE cfails
          /\ IF MaxReset > 0 /\ rg.att + 1 >= MaxReset
                THEN lock' = "none" /\ rip' = 0 /\ SetRG([rg EXCEPT !.pc = "err", !.att = rg.att + 1])
                ELSE UNCHANGED <<lock, rip>>
                     /\ SetRG([rg EXCEPT !.pc = "r2", !.att = IF MaxReset > 0 THEN rg.att + 1 ELSE 0])
          /\ UNCHANGED <<cons, nCons, curCons>>
    /\ UNCHANGED <<serial, curD, curS, delivered, doReset, oclosed, odone, curSub,
                   pendDel, conn, subs, nSubs, usr, ovars, gaps, hbs, extDels, discs>>

\* errHandler(c.serial)(c.currentSub, err) after a failed reset (ordered.go:177)
RGErr ==
    /\ rg.pc = "err" /\ Free
    /\ userErrs' = AddErr("createfail")
    /\ IF SelfStop("createfail")
          THEN oclosed' = TRUE /\ lock' = "rg" /\ SetRG([rg EXCEPT !.pc = "dead"])
          ELSE UNCHANGED <<oclosed, lock>> /\ SetRG([rg EXCEPT !.pc = "opts"])
    /\ UNCHANGED <<serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, subs, nSubs, usr, obs, active, stopRet, late, lateNew, bvars>>

RGOpts ==  \* ordered.go:179-182, innerOpts (107-113)
    /\ rg.pc = "opts" /\ Free
    /\ SetRG([rg EXCEPT !.pc = "cons", !.snap = serial,
                        !.isa = IF SA > 0 THEN SA - delivered ELSE 0])
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, ovars, bvars>>

\* currentConsumer.Consume(...) (ordered.go:183). No c.Mutex, no closed check.
\* StopAfter(n) with n <= 0 is rejected (jetstream_options.go:433-436).
\* A pull on a consumer that does not exist gets a 503 (no responders).
RGCons ==
    /\ rg.pc = "cons"
    /\ IF SA > 0 /\ rg.isa <= 0
          THEN SetRG([rg EXCEPT !.pc = "cerr"]) /\ UNCHANGED <<subs, nSubs>>
          ELSE /\ nSubs < MaxSubs
               /\ nSubs' = nSubs + 1
               /\ subs' = [subs EXCEPT ![nSubs + 1] =
                             NewSub(curCons, rg.snap, rg.isa,
                                    IF ~cons[curCons].exists /\ conn = "up"
                                       THEN <<[k |-> "503", d |-> 0, s |-> 0]>> ELSE <<>>,
                                    stopRet)]
               /\ SetRG([rg EXCEPT !.pc = "asg", !.nw = nSubs + 1])
    /\ UNCHANGED <<cvars, evars, usr, ovars, bvars>>

\* errHandler(serial)(cc, err) with cc == nil (ordered.go:184)
RGCErr ==
    /\ rg.pc = "cerr" /\ Free
    /\ userErrs' = AddErr("invalid")
    /\ IF SelfStop("invalid")
          THEN oclosed' = TRUE /\ lock' = "rg" /\ SetRG([rg EXCEPT !.pc = "dead"])
          ELSE UNCHANGED <<oclosed, lock>> /\ SetRG([rg EXCEPT !.pc = "sel"])
    /\ UNCHANGED <<serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, subs, nSubs, usr, obs, active, stopRet, late, lateNew, bvars>>

RGAsg ==   \* ordered.go:186-188
    /\ rg.pc = "asg" /\ Free
    /\ curSub' = rg.nw
    /\ SetRG([rg EXCEPT !.pc = "sel"])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, odone, curCons,
                   evars, subs, nSubs, usr, ovars, bvars>>

RGDn ==    \* case <-sub.done (ordered.go:190-197)
    /\ rg.pc = "dn" /\ Free
    /\ subs' = StopInner(subs, curSub)
    /\ SetRG([rg EXCEPT !.pc = "end"])
    /\ UNCHANGED <<cvars, evars, nSubs, usr, ovars, bvars>>

RGSa1 ==   \* sub.Stop() when StopAfter is reached (ordered.go:169-171)
    /\ rg.pc = "sa1"
    /\ IF oclosed THEN SetRG([rg EXCEPT !.pc = "end"]) /\ UNCHANGED oclosed
       ELSE oclosed' = TRUE /\ SetRG([rg EXCEPT !.pc = "sa2"])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, subs, nSubs, usr, ovars, bvars>>

RGSa2 ==
    /\ rg.pc = "sa2" /\ Free
    /\ odone' = TRUE
    /\ subs' = StopInner(subs, curSub)
    /\ SetRG([rg EXCEPT !.pc = "end"])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, curSub, curCons,
                   evars, nSubs, usr, ovars, bvars>>

ResetG == RGSel \/ RGChk \/ RGR1 \/ RGR2 \/ RGR3 \/ RGErr \/ RGOpts \/ RGCons
          \/ RGCErr \/ RGAsg \/ RGDn \/ RGSa1 \/ RGSa2

\* ===========================================================================
\* User calling Stop() on the ordered ConsumeContext (ordered.go:358-368)
\* ===========================================================================
UStart ==
    /\ UserStops /\ usr = "idle"
    /\ usr' = "st1"
    /\ UNCHANGED <<cvars, evars, subs, nSubs, rg, ovars, bvars>>

USt1 ==
    /\ usr = "st1"
    /\ IF oclosed
          THEN usr' = "ret" /\ stopRet' = TRUE /\ UNCHANGED oclosed
          ELSE usr' = "st2" /\ oclosed' = TRUE /\ UNCHANGED stopRet
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, odone, curSub, curCons,
                   evars, subs, nSubs, rg, obs, active, late, lateNew, userErrs, bvars>>

USt2 ==
    /\ usr = "st2" /\ Free
    /\ subs' = StopInner(subs, curSub)
    /\ odone' = TRUE
    /\ usr' = "ret" /\ stopRet' = TRUE
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, rip, doReset, oclosed, curSub, curCons,
                   evars, nSubs, rg, obs, active, late, lateNew, userErrs, bvars>>

User == UStart \/ USt1 \/ USt2

\* ===========================================================================
\* Environment
\* ===========================================================================
Subscribed(i) == i \in 1..nSubs /\ ~subs[i].unsub

\* the server-side consumer of sub i delivers its next message
Deliver(i) ==
    LET k == subs[i].cons IN
    /\ conn = "up" /\ Subscribed(i) /\ cons[k].exists /\ cons[k].nS <= N
    /\ Len(subs[i].q) < 2
    /\ cons' = [cons EXCEPT ![k].nD = @ + 1, ![k].nS = @ + 1]
    /\ SetD(i, [subs[i] EXCEPT !.q = Append(@, [k |-> "msg", d |-> cons[k].nD, s |-> cons[k].nS])])
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, nSubs, rg, usr, ovars, bvars>>

\* ... or the message is lost on the way (slow consumer drop etc.)
Lose(i) ==
    LET k == subs[i].cons IN
    /\ gaps < MaxGaps
    /\ conn = "up" /\ Subscribed(i) /\ cons[k].exists /\ cons[k].nS <= N
    /\ cons' = [cons EXCEPT ![k].nD = @ + 1, ![k].nS = @ + 1]
    /\ gaps' = gaps + 1
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, gvars, ovars, hbs, extDels, discs, cfails>>

\* a waiting pull request gets 409 Consumer Deleted when its consumer goes away
With409(k) ==
    [i \in 1..MaxSubs |->
        IF i <= nSubs /\ subs[i].cons = k /\ ~subs[i].unsub /\ conn = "up"
           THEN [subs[i] EXCEPT !.q = Append(@, [k |-> "409", d |-> 0, s |-> 0])]
           ELSE subs[i]]

ExtDelete ==
    /\ extDels < MaxExtDel /\ cons[curCons].exists
    /\ extDels' = extDels + 1
    /\ cons' = [cons EXCEPT ![curCons].exists = FALSE]
    /\ subs' = With409(curCons)
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, nSubs, rg, usr, ovars, gaps, hbs, discs, cfails>>

\* the DeleteConsumer goroutine of reset() (ordered.go:563-567)
DelDone(k) ==
    /\ k \in pendDel /\ conn = "up"
    /\ pendDel' = pendDel \ {k}
    /\ cons' = [cons EXCEPT ![k].exists = FALSE]
    /\ subs' = IF cons[k].exists THEN With409(k) ELSE subs
    /\ UNCHANGED <<cvars, nCons, conn, nSubs, rg, usr, ovars, bvars>>

DelFail(k) ==
    /\ k \in pendDel /\ conn /= "up"
    /\ pendDel' = pendDel \ {k}
    /\ UNCHANGED <<cvars, cons, nCons, conn, gvars, ovars, bvars>>

\* status events go to every registered listener (nats.go:6833-6850)
Broadcast(st) ==
    [i \in 1..MaxSubs |->
        IF Subscribed(i) /\ Len(subs[i].ev) < 10
           THEN [subs[i] EXCEPT !.ev = Append(@, st)] ELSE subs[i]]

Disconnect ==
    /\ conn = "up" /\ discs < MaxDisc
    /\ conn' = "down" /\ discs' = discs + 1
    /\ subs' = Broadcast("RECONNECTING")
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, rg, usr, ovars, gaps, hbs, extDels, cfails>>

Reconnect ==
    /\ conn = "down"
    /\ conn' = "up"
    /\ subs' = Broadcast("CONNECTED")
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, rg, usr, ovars, bvars>>

Close ==
    /\ AllowClose /\ conn /= "closed"
    /\ conn' = "closed"
    /\ subs' = Broadcast("CLOSED")
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, rg, usr, ovars, bvars>>

\* the heartbeat timer of sub i fires (pull.go:1106-1118)
HbFire(i) ==
    /\ i \in 1..nSubs /\ subs[i].hb /\ ~subs[i].closed /\ Len(subs[i].errs) < 10
    /\ SetD(i, [subs[i] EXCEPT !.hb = FALSE, !.errs = Append(@, "hb")])
    /\ UNCHANGED <<cvars, evars, nSubs, rg, usr, ovars>>

\* ... spuriously (budgeted), or because nothing can send heartbeats: the
\* consumer is gone or the connection is not up (forced, weakly fair)
HbFault(i) == hbs < MaxHb /\ HbFire(i) /\ hbs' = hbs + 1
              /\ UNCHANGED <<gaps, extDels, discs, cfails>>
HbForced(i) == /\ i \in 1..nSubs
               /\ (~cons[subs[i].cons].exists \/ conn /= "up")
               /\ HbFire(i) /\ UNCHANGED bvars

Env ==
    \/ \E i \in 1..nSubs : Deliver(i) \/ Lose(i) \/ HbFault(i) \/ HbForced(i)
    \/ ExtDelete \/ Disconnect \/ Reconnect \/ Close
    \/ \E k \in 1..nCons : DelDone(k) \/ DelFail(k)

Next ==
    \/ \E i \in 1..nSubs : Dispatcher(i) \/ Status(i) \/ Cleanup(i)
    \/ ResetG \/ User \/ Env

Fairness ==
    /\ \A i \in 1..MaxSubs :
          /\ WF_vars(Dispatcher(i)) /\ WF_vars(Status(i)) /\ WF_vars(Cleanup(i))
          /\ WF_vars(Deliver(i)) /\ WF_vars(HbForced(i))
    /\ WF_vars(ResetG) /\ WF_vars(USt1) /\ WF_vars(USt2) /\ WF_vars(Reconnect)
    /\ \A k \in 1..MaxCons : WF_vars(DelDone(k)) /\ WF_vars(DelFail(k))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ===========================================================================
\* Properties
\* ===========================================================================
\* the subscription table was large enough (a modeling bound, not a property)
BoundOK == rg.pc = "cons" => (SA > 0 /\ rg.isa <= 0) \/ nSubs < MaxSubs

\* The user sees stream sequences 1, 2, 3, ... with no gap and no duplicate.
InOrder == \A j \in 1..Len(obs) : obs[j] = j

\* Weaker: no message is handed over twice, and every message before one
\* that was handed over has been handed over too or is about to be (its
\* dispatcher advanced the cursor and is between c.Unlock and handler(msg)).
Range(f) == {f[j] : j \in DOMAIN f}
InHand == {subs[i].dmsg.s : i \in {i \in 1..nSubs : subs[i].dpc = "hrun"}}
NoDupNoGap == /\ \A j1, j2 \in 1..Len(obs) : j1 /= j2 => obs[j1] /= obs[j2]
              /\ \A j \in 1..Len(obs) : \A m \in 1..obs[j] : m \in Range(obs) \cup InHand

\* The handler is never running for two messages at once.
SerialHandler == Cardinality(active) <= 1

\* StopAfter delivers at most StopAfter messages.
StopAfterBound == SA > 0 => Len(obs) <= SA

\* No goroutine blocks forever acquiring c.Mutex while holding it.
NoSelfDeadlock ==
    /\ rg.pc /= "dead"
    /\ \A i \in 1..nSubs : subs[i].dpc /= "dead" /\ subs[i].spc /= "dead"

\* The user's error handler never gets ErrInvalidOption (with a nil
\* ConsumeContext) from an inner Consume the ordered consumer set up itself.
NoSpuriousInvalid == "invalid" \notin userErrs

\* No inner subscription created after the user's Stop() returned ever
\* hands a message to the handler.
NoResurrectedDelivery == ~lateNew

\* Strict form: no message that the subscription dequeues after Stop()
\* returned reaches the handler.
NoDeliveryAfterStop == ~late

\* At most one inner subscription is live.
AtMostOneLive == Cardinality({i \in 1..nSubs : ~subs[i].closed}) <= 1

\* ---- liveness
\* Without user Stop and with the connection eventually up, every message
\* (or StopAfter of them) reaches the handler.
AllDelivered == <>(Len(obs) = Target)

\* StopAfter: the ordered subscription closes itself after the last message.
StopAfterCloses == (SA > 0 /\ SA <= N) => <>odone

\* A closed connection ends the ordered consume (cf. #1885).
CloseEndsConsume == (conn = "closed") ~> odone

\* Stop() returns.
StopReturns == (usr = "st1") ~> (usr = "ret")

\* Once the ordered subscription is done every inner subscription is
\* eventually unsubscribed (no leaked subscription keeps delivering).
NoLeakedSub == odone ~> (\A i \in 1..nSubs : subs[i].unsub)
\* compact trace view (ALIAS in the cfg files)
SubView(i) == [d |-> subs[i].dpc, s |-> subs[i].spc, ser |-> subs[i].ser,
               cons |-> subs[i].cons, closed |-> subs[i].closed, unsub |-> subs[i].unsub,
               q |-> subs[i].q, errs |-> subs[i].errs, ev |-> subs[i].ev, hb |-> subs[i].hb]
Alias == [lock |-> lock, serial |-> serial, cur |-> <<curD, curS>>, delivered |-> delivered,
          rip |-> rip, doReset |-> doReset, oclosed |-> oclosed, odone |-> odone,
          curSub |-> curSub, curCons |-> curCons, conn |-> conn,
          cons |-> [k \in 1..nCons |-> cons[k]], pendDel |-> pendDel,
          subs |-> [i \in 1..nSubs |-> SubView(i)], rg |-> rg, usr |-> usr,
          obs |-> obs, active |-> active, stopRet |-> stopRet, late |-> late,
          lateNew |-> lateNew, userErrs |-> userErrs]
=============================================================================
