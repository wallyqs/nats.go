---------------------------- MODULE OrderedMessages ----------------------------
(***************************************************************************)
(* The ordered consumer's Messages path: orderedSubscription.Next,        *)
(* resetMessages and Stop (jetstream/ordered.go:241-368) over the inner    *)
(* pull iterator (jetstream/pull.go:511-746, 1074-1150).                   *)
(*                                                                         *)
(* Goroutines: the user's Next loop (nx), the user's Stop (usr), for each  *)
(* inner iterator its status goroutine (pull.go:572-609) and its           *)
(* pullMessages/cleanup (pull.go:1074-1150), and the per-Next heartbeat    *)
(* timer (pull.go:659-664, 1106-1118). The environment is the same as in  *)
(* OrderedConsumer.tla plus pull expiry (408) and "silent" consumer loss   *)
(* (deleted while no pull request waits: no 409 is sent, e.g. the server  *)
(* removed it after InactiveThreshold).                                    *)
(*                                                                         *)
(* Fixes (fix/ordered) are switched on by overriding the definition of    *)
(* Fixes in a cfg (CONSTANT Fixes <- AllFixes); left alone, the spec is    *)
(* the code at 31a3ee4, which the *_bug.cfg reproducers check.            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    N,              \* messages in the stream
    SA,             \* StopAfter, 0 = not set
    MaxSubs,        \* inner iterator table size (checked by BoundOK)
    MaxGaps, MaxHb, MaxExtDel, MaxSilent, MaxDisc,
    AllowClose,     \* the connection may close for good
    DownForever,    \* the connection may stay down (no fairness on Reconnect)
    MaxReset,       \* MaxResetAttempts, 0 = unlimited
    UserStops,      \* another goroutine calls Stop() on the iterator
    AllowExpire,    \* pull requests may expire (408)
    Deadline,       \* Next is called with NextMaxWait / NextContext
    ShortDeadline   \* ... shorter than two heartbeats (the per-call timer never fires)

ASSUME Deadline \/ ~ShortDeadline

Fixes == {}                    \* the code at 31a3ee4
AllFixes == {"ORD-5", "ORD-7", "ORD-8"}
\* ORD-5: resetMessages does not install the new iterator once closed; it
\*        stops it and Next returns ErrMsgIteratorClosed.
\* ORD-7: a closed connection ends reset() (and a failing inner Messages)
\*        with ErrConnectionClosed: Next stops the iterator and returns.
\* ORD-8: the call's deadline bounds reset(): it returns when the deadline
\*        passed, leaving the reset pending for the next call (nx.pend), and
\*        it is not restarted for the inner Next calls after a reset.
Fixed(id) == id \in Fixes

NullSub == [cons |-> 0, ser |-> 0, lim |-> 0, idel |-> 0, closed |-> TRUE,
            unsub |-> TRUE, ch |-> <<>>, errs |-> <<>>, ev |-> <<>>,
            st |-> "exit", isConn |-> TRUE, pull |-> FALSE, born |-> FALSE]

VARIABLES
    lock, serial, curD, curS, delivered, oclosed, odone, curSub, curCons,
    cons, nCons, pendDel, conn,
    subs, nSubs,
    nx,       \* Next goroutine: [pc, item, err, hb (per-call timer armed), tmo (deadline passed), after (call began after Stop returned), att, nw,
              \*                  pend (a reset interrupted by the deadline, ORD-8 fix)]
    usr,
    obs,      \* stream seqs returned by Next
    stopRet,  \* Stop() returned
    lateMsg,  \* a Next call that began after Stop returned returned a message
    errsRet,  \* errors returned by Next
    gaps, hbs, extDels, silents, discs

cvars == <<lock, serial, curD, curS, delivered, oclosed, odone, curSub, curCons>>
evars == <<cons, nCons, pendDel, conn>>
ovars == <<obs, stopRet, lateMsg, errsRet>>
bvars == <<gaps, hbs, extDels, silents, discs>>
vars == <<cvars, evars, subs, nSubs, nx, usr, ovars, bvars>>

MaxCons == MaxSubs + 1
Free == lock = "none"
Target == IF SA > 0 /\ SA < N THEN SA ELSE N

NewSub(k, ser, lim, born) ==
    [cons |-> k, ser |-> ser, lim |-> lim, idel |-> 0, closed |-> FALSE,
     unsub |-> FALSE, ch |-> <<>>, errs |-> <<>>, ev |-> <<>>, st |-> "loop",
     isConn |-> TRUE, pull |-> FALSE, born |-> born]

StopInner(sf, i) == IF i = 0 THEN sf ELSE [sf EXCEPT ![i].closed = TRUE]

Init ==
    \* OrderedConsumer() made consumer 1; Messages() opens inner iterator 1
    \* with StopAfter(SA) (ordered.go:241-278). No pull is issued until Next
    \* (pull.go:570, checkPending at 672).
    /\ lock = "none" /\ serial = 1 /\ curD = 0 /\ curS = 0 /\ delivered = 0
    /\ oclosed = FALSE /\ odone = FALSE /\ curSub = 1 /\ curCons = 1
    /\ cons = [k \in 1..MaxCons |-> [exists |-> k = 1, nD |-> 1, nS |-> 1]]
    /\ nCons = 1 /\ pendDel = {} /\ conn = "up"
    /\ subs = [i \in 1..MaxSubs |-> IF i = 1 THEN NewSub(1, 1, SA, FALSE) ELSE NullSub]
    /\ nSubs = 1
    /\ nx = [pc |-> "idle", item |-> [k |-> "none", d |-> 0, s |-> 0, ser |-> 0],
             err |-> "none", hb |-> FALSE, tmo |-> FALSE, after |-> FALSE, att |-> 0, nw |-> 0,
             pend |-> FALSE]
    /\ usr = "idle"
    /\ obs = <<>> /\ stopRet = FALSE /\ lateMsg = FALSE /\ errsRet = {}
    /\ gaps = 0 /\ hbs = 0 /\ extDels = 0 /\ silents = 0 /\ discs = 0

SetNx(f) == nx' = f
Ret(err) ==   \* Next returns an error
    /\ SetNx([nx EXCEPT !.pc = "idle", !.hb = FALSE, !.tmo = FALSE])
    /\ errsRet' = errsRet \cup {err}

\* ===========================================================================
\* The user's Next loop
\* ===========================================================================
NCall ==   \* the user calls Next (again); the call's timer starts (pull.go:659)
    /\ nx.pc = "idle" /\ Len(obs) < N
    /\ SetNx([nx EXCEPT !.pc = IF nx.pend THEN "reset" ELSE "inner",
                        !.hb = ~ShortDeadline, !.after = stopRet])
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, ovars, bvars>>

\* inner pullSubscription.Next on c.currentSub (ordered.go:282; pull.go:648-746)
NInner ==
    LET i == curSub  s == subs[i] IN
    /\ nx.pc = "inner"
    /\ \/ \* closed and not draining (pull.go:651-658), or StopAfter reached (666-669)
          /\ s.closed \/ (s.lim > 0 /\ s.idel >= s.lim)
          /\ subs' = StopInner(subs, i)
          /\ SetNx([nx EXCEPT !.pc = "cst1", !.err = "closed"])   \* ordered.go:295-298
          /\ UNCHANGED errsRet
       \/ /\ ~s.closed /\ ~(s.lim > 0 /\ s.idel >= s.lim)
          /\ ~s.pull                                     \* checkPending issues a pull (672)
          /\ subs' = [subs EXCEPT ![i].pull = TRUE,
                       ![i].ch = IF ~cons[s.cons].exists /\ conn = "up" /\ ~s.unsub
                                    THEN Append(@, [k |-> "503", d |-> 0, s |-> 0, ser |-> 0]) ELSE @]
          /\ UNCHANGED <<nx, errsRet>>
       \/ /\ ~s.closed /\ ~(s.lim > 0 /\ s.idel >= s.lim) /\ s.pull
          /\ s.ch /= <<>>
          /\ LET it == Head(s.ch) IN
             CASE it.k = "msg" ->                         \* pull.go:704-709
                    /\ subs' = [subs EXCEPT ![i].ch = Tail(@), ![i].idel = @ + 1]
                    /\ SetNx([nx EXCEPT !.pc = "meta", !.item = it, !.hb = nx.hb \/ FALSE])
                    /\ UNCHANGED errsRet
               [] it.k = "409" ->                         \* termErr (pull.go:698-700)
                    /\ subs' = [subs EXCEPT ![i].ch = Tail(@), ![i].closed = TRUE]
                    /\ SetNx([nx EXCEPT !.pc = "reset", !.err = "deleted"])
                    /\ UNCHANGED errsRet
               [] it.k = "503" ->                         \* swallowed (pull.go:697-702)
                    /\ subs' = [subs EXCEPT ![i].ch = Tail(@)]
                    /\ UNCHANGED <<nx, errsRet>>
               [] it.k = "408" ->                         \* pending reset -> re-pull
                    /\ subs' = [subs EXCEPT ![i].ch = Tail(@), ![i].pull = FALSE]
                    /\ UNCHANGED <<nx, errsRet>>
       \/ /\ ~s.closed /\ s.ch = <<>> /\ s.unsub         \* channel closed (pull.go:675-687)
          /\ subs' = StopInner(subs, i)
          /\ SetNx([nx EXCEPT !.pc = "cst1", !.err = "closed"])
          /\ UNCHANGED errsRet
       \/ /\ ~s.closed /\ s.errs /= <<>>                \* pull.go:710-739
          /\ LET e == Head(s.errs) IN
             /\ subs' = [subs EXCEPT ![i].errs = Tail(@),
                                     ![i].pull = IF e \in {"hb"} THEN FALSE ELSE @]
             /\ CASE e = "hb" -> SetNx([nx EXCEPT !.pc = "reset", !.err = "hb"])
                  [] e = "connected" -> SetNx([nx EXCEPT !.pc = "reset", !.err = "connected"])
                  [] e = "disconnected" -> SetNx([nx EXCEPT !.hb = FALSE])
             /\ UNCHANGED errsRet
    /\ UNCHANGED <<cvars, evars, nSubs, usr, obs, stopRet, lateMsg, bvars>>

\* the deadline case of the same select (pull.go:740-743); Go picks among
\* ready cases at random, hence its own fairness condition below
NTimeout ==
    /\ nx.pc = "inner" /\ nx.tmo
    /\ Ret("timeout")
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, obs, stopRet, lateMsg, bvars>>

\* ordered.go:305-323 (Next reads serial and cursor without c.Mutex; it is
\* their only writer besides reset(), which it calls itself)
NMeta ==
    LET m == nx.item IN
    /\ nx.pc = "meta"
    /\ IF m.ser /= serial THEN    \* `continue`: new inner Next call
          SetNx([nx EXCEPT !.pc = "inner", !.hb = ~ShortDeadline, !.tmo = Fixed("ORD-8") /\ nx.tmo])
          /\ UNCHANGED <<curD, curS, delivered, ovars>>
       ELSE IF m.d /= curD + 1 THEN
          SetNx([nx EXCEPT !.pc = "reset", !.err = "mismatch"]) /\ UNCHANGED <<curD, curS, delivered, ovars>>
       ELSE
          /\ curD' = m.d /\ curS' = m.s /\ delivered' = delivered + 1
          /\ obs' = Append(obs, m.s)
          /\ lateMsg' = (lateMsg \/ nx.after)
          /\ SetNx([nx EXCEPT !.pc = "idle", !.hb = FALSE, !.tmo = FALSE])
          /\ UNCHANGED <<stopRet, errsRet>>
    /\ UNCHANGED <<lock, serial, oclosed, odone, curSub, curCons, evars, subs, nSubs, usr, bvars>>

\* ordered Stop() called by Next on ErrMsgIteratorClosed (ordered.go:295-298)
NCst1 ==
    /\ nx.pc = "cst1"
    /\ IF oclosed THEN Ret("closed") /\ UNCHANGED oclosed
       ELSE oclosed' = TRUE /\ SetNx([nx EXCEPT !.pc = "cst2"]) /\ UNCHANGED errsRet
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, odone, curSub, curCons,
                   evars, subs, nSubs, usr, obs, stopRet, lateMsg, bvars>>
NCst2 ==
    /\ nx.pc = "cst2" /\ Free
    /\ odone' = TRUE /\ subs' = StopInner(subs, curSub)
    /\ Ret("closed")
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, oclosed, curSub, curCons,
                   evars, nSubs, usr, obs, stopRet, lateMsg, bvars>>

\* resetMessages (ordered.go:331-356) and reset() (ordered.go:552-601)
NReset ==
    /\ nx.pc = "reset"
    /\ IF SA > 0 /\ SA - delivered <= 0
          THEN SetNx([nx EXCEPT !.pc = "cst1"])            \* ordered.go:334-339
          ELSE SetNx([nx EXCEPT !.pc = "r1"])
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, ovars, bvars>>
\* A reset retried after the deadline interrupted it (nx.pend, ORD-8 fix)
\* reuses the serial: the code takes the next one, but the interrupted
\* reset created no consumer here (creates are atomic in NR3), so the label
\* is unused, and reusing it keeps the state space finite when the
\* connection stays down and every Next call times out.
NR1 ==
    /\ nx.pc = "r1" /\ Free
    /\ lock' = "nx"
    /\ subs' = StopInner(subs, curSub)
    /\ pendDel' = pendDel \cup {curCons}
    /\ curD' = 0 /\ serial' = IF nx.pend THEN serial ELSE serial + 1
    /\ SetNx([nx EXCEPT !.pc = "r2", !.att = 0])
    /\ UNCHANGED <<curS, delivered, oclosed, odone, curSub, curCons, cons, nCons, conn,
                   nSubs, usr, ovars, bvars>>
NR2 ==
    /\ nx.pc = "r2"
    /\ IF oclosed
          THEN lock' = "none" /\ Ret("closed")              \* ordered.go:343-345
       ELSE IF Fixed("ORD-7") /\ conn = "closed"            \* ErrConnectionClosed: Stop, return
          THEN lock' = "none" /\ SetNx([nx EXCEPT !.pc = "cst1", !.err = "closed"])
               /\ UNCHANGED errsRet
       ELSE IF Fixed("ORD-8") /\ nx.tmo                     \* ctx done: return, reset pending
          THEN /\ lock' = "none"
               /\ SetNx([nx EXCEPT !.pc = "idle", !.hb = FALSE, !.tmo = FALSE, !.pend = TRUE])
               /\ errsRet' = errsRet \cup {"timeout"}
          ELSE UNCHANGED <<lock, errsRet>> /\ SetNx([nx EXCEPT !.pc = "r3"])
    /\ UNCHANGED <<serial, curD, curS, delivered, oclosed, odone, curSub, curCons,
                   evars, subs, nSubs, usr, obs, stopRet, lateMsg, bvars>>
NR3 ==
    /\ nx.pc = "r3"
    /\ \/ /\ conn = "up" /\ nCons < MaxCons
          /\ nCons' = nCons + 1
          /\ cons' = [cons EXCEPT ![nCons + 1] = [exists |-> TRUE, nD |-> 1, nS |-> curS + 1]]
          /\ curCons' = nCons + 1 /\ lock' = "none"
          /\ SetNx([nx EXCEPT !.pc = "msgs", !.pend = FALSE])
          /\ UNCHANGED errsRet
       \/ /\ conn /= "up"
          /\ UNCHANGED <<cons, nCons, curCons>>
          /\ IF Fixed("ORD-7") /\ conn = "closed"
                THEN lock' = "none" /\ SetNx([nx EXCEPT !.pc = "cst1", !.err = "closed"])
                     /\ UNCHANGED errsRet
             ELSE IF MaxReset > 0 /\ nx.att + 1 >= MaxReset
                THEN lock' = "none" /\ Ret("createfail")
                ELSE UNCHANGED <<lock, errsRet>>
                     /\ SetNx([nx EXCEPT !.pc = "r2", !.att = IF MaxReset > 0 THEN @ + 1 ELSE 0])
    /\ UNCHANGED <<serial, curD, curS, delivered, oclosed, odone, curSub,
                   pendDel, conn, subs, nSubs, usr, obs, stopRet, lateMsg, bvars>>
NMsgs ==   \* currentConsumer.Messages(opts...) (ordered.go:348), no c.Mutex, no closed check
    /\ nx.pc = "msgs" /\ conn = "closed"     \* ChanSubscribe fails: Next returns the error
    /\ IF Fixed("ORD-7")                     \* ... after stopping the iterator
          THEN SetNx([nx EXCEPT !.pc = "cst1", !.err = "closed"]) /\ UNCHANGED errsRet
          ELSE Ret("connclosed")
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, obs, stopRet, lateMsg, bvars>>
NMsgsOK ==
    /\ nx.pc = "msgs" /\ conn /= "closed" /\ nSubs < MaxSubs
    /\ nSubs' = nSubs + 1
    /\ subs' = [subs EXCEPT ![nSubs + 1] = NewSub(curCons, serial,
                                 IF SA > 0 THEN SA - delivered ELSE 0, stopRet)]
    /\ SetNx([nx EXCEPT !.pc = "asg", !.nw = nSubs + 1])
    /\ UNCHANGED <<cvars, evars, usr, ovars, bvars>>
NAsg ==    \* ordered.go:352-354, then `continue`: a new inner Next call, with a new
           \* heartbeat timer and a new NextMaxWait timer (pull.go:635-664)
    /\ nx.pc = "asg" /\ Free
    /\ IF Fixed("ORD-5") /\ oclosed
          THEN \* cc.Stop(); return ErrMsgIteratorClosed
               /\ subs' = StopInner(subs, nx.nw) /\ UNCHANGED curSub
               /\ Ret("closed")
          ELSE /\ curSub' = nx.nw /\ UNCHANGED <<subs, errsRet>>
               /\ SetNx([nx EXCEPT !.pc = "inner", !.hb = ~ShortDeadline,
                                   !.tmo = Fixed("ORD-8") /\ nx.tmo])
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, oclosed, odone, curCons,
                   evars, nSubs, usr, obs, stopRet, lateMsg, bvars>>

NextG == NCall \/ NInner \/ NTimeout \/ NMeta \/ NCst1 \/ NCst2 \/ NReset \/ NR1 \/ NR2 \/ NR3
         \/ NMsgs \/ NMsgsOK \/ NAsg

\* ===========================================================================
\* Stop() from another goroutine (ordered.go:358-368)
\* ===========================================================================
UStart == UserStops /\ usr = "idle" /\ usr' = "st1"
          /\ UNCHANGED <<cvars, evars, subs, nSubs, nx, ovars, bvars>>
USt1 ==
    /\ usr = "st1"
    /\ IF oclosed THEN usr' = "ret" /\ stopRet' = TRUE /\ UNCHANGED oclosed
       ELSE usr' = "st2" /\ oclosed' = TRUE /\ UNCHANGED stopRet
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, odone, curSub, curCons,
                   evars, subs, nSubs, nx, obs, lateMsg, errsRet, bvars>>
USt2 ==
    /\ usr = "st2" /\ Free
    /\ subs' = StopInner(subs, curSub) /\ odone' = TRUE
    /\ usr' = "ret" /\ stopRet' = TRUE
    /\ UNCHANGED <<lock, serial, curD, curS, delivered, oclosed, curSub, curCons,
                   evars, nSubs, nx, obs, lateMsg, errsRet, bvars>>
User == UStart \/ USt1 \/ USt2

\* ===========================================================================
\* Inner iterator goroutines
\* ===========================================================================
\* status goroutine: RECONNECTING -> errDisconnected, CONNECTED after it ->
\* errConnected (pull.go:582-604); exits on done
SEv(i) ==
    LET s == subs[i]  e == Head(s.ev) IN
    /\ s.st = "loop" /\ s.ev /= <<>> /\ ~s.closed
    /\ subs' = [subs EXCEPT ![i].ev = Tail(@),
                 ![i].isConn = IF e = "RECONNECTING" THEN FALSE ELSE TRUE,
                 ![i].errs = CASE e = "RECONNECTING" -> Append(@, "disconnected")
                               [] e = "CONNECTED" /\ ~s.isConn -> Append(@, "connected")
                               [] OTHER -> @]
    /\ UNCHANGED <<cvars, evars, nSubs, nx, usr, ovars, bvars>>
SExit(i) ==
    /\ subs[i].st = "loop" /\ subs[i].closed
    /\ subs' = [subs EXCEPT ![i].st = "exit"]
    /\ UNCHANGED <<cvars, evars, nSubs, nx, usr, ovars, bvars>>
\* pullMessages cleanup: unsubscribe; the closed handler closes the channel
\* (pull.go:554-565, 1093-1150)
Cleanup(i) ==
    /\ subs[i].closed /\ ~subs[i].unsub
    /\ subs' = [subs EXCEPT ![i].unsub = TRUE]
    /\ UNCHANGED <<cvars, evars, nSubs, nx, usr, ovars, bvars>>

\* ===========================================================================
\* Environment
\* ===========================================================================
Subscribed(i) == i \in 1..nSubs /\ ~subs[i].unsub
CanDeliver(i) ==
    LET k == subs[i].cons IN
    conn = "up" /\ Subscribed(i) /\ subs[i].pull /\ cons[k].exists /\ cons[k].nS <= N

Deliver(i) ==
    LET k == subs[i].cons IN
    /\ CanDeliver(i) /\ Len(subs[i].ch) < 2
    /\ cons' = [cons EXCEPT ![k].nD = @ + 1, ![k].nS = @ + 1]
    /\ subs' = [subs EXCEPT ![i].ch = Append(@, [k |-> "msg", d |-> cons[k].nD,
                                                 s |-> cons[k].nS, ser |-> k])]
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, nSubs, nx, usr, ovars, bvars>>
Lose(i) ==
    LET k == subs[i].cons IN
    /\ gaps < MaxGaps /\ CanDeliver(i)
    /\ cons' = [cons EXCEPT ![k].nD = @ + 1, ![k].nS = @ + 1]
    /\ gaps' = gaps + 1
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, subs, nSubs, nx, usr, ovars, hbs, extDels, silents, discs>>
\* the pull request expires (408) while nothing is delivered
Expire(i) ==
    /\ AllowExpire /\ conn = "up" /\ Subscribed(i) /\ subs[i].pull /\ Len(subs[i].ch) < 2
    /\ subs' = [subs EXCEPT ![i].pull = FALSE,
                            ![i].ch = Append(@, [k |-> "408", d |-> 0, s |-> 0, ser |-> 0])]
    /\ UNCHANGED <<cvars, evars, nSubs, nx, usr, ovars, bvars>>

With409(k) ==
    [i \in 1..MaxSubs |->
        IF i <= nSubs /\ subs[i].cons = k /\ ~subs[i].unsub /\ subs[i].pull /\ conn = "up"
           THEN [subs[i] EXCEPT !.ch = Append(@, [k |-> "409", d |-> 0, s |-> 0, ser |-> 0]),
                                !.pull = FALSE]
           ELSE subs[i]]
ExtDelete ==
    /\ extDels < MaxExtDel /\ cons[curCons].exists
    /\ extDels' = extDels + 1
    /\ cons' = [cons EXCEPT ![curCons].exists = FALSE]
    /\ subs' = With409(curCons)
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, nSubs, nx, usr, ovars, gaps, hbs, silents, discs>>
\* removed while no pull request waits (InactiveThreshold): nobody is told
SilentLoss ==
    /\ silents < MaxSilent /\ cons[curCons].exists
    /\ \A i \in 1..nSubs : subs[i].cons = curCons => ~subs[i].pull \/ subs[i].unsub
    /\ silents' = silents + 1
    /\ cons' = [cons EXCEPT ![curCons].exists = FALSE]
    /\ UNCHANGED <<cvars, nCons, pendDel, conn, subs, nSubs, nx, usr, ovars, gaps, hbs, extDels, discs>>
DelDone(k) ==
    /\ k \in pendDel /\ conn = "up"
    /\ pendDel' = pendDel \ {k}
    /\ cons' = [cons EXCEPT ![k].exists = FALSE]
    /\ subs' = With409(k)
    /\ UNCHANGED <<cvars, nCons, conn, nSubs, nx, usr, ovars, bvars>>
DelFail(k) ==
    /\ k \in pendDel /\ conn /= "up"
    /\ pendDel' = pendDel \ {k}
    /\ UNCHANGED <<cvars, cons, nCons, conn, subs, nSubs, nx, usr, ovars, bvars>>

Broadcast(st) ==
    [i \in 1..MaxSubs |-> IF Subscribed(i) /\ Len(subs[i].ev) < 10
                             THEN [subs[i] EXCEPT !.ev = Append(@, st)] ELSE subs[i]]
Disconnect ==
    /\ conn = "up" /\ discs < MaxDisc
    /\ conn' = "down" /\ discs' = discs + 1 /\ subs' = Broadcast("RECONNECTING")
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, nx, usr, ovars, gaps, hbs, extDels, silents>>
Reconnect ==
    /\ conn = "down"
    /\ conn' = "up" /\ subs' = Broadcast("CONNECTED")
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, nx, usr, ovars, bvars>>
\* Messages registers for CONNECTED and RECONNECTING only (pull.go:546);
\* a closed connection closes the subscriptions (nats.go close()), so the
\* inner channel is closed and Next sees ErrMsgIteratorClosed.
Close ==
    /\ AllowClose /\ conn /= "closed"
    /\ conn' = "closed"
    /\ subs' = [i \in 1..MaxSubs |-> IF i <= nSubs THEN [subs[i] EXCEPT !.unsub = TRUE] ELSE subs[i]]
    /\ UNCHANGED <<cvars, cons, nCons, pendDel, nSubs, nx, usr, ovars, bvars>>

\* the per-call heartbeat timer fires (pull.go:1106-1118)
HbFire ==
    /\ nx.hb /\ nx.pc \in {"inner", "meta"} /\ ~subs[curSub].closed
    /\ Len(subs[curSub].errs) < 10
    /\ subs' = [subs EXCEPT ![curSub].errs = Append(@, "hb")]
    /\ SetNx([nx EXCEPT !.hb = FALSE])
    /\ UNCHANGED <<cvars, evars, nSubs, usr, ovars>>
HbFault == hbs < MaxHb /\ HbFire /\ hbs' = hbs + 1 /\ UNCHANGED <<gaps, extDels, silents, discs>>
\* nothing sends heartbeats: the consumer is gone or the connection is not
\* up. (A pull not issued yet is not a cause: Next issues it at once, long
\* before two heartbeat intervals pass.)
HbForced ==
    /\ (~cons[subs[curSub].cons].exists \/ conn /= "up")
    /\ HbFire /\ UNCHANGED bvars
\* the deadline of the current Next call passes
Deadline1 ==
    /\ Deadline /\ nx.pc \notin {"idle"} /\ ~nx.tmo
    /\ SetNx([nx EXCEPT !.tmo = TRUE])
    /\ UNCHANGED <<cvars, evars, subs, nSubs, usr, ovars, bvars>>

Env ==
    \/ \E i \in 1..nSubs : Deliver(i) \/ Lose(i) \/ Expire(i)
    \/ ExtDelete \/ SilentLoss \/ Disconnect \/ Reconnect \/ Close
    \/ \E k \in 1..nCons : DelDone(k) \/ DelFail(k)
    \/ HbFault \/ HbForced \/ Deadline1

Next ==
    \/ NextG \/ User \/ Env
    \/ \E i \in 1..nSubs : SEv(i) \/ SExit(i) \/ Cleanup(i)

Fairness ==
    /\ WF_vars(NextG) /\ WF_vars(NTimeout) /\ WF_vars(USt1) /\ WF_vars(USt2)
    /\ \A i \in 1..MaxSubs : WF_vars(SEv(i)) /\ WF_vars(SExit(i)) /\ WF_vars(Cleanup(i))
                             /\ WF_vars(i \in 1..nSubs /\ Deliver(i))
    /\ \A k \in 1..MaxCons : WF_vars(DelDone(k)) /\ WF_vars(DelFail(k))
    /\ WF_vars(HbForced) /\ WF_vars(Deadline1)
    /\ ~DownForever => WF_vars(Reconnect)

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---------------------------------------------------------------------------
BoundOK == nx.pc = "msgs" /\ conn /= "closed" => nSubs < MaxSubs
InOrder == \A j \in 1..Len(obs) : obs[j] = j
StopAfterBound == SA > 0 => Len(obs) <= SA
\* "Calling Next after calling Stop will return ErrMsgIteratorClosed"
\* (pull.go:43-45)
NoMsgAfterStop == ~lateMsg
AtMostOneLive == Cardinality({i \in 1..nSubs : ~subs[i].closed}) <= 1

\* the user, calling Next in a loop, eventually gets every message
AllDelivered == <>(Len(obs) = Target)
\* a Next call whose deadline passed returns
DeadlineHonored == nx.tmo ~> (nx.pc = "idle")
\* a closed connection ends the iteration: Next returns
CloseEndsNext == (conn = "closed" /\ nx.pc /= "idle") ~> (nx.pc = "idle")
NoLeakedSub == odone ~> (\A i \in 1..nSubs : subs[i].closed)

Alias == [lock |-> lock, serial |-> serial, cur |-> <<curD, curS>>, delivered |-> delivered,
          oclosed |-> oclosed, odone |-> odone, curSub |-> curSub, curCons |-> curCons,
          conn |-> conn, cons |-> [k \in 1..nCons |-> cons[k]], pendDel |-> pendDel,
          subs |-> [i \in 1..nSubs |-> subs[i]], nx |-> nx, usr |-> usr, obs |-> obs,
          stopRet |-> stopRet, lateMsg |-> lateMsg, errsRet |-> errsRet]
=============================================================================
