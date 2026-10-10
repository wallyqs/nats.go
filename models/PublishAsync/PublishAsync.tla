---------------------------- MODULE PublishAsync ----------------------------
(***************************************************************************)
(* JetStream asynchronous publishing in nats.go (jetstream/publish.go and  *)
(* the publisher half of jetstream/jetstream.go), at the granularity of    *)
(* goroutines and the publisher mutex.                                     *)
(*                                                                         *)
(* Goroutines (one pc each):                                               *)
(*   <<"pub", c>>  the user goroutine running PublishMsgAsync for call c   *)
(*   <<"tmo", c>>  the ack-timeout time.AfterFunc of c's future            *)
(*   <<"rty", c>>  the 503 retry time.AfterFunc of c's future              *)
(*   <<"disp", 0>> the reply subscription's callback (handleAsyncReply)    *)
(*   <<"rst", l>>  resetPendingAcksOnReconnect bound to listener chan l    *)
(*   <<"cln", 0>>  a user goroutine running CleanupPublisher               *)
(* PublishAsyncComplete / Err() are atomic critical sections (user steps). *)
(*                                                                         *)
(* The FIX_* constants switch in the fixes described in README.md (all    *)
(* but FIX_StatusProbe are on branch fix/pub, FIX_StatusProbe is the       *)
(* nats.go change on fix/conn); with all of them FALSE the spec is the     *)
(* code before those fixes, which the *_bug.cfg configs keep checking.    *)
(* Line numbers refer to jetstream/publish.go unless noted (js.go = the    *)
(* legacy API, nats.go = the core connection).                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Calls,          \* PublishMsgAsync calls, one future each
    MaxPA,          \* WithPublishAsyncMaxPending
    MaxRetries,     \* WithRetryAttempts: 503 (no responders) retries
    UseTimeout,     \* WithPublishAsyncTimeout > 0
    HasErrCb,       \* WithPublishAsyncErrHandler set
    ReentrantCb,    \* the error handler calls back into the publisher
                    \* (PublishAsyncPending / re-publish, as MsgErrHandler's
                    \* doc invites: "for possible retransmitting")
    RespKinds,      \* subset of {"ok","err","nr"} the server may answer with
    MaxDisc,        \* bound on CONNECTED -> RECONNECTING transitions
    AllowClose,     \* environment may nc.Close()
    MaxCleanup,     \* bound on CleanupPublisher calls
    MaxComplete,    \* bound on PublishAsyncComplete calls
    NoReconnectBuf, \* nats.ReconnectBufSize(-1): publish fails while reconnecting
    UserErr,        \* user calls paf.Err() early (and does not read it yet)
    FIX_TimeoutCb,      \* PUB-1: timeout callback runs aecb after Unlock
    FIX_CleanupUnstall, \* PUB-2: CleanupPublisher calls maybeUnstall
    FIX_SyncFailDone,   \* PUB-3/PUB-8: clearPAF closes doneCh when pending hits 0
                        \* and a sync failure is not reported twice
    FIX_RetryOwner,     \* PUB-4: retry error path resolves only if it still owned the future
    FIX_CloseResolves,  \* PUB-5: CLOSED fails pending futures
    FIX_CleanupPrefix,  \* PUB-6: cleanup clears the prefix; registerPAF then
                        \* re-creates the reply sub
    FIX_StatusProbe     \* PUB-7: sendStatusEvent/RemoveStatusListener do not eat buffered events

None == <<"none", 0>>
LCap == 10                      \* StatusChanged buffer: make(chan Status, 10), nats.go
Lsns == 1..(1 + MaxCleanup)     \* listener channels ever created
Pub(c) == <<"pub", c>>
Tmo(c) == <<"tmo", c>>
Rty(c) == <<"rty", c>>
Rst(l) == <<"rst", l>>
Disp == <<"disp", 0>>
Cln == <<"cln", 0>>
Goroutines == {Pub(c) : c \in Calls} \cup {Tmo(c) : c \in Calls}
               \cup {Rty(c) : c \in Calls} \cup {Rst(l) : l \in Lsns}
               \cup {Disp, Cln}

VARIABLES
    pc,         \* [Goroutines -> label]
    mu,         \* holder of js.publisher (RW)Mutex, or None
    acks,       \* calls whose future is in js.publisher.acks
    pfx,        \* [Calls -> Nat] reply-prefix generation c registered under
    subGen,     \* generation of js.publisher.replySub (0 = nil)
    prefixGen,  \* generation of js.publisher.replyPrefix (0 = cleared; kept by cleanup without FIX_CleanupPrefix)
    ngen,       \* generation counter (subs)
    stallCh,    \* id of js.publisher.stallCh (0 = nil)
    stallClosed,\* stall channel ids that have been closed
    waitOn,     \* [Calls -> Nat] stall channel a stalled caller waits on
    doneCh,     \* id of js.publisher.doneCh (0 = nil)
    doneClosed, \* done channel ids that have been closed
    nids,       \* channel id counter
    dchOf,      \* [Goroutines -> Nat] done channel captured to close after Unlock
    waiters,    \* done channel ids handed out by PublishAsyncComplete
    res,        \* [Calls -> Nat] times the future was resolved (ack or err)
    aecbN,      \* [Calls -> Nat] times the error handler ran for c's msg
    syncErr,    \* [Calls -> BOOLEAN] PublishMsgAsync returned an error after registering
    errMade,    \* [Calls -> BOOLEAN] paf.errCh created (cap 1) by paf.Err()
    errBuf,     \* [Calls -> 0..1] values buffered in paf.errCh
    hasErr,     \* [Calls -> BOOLEAN] paf.err set
    retries,    \* [Calls -> Nat] paf.retries
    tmo,        \* [Calls -> {"off","armed"}] paf.timeout state
    collected,  \* [Goroutines -> SUBSET Calls] msgs to report after Unlock
    cur,        \* message being processed by the dispatcher
    conn,       \* "CONNECTED" | "RECONNECTING" | "CLOSED"
    srv,        \* published, awaiting a server response
    pend,       \* buffered in the reconnect buffer
    dq,         \* reply messages queued for handleAsyncReply
    lsn,        \* js.publisher.connStatusCh listener id (0 = nil)
    lreg,       \* [Lsns -> SUBSET {"R","C"}] statuses the chan is registered for
    lbuf,       \* [Lsns -> Seq] buffered status events
    lclosed,    \* [Lsns -> BOOLEAN]
    nlsn,       \* listener counter
    discN, cleanN, complN

vars == <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
          waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
          syncErr, errMade, errBuf, hasErr, retries, tmo, collected,
          cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
          cleanN, complN>>

-----------------------------------------------------------------------------
Free == mu = None
ErrSendOK(c) == ~errMade[c] \/ errBuf[c] = 0      \* errCh <- err would not block

\* maybeUnstall (675-680), applied to the post-delete acks set a.
Unstall(a) ==
    IF stallCh # 0 /\ Cardinality(a) < MaxPA
    THEN /\ stallClosed' = stallClosed \cup {stallCh}
         /\ stallCh' = 0
    ELSE UNCHANGED <<stallClosed, stallCh>>

\* paf.err = err; if paf.errCh != nil { paf.errCh <- paf.err }   (390-393, 562-565, 586-589)
ResolveErr(c) ==
    /\ res' = [res EXCEPT ![c] = @ + 1]
    /\ hasErr' = [hasErr EXCEPT ![c] = TRUE]
    /\ errBuf' = IF errMade[c] THEN [errBuf EXCEPT ![c] = 1] ELSE errBuf

\* nc.sendStatusEvent (nats.go): the "is it closed?" probe is a receive, so it
\* also succeeds on a buffered event, consuming it and unregistering the chan.
Probe(l) == lclosed[l] \/ (~FIX_StatusProbe /\ lbuf[l] # <<>>)
SendStatus(s) ==
    /\ lbuf' = [l \in Lsns |->
                 IF s \notin lreg[l] THEN lbuf[l]
                 ELSE IF Probe(l)
                      THEN (IF lbuf[l] # <<>> /\ ~FIX_StatusProbe THEN Tail(lbuf[l]) ELSE lbuf[l])
                      ELSE IF Len(lbuf[l]) < LCap THEN Append(lbuf[l], s) ELSE lbuf[l]]
    /\ lreg' = [l \in Lsns |-> IF s \in lreg[l] /\ Probe(l) THEN lreg[l] \ {s} ELSE lreg[l]]

\* newAsyncReply's subscription + listener setup (441-460), shared with the
\* PUB-6 fix. Produces the primed sub/listener/rst state.
SetupReply ==
    /\ IF subGen = 0
       THEN /\ ngen' = ngen + 1 /\ subGen' = ngen + 1 /\ prefixGen' = ngen + 1
       ELSE UNCHANGED <<ngen, subGen, prefixGen>>
    /\ IF lsn = 0 /\ nlsn < Cardinality(Lsns)
       THEN /\ nlsn' = nlsn + 1 /\ lsn' = nlsn + 1
            /\ lreg' = [lreg EXCEPT ![nlsn + 1] = {"R","C"}]   \* StatusChanged(RECONNECTING, CLOSED)
       ELSE UNCHANGED <<nlsn, lsn, lreg>>
\* ... and `go js.resetPendingAcksOnReconnect()` (459) starts Rst(nlsn + 1).

-----------------------------------------------------------------------------
Init ==
    /\ pc = [g \in Goroutines |->
              CASE g[1] = "pub" -> "idle" [] g[1] = "tmo" -> "t_idle"
                [] g[1] = "rty" -> "r_idle" [] g[1] = "rst" -> "s_none"
                [] g[1] = "disp" -> "d_idle" [] g[1] = "cln" -> "c_idle"]
    /\ mu = None
    /\ acks = {}
    /\ pfx = [c \in Calls |-> 0]
    /\ subGen = 0 /\ prefixGen = 0 /\ ngen = 0
    /\ stallCh = 0 /\ stallClosed = {} /\ waitOn = [c \in Calls |-> 0]
    /\ doneCh = 0 /\ doneClosed = {} /\ nids = 0
    /\ dchOf = [g \in Goroutines |-> 0]
    /\ waiters = {}
    /\ res = [c \in Calls |-> 0] /\ aecbN = [c \in Calls |-> 0]
    /\ syncErr = [c \in Calls |-> FALSE]
    /\ errMade = [c \in Calls |-> FALSE] /\ errBuf = [c \in Calls |-> 0]
    /\ hasErr = [c \in Calls |-> FALSE]
    /\ retries = [c \in Calls |-> 0] /\ tmo = [c \in Calls |-> "off"]
    /\ collected = [g \in Goroutines |-> {}]
    /\ cur = <<0, "none">>
    /\ conn = "CONNECTED" /\ srv = {} /\ pend = {} /\ dq = <<>>
    /\ lsn = 0 /\ lreg = [l \in Lsns |-> {}] /\ lbuf = [l \in Lsns |-> <<>>]
    /\ lclosed = [l \in Lsns |-> FALSE] /\ nlsn = 0
    /\ discN = 0 /\ cleanN = 0 /\ complN = 0

-----------------------------------------------------------------------------
(* PublishMsgAsync, first attempt (289-428).                               *)

\* newAsyncReply (438-462): one critical section. nc.Subscribe fails on a
\* closed connection (363).
PubStart(c) ==
    /\ pc[Pub(c)] = "idle" /\ Free
    /\ IF subGen = 0 /\ conn = "CLOSED"
       THEN /\ pc' = [pc EXCEPT ![Pub(c)] = "done_err"]
            /\ UNCHANGED <<ngen, subGen, prefixGen, nlsn, lsn, lreg>>
       ELSE /\ SetupReply
            /\ IF lsn = 0 /\ nlsn < Cardinality(Lsns)
               THEN pc' = [pc EXCEPT ![Pub(c)] = "reg", ![Rst(nlsn + 1)] = "s_wait"]
               ELSE pc' = [pc EXCEPT ![Pub(c)] = "reg"]
    /\ UNCHANGED <<mu, acks, pfx, stallCh, stallClosed, waitOn, doneCh, doneClosed,
                   nids, dchOf, waiters, res, aecbN, syncErr, errMade, errBuf,
                   hasErr, retries, tmo, collected, cur, conn, srv, pend,
                   dq, lbuf, lclosed, discN, cleanN, complN>>

\* registerPAF (613-663): stall at the cap, else draw the token and insert
\* under the same lock hold. The ack timer (372-406) is armed right after,
\* folded into this step.
PubReg(c) ==
    /\ pc[Pub(c)] = "reg" /\ Free
    /\ IF Cardinality(acks) >= MaxPA
       THEN /\ IF stallCh = 0
               THEN /\ nids' = nids + 1 /\ stallCh' = nids + 1
                    /\ waitOn' = [waitOn EXCEPT ![c] = nids + 1]
               ELSE /\ waitOn' = [waitOn EXCEPT ![c] = stallCh]
                    /\ UNCHANGED <<nids, stallCh>>
            /\ pc' = [pc EXCEPT ![Pub(c)] = "stall"]
            /\ UNCHANGED <<acks, pfx, tmo, ngen, subGen, prefixGen, nlsn, lsn, lreg>>
       ELSE IF FIX_CleanupPrefix /\ prefixGen = 0
       THEN \* fix: CleanupPublisher cleared the prefix (and removed the reply
            \* sub) since newAsyncReply: re-create both under the insert lock
            IF conn = "CLOSED"
            THEN /\ pc' = [pc EXCEPT ![Pub(c)] = "done_err"]
                 /\ UNCHANGED <<acks, pfx, tmo, nids, stallCh, waitOn, ngen,
                                subGen, prefixGen, nlsn, lsn, lreg>>
            ELSE /\ ngen' = ngen + 1 /\ subGen' = ngen + 1 /\ prefixGen' = ngen + 1
                 /\ acks' = acks \cup {c}
                 /\ pfx' = [pfx EXCEPT ![c] = ngen + 1]
                 /\ tmo' = IF UseTimeout THEN [tmo EXCEPT ![c] = "armed"] ELSE tmo
                 /\ IF lsn = 0 /\ nlsn < Cardinality(Lsns)
                    THEN /\ nlsn' = nlsn + 1 /\ lsn' = nlsn + 1
                         /\ lreg' = [lreg EXCEPT ![nlsn + 1] = {"R","C"}]
                         /\ pc' = [pc EXCEPT ![Pub(c)] = "pub", ![Rst(nlsn + 1)] = "s_wait"]
                    ELSE /\ pc' = [pc EXCEPT ![Pub(c)] = "pub"]
                         /\ UNCHANGED <<nlsn, lsn, lreg>>
                 /\ UNCHANGED <<nids, stallCh, waitOn>>
       ELSE /\ acks' = acks \cup {c}
            /\ pfx' = [pfx EXCEPT ![c] = prefixGen]     \* reply := replyPrefix + id (657)
            /\ tmo' = IF UseTimeout THEN [tmo EXCEPT ![c] = "armed"] ELSE tmo
            /\ pc' = [pc EXCEPT ![Pub(c)] = "pub"]
            /\ UNCHANGED <<nids, stallCh, waitOn, ngen, subGen, prefixGen, nlsn, lsn, lreg>>
    /\ UNCHANGED <<mu, stallClosed, doneCh, doneClosed, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, collected,
                   cur, conn, srv, pend, dq, lbuf, lclosed, discN, cleanN, complN>>

\* select { case <-stc: continue; case <-timer.C: ErrTooManyStalledMsgs } (635-640)
PubWake(c) ==
    /\ pc[Pub(c)] = "stall" /\ waitOn[c] \in stallClosed
    /\ pc' = [pc EXCEPT ![Pub(c)] = "reg"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed,
                   nlsn, discN, cleanN, complN>>
PubStallTimeout(c) ==
    /\ pc[Pub(c)] = "stall"
    /\ pc' = [pc EXCEPT ![Pub(c)] = "done_stall"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed,
                   nlsn, discN, cleanN, complN>>

\* nc.PublishMsg (422; nats.go publish): ErrConnectionClosed when closed,
\* ErrReconnectBufExceeded while reconnecting with the buffer disabled.
PubFails == conn = "CLOSED" \/ (conn = "RECONNECTING" /\ NoReconnectBuf)
Send(c) ==
    IF conn = "RECONNECTING"
    THEN /\ pend' = pend \cup {c} /\ UNCHANGED srv
    ELSE /\ srv' = srv \cup {c} /\ UNCHANGED pend

PubSend(c) ==
    /\ pc[Pub(c)] = "pub"
    /\ IF PubFails
       THEN /\ pc' = [pc EXCEPT ![Pub(c)] = "clear"]
            /\ UNCHANGED <<srv, pend>>
       ELSE /\ Send(c)
            /\ pc' = [pc EXCEPT ![Pub(c)] = "done_ok"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, dq, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* clearPAF (683-688): delete + maybeUnstall; doneCh is NOT looked at.
CloseDoneIfEmpty(a) ==
    IF doneCh # 0 /\ a = {}
    THEN /\ doneClosed' = doneClosed \cup {doneCh} /\ doneCh' = 0
    ELSE UNCHANGED <<doneClosed, doneCh>>

\* With FIX_SyncFailDone (fix/pub: lock; clearPAF; unlock; close(dch)) the
\* failure branch first checks that it still owns the future: if a
\* reconnect/timeout/cleanup already resolved (and reported) it,
\* PublishMsgAsync returns that future instead of a second, synchronous error
\* (PUB-8); otherwise clearPAF also hands back doneCh to be closed (PUB-3).
PubClear(c) ==
    /\ pc[Pub(c)] = "clear" /\ Free
    /\ IF FIX_SyncFailDone /\ c \notin acks
       THEN /\ pc' = [pc EXCEPT ![Pub(c)] = "done_ok"]
            /\ UNCHANGED <<acks, stallCh, stallClosed, doneClosed, doneCh, syncErr>>
       ELSE /\ acks' = acks \ {c}
            /\ Unstall(acks \ {c})
            /\ IF FIX_SyncFailDone THEN CloseDoneIfEmpty(acks \ {c})
                                   ELSE UNCHANGED <<doneClosed, doneCh>>
            /\ syncErr' = [syncErr EXCEPT ![c] = TRUE]
            /\ pc' = [pc EXCEPT ![Pub(c)] = "done_err"]
    /\ UNCHANGED <<mu, pfx, subGen, prefixGen, ngen, waitOn, nids, dchOf, waiters,
                   res, aecbN, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed,
                   nlsn, discN, cleanN, complN>>

-----------------------------------------------------------------------------
(* Ack timeout: time.AfterFunc(ackTimeout, ...) (373-405).                 *)

TmoFire(c) ==
    /\ tmo[c] = "armed" /\ pc[Tmo(c)] = "t_idle"
    /\ tmo' = [tmo EXCEPT ![c] = "off"]
    /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_lock"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, collected,
                   cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* Lock; if not in acks return (377-381); delete + maybeUnstall (384-387).
TmoLock(c) ==
    /\ pc[Tmo(c)] = "t_lock" /\ Free
    /\ IF c \notin acks
       THEN /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_idle"]
            /\ UNCHANGED <<mu, acks, stallCh, stallClosed>>
       ELSE /\ mu' = Tmo(c)
            /\ acks' = acks \ {c}
            /\ Unstall(acks \ {c})
            /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_send"]
    /\ UNCHANGED <<pfx, subGen, prefixGen, ngen, waitOn, doneCh, doneClosed, nids,
                   dchOf, waiters, res, aecbN, syncErr, errMade, errBuf, 
                   hasErr, retries, tmo, collected, cur, conn, srv, pend, dq, lsn,
                   lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* paf.errCh <- ErrAsyncPublishTimeout (390-393), then aecb UNDER THE LOCK
\* (396-398). A re-entrant handler blocks on the mutex its own goroutine holds.
TmoSend(c) ==
    /\ pc[Tmo(c)] = "t_send" /\ ErrSendOK(c)
    /\ ResolveErr(c)
    /\ IF HasErrCb /\ ~FIX_TimeoutCb
       THEN /\ aecbN' = [aecbN EXCEPT ![c] = @ + 1]
            /\ pc' = [pc EXCEPT ![Tmo(c)] = IF ReentrantCb THEN "t_reenter" ELSE "t_done"]
       ELSE /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_done"]
            /\ UNCHANGED aecbN
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, syncErr,
                   errMade, retries, tmo, collected, cur, conn, srv,
                   pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* "t_reenter" has no successor: Lock() on a mutex this goroutine holds.

\* close doneCh if empty (401-404) and Unlock (deferred). With FIX_TimeoutCb
\* the channel is captured, the lock dropped, aecb run, then the close.
TmoDone(c) ==
    /\ pc[Tmo(c)] = "t_done"
    /\ mu' = None
    /\ IF FIX_TimeoutCb
       THEN /\ IF doneCh # 0 /\ acks = {}
               THEN /\ dchOf' = [dchOf EXCEPT ![Tmo(c)] = doneCh] /\ doneCh' = 0
               ELSE UNCHANGED <<dchOf, doneCh>>
            /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_cb"]
            /\ UNCHANGED doneClosed
       ELSE /\ CloseDoneIfEmpty(acks)
            /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_idle"]
            /\ UNCHANGED dchOf
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   nids, waiters, res, aecbN, syncErr, errMade, errBuf, 
                   hasErr, retries, tmo, collected, cur, conn, srv, pend, dq, lsn,
                   lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

TmoCb(c) ==
    /\ pc[Tmo(c)] = "t_cb"
    /\ (ReentrantCb /\ HasErrCb) => Free
    /\ aecbN' = IF HasErrCb THEN [aecbN EXCEPT ![c] = @ + 1] ELSE aecbN
    /\ IF dchOf[Tmo(c)] # 0
       THEN /\ doneClosed' = doneClosed \cup {dchOf[Tmo(c)]}
            /\ dchOf' = [dchOf EXCEPT ![Tmo(c)] = 0]
       ELSE UNCHANGED <<doneClosed, dchOf>>
    /\ pc' = [pc EXCEPT ![Tmo(c)] = "t_idle"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, nids, waiters, res, syncErr, errMade, errBuf,
                   hasErr, retries, tmo, collected, cur, conn, srv, pend,
                   dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

-----------------------------------------------------------------------------
(* Reply subscription callback: handleAsyncReply (465-559).                *)

DispTake ==
    /\ pc[Disp] = "d_idle" /\ dq # <<>>
    /\ cur' = Head(dq) /\ dq' = Tail(dq)
    /\ pc' = [pc EXCEPT ![Disp] = "d_lock"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, conn, srv, pend, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* Lock; getPAF (473-477); timeout.Stop (496-498); 503 retry scheduling
\* (501-521) or delete + maybeUnstall + capture doneCh (523-533). A good ack
\* is resolved and the lock dropped here (550-555).
DispLock ==
    LET c == cur[1]  k == cur[2] IN
    /\ pc[Disp] = "d_lock" /\ Free
    /\ IF c \notin acks
       THEN /\ pc' = [pc EXCEPT ![Disp] = "d_idle"]
            /\ UNCHANGED <<mu, acks, stallCh, stallClosed, doneCh, dchOf, res,
                           retries, tmo>>
       ELSE /\ tmo' = [tmo EXCEPT ![c] = "off"]
            /\ IF k = "nr" /\ retries[c] < MaxRetries
               THEN /\ retries' = [retries EXCEPT ![c] = @ + 1]
                    /\ pc' = [pc EXCEPT ![Disp] = "d_idle", ![Rty(c)] = "r_sched"]
                    /\ UNCHANGED <<mu, acks, stallCh, stallClosed, doneCh, dchOf, res>>
               ELSE /\ acks' = acks \ {c}
                    /\ Unstall(acks \ {c})
                    /\ IF doneCh # 0 /\ acks \ {c} = {}
                       THEN /\ dchOf' = [dchOf EXCEPT ![Disp] = doneCh] /\ doneCh' = 0
                       ELSE UNCHANGED <<dchOf, doneCh>>
                    /\ UNCHANGED retries
                    /\ IF k = "ok"
                       THEN /\ res' = [res EXCEPT ![c] = @ + 1]
                            /\ pc' = [pc EXCEPT ![Disp] = "d_close"]
                            /\ UNCHANGED mu
                       ELSE /\ mu' = Disp
                            /\ pc' = [pc EXCEPT ![Disp] = "d_send"]
                            /\ UNCHANGED res
    /\ UNCHANGED <<pfx, subGen, prefixGen, ngen, waitOn, doneClosed, nids, waiters,
                   aecbN, syncErr, errMade, errBuf, hasErr, collected,
                   cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* handleAsyncErrAndUnlock (561-571): send, Unlock, then aecb.
DispSend ==
    LET c == cur[1] IN
    /\ pc[Disp] = "d_send" /\ ErrSendOK(c)
    /\ ResolveErr(c)
    /\ mu' = None
    /\ pc' = [pc EXCEPT ![Disp] = "d_cb"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, aecbN, syncErr,
                   errMade, retries, tmo, collected, cur, conn, srv,
                   pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

DispCb ==
    LET c == cur[1] IN
    /\ pc[Disp] = "d_cb"
    /\ (ReentrantCb /\ HasErrCb) => Free
    /\ aecbN' = IF HasErrCb THEN [aecbN EXCEPT ![c] = @ + 1] ELSE aecbN
    /\ pc' = [pc EXCEPT ![Disp] = "d_close"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, collected, cur,
                   conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
                   cleanN, complN>>

\* defer closeDchFn()() (479-494, 525/533)
DispClose ==
    /\ pc[Disp] = "d_close"
    /\ IF dchOf[Disp] # 0
       THEN /\ doneClosed' = doneClosed \cup {dchOf[Disp]}
            /\ dchOf' = [dchOf EXCEPT ![Disp] = 0]
       ELSE UNCHANGED <<doneClosed, dchOf>>
    /\ pc' = [pc EXCEPT ![Disp] = "d_idle"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, nids, waiters, res, aecbN, syncErr, errMade,
                   errBuf, hasErr, retries, tmo, collected, cur, conn,
                   srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN,
                   complN>>

-----------------------------------------------------------------------------
(* 503 retry: time.AfterFunc(paf.retryWait, ...) (504-519) re-entering     *)
(* PublishMsgAsync with pafRetry (407-414, 416-425).                       *)

RtyFire(c) ==
    /\ pc[Rty(c)] = "r_sched"
    /\ pc' = [pc EXCEPT ![Rty(c)] = "r_get"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed,
                   nlsn, discN, cleanN, complN>>

\* Lock; getPAF; Unlock (505-510); then paf.timeout.Reset (410-412).
RtyGet(c) ==
    /\ pc[Rty(c)] = "r_get" /\ Free
    /\ IF c \notin acks
       THEN /\ pc' = [pc EXCEPT ![Rty(c)] = "r_idle"] /\ UNCHANGED tmo
       ELSE /\ pc' = [pc EXCEPT ![Rty(c)] = "r_pub"]
            /\ tmo' = IF UseTimeout THEN [tmo EXCEPT ![c] = "armed"] ELSE tmo
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, collected,
                   cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* js.conn.PublishMsg (422); on error clearPAF (423) and return err.
RtyPub(c) ==
    /\ pc[Rty(c)] = "r_pub"
    /\ IF PubFails
       THEN /\ pc' = [pc EXCEPT ![Rty(c)] = "r_clear"] /\ UNCHANGED <<srv, pend>>
       ELSE /\ Send(c) /\ pc' = [pc EXCEPT ![Rty(c)] = "r_idle"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, dq, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* clearPAF(id) (683-688). With FIX_RetryOwner (fix/pub: the same failure
\* branch as PubClear, lock held throughout) the delete happens only if this
\* attempt still owns the future, and only then is the error reported
\* (handleAsyncErrAndUnlock) and doneCh closed after it.
RtyClear(c) ==
    /\ pc[Rty(c)] = "r_clear" /\ Free
    /\ IF FIX_RetryOwner
       THEN IF c \in acks
            THEN /\ acks' = acks \ {c}
                 /\ Unstall(acks \ {c})
                 /\ mu' = Rty(c)
                 /\ IF doneCh # 0 /\ acks \ {c} = {}
                    THEN /\ dchOf' = [dchOf EXCEPT ![Rty(c)] = doneCh] /\ doneCh' = 0
                    ELSE UNCHANGED <<dchOf, doneCh>>
                 /\ pc' = [pc EXCEPT ![Rty(c)] = "r_send"]
            ELSE /\ pc' = [pc EXCEPT ![Rty(c)] = "r_idle"]
                 /\ UNCHANGED <<acks, stallCh, stallClosed, mu, dchOf, doneCh>>
       ELSE /\ acks' = acks \ {c}
            /\ Unstall(acks \ {c})
            /\ pc' = [pc EXCEPT ![Rty(c)] = "r_elock"]
            /\ UNCHANGED <<mu, dchOf, doneCh>>
    /\ UNCHANGED <<pfx, subGen, prefixGen, ngen, waitOn, doneClosed, nids, waiters,
                   res, aecbN, syncErr, errMade, errBuf, hasErr, retries,
                   tmo, collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf,
                   lclosed, nlsn, discN, cleanN, complN>>

\* js.publisher.Lock(); js.handleAsyncErrAndUnlock(paf, err) (515-518):
\* unconditional, whoever else resolved the future in the meantime.
RtyELock(c) ==
    /\ pc[Rty(c)] = "r_elock" /\ Free
    /\ mu' = Rty(c)
    /\ pc' = [pc EXCEPT ![Rty(c)] = "r_send"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, res, aecbN, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, collected, cur,
                   conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
                   cleanN, complN>>

RtySend(c) ==
    /\ pc[Rty(c)] = "r_send" /\ ErrSendOK(c)
    /\ ResolveErr(c)
    /\ mu' = None
    /\ pc' = [pc EXCEPT ![Rty(c)] = "r_cb"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, aecbN, syncErr,
                   errMade, retries, tmo, collected, cur, conn, srv, pend,
                   dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

RtyCb(c) ==
    /\ pc[Rty(c)] = "r_cb"
    /\ (ReentrantCb /\ HasErrCb) => Free
    /\ aecbN' = IF HasErrCb THEN [aecbN EXCEPT ![c] = @ + 1] ELSE aecbN
    /\ IF dchOf[Rty(c)] # 0
       THEN /\ doneClosed' = doneClosed \cup {dchOf[Rty(c)]}
            /\ dchOf' = [dchOf EXCEPT ![Rty(c)] = 0]
       ELSE UNCHANGED <<doneClosed, dchOf>>
    /\ pc' = [pc EXCEPT ![Rty(c)] = "r_idle"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, nids, waiters, res, syncErr, errMade, errBuf,
                   hasErr, retries, tmo, collected, cur, conn, srv, pend,
                   dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

-----------------------------------------------------------------------------
(* resetPendingAcksOnReconnect (573-606), one per listener channel.        *)

\* newStatus, ok := <-connStatusCh; if !ok || CLOSED { return } (578-581)
RstRecv(l) ==
    /\ pc[Rst(l)] = "s_wait"
    /\ \/ /\ lbuf[l] # <<>>
          /\ lbuf' = [lbuf EXCEPT ![l] = Tail(@)]
          /\ pc' = [pc EXCEPT ![Rst(l)] =
                      IF Head(lbuf[l]) = "R" THEN "s_lock"
                      ELSE IF FIX_CloseResolves THEN "s_lockc" ELSE "s_exit"]
       \/ /\ lbuf[l] = <<>> /\ lclosed[l]
          /\ pc' = [pc EXCEPT ![Rst(l)] = "s_exit"]
          /\ UNCHANGED lbuf
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lclosed, nlsn,
                   discN, cleanN, complN>>

RstLock(l) ==
    /\ pc[Rst(l)] \in {"s_lock", "s_lockc"} /\ Free
    /\ mu' = Rst(l)
    /\ pc' = [pc EXCEPT ![Rst(l)] = IF pc[Rst(l)] = "s_lock" THEN "s_iter" ELSE "s_iterc"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, res, aecbN, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, collected, cur,
                   conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
                   cleanN, complN>>

\* for id, paf := range acks { paf.err = ErrDisconnected; errCh <- ...; delete } (585-594)
RstIter(l) ==
    /\ pc[Rst(l)] \in {"s_iter", "s_iterc"}
    /\ \E c \in acks :
         /\ ErrSendOK(c)
         /\ ResolveErr(c)
         /\ acks' = acks \ {c}
         /\ collected' = IF HasErrCb THEN [collected EXCEPT ![Rst(l)] = @ \cup {c}] ELSE collected
    /\ UNCHANGED <<pc, mu, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, aecbN, syncErr,
                   errMade, retries, tmo, cur, conn, srv, pend, dq, lsn,
                   lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* maybeUnstall; dch := doneCh; doneCh = nil; Unlock (595-598)
RstEnd(l) ==
    /\ pc[Rst(l)] \in {"s_iter", "s_iterc"} /\ acks = {}
    /\ Unstall(acks)
    /\ dchOf' = [dchOf EXCEPT ![Rst(l)] = doneCh] /\ doneCh' = 0
    /\ mu' = None
    /\ pc' = [pc EXCEPT ![Rst(l)] = IF pc[Rst(l)] = "s_iter" THEN "s_cb" ELSE "s_cbc"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, waitOn, doneClosed, nids,
                   waiters, res, aecbN, syncErr, errMade, errBuf, hasErr,
                   retries, tmo, collected, cur, conn, srv, pend, dq, lsn, lreg,
                   lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* errCb(js, msg, ErrDisconnected) for each collected msg (599-601)
RstCb(l) ==
    /\ pc[Rst(l)] \in {"s_cb", "s_cbc"} /\ collected[Rst(l)] # {}
    /\ ReentrantCb => Free
    /\ \E c \in collected[Rst(l)] :
         /\ aecbN' = [aecbN EXCEPT ![c] = @ + 1]
         /\ collected' = [collected EXCEPT ![Rst(l)] = @ \ {c}]
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, cur, conn, srv,
                   pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* close(dch) (602-604), back to the receive.
RstClose(l) ==
    /\ pc[Rst(l)] \in {"s_cb", "s_cbc"} /\ collected[Rst(l)] = {}
    /\ IF dchOf[Rst(l)] # 0
       THEN /\ doneClosed' = doneClosed \cup {dchOf[Rst(l)]}
            /\ dchOf' = [dchOf EXCEPT ![Rst(l)] = 0]
       ELSE UNCHANGED <<doneClosed, dchOf>>
    /\ pc' = [pc EXCEPT ![Rst(l)] = IF pc[Rst(l)] = "s_cb" THEN "s_wait" ELSE "s_exit"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, nids, waiters, res, aecbN, syncErr, errMade,
                   errBuf, hasErr, retries, tmo, collected, cur, conn,
                   srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN,
                   complN>>

-----------------------------------------------------------------------------
(* CleanupPublisher (jetstream.go:1295-1331).                              *)

ClnStart ==
    /\ pc[Cln] = "c_idle" /\ cleanN < MaxCleanup
    /\ cleanN' = cleanN + 1
    /\ pc' = [pc EXCEPT ![Cln] = "c_sub"]
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf, lclosed,
                   nlsn, discN, complN>>

\* cleanupReplySub (jetstream.go:1317-1331): Unsubscribe; RemoveStatusListener
\* (nats.go): select { case <-ch: default: close(ch) } -- a buffered event is
\* consumed and the channel is left open. With FIX_CleanupPrefix the prefix
\* is cleared along with the subscription.
ClnSub ==
    /\ pc[Cln] = "c_sub" /\ Free
    /\ subGen' = 0
    /\ prefixGen' = IF FIX_CleanupPrefix THEN 0 ELSE prefixGen
    /\ IF lsn # 0
       THEN /\ IF lbuf[lsn] # <<>> /\ ~FIX_StatusProbe
               THEN /\ lbuf' = [lbuf EXCEPT ![lsn] = Tail(@)] /\ UNCHANGED lclosed
               ELSE /\ lclosed' = [lclosed EXCEPT ![lsn] = TRUE] /\ UNCHANGED lbuf
            /\ lreg' = [lreg EXCEPT ![lsn] = {}]
            /\ lsn' = 0
       ELSE UNCHANGED <<lbuf, lclosed, lreg, lsn>>
    /\ pc' = [pc EXCEPT ![Cln] = "c_lock"]
    /\ UNCHANGED <<mu, acks, pfx, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, res, aecbN, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, collected, cur,
                   conn, srv, pend, dq, nlsn, discN, cleanN, complN>>

ClnLock ==
    /\ pc[Cln] = "c_lock" /\ Free
    /\ mu' = Cln
    /\ pc' = [pc EXCEPT ![Cln] = "c_iter"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, res, aecbN, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, collected, cur,
                   conn, srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
                   cleanN, complN>>

\* for id, paf := range acks { paf.err = ErrJetStreamPublisherClosed; errCh <- ; delete }
ClnIter ==
    /\ pc[Cln] = "c_iter"
    /\ \E c \in acks :
         /\ ErrSendOK(c)
         /\ ResolveErr(c)
         /\ acks' = acks \ {c}
         /\ collected' = IF HasErrCb THEN [collected EXCEPT ![Cln] = @ \cup {c}] ELSE collected
    /\ UNCHANGED <<pc, mu, pfx, subGen, prefixGen, ngen, stallCh, stallClosed, waitOn,
                   doneCh, doneClosed, nids, dchOf, waiters, aecbN, syncErr,
                   errMade, retries, tmo, cur, conn, srv, pend, dq, lsn,
                   lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

\* close(doneCh); Unlock. No maybeUnstall (PUB-2).
ClnEnd ==
    /\ pc[Cln] = "c_iter" /\ acks = {}
    /\ CloseDoneIfEmpty(acks)
    /\ IF FIX_CleanupUnstall THEN Unstall(acks) ELSE UNCHANGED <<stallCh, stallClosed>>
    /\ mu' = None
    /\ pc' = [pc EXCEPT ![Cln] = "c_cb"]
    /\ UNCHANGED <<acks, pfx, subGen, prefixGen, ngen, waitOn, nids, dchOf, waiters,
                   res, aecbN, syncErr, errMade, errBuf, hasErr, retries,
                   tmo, collected, cur, conn, srv, pend, dq, lsn, lreg, lbuf,
                   lclosed, nlsn, discN, cleanN, complN>>

\* deferred errCb calls run after Unlock
ClnCb ==
    /\ pc[Cln] = "c_cb"
    /\ IF collected[Cln] = {}
       THEN /\ pc' = [pc EXCEPT ![Cln] = "c_idle"] /\ UNCHANGED <<aecbN, collected>>
       ELSE /\ ReentrantCb => Free
            /\ \E c \in collected[Cln] :
                 /\ aecbN' = [aecbN EXCEPT ![c] = @ + 1]
                 /\ collected' = [collected EXCEPT ![Cln] = @ \ {c}]
            /\ UNCHANGED pc
    /\ UNCHANGED <<mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, syncErr,
                   errMade, errBuf, hasErr, retries, tmo, cur, conn, srv,
                   pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN, complN>>

-----------------------------------------------------------------------------
(* User calls on the future and the context.                              *)

\* paf.Err() (704-716): make(chan error, 1), pre-loaded if paf.err is set.
UserErrCall(c) ==
    /\ UserErr /\ pc[Pub(c)] = "done_ok" /\ ~errMade[c] /\ Free
    /\ errMade' = [errMade EXCEPT ![c] = TRUE]
    /\ errBuf' = [errBuf EXCEPT ![c] = IF hasErr[c] THEN 1 ELSE 0]
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, hasErr, retries, tmo, collected, cur, conn,
                   srv, pend, dq, lsn, lreg, lbuf, lclosed, nlsn, discN, cleanN,
                   complN>>

\* The user never reads paf.Err() in the model: a read only frees the
\* buffer, so leaving it out keeps every double resolution visible
\* (ResolvedAtMostOnce) and makes a second send block (NoBlockedSendUnderLock).

\* PublishAsyncComplete (739-751)
UserComplete ==
    /\ complN < MaxComplete /\ Free
    /\ complN' = complN + 1
    /\ IF doneCh = 0
       THEN /\ nids' = nids + 1
            /\ waiters' = waiters \cup {nids + 1}
            /\ IF acks = {}
               THEN /\ doneClosed' = doneClosed \cup {nids + 1} /\ UNCHANGED doneCh
               ELSE /\ doneCh' = nids + 1 /\ UNCHANGED doneClosed
       ELSE /\ waiters' = waiters \cup {doneCh}
            /\ UNCHANGED nids
            /\ IF acks = {}
               THEN /\ doneClosed' = doneClosed \cup {doneCh} /\ doneCh' = 0
               ELSE UNCHANGED <<doneCh, doneClosed>>
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, dchOf, res, aecbN, syncErr, errMade, errBuf, 
                   hasErr, retries, tmo, collected, cur, conn, srv, pend, dq, lsn,
                   lreg, lbuf, lclosed, nlsn, discN, cleanN>>

-----------------------------------------------------------------------------
(* Environment: the server and the connection.                           *)

\* The server answers a publish. The reply reaches handleAsyncReply only if
\* the reply sub for that prefix is still live.
Respond(c, k) ==
    /\ c \in srv /\ conn = "CONNECTED" /\ k \in RespKinds
    /\ srv' = srv \ {c}
    /\ dq' = IF subGen # 0 /\ pfx[c] = subGen THEN Append(dq, <<c, k>>) ELSE dq
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, conn, pend, lsn, lreg, lbuf, lclosed, nlsn,
                   discN, cleanN, complN>>

\* processOpErr: changeConnStatus(RECONNECTING); in-flight replies are lost.
Disconnect ==
    /\ conn = "CONNECTED" /\ discN < MaxDisc
    /\ conn' = "RECONNECTING" /\ discN' = discN + 1
    /\ srv' = {}
    /\ SendStatus("R")
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, pend, dq, lsn, lclosed, nlsn, cleanN, complN>>

\* doReconnect succeeds: the reconnect buffer is flushed.
Reconnect ==
    /\ conn = "RECONNECTING"
    /\ conn' = "CONNECTED"
    /\ srv' = srv \cup pend /\ pend' = {}
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, dq, lsn, lreg, lbuf, lclosed, nlsn, discN,
                   cleanN, complN>>

\* nc.Close(): subscriptions die, changeConnStatus(CLOSED).
Close ==
    /\ AllowClose /\ conn # "CLOSED"
    /\ conn' = "CLOSED"
    /\ srv' = {} /\ pend' = {} /\ dq' = <<>>
    /\ SendStatus("C")
    /\ UNCHANGED <<pc, mu, acks, pfx, subGen, prefixGen, ngen, stallCh, stallClosed,
                   waitOn, doneCh, doneClosed, nids, dchOf, waiters, res, aecbN,
                   syncErr, errMade, errBuf, hasErr, retries, tmo,
                   collected, cur, lsn, lclosed, nlsn, discN, cleanN, complN>>

-----------------------------------------------------------------------------
Next ==
    \/ \E c \in Calls :
         \/ PubStart(c) \/ PubReg(c) \/ PubWake(c) \/ PubStallTimeout(c)
         \/ PubSend(c) \/ PubClear(c)
         \/ TmoFire(c) \/ TmoLock(c) \/ TmoSend(c) \/ TmoDone(c) \/ TmoCb(c)
         \/ RtyFire(c) \/ RtyGet(c) \/ RtyPub(c) \/ RtyClear(c) \/ RtyELock(c)
         \/ RtySend(c) \/ RtyCb(c)
         \/ UserErrCall(c)
         \/ \E k \in RespKinds : Respond(c, k)
    \/ DispTake \/ DispLock \/ DispSend \/ DispCb \/ DispClose
    \/ \E l \in Lsns : RstRecv(l) \/ RstLock(l) \/ RstIter(l) \/ RstEnd(l)
                       \/ RstCb(l) \/ RstClose(l)
    \/ ClnStart \/ ClnSub \/ ClnLock \/ ClnIter \/ ClnEnd \/ ClnCb
    \/ UserComplete
    \/ Disconnect \/ Reconnect \/ Close

\* Fairness for the liveness configs: every goroutine step is weakly fair
\* (the scheduler runs runnable goroutines), timers fire, the server answers
\* and a reconnect eventually succeeds. User calls, disconnects, closes,
\* cleanup and stall timeouts are not forced.
Fairness ==
    /\ \A c \in Calls :
         /\ WF_vars(PubReg(c)) /\ WF_vars(PubWake(c)) /\ WF_vars(PubSend(c))
         /\ WF_vars(PubClear(c))
         /\ WF_vars(TmoFire(c)) /\ WF_vars(TmoLock(c)) /\ WF_vars(TmoSend(c))
         /\ WF_vars(TmoDone(c)) /\ WF_vars(TmoCb(c))
         /\ WF_vars(RtyFire(c)) /\ WF_vars(RtyGet(c)) /\ WF_vars(RtyPub(c))
         /\ WF_vars(RtyClear(c)) /\ WF_vars(RtyELock(c)) /\ WF_vars(RtySend(c))
         /\ WF_vars(RtyCb(c))
         /\ WF_vars(\E k \in RespKinds : Respond(c, k))
    /\ WF_vars(DispTake) /\ WF_vars(DispLock) /\ WF_vars(DispSend)
    /\ WF_vars(DispCb) /\ WF_vars(DispClose)
    /\ \A l \in Lsns : WF_vars(RstRecv(l)) /\ WF_vars(RstLock(l))
                       /\ WF_vars(RstIter(l)) /\ WF_vars(RstEnd(l))
                       /\ WF_vars(RstCb(l)) /\ WF_vars(RstClose(l))
    /\ WF_vars(ClnSub) /\ WF_vars(ClnLock) /\ WF_vars(ClnIter)
    /\ WF_vars(ClnEnd) /\ WF_vars(ClnCb)
    /\ WF_vars(Reconnect)

Spec == Init /\ [][Next]_vars

\* Calls are interchangeable; safety configs with model-value Calls use this.
Symm == Permutations(Calls)
FairSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Safety properties.                                                     *)

\* WithPublishAsyncMaxPending is a hard cap (#2126).
MaxPendingRespected == Cardinality(acks) <= MaxPA

\* Each future is resolved (Ok or Err) at most once.
ResolvedAtMostOnce == \A c \in Calls : res[c] <= 1

\* The error handler runs at most once per message.
ErrCbAtMostOnce == \A c \in Calls : aecbN[c] <= 1

\* A failure is reported through one channel: either PublishMsgAsync's return
\* value or the future/handler, not both.
SingleReport == \A c \in Calls : ~(syncErr[c] /\ (res[c] > 0 \/ aecbN[c] > 0))

\* No goroutine is parked on a full paf.errCh while holding the publisher
\* mutex (that wedges every publisher call until the user drains Err()).
SendTarget(g) ==
    CASE g[1] = "tmo" /\ pc[g] = "t_send" -> {g[2]}
      [] g[1] = "rty" /\ pc[g] = "r_send" -> {g[2]}
      [] g = Disp /\ pc[g] = "d_send" -> {cur[1]}
      [] g[1] = "rst" /\ pc[g] \in {"s_iter", "s_iterc"} -> acks
      [] g = Cln /\ pc[g] = "c_iter" -> acks
      [] OTHER -> {}
NoBlockedSendUnderLock ==
    \A g \in Goroutines : \A c \in SendTarget(g) : ErrSendOK(c)

\* The timeout goroutine never calls into the publisher while holding its lock.
NoSelfDeadlock == \A c \in Calls : pc[Tmo(c)] # "t_reenter"

\* Lost stall wakeup: between critical sections, a caller parked in
\* registerPAF on an open stall channel implies the publisher is at the cap.
NoLostStallWakeup ==
    Free => \A c \in Calls :
              (pc[Pub(c)] = "stall" /\ waitOn[c] \notin stallClosed)
                 => Cardinality(acks) >= MaxPA

\* Lost completion: between critical sections an open doneCh implies there
\* is something pending, so whatever drains acks will close it.
NoLostCompletion == Free => (doneCh # 0 => acks # {})

\* Every pending future can still be acked: it is registered under the live
\* reply subscription (outside CleanupPublisher's own critical sections).
NoOrphanedFuture ==
    (Free /\ pc[Cln] \notin {"c_lock", "c_iter"} /\ conn # "CLOSED")
        => \A c \in acks : subGen # 0 /\ pfx[c] = subGen

\* The publisher's status channel stays subscribed to RECONNECTING.
ListenerSubscribed ==
    (lsn # 0 /\ pc[Rst(lsn)] = "s_wait") => "R" \in lreg[lsn]

\* No resetPendingAcksOnReconnect goroutine waits on a channel that nobody
\* will ever send on or close.
NoLeakedResetGoroutine ==
    \A l \in Lsns : ~(pc[Rst(l)] = "s_wait" /\ lbuf[l] = <<>> /\ ~lclosed[l]
                      /\ lreg[l] = {} /\ lsn # l)

\* Stranded future (safety form of PUB-5): the connection is CLOSED, every
\* goroutine is at rest and nothing is buffered, yet a future is pending with
\* no ack timer armed. No ack can arrive any more, so only a user call to
\* CleanupPublisher could ever resolve it.
AtRest ==
    /\ \A c \in Calls : /\ pc[Pub(c)] \in {"idle", "done_ok", "done_err", "done_stall", "stall"}
                       /\ pc[Tmo(c)] = "t_idle" /\ pc[Rty(c)] = "r_idle"
    /\ pc[Disp] = "d_idle" /\ dq = <<>> /\ pc[Cln] = "c_idle"
    /\ \A l \in Lsns : pc[Rst(l)] \in {"s_none", "s_exit"}
                      \/ (pc[Rst(l)] = "s_wait" /\ lbuf[l] = <<>> /\ ~lclosed[l])
NoStrandedFuture ==
    ~(conn = "CLOSED" /\ Free /\ AtRest /\ \E c \in acks : tmo[c] = "off")

-----------------------------------------------------------------------------
(* Liveness.                                                              *)

\* Every registered future is eventually resolved -- or, if its publish failed
\* synchronously, reported through PublishMsgAsync's return value instead.
EventuallyResolved ==
    \A c \in Calls : [](c \in acks => <>(c \notin acks /\ (res[c] > 0 \/ syncErr[c])))

\* Every channel returned by PublishAsyncComplete is eventually closed.
CompleteCloses == \A d \in 1..10 : [](d \in waiters => <>(d \in doneClosed))
=============================================================================
