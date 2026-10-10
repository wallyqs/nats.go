---------------------------- MODULE ConnLifecycle ----------------------------
(***************************************************************************)
(* The connection lifecycle of nats.go: CONNECTED -> RECONNECTING ->       *)
(* CONNECTED / CLOSED, ForceReconnect, Close, Flush / PING-PONG tracking   *)
(* (nc.pongs), the reconnect pending buffer, the ping timer, and the async *)
(* callback queue (nc.ach).                                                *)
(*                                                                         *)
(* Granularity: one action per critical section of nc.mu (the code holds  *)
(* the lock across each of them), plus the lock-free steps that matter:    *)
(* readLoop's Read and parse are separate actions, because parse runs      *)
(* without the lock on bytes Read returned earlier, and doReconnect's      *)
(* select on rqch / the back-off timer is its own action. Goroutines:      *)
(*   readLoop(c)      one per socket c        nats.go:3736                 *)
(*   doReconnect      at most one at a time   nats.go:3356                 *)
(*   ping timer       time.AfterFunc          nats.go:6043                 *)
(*   async dispatcher                         nats.go:3673                 *)
(*   users            Flush(f), Close, ForceReconnect                      *)
(*   server / network environment actions                                  *)
(* The flusher goroutine is folded into the writers: every write the model *)
(* makes is the in-place flush that sendPing does (nats.go:6033).          *)
(*                                                                         *)
(* Socket bytes are abstracted: client->server carries PING tokens (the    *)
(* PUB a user publishes before Flush rides in front of its PING, so "the   *)
(* server got PING f" implies "the server got the message published before *)
(* Flush f"); server->client carries PONGs only, so it is a count.         *)
(*                                                                         *)
(* The Fix* constants switch on the minimal fixes proposed in the README;  *)
(* *_bug.cfg runs the code as it is.                                       *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Flushes,          \* user Flush calls (ids > 0), each made once
    MaxSock,          \* sockets the client may own (socket 1 is the initial one)
    MaxDrops,         \* times the server/network may drop a socket
    MaxReconnect,     \* Options.MaxReconnect (attempts per server, one server)
    MaxTimer,         \* ping timer firings explored
    MaxPingsOut,      \* Options.MaxPingsOut
    MaxForce,         \* ForceReconnect calls
    AllowClose,       \* users may call Close
    AllowWriteFail,   \* the pending-buffer flush on reconnect may fail
    AllowHsFail,      \* the reconnect handshake may fail on a dialed socket
    AllowDialFail,    \* the reconnect dial may fail (server down)
    AllowFlushTimeout,\* FlushTimeout may expire
    FixStalePong,     \* CONN-6: processPong ignores PONGs unless the connection is CONNECTED
    FixPendingFail,   \* CONN-5: on pending-flush failure close the socket and fail the waiters
    FixRqch,          \* CONN-7: doReconnect re-reads nc.rqch after a bypassed sleep
    FixHsClose,       \* CONN-8: close and nil the socket on a failed reconnect handshake
    FixCloseDisc,     \* CONN-9: close() does not report a disconnect while RECONNECTING
    FixDiscPush       \* CONN-11: queue DisconnectedErrCB where the status changes

Socks == 1..MaxSock
RW    == -1           \* waiter of doReconnect's own trailing nc.Flush() (nats.go:3623)
NIL   == 0            \* nil waiter: ping timer PINGs, timed-out flush entries

VARIABLES
    status,     \* nc.status
    conn,       \* nc.conn (0 = nil)
    nsock,      \* sockets dialed so far
    sockOpen,   \* client side of socket not yet Close()d
    peerAlive,  \* server side of socket still serving
    c2s,        \* PING waiter ids in flight to the server, per socket
    s2c,        \* PONGs in flight to the client, per socket
    rl,         \* readLoop of socket c: "none" | "read" | "exit"
    rbuf,       \* PONGs a readLoop has Read but not parsed yet
    pongs,      \* nc.pongs
    bwMode,     \* "direct" | "pending"   (nc.bw.pending != nil)
    pend,       \* reconnect pending buffer: PING waiter ids
    flushSt,    \* per user Flush: "idle" | "waiting" | "ok" | "err" | "timeout"
    srvGot,     \* user Flush ids whose PING the server processed
    timerOn,    \* nc.ptmr armed
    timerFires, \* firings so far
    pout,       \* nc.pout
    dr,         \* doReconnect pc: "none" | "wfe" | "top" | "sleep" | "go" | "wfe2" | "attempt" | "closeit"
    drForce,    \* forceReconnect argument (cleared after the first iteration)
    drWaitGR,   \* waitForGoRoutines
    drTries,    \* cur.Reconnects
    drWake,     \* how the last back-off ended: "timer" | "rqch" | "none"
    ncRqch,     \* nc.rqch: "nil" | "local" (the channel doReconnect selects on) | "other"
    lrqClosed,  \* doReconnect's local rqch is closed
    ach,        \* async callback queue (nc.ach); "END" is the close marker
    achDone,    \* dispatcher returned
    cbDisc,     \* DisconnectedErrCB calls run
    cbDiscRun,  \* ... since the last ReconnectedCB
    cbClosed,   \* ClosedCB calls run
    cbAfter,    \* a callback ran after ClosedCB
    disconnects,\* established connections lost (each should yield one DisconnectedErrCB)
    forces,     \* ForceReconnect calls
    forcesR,    \* ForceReconnect calls made while RECONNECTING
    quick,      \* reconnect attempts whose back-off ended through a closed rqch
    closeCalled,
    drops

vars == <<status, conn, nsock, sockOpen, peerAlive, c2s, s2c, rl, rbuf, pongs, bwMode, pend, flushSt, srvGot, timerOn, timerFires,
          pout, dr, drForce, drWaitGR, drTries, drWake, ncRqch, lrqClosed, ach,
          achDone, cbDisc, cbDiscRun, cbClosed, cbAfter, disconnects, forces,
          forcesR, quick, closeCalled, drops>>

Init ==
    /\ status = "CONNECTED"
    /\ conn = 1
    /\ nsock = 1
    /\ sockOpen = [c \in Socks |-> c = 1]
    /\ peerAlive = [c \in Socks |-> c = 1]
    /\ c2s = [c \in Socks |-> <<>>]
    /\ s2c = [c \in Socks |-> 0]
    /\ rl = [c \in Socks |-> IF c = 1 THEN "read" ELSE "none"]
    /\ rbuf = [c \in Socks |-> 0]
    /\ pongs = <<>>
    /\ bwMode = "direct"
    /\ pend = <<>>
    /\ flushSt = [f \in Flushes |-> "idle"]
    /\ srvGot = {}
    /\ timerOn = TRUE
    /\ timerFires = 0
    /\ pout = 0
    /\ dr = "none"
    /\ drForce = FALSE
    /\ drWaitGR = FALSE
    /\ drTries = 0
    /\ drWake = "none"
    /\ ncRqch = "other"          \* nc.setup(): nc.rqch = make(chan struct{})  nats.go:2901
    /\ lrqClosed = FALSE
    /\ ach = <<>>
    /\ achDone = FALSE
    /\ cbDisc = 0
    /\ cbDiscRun = 0
    /\ cbClosed = 0
    /\ cbAfter = FALSE
    /\ disconnects = 0
    /\ forces = 0
    /\ forcesR = 0
    /\ quick = 0
    /\ closeCalled = FALSE
    /\ drops = 0

CbVars == <<achDone, cbDisc, cbDiscRun, cbClosed, cbAfter>>
DrLocals == <<drForce, drWaitGR, drTries, drWake, lrqClosed>>

(***************************************************************************)
(* Shared pieces                                                           *)
(***************************************************************************)

\* clearPendingFlushCalls  nats.go:6179 -- close every channel still in
\* nc.pongs. A Flush is "waiting" exactly while its channel is in nc.pongs.
FailWaiters(fs) == [f \in Flushes |-> IF fs[f] = "waiting" THEN "err" ELSE fs[f]]

\* Write one PING (waiter w) as sendPing does: append and flush in place
\* (nats.go:6033). In pending mode it lands in the pending buffer; otherwise on
\* the socket, lost if the peer is gone.
WritePing(w) ==
    IF bwMode = "pending"
    THEN /\ pend' = Append(pend, w)
         /\ UNCHANGED c2s
    ELSE /\ c2s' = IF conn # 0 /\ peerAlive[conn]
                   THEN [c2s EXCEPT ![conn] = Append(@, w)] ELSE c2s
         /\ UNCHANGED pend

\* close(nc.rqch); nc.rqch = nil   (ForceReconnect nats.go:2648, close nats.go:6218)
CloseRqch ==
    /\ ncRqch' = "nil"
    /\ lrqClosed' = (lrqClosed \/ ncRqch = "local")

\* processOpErr(err) with AllowReconnect, nats.go:3640-3670. Lock held.
ProcessOpErr ==
    IF status # "CONNECTED"
    THEN UNCHANGED <<status, conn, sockOpen, bwMode, pongs, flushSt, timerOn, dr, DrLocals, disconnects, ach>>
    ELSE /\ status' = "RECONNECTING"
         /\ timerOn' = FALSE
         /\ sockOpen' = [sockOpen EXCEPT ![conn] = FALSE]
         /\ conn' = 0
         /\ bwMode' = "pending"
         /\ pongs' = <<>>
         /\ flushSt' = FailWaiters(flushSt)
         /\ dr' = "wfe"                       \* go nc.doReconnect(err, false)
         /\ UNCHANGED DrLocals
         /\ disconnects' = disconnects + 1
         /\ ach' = IF FixDiscPush THEN Append(ach, "disc") ELSE ach

\* close(CLOSED, true, nil), nats.go:6204-6304. Lock held throughout.
CloseConn ==
    IF status = "CLOSED" THEN
        UNCHANGED <<status, ncRqch, lrqClosed, pongs, flushSt, timerOn,
                    sockOpen, ach, disconnects>>
    ELSE
        /\ status' = "CLOSED"
        /\ CloseRqch
        /\ pongs' = <<>>                                   \* nats.go:6225
        /\ flushSt' = FailWaiters(flushSt)
        /\ timerOn' = FALSE
        \* bw.flush(); defer nc.conn.Close()  (nc.conn is not set to nil)
        /\ sockOpen' = IF conn # 0 THEN [sockOpen EXCEPT ![conn] = FALSE] ELSE sockOpen
        \* DisconnectedErrCB iff nc.conn != nil, then ClosedCB, then the
        \* dispatcher's close marker                       nats.go:6281-6300
        /\ LET disc == IF FixCloseDisc THEN conn # 0 /\ status # "RECONNECTING" ELSE conn # 0
           IN ach' = ach \o (IF disc THEN <<"disc">> ELSE <<>>) \o <<"closed", "END">>
        /\ disconnects' = IF status = "CONNECTED" THEN disconnects + 1 ELSE disconnects

(***************************************************************************)
(* readLoop(c)  nats.go:3736-3784                                          *)
(***************************************************************************)

\* br.Read() returns what the socket has buffered (nats.go:2436).
RLRead(c) ==
    /\ rl[c] = "read"
    /\ rbuf[c] = 0
    /\ sockOpen[c]
    /\ s2c[c] > 0
    /\ rbuf' = [rbuf EXCEPT ![c] = s2c[c]]
    /\ s2c' = [s2c EXCEPT ![c] = 0]
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, c2s, rl, pongs, bwMode, pend, flushSt, srvGot, timerOn, timerFires, pout,
                   dr, DrLocals, ncRqch, ach, CbVars, disconnects, forces, forcesR,
                   quick, closeCalled, drops>>

\* Read error (the client closed the socket, or the peer is gone and nothing
\* is left) -> processOpErr(err, false) -> break -> wg.Done().
RLErr(c) ==
    /\ rl[c] = "read"
    /\ rbuf[c] = 0
    /\ ~sockOpen[c] \/ (~peerAlive[c] /\ s2c[c] = 0)
    /\ rl' = [rl EXCEPT ![c] = "exit"]
    /\ ProcessOpErr
    /\ UNCHANGED <<nsock, peerAlive, c2s, s2c, rbuf, pend, srvGot, timerFires,
                   pout, ncRqch, CbVars, forces, forcesR, quick, closeCalled, drops>>

\* parse() -> processPong(), nats.go:4272-4285. Nothing checks that this
\* readLoop's socket is the connection whose PINGs are in nc.pongs.
RLParsePong(c) ==
    /\ rl[c] = "read"
    /\ rbuf[c] > 0
    /\ rbuf' = [rbuf EXCEPT ![c] = @ - 1]
    \* FixStalePong: processPong returns early unless the connection is
    \* (still) connected.
    /\ IF FixStalePong /\ status # "CONNECTED"
       THEN UNCHANGED <<pongs, flushSt, pout>>
       ELSE /\ pout' = 0
            /\ IF pongs = <<>>
               THEN UNCHANGED <<pongs, flushSt>>
               ELSE LET w == Head(pongs) IN
                    /\ pongs' = Tail(pongs)
                    \* ch <- struct{}{} on a cap-1 channel; FlushTimeout receives it
                    /\ flushSt' = IF w > 0 /\ flushSt[w] = "waiting"
                                  THEN [flushSt EXCEPT ![w] = "ok"] ELSE flushSt
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, c2s, s2c, rl, bwMode, pend, srvGot, timerOn, timerFires, dr, DrLocals,
                   ncRqch, ach, CbVars, disconnects, forces, forcesR, quick,
                   closeCalled, drops>>

(***************************************************************************)
(* Ping timer  processPingTimer, nats.go:6043-6064                         *)
(***************************************************************************)
PingTimer ==
    /\ timerOn
    /\ status = "CONNECTED"
    /\ timerFires < MaxTimer
    /\ timerFires' = timerFires + 1
    /\ pout' = pout + 1
    /\ IF pout + 1 > MaxPingsOut
       THEN \* stale connection: processOpErr from the timer goroutine, while
            \* the readLoop of the socket may still be parsing.
            /\ ProcessOpErr
            /\ UNCHANGED <<c2s, pend>>
       ELSE /\ pongs' = Append(pongs, NIL)
            /\ WritePing(NIL)
            /\ UNCHANGED <<status, conn, sockOpen, bwMode, flushSt, timerOn,
                           dr, DrLocals, disconnects, ach>>
    /\ UNCHANGED <<nsock, peerAlive, s2c, rl, rbuf, srvGot, ncRqch, CbVars,
                   forces, forcesR, quick, closeCalled, drops>>

(***************************************************************************)
(* Users                                                                   *)
(***************************************************************************)

\* Publish(m) then FlushTimeout(t): nats.go:6067-6105.
FlushStart(f) ==
    /\ flushSt[f] = "idle"
    /\ IF status = "CLOSED"
       THEN /\ flushSt' = [flushSt EXCEPT ![f] = "err"]
            /\ UNCHANGED <<pongs, c2s, pend>>
       ELSE /\ flushSt' = [flushSt EXCEPT ![f] = "waiting"]
            /\ pongs' = Append(pongs, f)
            /\ WritePing(f)
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, s2c, rl, rbuf, bwMode, srvGot, timerOn, timerFires, pout, dr, DrLocals,
                   ncRqch, ach, CbVars, disconnects, forces, forcesR, quick,
                   closeCalled, drops>>

\* FlushTimeout's timer fires: removeFlushEntry nils the entry (nats.go:6017).
FlushTimeout(f) ==
    /\ AllowFlushTimeout
    /\ flushSt[f] = "waiting"
    /\ flushSt' = [flushSt EXCEPT ![f] = "timeout"]
    /\ pongs' = [i \in 1..Len(pongs) |-> IF pongs[i] = f THEN NIL ELSE pongs[i]]
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, c2s, s2c, rl, rbuf, bwMode, pend, srvGot, timerOn, timerFires, pout, dr,
                   DrLocals, ncRqch, ach, CbVars, disconnects, forces, forcesR, quick,
                   closeCalled, drops>>

\* Close(), nats.go:6308.
UserClose ==
    /\ AllowClose
    /\ ~closeCalled
    /\ closeCalled' = TRUE
    /\ CloseConn
    /\ UNCHANGED <<conn, nsock, peerAlive, c2s, s2c, rl, rbuf, bwMode, pend,
                   srvGot, timerFires, pout, dr, drForce, drWaitGR, drTries, drWake,
                   CbVars, forces, forcesR, quick, drops>>

\* ForceReconnect(), nats.go:2638-2673.
ForceReconnect ==
    /\ forces < MaxForce
    /\ status \in {"CONNECTED", "RECONNECTING"}
    /\ forces' = forces + 1
    /\ IF status = "RECONNECTING"
       THEN /\ forcesR' = forcesR + 1
            /\ CloseRqch
            /\ UNCHANGED <<status, sockOpen, bwMode, pongs, flushSt, timerOn,
                           dr, drForce, drWaitGR, drTries, drWake, disconnects, ach>>
       ELSE \* clearPendingFlushCalls, stopPingTimer, bw.flush, switchToPending,
            \* conn.Close() (nc.conn is NOT set to nil), go doReconnect(nil, true)
            /\ pongs' = <<>>
            /\ flushSt' = FailWaiters(flushSt)
            /\ timerOn' = FALSE
            /\ bwMode' = "pending"
            /\ sockOpen' = [sockOpen EXCEPT ![conn] = FALSE]
            /\ status' = "RECONNECTING"
            /\ dr' = "wfe"
            /\ drForce' = TRUE
            /\ disconnects' = disconnects + 1
            /\ ach' = IF FixDiscPush THEN Append(ach, "disc") ELSE ach
            /\ UNCHANGED <<forcesR, ncRqch, lrqClosed, drWaitGR, drTries, drWake>>
    /\ UNCHANGED <<conn, nsock, peerAlive, c2s, s2c, rl, rbuf, pend, srvGot,
                   timerFires, pout, CbVars, quick, closeCalled, drops>>

(***************************************************************************)
(* doReconnect, nats.go:3356-3637                                          *)
(***************************************************************************)
AllExited == \A c \in Socks : rl[c] # "read"

DRUnchanged == UNCHANGED <<nsock, peerAlive, s2c, rl, rbuf, srvGot, timerFires,
                           pout, CbVars, forces, forcesR, closeCalled, drops>>

\* waitForExits(), then lock: DisconnectedErrCB; rqch := nc.rqch, or a fresh
\* closed channel if nil.  nats.go:3359-3402
DRStart ==
    /\ dr = "wfe"
    /\ AllExited
    /\ dr' = "top"
    /\ ach' = IF FixDiscPush THEN ach ELSE Append(ach, "disc")
    /\ IF ncRqch = "nil"
       THEN /\ lrqClosed' = TRUE
            /\ UNCHANGED ncRqch
       ELSE /\ lrqClosed' = FALSE
            /\ ncRqch' = "local"
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drWaitGR, drTries, drWake, disconnects, quick>>
    /\ DRUnchanged

\* Loop head (lock held): selectNextServer, doSleep, unlock. One server, so
\* every iteration except a forced first one sleeps.  nats.go:3422-3489
DRTop ==
    /\ dr = "top"
    /\ dr' = IF drTries >= MaxReconnect THEN "closeit"        \* ErrNoServers -> break
             ELSE IF drForce THEN "go" ELSE "sleep"
    /\ drForce' = FALSE
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drWaitGR, drTries, drWake, ncRqch, lrqClosed, ach,
                   disconnects, quick>>
    /\ DRUnchanged

\* runtime.Gosched() branch (no back-off) of a forced first attempt.
DRGo ==
    /\ dr = "go"
    /\ dr' = IF drWaitGR THEN "wfe2" ELSE "attempt"
    /\ drWake' = "none"
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drWaitGR, drTries, ncRqch, lrqClosed, ach,
                   disconnects, quick>>
    /\ DRUnchanged

\* select { case <-rqch: ...; case <-rt.C: }   nats.go:3517-3535
DRSleepTimer ==
    /\ dr = "sleep"
    /\ dr' = IF drWaitGR THEN "wfe2" ELSE "attempt"
    /\ drWake' = "timer"
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drWaitGR, drTries, ncRqch, lrqClosed, ach,
                   disconnects, quick>>
    /\ DRUnchanged

DRSleepRqch ==
    /\ dr = "sleep"
    /\ lrqClosed
    /\ dr' = IF drWaitGR THEN "wfe2" ELSE "attempt"
    /\ drWake' = "rqch"
    \* nc.rqch = make(chan struct{}) -- the local rqch keeps the closed one
    /\ ncRqch' = IF FixRqch THEN "local" ELSE "other"
    /\ lrqClosed' = ~FixRqch
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drWaitGR, drTries, ach, disconnects, quick>>
    /\ DRUnchanged

\* if waitForGoRoutines { nc.waitForExits() }   nats.go:3540-3543
DRWait2 ==
    /\ dr = "wfe2"
    /\ AllExited
    /\ dr' = "attempt"
    /\ drWaitGR' = FALSE
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drTries, drWake, ncRqch, lrqClosed, ach,
                   disconnects, quick>>
    /\ DRUnchanged

\* Lock; if closed, break.  nats.go:3544-3547
DRAttemptClosed ==
    /\ dr = "attempt"
    /\ status = "CLOSED"
    /\ dr' = "closeit"
    /\ UNCHANGED <<status, conn, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, DrLocals, ncRqch, ach, disconnects, quick>>
    /\ DRUnchanged

QuickNext == IF drWake = "rqch" THEN quick + 1 ELSE quick

\* createConn fails (server down): ReconnectErrCB, continue. The failed
\* Dial assigns nil to nc.conn.  nats.go:2531, 3553-3561
DRDialFail ==
    /\ AllowDialFail
    /\ dr = "attempt"
    /\ status # "CLOSED"
    /\ dr' = "top"
    /\ drTries' = drTries + 1
    /\ quick' = QuickNext
    /\ conn' = 0
    /\ UNCHANGED <<status, sockOpen, c2s, pongs, bwMode, pend, flushSt,
                   timerOn, drForce, drWaitGR, drWake, ncRqch, lrqClosed, ach, disconnects>>
    /\ DRUnchanged

\* A dial succeeds on a fresh socket.
NewSock == nsock + 1

\* The handshake (INFO/CONNECT/PING/PONG) on the new socket fails (-ERR,
\* timeout): processConnectInit error -> changeConnStatus(RECONNECTING);
\* continue. The new socket stays open in nc.conn.  nats.go:3569-3576
DRHsFail ==
    /\ AllowHsFail
    /\ dr = "attempt"
    /\ status # "CLOSED"
    /\ nsock < MaxSock
    /\ nsock' = NewSock
    /\ dr' = "top"
    /\ drTries' = drTries + 1
    /\ quick' = QuickNext
    /\ conn' = IF FixHsClose THEN 0 ELSE NewSock
    /\ sockOpen' = [sockOpen EXCEPT ![NewSock] = ~FixHsClose]
    /\ peerAlive' = [peerAlive EXCEPT ![NewSock] = TRUE]
    /\ UNCHANGED <<status, c2s, pongs, bwMode, pend, flushSt, timerOn, drForce,
                   drWaitGR, drWake, ncRqch, lrqClosed, ach, disconnects>>
    /\ UNCHANGED <<s2c, rl, rbuf, srvGot, timerFires, pout, CbVars,
                   forces, forcesR, closeCalled, drops>>

\* The handshake succeeds: CONNECTED, readLoop/flusher started, subscriptions
\* re-sent, then the pending buffer is written (nats.go:3576-3627).
\* ok = TRUE: it all went out; ReconnectedCB; return (the trailing nc.Flush()
\* is a PING with waiter RW).
\* ok = FALSE: the write failed after k items (partial write); the pending
\* buffer is reset anyway (nats.go:2408) and the loop continues with
\* waitForGoRoutines = true; nc.pongs and the socket are left as they are.
DRConnect(ok, k) ==
    /\ dr = "attempt"
    /\ status # "CLOSED"
    /\ nsock < MaxSock
    \* flushPendingBuffer cannot fail on an empty buffer (nats.go:2404)
    /\ IF ok THEN k = 0 ELSE AllowWriteFail /\ pend # <<>> /\ k < Len(pend)
    /\ nsock' = NewSock
    /\ quick' = QuickNext
    /\ peerAlive' = [peerAlive EXCEPT ![NewSock] = TRUE]
    /\ rl' = [rl EXCEPT ![NewSock] = "read"]
    /\ pend' = <<>>
    /\ pout' = 0
    /\ IF ok
       THEN /\ status' = "CONNECTED"
            /\ conn' = NewSock
            /\ sockOpen' = [sockOpen EXCEPT ![NewSock] = TRUE]
            /\ timerOn' = TRUE
            /\ bwMode' = "direct"
            /\ c2s' = [c2s EXCEPT ![NewSock] = pend \o <<RW>>]
            /\ pongs' = Append(pongs, RW)
            /\ ach' = Append(ach, "reconn")
            /\ dr' = "none"
            \* doReconnect returns: its locals are gone
            /\ drForce' = FALSE /\ drWaitGR' = FALSE /\ drTries' = 0
            /\ drWake' = "none" /\ lrqClosed' = FALSE
            /\ ncRqch' = IF ncRqch = "local" THEN "other" ELSE ncRqch
            /\ UNCHANGED <<flushSt>>
       ELSE /\ status' = "RECONNECTING"
            /\ timerOn' = FALSE
            /\ bwMode' = "pending"
            /\ c2s' = [c2s EXCEPT ![NewSock] = SubSeq(pend, 1, k)]
            /\ dr' = "top"
            /\ drWaitGR' = TRUE
            /\ drTries' = 0
            /\ UNCHANGED <<ach, drForce, drWake, lrqClosed, ncRqch>>
            /\ IF FixPendingFail
               THEN /\ conn' = 0
                    /\ sockOpen' = [sockOpen EXCEPT ![NewSock] = FALSE]
                    /\ pongs' = <<>>
                    /\ flushSt' = FailWaiters(flushSt)
               ELSE /\ conn' = NewSock
                    /\ sockOpen' = [sockOpen EXCEPT ![NewSock] = TRUE]
                    /\ UNCHANGED <<pongs, flushSt>>
    /\ UNCHANGED <<disconnects, s2c, rbuf, srvGot, timerFires, CbVars, forces, forcesR,
                   closeCalled, drops>>

\* After the loop: close(CLOSED, true, nil) (a no-op when already closed).
DRClose ==
    /\ dr = "closeit"
    /\ dr' = "none"
    /\ CloseConn
    /\ drForce' = FALSE /\ drWaitGR' = FALSE /\ drTries' = 0 /\ drWake' = "none"
    /\ UNCHANGED <<conn, c2s, bwMode, pend, quick>>
    /\ DRUnchanged

(***************************************************************************)
(* Async callback dispatcher, nats.go:3673-3698                            *)
(***************************************************************************)
Dispatch ==
    /\ ~achDone
    /\ ach # <<>>
    /\ ach' = Tail(ach)
    /\ LET h == Head(ach) IN
       /\ achDone' = (h = "END")
       /\ cbAfter' = (cbAfter \/ (h # "END" /\ cbClosed > 0))
       /\ cbClosed' = IF h = "closed" THEN cbClosed + 1 ELSE cbClosed
       /\ cbDisc' = IF h = "disc" THEN cbDisc + 1 ELSE cbDisc
       /\ cbDiscRun' = IF h = "disc" THEN cbDiscRun + 1
                       ELSE IF h = "reconn" THEN 0 ELSE cbDiscRun
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, c2s, s2c, rl, rbuf, pongs, bwMode, pend, flushSt, srvGot, timerOn, timerFires,
                   pout, dr, DrLocals, ncRqch, disconnects, forces, forcesR, quick,
                   closeCalled, drops>>

(***************************************************************************)
(* Server and network                                                      *)
(***************************************************************************)
ServerProcess(c) ==
    /\ peerAlive[c]
    /\ c2s[c] # <<>>
    /\ srvGot' = srvGot \cup ({Head(c2s[c])} \cap Flushes)
    /\ c2s' = [c2s EXCEPT ![c] = Tail(@)]
    /\ s2c' = [s2c EXCEPT ![c] = @ + 1]
    /\ UNCHANGED <<status, conn, nsock, sockOpen, peerAlive, rl, rbuf, pongs, bwMode, pend, flushSt, timerOn, timerFires, pout, dr,
                   DrLocals, ncRqch, ach, CbVars, disconnects, forces, forcesR, quick,
                   closeCalled, drops>>

\* The server (or the network) drops socket c: unprocessed bytes are lost.
ServerDrop(c) ==
    /\ peerAlive[c]
    /\ drops < MaxDrops
    /\ drops' = drops + 1
    /\ peerAlive' = [peerAlive EXCEPT ![c] = FALSE]
    /\ c2s' = [c2s EXCEPT ![c] = <<>>]
    /\ UNCHANGED <<status, conn, nsock, sockOpen, s2c, rl, rbuf, pongs, bwMode, pend, flushSt, srvGot, timerOn, timerFires, pout, dr,
                   DrLocals, ncRqch, ach, CbVars, disconnects, forces, forcesR, quick,
                   closeCalled>>

Next ==
    \/ \E c \in Socks : RLRead(c) \/ RLErr(c) \/ RLParsePong(c)
    \/ \E c \in Socks : ServerProcess(c) \/ ServerDrop(c)
    \/ PingTimer
    \/ \E f \in Flushes : FlushStart(f) \/ FlushTimeout(f)
    \/ UserClose
    \/ ForceReconnect
    \/ DRStart \/ DRTop \/ DRGo \/ DRSleepTimer \/ DRSleepRqch \/ DRWait2
    \/ DRAttemptClosed \/ DRDialFail \/ DRHsFail
    \/ \E ok \in BOOLEAN, k \in 0..Cardinality(Flushes) : DRConnect(ok, k)
    \/ DRClose
    \/ Dispatch

Spec == Init /\ [][Next]_vars

\* Weak fairness for the client's own goroutines and the back-off timer (not
\* for users or the environment), used by the liveness properties.
Internal ==
    \/ \E c \in Socks : RLRead(c) \/ RLErr(c) \/ RLParsePong(c)
    \/ DRStart \/ DRTop \/ DRGo \/ DRSleepTimer \/ DRWait2
    \/ DRAttemptClosed \/ DRClose \/ Dispatch
FairSpec == Spec /\ WF_vars(Internal)

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ status \in {"CONNECTED", "RECONNECTING", "CLOSED"}
    /\ conn \in 0..MaxSock
    /\ bwMode \in {"direct", "pending"}
    /\ ncRqch \in {"nil", "local", "other"}

\* Flush() returns nil only if the server processed its PING (and therefore
\* the messages published before it).
FlushIntegrity == \A f \in Flushes : flushSt[f] = "ok" => f \in srvGot

\* A doReconnect that waits for the old goroutines must not depend on the
\* server to drop a socket the client has abandoned: every readLoop it waits
\* for is parsing, or reading a socket that will return an error.
WaitForExitsUnblocked ==
    dr \in {"wfe", "wfe2"} =>
        \A c \in Socks : rl[c] = "read" => (~sockOpen[c] \/ ~peerAlive[c] \/ rbuf[c] > 0)

\* While doReconnect is backing off, the client holds no open socket.
NoSocketWhileBackingOff == dr = "sleep" => \A c \in Socks : ~sockOpen[c]

\* Every open socket is nc.conn (no leaked sockets).
NoLeakedSocket == \A c \in Socks : sockOpen[c] => c = conn

\* ForceReconnect bypasses at most one back-off sleep per call.
BackoffRespected == quick <= forcesR

\* Callbacks: none after ClosedCB, ClosedCB at most once, no two
\* DisconnectedErrCB without a ReconnectedCB between them, and by the time
\* ClosedCB runs every lost connection has been reported.
NoCallbackAfterClosed == ~cbAfter
ClosedOnce == cbClosed <= 1
NoDoubleDisconnect == cbDiscRun <= 1
DisconnectReportedBeforeClosed == cbClosed > 0 => cbDisc = disconnects
DisconnectNotLost == cbClosed > 0 => cbDisc >= disconnects

\* Documented status machine (CONNECTING is internal to one critical section).
StatusTransitions ==
    [][status' # status =>
        <<status, status'>> \in {<<"CONNECTED", "RECONNECTING">>,
                                 <<"RECONNECTING", "CONNECTED">>,
                                 <<"CONNECTED", "CLOSED">>,
                                 <<"RECONNECTING", "CLOSED">>}]_status

\* Liveness: doReconnect never waits forever for the old goroutines.
ReconnectNotStuck ==
    [](dr \in {"wfe", "wfe2"} => <>(dr \notin {"wfe", "wfe2"}))

\* Liveness: after Close, doReconnect ends and the dispatcher delivers ClosedCB
\* and exits.
CloseTerminates ==
    [](status = "CLOSED" => <>(dr = "none" /\ achDone /\ cbClosed = 1))

=============================================================================
