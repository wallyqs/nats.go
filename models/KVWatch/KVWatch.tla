------------------------------ MODULE KVWatch ------------------------------
(***************************************************************************)
(* The JetStream KeyValue watcher of jetstream/kv.go (WatchFiltered and    *)
(* Keys / ListKeys / History built on it) running over the legacy ordered  *)
(* push consumer of js.go, against a JetStream stream that other clients   *)
(* write to concurrently.                                                  *)
(*                                                                         *)
(* Processes (granularity = goroutines, locks and channel operations):     *)
(*   setup   - the WatchFiltered goroutine (kv.go:1244-1387), holds w.mu   *)
(*             from before js.Subscribe until it returns                   *)
(*   server  - the ordered consumer on the server: DeliverLastPerSubject   *)
(*             skip list / start-sequence cursor, num_pending (delta),     *)
(*             heartbeats, restart (memory consumer lost)                  *)
(*   reader  - the connection readLoop: processMsg + ordered gap checks    *)
(*             (nats.go:3888-4130, js.go:2179-2280, js.go:2559-2585)       *)
(*   reset   - the goroutine spawned by resetOrderedConsumer (js.go:2309)  *)
(*   cb      - the subscription's waitForMsgs goroutine running the KV     *)
(*             update callback (nats.go:3786-3878, kv.go:1278-1331) and    *)
(*             finally the closed handler (kv.go:1366-1370)                *)
(*   user    - Watch reader / Keys() / ListKeys() (+ its lister goroutine) *)
(*   env     - other clients: Put / Delete / Purge, raw removals (MaxAge,  *)
(*             PurgeDeletes, stream purge), message loss, server restart,  *)
(*             context cancellation                                        *)
(*                                                                         *)
(* Fixes lists the proposed fixes (README) that the model applies.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,          \* key names (model values)
    Filter,        \* the watched keys (subset of Keys)
    History,       \* bucket history (max msgs per subject)
    MaxSeq,        \* bound on stream length (number of writes)
    Modes,         \* subset of {"LPS","ALL","NEW","RESUME"}, chosen at Init
    ResumeRev,     \* ResumeFromRevision value used by mode RESUME
    IgnoreOpts,    \* subset of BOOLEAN: IgnoreDeletes() choices
    Users,         \* subset of {"Watch","Keys","ListKeys"}, chosen at Init
    ChanCap,       \* capacity of w.updates (256 in kv.go:1276)
    KeysCap,       \* capacity of keyLister.keys (256 in kv.go:1438)
    MaxDrops,      \* client-side message losses (reconnect / slow consumer)
    MaxRestarts,   \* server restarts (R1 memory consumer lost)
    MaxRemoves,    \* removals of a stored message without a marker
    MaxCancels,    \* context cancellations of the Watch/Keys context
    UserStops,     \* TRUE: a Watch/ListKeys user may call Stop early
    Fixes          \* subset of {"reset","marker","keysctx","lister",
                   \*            "stopdrain","closedh","pendingerr"}

ASSUME Filter \subseteq Keys /\ History \in 1..3 /\ ChanCap >= 1

Ops == {"PUT", "DEL", "PURGE"}
NoMsg == [key |-> "none", op |-> "PUT", live |-> FALSE]

VARIABLES
    \* ---- stream (server) ----
    log,          \* Seq of [key, op, live]; index = stream sequence = revision
    srv,          \* the ordered consumer currently bound to the sub
    net,          \* in-order TCP stream server -> client
    \* ---- environment budgets ----
    budget,       \* [drops, restarts, removes, cancels]
    envDone,      \* the environment has stopped acting
    \* ---- client subscription (nats.go / js.go) ----
    gen,          \* current sid generation (applyNewSID)
    jsi,          \* [dseq, sseq, pending, named, skipLE]
    resetting,    \* a reset goroutine is in flight
    resetStart,   \* the start sequence it was given (js.go:2274)
    plist,        \* async pending list (pHead..pTail)
    subClosed,    \* sub.closed
    pDoneSet,     \* closed handler installed (SetClosedHandler)
    \* ---- watcher (kv.go) ----
    wmu,          \* owner of w.mu: "none", "setup", "cb", "fix"
    initDone, initPending, received,
    ch,           \* w.updates contents (Seq of entries / nil markers)
    chClosed,
    wClosed,      \* fix "closedh": watcher-level closed flag
    cbPc, cur,    \* waitForMsgs / callback program counter, current msg
    setupPc,
    \* ---- users ----
    mode, ign, user,
    upc,          \* Keys user pc / Watch user pc
    lpc, lkey,    \* ListKeys lister goroutine pc and key being sent
    keysCh, keysClosed, uAband,
    \* ---- ghosts ----
    snap,         \* [latest: key->seq, op: key->op, last, floor, initSet]
    everLive, touched,
    sentRevs, sentMax, lastRev, markers,
    gotKeys,
    bad           \* set of violated-property tags, recorded at the step

vars == <<log, srv, net, budget, envDone, gen, jsi, resetting, resetStart,
          plist, subClosed, pDoneSet, wmu, initDone, initPending, received,
          ch, chClosed, wClosed, cbPc, cur, setupPc, mode, ign, user, upc,
          lpc, lkey, keysCh, keysClosed, uAband, snap, everLive, touched,
          sentRevs, sentMax, lastRev, markers, gotKeys, bad>>

-----------------------------------------------------------------------------
(* Helpers *)
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(s) == {s[i] : i \in 1..Len(s)}
Sorted(S) == CHOOSE f \in [1..Cardinality(S) -> S] :
                  \A i, j \in 1..Cardinality(S) : i < j => f[i] < f[j]

LiveSeqs == {s \in 1..Len(log) : log[s].live}
LiveOf(k) == {s \in LiveSeqs : log[s].key = k}
LatestOf(k) == IF LiveOf(k) = {} THEN 0 ELSE MaxOf(LiveOf(k))
IsDel(op) == op \in {"DEL", "PURGE"}

\* What the server still has to deliver for consumer c (its num_pending).
Pending(c) == {s \in Range(c.lss) : log[s].live}
              \cup {s \in LiveSeqs : s >= c.next /\ log[s].key \in Filter}

\* DeliverLastPerSubject skip list (server consumer.go:6416-6427).
LpsSeqs == {LatestOf(k) : k \in {k2 \in Filter : LatestOf(k2) > 0}}

NewConsumer(g, lss, next) ==
    [alive |-> TRUE, gen |-> g, lss |-> lss, next |-> next, dseq |-> 0]
DeadConsumer == [alive |-> FALSE, gen |-> 0, lss |-> <<>>, next |-> 1, dseq |-> 0]

\* Lowest revision each mode may deliver for key k (NoStale invariant).
Floor(k) == CASE mode = "LPS"    -> snap.latest[k]
              [] mode = "NEW"    -> snap.last + 1
              [] mode = "RESUME" -> ResumeRev
              [] OTHER           -> 0

Fixed(f) == f \in Fixes

-----------------------------------------------------------------------------
Init ==
    /\ log = <<>>
    /\ srv = DeadConsumer
    /\ net = <<>>
    /\ budget = [drops |-> MaxDrops, restarts |-> MaxRestarts,
                 removes |-> MaxRemoves, cancels |-> MaxCancels]
    /\ envDone = FALSE
    /\ gen = 0
    /\ jsi = [dseq |-> 1, sseq |-> 0, pending |-> 0, named |-> FALSE, skipLE |-> 0]
    /\ resetting = FALSE
    /\ resetStart = 0
    /\ plist = <<>>
    /\ subClosed = FALSE
    /\ pDoneSet = FALSE
    /\ wmu = "none"
    /\ initDone = FALSE
    /\ initPending = 0
    /\ received = 0
    /\ ch = <<>>
    /\ chClosed = FALSE
    /\ wClosed = FALSE
    /\ cbPc = "idle"
    /\ cur = [key |-> "none", rev |-> 0, op |-> "PUT", delta |-> 0]
    /\ setupPc = "init"
    /\ mode \in Modes
    /\ ign \in IgnoreOpts
    /\ user \in Users
    \* Keys()/ListKeys() always add IgnoreDeletes()+MetaOnly() on an LPS
    \* WatchAll (kv.go:1403, 1433); History() is a Keys-style reader of
    \* an IncludeHistory watch (kv.go:1499-1519).
    /\ user = "Keys" => \/ mode = "LPS" /\ ign       \* Keys()
                        \/ mode = "ALL" /\ ~ign      \* History()
    /\ user = "ListKeys" => mode = "LPS" /\ ign
    /\ upc = "wait"
    /\ lpc = "wait"
    /\ lkey = "none"
    /\ keysCh = <<>>
    /\ keysClosed = FALSE
    /\ uAband = FALSE
    /\ snap = [latest |-> [k \in Keys |-> 0], op |-> [k \in Keys |-> "PUT"],
               last |-> 0, floor |-> 0, collapse |-> 0, initSet |-> {}]
    /\ everLive = {}
    /\ touched = {}
    /\ sentRevs = {}
    /\ sentMax = [k \in Keys |-> 0]
    /\ lastRev = 0
    /\ markers = 0
    /\ gotKeys = {}
    /\ bad = {}

Created == setupPc # "init"

-----------------------------------------------------------------------------
(* Environment: other clients, the server's limits, faults.               *)

\* Append a message for k, then enforce MaxMsgsPerSubject = History.
AppendMsg(k, op) ==
    LET l1 == Append(log, [key |-> k, op |-> op, live |-> TRUE])
        live1 == {s \in 1..Len(l1) : l1[s].live /\ l1[s].key = k}
        oldest == MinOf(live1)
    IN IF Cardinality(live1) > History
       THEN [l1 EXCEPT ![oldest].live = FALSE]
       ELSE l1

EnvGuard == ~envDone /\ Len(log) < MaxSeq

Put(k) ==
    /\ EnvGuard
    /\ log' = AppendMsg(k, "PUT")
    /\ everLive' = IF Created THEN everLive \cup {k} ELSE everLive
    /\ UNCHANGED <<srv, net, budget, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   touched, sentRevs, sentMax, lastRev, markers, gotKeys, bad>>

\* kv.Delete: a DEL marker. kv.Purge: rollup (drops all prior) + PURGE marker.
Del(k, op) ==
    /\ EnvGuard
    /\ log' = IF op = "DEL" THEN AppendMsg(k, "DEL")
              ELSE Append([s \in 1..Len(log) |->
                              IF log[s].key = k THEN [log[s] EXCEPT !.live = FALSE]
                              ELSE log[s]],
                          [key |-> k, op |-> "PURGE", live |-> TRUE])
    /\ touched' = IF Created THEN touched \cup {k} ELSE touched
    /\ UNCHANGED <<srv, net, budget, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, sentRevs, sentMax, lastRev, markers, gotKeys, bad>>

\* Stored messages vanish without a new message being written. Two shapes
\* happen in practice: every message of a key goes (PurgeDeletes() with
\* keep=0 for old markers, kv.go:1567-1580; a stream purge by subject), or
\* the oldest message of the stream expires (MaxAge).
Removed(S) ==
    /\ ~envDone /\ budget.removes > 0
    /\ S # {}
    /\ log' = [s \in 1..Len(log) |-> IF s \in S THEN [log[s] EXCEPT !.live = FALSE]
                                    ELSE log[s]]
    /\ budget' = [budget EXCEPT !.removes = @ - 1]
    /\ touched' = IF Created THEN touched \cup {log[s].key : s \in S} ELSE touched
    /\ UNCHANGED <<srv, net, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, sentRevs, sentMax, lastRev, markers, gotKeys, bad>>

RemoveKey(k) == Removed(LiveOf(k))
Expire == LiveSeqs # {} /\ Removed({MinOf(LiveSeqs)})

\* A delivered message never reaches processMsg (lost with the TCP
\* connection on a reconnect, or AddMsgFilter in the Go tests).
NetDrop ==
    /\ ~envDone /\ budget.drops > 0
    /\ net # <<>> /\ ~Head(net).hb
    /\ net' = Tail(net)
    /\ budget' = [budget EXCEPT !.drops = @ - 1]
    /\ UNCHANGED <<log, srv, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

\* Server restart: the R1 memory ordered consumer is gone and whatever was
\* in flight on the connection is lost; the client reconnects at once and
\* re-sends its SUB (same sid), the stream is file backed and survives.
ServerRestart ==
    /\ ~envDone /\ budget.restarts > 0
    /\ setupPc \notin {"init", "info"}  \* (during creation: Watch returns an error)
    /\ srv' = [srv EXCEPT !.alive = FALSE]
    /\ net' = <<>>
    /\ budget' = [budget EXCEPT !.restarts = @ - 1]
    /\ UNCHANGED <<log, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

\* nats.Context(ctx) (kv.go:1350): js.go:2068-2073 unsubscribes on cancel.
Unsub ==
    /\ subClosed' = TRUE
    /\ srv' = [srv EXCEPT !.alive = FALSE]   \* jsi.dc: consumer deleted

CtxCancel ==
    /\ ~envDone /\ budget.cancels > 0
    /\ Created /\ setupPc # "info"     \* goroutine starts after subscribe returns
    /\ ~subClosed
    /\ Unsub
    /\ budget' = [budget EXCEPT !.cancels = @ - 1]
    /\ UNCHANGED <<log, net, envDone, gen, jsi, resetting, resetStart,
                   plist, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

EnvQuit ==
    /\ ~envDone
    /\ envDone' = TRUE
    /\ UNCHANGED <<log, srv, net, budget, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

-----------------------------------------------------------------------------
(* Server: ordered push consumer.                                          *)

\* One step of loopAndGatherMsgs/getNextMsg (consumer.go:5025-5040): pop
\* the LPS skip list (skipping vanished seqs) or take the next stored msg
\* at/after the cursor; the ack reply carries num_pending as delta.
Deliver(s, c2) ==
    /\ srv' = [c2 EXCEPT !.dseq = @ + 1]
    /\ net' = Append(net, [gen |-> srv.gen, hb |-> FALSE, dseq |-> srv.dseq + 1,
                           sseq |-> s, key |-> log[s].key, op |-> log[s].op,
                           delta |-> Cardinality(Pending(c2))])

ServerStep ==
    /\ srv.alive
    /\ Pending(srv) # {}
    /\ IF Len(srv.lss) > 0
       THEN LET s == Head(srv.lss)
                c2 == [srv EXCEPT !.lss = Tail(@)]
            IN IF log[s].live THEN Deliver(s, c2)
               ELSE srv' = c2 /\ UNCHANGED net
       ELSE LET s == MinOf({x \in LiveSeqs : x >= srv.next /\ log[x].key \in Filter})
            IN Deliver(s, [srv EXCEPT !.next = s + 1])
    /\ UNCHANGED <<log, budget, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

\* Idle heartbeat carrying Nats-Last-Consumer. Only the heartbeats that
\* reveal a gap are generated (the others are no-ops for the client).
ServerHB ==
    /\ srv.alive /\ srv.gen = gen /\ ~resetting /\ ~subClosed
    /\ Pending(srv) = {}
    /\ \A i \in 1..Len(net) : net[i].gen # srv.gen
    /\ srv.dseq >= jsi.dseq
    /\ net' = Append(net, [gen |-> srv.gen, hb |-> TRUE, dseq |-> 0, sseq |-> 0,
                           key |-> "none", op |-> "PUT", delta |-> srv.dseq])
    /\ UNCHANGED <<log, srv, budget, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, wmu, initDone, initPending,
                   received, ch, chClosed, wClosed, cbPc, cur, setupPc, mode,
                   ign, user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, markers,
                   gotKeys, bad>>

-----------------------------------------------------------------------------
(* Client readLoop: processMsg and the ordered-consumer checks.            *)

\* resetOrderedConsumer(jsi.sseq+1) (js.go:2274, 2309-2335): new sid now,
\* consumer recreation in a goroutine.
ResetBegin ==
    /\ gen' = gen + 1
    /\ resetting' = TRUE
    /\ resetStart' = jsi.sseq + 1
    /\ jsi' = [jsi EXCEPT !.named = FALSE]   \* jsi.consumer = "" (js.go:2376)

\* Would processing m start a reset?
TriggersReset(m) ==
    /\ ~subClosed /\ m.gen = gen /\ Created /\ ~resetting
    /\ IF m.hb THEN m.delta >= jsi.dseq ELSE m.dseq # jsi.dseq

ClientRecv ==
    /\ net # <<>>
    \* Abstraction: a gap is not acted upon before the consumer-create
    \* response has been processed (see README).
    /\ setupPc = "info" => ~TriggersReset(Head(net))
    /\ LET m == Head(net) IN
       /\ net' = Tail(net)
       /\ IF subClosed \/ m.gen # gen \/ ~Created
          THEN \* sub closed or old sid: nc.subs lookup misses (nats.go:3900)
               UNCHANGED <<gen, jsi, resetting, resetStart, plist>>
          ELSE IF m.hb
          THEN \* checkForSequenceMismatch (js.go:2572-2584)
               IF m.delta >= jsi.dseq /\ ~resetting
               THEN ResetBegin /\ UNCHANGED plist
               ELSE UNCHANGED <<gen, jsi, resetting, resetStart, plist>>
          ELSE IF m.dseq # jsi.dseq
          THEN \* checkOrderedMsgs gap (js.go:2193) -> tryResetOrderedConsumer
               IF ~resetting THEN ResetBegin /\ UNCHANGED plist
               ELSE UNCHANGED <<gen, jsi, resetting, resetStart, plist>>
          ELSE IF m.sseq <= jsi.skipLE
          THEN \* fix "reset": re-sent LPS entry already delivered
               /\ jsi' = [jsi EXCEPT !.dseq = m.dseq + 1]
               /\ UNCHANGED <<gen, resetting, resetStart, plist>>
          ELSE \* queue on the async list; commitOrderedMsg (js.go:2202-2207)
               /\ plist' = Append(plist, [key |-> m.key, rev |-> m.sseq,
                                          op |-> m.op, delta |-> m.delta])
               /\ jsi' = [jsi EXCEPT !.dseq = m.dseq + 1, !.sseq = m.sseq]
               /\ UNCHANGED <<gen, resetting, resetStart>>
    /\ UNCHANGED <<log, srv, budget, envDone, subClosed, pDoneSet, wmu,
                   initDone, initPending, received, ch, chClosed, wClosed, cbPc,
                   cur, setupPc, mode, ign, user, upc, lpc, lkey, keysCh,
                   keysClosed, uAband, snap, everLive, touched, sentRevs,
                   sentMax, lastRev, markers, gotKeys, bad>>

\* activityCheck (js.go:2501-2524): no heartbeats for 2*hbi while
\* connected -> reset. Enabled when the consumer serving our sid is gone.
HbTimeout ==
    /\ setupPc \notin {"init", "info"}
    /\ ~subClosed /\ ~resetting
    /\ ~(srv.alive /\ srv.gen = gen)
    /\ ResetBegin
    /\ UNCHANGED <<log, srv, net, budget, envDone, plist, subClosed, pDoneSet,
                   wmu, initDone, initPending, received, ch, chClosed, wClosed,
                   cbPc, cur, setupPc, mode, ign, user, upc, lpc, lkey, keysCh,
                   keysClosed, uAband, snap, everLive, touched, sentRevs,
                   sentMax, lastRev, markers, gotKeys, bad>>

\* The reset goroutine (js.go:2343-2410): jsi.dseq = 1, new consumer
\* DeliverByStartSequence(resetStart), jsi.consumer = name. If the sub was
\* closed meanwhile the consumer is deleted again (js.go:2404-2411).
\* Fix "reset": while the LPS initial phase is not over, recreate with
\* DeliverLastPerSubject and skip seqs <= jsi.sseq; otherwise never start
\* below the creation floor (ConsumerInfo.Delivered.Stream).
ResetCreate ==
    /\ resetting
    /\ resetting' = FALSE
    /\ LET lpsAgain == Fixed("reset") /\ mode = "LPS" /\ ~initDone
           start == IF Fixed("reset") THEN Max(resetStart, snap.floor + 1)
                    ELSE resetStart
           newC == NewConsumer(gen, Sorted(LpsSeqs), Len(log) + 1)
           \* entries still owed: already queued + what the new LPS consumer
           \* will hand over beyond the skip point
           owed == received + Len(plist) + (IF cbPc = "send" THEN 1 ELSE 0)
                   + Cardinality({s \in Pending(newC) : s > jsi.sseq})
       IN /\ jsi' = [jsi EXCEPT !.dseq = 1, !.named = TRUE,
                                !.skipLE = IF lpsAgain THEN jsi.sseq ELSE @,
                                !.pending = IF lpsAgain THEN owed ELSE @]
          /\ initPending' = IF lpsAgain /\ setupPc = "done" THEN owed ELSE initPending
          \* (the LPS restart collapses writes made so far to their latest
          \* revision, like LPS does for writes made before the watch)
          /\ snap' = IF lpsAgain THEN [snap EXCEPT !.collapse = Len(log)] ELSE snap
          /\ srv' = IF subClosed THEN DeadConsumer
                    ELSE IF lpsAgain THEN newC
                    ELSE NewConsumer(gen, <<>>, start)
    /\ UNCHANGED <<log, net, budget, envDone, gen, resetStart, plist, subClosed,
                   pDoneSet, wmu, initDone, received, ch, chClosed,
                   wClosed, cbPc, cur, setupPc, mode, ign, user, upc, lpc, lkey,
                   keysCh, keysClosed, uAband, everLive, touched,
                   sentRevs, sentMax, lastRev, markers, gotKeys, bad>>

-----------------------------------------------------------------------------
(* Ghost bookkeeping when the watcher puts something on w.updates.         *)

EntryChecks(e) ==
    LET between == {s \in (lastRev + 1)..(e.rev - 1) :
                        /\ s > Max(snap.last, snap.collapse)
                        /\ s >= Floor(log[s].key)
                        /\ log[s].live /\ log[s].key \in Filter
                        /\ ~(ign /\ IsDel(log[s].op))}
    IN  (IF e.rev <= lastRev THEN {"RevisionOrder"} ELSE {})
        \cup (IF e.rev < Floor(e.key) THEN {"NoStaleEntry"} ELSE {})
        \cup (IF between # {} /\ ~subClosed THEN {"NoLostUpdate"} ELSE {})
        \cup (IF chClosed THEN {"NoSendOnClosed"} ELSE {})

SendEntry(e) ==
    /\ ch' = Append(ch, [nil |-> FALSE, key |-> e.key, rev |-> e.rev, op |-> e.op])
    /\ bad' = bad \cup EntryChecks(e)
    /\ sentRevs' = sentRevs \cup {e.rev}
    /\ sentMax' = [sentMax EXCEPT ![e.key] = Max(@, e.rev)]
    /\ lastRev' = e.rev

\* MarkerComplete: every value the initial phase owes was delivered (or no
\* longer exists, or, for LPS, a newer revision of that key was delivered).
MarkerOk ==
    \A s \in snap.initSet :
        \/ s \in sentRevs
        \/ ~log[s].live
        \/ (mode = "LPS" /\ sentMax[log[s].key] > s)

SendMarker ==
    /\ ch' = Append(ch, [nil |-> TRUE, key |-> "none", rev |-> 0, op |-> "PUT"])
    /\ markers' = markers + 1
    /\ bad' = bad \cup (IF MarkerOk THEN {} ELSE {"MarkerComplete"})
                  \cup (IF markers >= 1 THEN {"MarkerAtMostOnce"} ELSE {})
                  \cup (IF mode = "NEW" THEN {"NoMarkerUpdatesOnly"} ELSE {})
                  \cup (IF chClosed THEN {"NoSendOnClosed"} ELSE {})

-----------------------------------------------------------------------------
(* waitForMsgs goroutine running the KV update callback.                   *)

CbUnchanged == UNCHANGED <<log, srv, net, budget, envDone, gen, jsi, resetting,
                   resetStart, subClosed, pDoneSet, setupPc, mode, ign, user,
                   upc, lpc, lkey, keysCh, keysClosed, uAband, snap, everLive,
                   touched, gotKeys>>

\* kv.go:1322-1330: initial-phase accounting, done right after the entry
\* was handed over (or skipped), still under w.mu. Ends the callback (w.mu
\* released) unless the marker is due.
Check(c) ==
    IF ~initDone /\ ~(Fixed("closedh") /\ wClosed)
    THEN /\ received' = received + 1
         /\ IF received + 1 >= initPending \/ c.delta = 0
            THEN initDone' = TRUE /\ cbPc' = "nil" /\ wmu' = "cb"
            ELSE cbPc' = "idle" /\ wmu' = "none" /\ UNCHANGED initDone
    ELSE cbPc' = "idle" /\ wmu' = "none" /\ UNCHANGED <<received, initDone>>

SkipEntry(c) == (ign /\ IsDel(c.op)) \/ (Fixed("closedh") /\ wClosed)

\* nats.go:3801-3815 pop + mcb(m); kv.go:1279-1308 parse, w.mu.Lock().
\* (Popping and taking w.mu are merged: nothing observable happens between.)
CbPop ==
    /\ cbPc = "idle" /\ ~subClosed /\ plist # <<>> /\ wmu = "none"
    /\ LET c == Head(plist) IN
       /\ cur' = c /\ plist' = Tail(plist)
       /\ IF SkipEntry(c)
          THEN Check(c)
          ELSE cbPc' = "send" /\ wmu' = "cb" /\ UNCHANGED <<received, initDone>>
    /\ CbUnchanged
    /\ UNCHANGED <<initPending, ch, chClosed, wClosed, sentRevs, sentMax,
                   lastRev, markers, bad>>

\* nats.go:3833-3836 + 3871-3877: closed -> leave the loop, call pDone.
CbExit ==
    /\ cbPc = "idle" /\ subClosed
    /\ cbPc' = IF pDoneSet THEN "closing" ELSE "done"
    /\ plist' = <<>>
    /\ CbUnchanged
    /\ UNCHANGED <<cur, wmu, initDone, initPending, received, ch, chClosed,
                   wClosed, sentRevs, sentMax, lastRev, markers, bad>>

\* Fix "stopdrain": a blocked send gives up once the watcher is stopped.
GiveUp == Fixed("stopdrain") /\ subClosed

\* kv.go:1319: w.updates <- entry (blocks while full), then Check.
CbSend ==
    /\ cbPc = "send"
    /\ \/ /\ Len(ch) < ChanCap
          /\ SendEntry(cur)
          /\ Check(cur)
       \/ /\ GiveUp /\ Len(ch) >= ChanCap
          /\ cbPc' = "idle" /\ wmu' = "none"
          /\ UNCHANGED <<ch, bad, sentRevs, sentMax, lastRev, received, initDone>>
    /\ CbUnchanged
    /\ UNCHANGED <<plist, cur, initPending, chClosed, wClosed, markers>>

\* kv.go:1328: w.updates <- nil.
CbNil ==
    /\ cbPc = "nil"
    /\ \/ /\ Len(ch) < ChanCap
          /\ SendMarker
       \/ /\ GiveUp /\ Len(ch) >= ChanCap
          /\ UNCHANGED <<ch, markers, bad>>
    /\ cbPc' = "idle" /\ wmu' = "none"
    /\ CbUnchanged
    /\ UNCHANGED <<plist, cur, initDone, initPending, received, chClosed,
                   wClosed, sentRevs, sentMax, lastRev>>

\* The closed handler (kv.go:1366-1370), run by waitForMsgs at exit.
CbClose ==
    /\ cbPc = "closing" /\ wmu = "none"
    /\ IF Fixed("closedh") /\ wClosed
       THEN UNCHANGED <<chClosed, bad>>
       ELSE /\ chClosed' = TRUE
            /\ bad' = bad \cup (IF chClosed THEN {"CloseOnce"} ELSE {})
    /\ wClosed' = TRUE
    /\ cbPc' = "done"
    /\ CbUnchanged
    /\ UNCHANGED <<plist, cur, wmu, initDone, initPending, received, ch,
                   sentRevs, sentMax, lastRev, markers>>

-----------------------------------------------------------------------------
(* The WatchFiltered goroutine (kv.go:1342-1386).                          *)

SetupUnchanged == UNCHANGED <<log, net, budget, envDone, resetting, resetStart,
                   plist, subClosed, cbPc, cur, mode, ign, user, upc, lpc, lkey,
                   keysCh, keysClosed, uAband, everLive, touched, sentRevs,
                   sentMax, lastRev, gotKeys>>

\* w.mu.Lock(); js.Subscribe: SUB is sent (nats.go subscribe) and the
\* consumer is created (js.go:1983-1990). The server picks its start set
\* atomically at creation and starts delivering at once.
SetupStart ==
    /\ setupPc = "init" /\ wmu = "none"
    /\ wmu' = "setup"
    /\ LET lss == IF mode = "LPS" THEN Sorted(LpsSeqs) ELSE <<>>
           next == CASE mode = "LPS"    -> Len(log) + 1
                     [] mode = "ALL"    -> 1
                     [] mode = "NEW"    -> Len(log) + 1
                     [] mode = "RESUME" -> ResumeRev
           initSet0 == CASE mode = "LPS" -> LpsSeqs
                         [] mode = "ALL" -> {s \in LiveSeqs : log[s].key \in Filter}
                         [] mode = "RESUME" -> {s \in LiveSeqs : log[s].key \in Filter
                                                                /\ s >= ResumeRev}
                         [] OTHER -> {}
           \* ConsumerInfo.Delivered.Stream at creation
           floor == CASE mode = "LPS"    -> Len(log)   \* once the initial phase is over
                      [] mode = "ALL"    -> 0
                      [] mode = "NEW"    -> Len(log)
                      [] mode = "RESUME" -> ResumeRev - 1
       IN /\ srv' = NewConsumer(0, lss, next)
          /\ snap' = [latest |-> [k \in Keys |-> IF k \in Filter THEN LatestOf(k) ELSE 0],
                      op |-> [k \in Keys |-> IF LatestOf(k) > 0
                                             THEN log[LatestOf(k)].op ELSE "PUT"],
                      last |-> Len(log), floor |-> floor, collapse |-> 0,
                      initSet |-> {s \in initSet0 : ~(ign /\ IsDel(log[s].op))}]
          /\ everLive' = {k \in Filter : LatestOf(k) > 0 /\ log[LatestOf(k)].op = "PUT"}
    /\ setupPc' = "info"
    /\ UNCHANGED <<log, net, budget, envDone, gen, jsi, resetting, resetStart,
                   plist, subClosed, pDoneSet, initDone, initPending, received,
                   ch, chClosed, wClosed, cbPc, cur, mode, ign, user, upc, lpc,
                   lkey, keysCh, keysClosed, uAband, touched, sentRevs, sentMax,
                   lastRev, markers, gotKeys, bad>>

\* The consumer-create response: jsi.pending = NumPending + Delivered
\* (js.go:2027), from the server state at the time the info is built
\* (deliveries and writes may already have happened).
SetupInfo ==
    /\ setupPc = "info"
    /\ jsi' = [jsi EXCEPT !.pending = Cardinality(Pending(srv)) + srv.dseq,
                          !.named = TRUE]
    /\ setupPc' = "closedh"
    /\ UNCHANGED <<srv, gen, pDoneSet, wmu, initDone, initPending, received, ch,
                   chClosed, wClosed, snap, markers, bad>>
    /\ SetupUnchanged

\* sub.SetClosedHandler (kv.go:1366). Fix "closedh": if the sub is already
\* closed, close the channel here (once) and mark the watcher closed.
SetupClosedH ==
    /\ setupPc = "closedh"
    /\ pDoneSet' = TRUE
    /\ IF Fixed("closedh") /\ subClosed
       THEN chClosed' = TRUE /\ wClosed' = TRUE
       ELSE UNCHANGED <<chClosed, wClosed>>
    /\ setupPc' = "pending"
    /\ UNCHANGED <<srv, gen, jsi, wmu, initDone, initPending, received, ch,
                   snap, markers, bad>>
    /\ SetupUnchanged

\* kv.go:1372-1385: InitialConsumerPending (js.go:2081-2088 errors while
\* jsi.consumer is ""), marker if 0 (w.updates is empty: cb needs w.mu).
SetupPending ==
    /\ setupPc = "pending"
    /\ CASE mode = "NEW" ->
              initDone' = TRUE /\ UNCHANGED <<initPending, ch, markers, bad>>
         [] ~jsi.named /\ ~Fixed("pendingerr") ->
              UNCHANGED <<initDone, initPending, ch, markers, bad>>
         [] jsi.pending = 0 /\ ~(Fixed("closedh") /\ wClosed) ->
              initDone' = TRUE /\ SendMarker /\ UNCHANGED initPending
         [] OTHER ->
              initPending' = jsi.pending /\ UNCHANGED <<initDone, ch, markers, bad>>
    \* w.sub = sub; return (deferred w.mu.Unlock()).
    /\ wmu' = "none" /\ setupPc' = "done"
    /\ UNCHANGED <<srv, gen, jsi, pDoneSet, received, chClosed, wClosed, snap>>
    /\ SetupUnchanged

-----------------------------------------------------------------------------
(* Fix "marker": when the consumer is idle and every delivered message has *)
(* been handled, a still-open initial phase is over (ConsumerInfo check).  *)
IdleMarker ==
    /\ Fixed("marker")
    /\ setupPc = "done" /\ ~initDone /\ ~subClosed /\ mode # "NEW"
    /\ srv.alive /\ srv.gen = gen /\ ~resetting /\ Pending(srv) = {}
    \* ConsumerInfo: NumPending = 0 and Delivered.Consumer equals what the
    \* client accepted from this consumer (no undetected gap) ...
    /\ srv.dseq = jsi.dseq - 1
    \* ... and the callback has handled all of it.
    /\ net = <<>> /\ plist = <<>> /\ cbPc = "idle" /\ wmu = "none"
    /\ Len(ch) < ChanCap
    /\ initDone' = TRUE
    /\ SendMarker
    /\ UNCHANGED <<log, srv, net, budget, envDone, gen, jsi, resetting,
                   resetStart, plist, subClosed, pDoneSet, wmu, initPending,
                   received, chClosed, wClosed, cbPc, cur, setupPc, mode, ign,
                   user, upc, lpc, lkey, keysCh, keysClosed, uAband, snap,
                   everLive, touched, sentRevs, sentMax, lastRev, gotKeys>>

-----------------------------------------------------------------------------
(* Users.                                                                  *)

UserUnchanged == UNCHANGED <<log, net, budget, envDone, gen, jsi, resetting,
                   resetStart, plist, pDoneSet, wmu, initDone, initPending,
                   received, chClosed, wClosed, cbPc, cur, setupPc, mode, ign,
                   user, snap, everLive, touched, sentRevs, sentMax, lastRev,
                   markers>>

\* Watch user: keeps draining Updates() (also after Stop, until closed).
WatchRead ==
    /\ user = "Watch" /\ setupPc = "done" /\ ch # <<>>
    /\ ch' = Tail(ch)
    /\ UNCHANGED <<srv, subClosed, upc, lpc, lkey, keysCh, keysClosed, uAband,
                   gotKeys, bad>>
    /\ UserUnchanged

WatchStop ==
    /\ user = "Watch" /\ UserStops /\ setupPc = "done" /\ upc = "wait"
    /\ upc' = "stopped"
    /\ IF subClosed THEN UNCHANGED <<subClosed, srv>> ELSE Unsub
    /\ UNCHANGED <<ch, lpc, lkey, keysCh, keysClosed, uAband, gotKeys, bad>>
    /\ UserUnchanged

\* Keys() (kv.go:1402-1424) / History(): range until nil, Stop, return.
KeysRead ==
    /\ user = "Keys" /\ setupPc = "done" /\ upc = "wait"
    /\ \/ /\ ch # <<>>
          /\ ch' = Tail(ch)
          /\ IF Head(ch).nil
             THEN upc' = "stop" /\ UNCHANGED gotKeys
             ELSE gotKeys' = gotKeys \cup {Head(ch).key} /\ UNCHANGED upc
          /\ UNCHANGED bad
       \/ /\ ch = <<>> /\ chClosed      \* range loop ends on close
          /\ upc' = "truncated"
          /\ bad' = bad \cup (IF Fixed("keysctx") THEN {} ELSE {"KeysNoSilentTruncation"})
          /\ UNCHANGED <<ch, gotKeys>>
    /\ UNCHANGED <<srv, subClosed, lpc, lkey, keysCh, keysClosed, uAband>>
    /\ UserUnchanged

\* KeysSound is about Keys() (mode LPS) results; a truncated return is
\* KeysNoSilentTruncation's business (with fix "keysctx" it is an error).
KeysReturn ==
    /\ user = "Keys" /\ upc \in {"stop", "truncated"}
    /\ upc' = "returned"
    /\ IF subClosed THEN UNCHANGED <<subClosed, srv>> ELSE Unsub   \* defer Stop
    /\ bad' = bad \cup (IF upc = "truncated" \/ mode # "LPS" \/ gotKeys \subseteq everLive
                        THEN {} ELSE {"KeysSound"})
    /\ UNCHANGED <<ch, lpc, lkey, keysCh, keysClosed, uAband, gotKeys>>
    /\ UserUnchanged

\* ListKeys() lister goroutine (kv.go:1440-1455).
ListerRecv ==
    /\ user = "ListKeys" /\ setupPc = "done" /\ lpc \in {"wait", "recv"}
    /\ \/ /\ ch # <<>>
          /\ ch' = Tail(ch)
          /\ IF Head(ch).nil
             THEN lpc' = "stop" /\ UNCHANGED lkey
             ELSE lpc' = "send" /\ lkey' = Head(ch).key
       \/ /\ ch = <<>> /\ chClosed /\ lpc' = "stop" /\ UNCHANGED <<ch, lkey>>
    /\ UNCHANGED <<srv, subClosed, upc, keysCh, keysClosed, uAband, gotKeys, bad>>
    /\ UserUnchanged

\* kl.keys <- key (blocks while full; fix "lister": select on stop).
ListerSend ==
    /\ lpc = "send"
    /\ \/ /\ Len(keysCh) < KeysCap
          /\ keysCh' = Append(keysCh, lkey) /\ lpc' = "recv"
       \/ /\ Fixed("lister") /\ subClosed /\ lpc' = "stop" /\ UNCHANGED keysCh
    /\ UNCHANGED <<ch, srv, subClosed, upc, lkey, keysClosed, uAband, gotKeys, bad>>
    /\ UserUnchanged

\* defer watcher.Stop(); defer close(kl.keys)
ListerStop ==
    /\ lpc = "stop"
    /\ IF subClosed THEN UNCHANGED <<subClosed, srv>> ELSE Unsub
    /\ keysClosed' = TRUE /\ lpc' = "done"
    /\ UNCHANGED <<ch, upc, lkey, keysCh, uAband, gotKeys, bad>>
    /\ UserUnchanged

ListUserRead ==
    /\ user = "ListKeys" /\ ~uAband /\ keysCh # <<>>
    /\ keysCh' = Tail(keysCh)
    /\ gotKeys' = gotKeys \cup {Head(keysCh)}
    /\ bad' = bad \cup (IF Head(keysCh) \in everLive THEN {} ELSE {"KeysSound"})
    /\ UNCHANGED <<ch, srv, subClosed, upc, lpc, lkey, keysClosed, uAband>>
    /\ UserUnchanged

\* kl.Stop() before all keys were read, then the caller moves on
\* (the KeyLister doc allows exactly this, kv.go:283-284).
ListUserStop ==
    /\ user = "ListKeys" /\ UserStops /\ setupPc = "done" /\ ~uAband /\ ~keysClosed
    /\ uAband' = TRUE
    /\ IF subClosed THEN UNCHANGED <<subClosed, srv>> ELSE Unsub
    /\ UNCHANGED <<ch, upc, lpc, lkey, keysCh, keysClosed, gotKeys, bad>>
    /\ UserUnchanged

-----------------------------------------------------------------------------
Env ==
    \/ \E k \in Keys : Put(k)
    \/ \E k \in Keys, op \in {"DEL", "PURGE"} : Del(k, op)
    \/ \E k \in Keys : RemoveKey(k)
    \/ Expire
    \/ NetDrop \/ ServerRestart \/ CtxCancel \/ EnvQuit

Sys ==
    \/ ServerStep \/ ServerHB
    \/ ClientRecv \/ HbTimeout \/ ResetCreate
    \/ CbPop \/ CbExit \/ CbSend \/ CbNil \/ CbClose
    \/ SetupStart \/ SetupInfo \/ SetupClosedH \/ SetupPending
    \/ IdleMarker
    \/ WatchRead \/ WatchStop \/ KeysRead \/ KeysReturn
    \/ ListerRecv \/ ListerSend \/ ListerStop \/ ListUserRead \/ ListUserStop

Next == Env \/ Sys

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ wmu \in {"none", "setup", "cb"}
    /\ Len(ch) <= ChanCap
    /\ Len(keysCh) <= KeysCap
    /\ markers \in 0..2

\* Safety, recorded at the step that violates them (see SendEntry etc.).
MarkerAtMostOnce    == "MarkerAtMostOnce" \notin bad
NoMarkerUpdatesOnly == "NoMarkerUpdatesOnly" \notin bad
MarkerComplete      == "MarkerComplete" \notin bad
RevisionOrder       == "RevisionOrder" \notin bad
NoStaleEntry        == "NoStaleEntry" \notin bad
NoLostUpdate        == "NoLostUpdate" \notin bad
NoSendOnClosed      == "NoSendOnClosed" \notin bad
CloseOnce           == "CloseOnce" \notin bad
KeysSound           == "KeysSound" \notin bad
KeysNoSilentTruncation == "KeysNoSilentTruncation" \notin bad

\* Quiescence: the environment has stopped and no process can move.
Quiescent == envDone /\ ~ENABLED Sys

\* The marker is never left owed: a live (not stopped) watcher whose
\* environment has gone quiet has sent it.
NoStuckInit ==
    Quiescent /\ setupPc = "done" /\ ~subClosed /\ mode # "NEW" => markers = 1

\* A quiescent live watcher has delivered the latest revision of every
\* watched key it is supposed to see.
Converged ==
    (Quiescent /\ user = "Watch" /\ setupPc = "done" /\ ~subClosed) =>
        \A k \in Filter :
            LET L == LatestOf(k) IN
            \/ L = 0
            \/ L < Floor(k)
            \/ (mode = "NEW" /\ L <= snap.last)
            \/ (ign /\ IsDel(log[L].op))
            \/ sentMax[k] >= L

\* After Stop (or ctx cancel) everything winds down: the callback goroutine
\* exits, Updates() is closed, and the ListKeys lister goroutine exits and
\* closes Keys().
NoLeakAfterStop ==
    (Quiescent /\ subClosed /\ Created) =>
        /\ cbPc = "done"
        /\ chClosed
        /\ (user = "ListKeys" => lpc = "done" /\ keysClosed)

Safety == /\ TypeOK /\ MarkerAtMostOnce /\ NoMarkerUpdatesOnly /\ MarkerComplete
          /\ RevisionOrder /\ NoStaleEntry /\ NoLostUpdate /\ NoSendOnClosed
          /\ CloseOnce /\ KeysSound /\ KeysNoSilentTruncation
=============================================================================
