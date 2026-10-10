---------------------------- MODULE RequestReply ----------------------------
(***************************************************************************)
(* Core request/reply in nats.go (nats.go, context.go).                    *)
(*                                                                         *)
(* New style (default): one wildcard "mux" subscription per connection     *)
(* (nc.respMux on "<prefix>.*"),  a map token -> buffered chan (nc.respMap),  *)
(* and nc.respHandler running on the mux subscription's delivery           *)
(* goroutine (waitForMsgs).                                                *)
(* Old style (UseOldRequestStyle): one sync subscription per request,      *)
(* AutoUnsubscribe(1), NextMsg, deferred Unsubscribe.                      *)
(*                                                                         *)
(* Every nc.mu critical section in the code is free of blocking            *)
(* operations, so each one is modeled as a single atomic action; the       *)
(* points where a goroutine RELEASES nc.mu and later re-acquires it are    *)
(* separate actions, which is where the interesting interleavings live.    *)
(* Line numbers refer to nats.go / context.go of the checked-out tree.     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Req,              \* requester goroutines
    NoReq,            \* model value used to build the "no token" marker
    MaxAttempts,      \* sequential requests per requester goroutine
    ChanCap,          \* RequestChanLen (nats.go:66) is 8; see README
    MaxReplies,       \* deliveries the responder side makes per request (2 = duplicate)
    Styles,           \* subset of {"new", "old"}
    WithTimeouts,     \* FALSE: RequestWithContext(context.Background()) - no deadline
    WithPubFail,      \* publish() may fail (bad subject, max payload, reconnect buf, ...)
    WithForeign,      \* replies whose subject lacks the mux prefix (respHandler fallback)
    WithClose,        \* nc.Close()
    MaxDisconnects,   \* connection drops (RECONNECTING) the environment may cause
    WithUserUnsubMux, \* user calls Unsubscribe on msg.Sub of a reply (== nc.respMux)
    FixLeak,          \* proposed fix for REQ-1 (delete token on every error exit)
    FixMuxReset       \* proposed fix for REQ-2 (removeSub resets nc.respMux)

Tokens == Req \X (1..MaxAttempts)
NoTok  == <<NoReq, 0>>
Kinds  == {"reply", "503"}
Msgs   == [tok : Tokens, kind : Kinds, prefixed : BOOLEAN]
Styles0 == {"none", "new", "old"}
Results == {"none", "ok", "noresp", "timeout", "closed", "err"}

VARIABLES
    status,     \* "CONNECTED" | "RECONNECTING" | "CLOSED"           (nc.status)
    respInit,   \* nc.respMap != nil                                (nats.go:5012)
    respMap,    \* DOMAIN of nc.respMap (one chan per token)        (nats.go:700)
    respMux,    \* sid of nc.respMux, 0 = nil                       (nats.go:699)
    muxCreated, \* number of mux subscriptions created
    subs,       \* sids in nc.subs                                  (client side)
    srvSubs,    \* sids the server has interest in
    oldSubs,    \* sids that are old-style per-request sync subs
    nextSid,
    ch,         \* [Tokens -> [buf, closed]] the per-request channel
    sends,      \* [Tokens -> Nat] sends performed on ch[t] by the client
    published,  \* tokens whose request reached the server
    pending,    \* tokens buffered in the reconnect pending buffer
    delivered,  \* [Tokens -> Nat] responses the server has sent for t
    pList,      \* mux subscription's async pending list (processMsg -> waitForMsgs)
    hpc, htgt, hmsg, \* respHandler goroutine
    pc, att,    \* requester goroutines: program counter, attempt number
    style, oldSid, \* per token: "new"/"old", and the old-style inbox sid
    result, got,
    disconnects,
    unsubs,     \* times the user unsubscribed the mux (REQ-2), at most once
    postUnsub,  \* tokens created after that unsubscribe
    panicked,   \* send on closed channel happened
    blocked     \* respHandler found its target channel full

vars == <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs, oldSubs,
          nextSid, ch, sends, published, pending, delivered, pList, hpc, htgt,
          hmsg, pc, att, style, oldSid, result, got, disconnects, unsubs,
          postUnsub, panicked, blocked>>

tok(r) == <<r, att[r]>>

NoMsg == [tok |-> NoTok, kind |-> "reply", prefixed |-> TRUE]

Init ==
    /\ status = "CONNECTED"
    /\ respInit = FALSE
    /\ respMap = {}
    /\ respMux = 0
    /\ muxCreated = 0
    /\ subs = {}
    /\ srvSubs = {}
    /\ oldSubs = {}
    /\ nextSid = 1
    /\ ch = [t \in Tokens |-> [buf |-> <<>>, closed |-> FALSE]]
    /\ sends = [t \in Tokens |-> 0]
    /\ published = {}
    /\ pending = {}
    /\ delivered = [t \in Tokens |-> 0]
    /\ pList = <<>>
    /\ hpc = "idle" /\ htgt = NoTok /\ hmsg = NoMsg
    /\ pc = [r \in Req |-> "idle"]
    /\ att = [r \in Req |-> 1]
    /\ style = [t \in Tokens |-> "none"]
    /\ oldSid = [t \in Tokens |-> 0]
    /\ result = [t \in Tokens |-> "none"]
    /\ got = [t \in Tokens |-> NoTok]
    /\ disconnects = 0
    /\ unsubs = 0
    /\ postUnsub = {}
    /\ panicked = FALSE
    /\ blocked = FALSE

\* Finish the current attempt with result res and move on.
Finish(r, res) ==
    /\ result' = [result EXCEPT ![tok(r)] = res]
    /\ pc' = [pc EXCEPT ![r] = "done"]

----------------------------------------------------------------------------
(* New-style requester: Request / RequestWithContext                       *)
(* createNewRequestAndSend nats.go:4839-4863, newRequest nats.go:4915-4939,*)
(* requestWithContext context.go:60-77.                                     *)
----------------------------------------------------------------------------

\* nats.go:4840-4857, one nc.mu critical section: newRespInbox (init on first
\* use, nats.go:5012), respMap[token] = mch, create the mux on first use.
NewCreate(r) ==
    /\ pc[r] = "idle" /\ "new" \in Styles
    /\ style' = [style EXCEPT ![tok(r)] = "new"]
    /\ respInit' = TRUE
    /\ ch' = [ch EXCEPT ![tok(r)] = [buf |-> <<>>, closed |-> FALSE]]
    /\ postUnsub' = IF unsubs > 0 THEN postUnsub \cup {tok(r)} ELSE postUnsub
    /\ IF respMux = 0
       THEN IF status = "CLOSED"           \* subscribeLocked: ErrConnectionClosed (nats.go:5164)
            THEN /\ respMap' = IF FixLeak THEN respMap ELSE respMap \cup {tok(r)}
                 /\ Finish(r, "err")
                 /\ UNCHANGED <<respMux, muxCreated, subs, srvSubs, nextSid>>
            ELSE /\ respMap' = respMap \cup {tok(r)}
                 /\ respMux' = nextSid
                 /\ muxCreated' = muxCreated + 1
                 /\ subs' = subs \cup {nextSid}
                 \* SUB is suppressed while reconnecting (nats.go:5220) and
                 \* resent by resendSubscriptions.
                 /\ srvSubs' = IF status = "CONNECTED" THEN srvSubs \cup {nextSid} ELSE srvSubs
                 /\ nextSid' = nextSid + 1
                 /\ pc' = [pc EXCEPT ![r] = "pub"]
                 /\ UNCHANGED result
       ELSE /\ respMap' = respMap \cup {tok(r)}
            /\ pc' = [pc EXCEPT ![r] = "pub"]
            /\ UNCHANGED <<respMux, muxCreated, subs, srvSubs, nextSid, result>>
    /\ UNCHANGED <<status, oldSubs, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, att, oldSid, got, disconnects, unsubs, panicked, blocked>>

\* nats.go:4860 -> publish nats.go:4681-4790 (its own nc.mu section).
NewPublishOk(r) ==
    /\ pc[r] = "pub" /\ style[tok(r)] = "new"
    /\ status # "CLOSED"
    /\ IF status = "CONNECTED"
       THEN /\ published' = published \cup {tok(r)} /\ UNCHANGED pending
       ELSE /\ pending' = pending \cup {tok(r)} /\ UNCHANGED published
    /\ pc' = [pc EXCEPT ![r] = "wait"]
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, ch, sends, delivered, pList, hpc, htgt, hmsg,
                   att, style, oldSid, result, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* publish returns an error: ErrConnectionClosed (nats.go:4705) or, with
\* WithPubFail, ErrBadSubject / ErrMaxPayload / ErrReconnectBufExceeded /
\* ErrHeadersNotSupported (nats.go:4685-4727). newRequest returns at
\* nats.go:4917 without deleting respMap[token].
NewPublishErr(r) ==
    /\ pc[r] = "pub" /\ style[tok(r)] = "new"
    /\ status = "CLOSED" \/ WithPubFail
    /\ respMap' = IF FixLeak THEN respMap \ {tok(r)} ELSE respMap
    /\ Finish(r, "err")
    /\ UNCHANGED <<status, respInit, respMux, muxCreated, subs, srvSubs, oldSubs,
                   nextSid, ch, sends, published, pending, delivered, pList, hpc,
                   htgt, hmsg, att, style, oldSid, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* select case msg, ok = <-mch with ok (nats.go:4924, context.go:67) and the
\* no-responders check (nats.go:4909, context.go:80).
NewRecv(r) ==
    /\ pc[r] = "wait" /\ style[tok(r)] = "new"
    /\ ch[tok(r)].buf # <<>>
    /\ LET m == Head(ch[tok(r)].buf) IN
       /\ ch' = [ch EXCEPT ![tok(r)].buf = Tail(@)]
       /\ got' = [got EXCEPT ![tok(r)] = m.tok]
       /\ Finish(r, IF m.kind = "503" THEN "noresp" ELSE "ok")
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, att, style, oldSid, disconnects, panicked, blocked, unsubs, postUnsub>>

\* !ok: channel closed by clearPendingRequestCalls -> ErrConnectionClosed
\* (nats.go:4925, context.go:68).
NewRecvClosed(r) ==
    /\ pc[r] = "wait" /\ style[tok(r)] = "new"
    /\ ch[tok(r)].buf = <<>> /\ ch[tok(r)].closed
    /\ Finish(r, "closed")
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, ch, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, att, style, oldSid, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* case <-t.C (nats.go:4931) / <-ctx.Done() (context.go:72). A timer or
\* deadline can be ready at any point, so the select may pick it whenever
\* the requester is waiting, even if mch is also ready.
NewTimeout(r) ==
    /\ pc[r] = "wait" /\ style[tok(r)] = "new" /\ WithTimeouts
    /\ pc' = [pc EXCEPT ![r] = "tdel"]
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, ch, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, att, style, oldSid, result, got, disconnects,
                   panicked, blocked, unsubs, postUnsub>>

\* nats.go:4932-4935 / context.go:73-75: re-acquire nc.mu, delete, return.
NewTimeoutDelete(r) ==
    /\ pc[r] = "tdel"
    /\ respMap' = respMap \ {tok(r)}
    /\ Finish(r, "timeout")
    /\ UNCHANGED <<status, respInit, respMux, muxCreated, subs, srvSubs, oldSubs,
                   nextSid, ch, sends, published, pending, delivered, pList, hpc,
                   htgt, hmsg, att, style, oldSid, got, disconnects, panicked, blocked, unsubs, postUnsub>>

----------------------------------------------------------------------------
(* Old-style requester: oldRequest nats.go:4945-4962,                      *)
(* oldRequestWithContext context.go:87-103.                                *)
----------------------------------------------------------------------------

\* nc.subscribe(inbox, ..., ch, isSync=true) + s.AutoUnsubscribe(1)
\* (nats.go:4949-4954). The server honors the max of 1.
OldSubscribe(r) ==
    /\ pc[r] = "idle" /\ "old" \in Styles
    /\ style' = [style EXCEPT ![tok(r)] = "old"]
    /\ ch' = [ch EXCEPT ![tok(r)] = [buf |-> <<>>, closed |-> FALSE]]
    /\ IF status = "CLOSED"
       THEN /\ Finish(r, "err")
            /\ UNCHANGED <<subs, srvSubs, oldSubs, nextSid, oldSid>>
       ELSE /\ subs' = subs \cup {nextSid}
            /\ oldSubs' = oldSubs \cup {nextSid}
            /\ srvSubs' = IF status = "CONNECTED" THEN srvSubs \cup {nextSid} ELSE srvSubs
            /\ oldSid' = [oldSid EXCEPT ![tok(r)] = nextSid]
            /\ nextSid' = nextSid + 1
            /\ pc' = [pc EXCEPT ![r] = "opub"]
            /\ UNCHANGED result
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, sends, published,
                   pending, delivered, pList, hpc, htgt, hmsg, att, got,
                   disconnects, panicked, blocked, unsubs, postUnsub>>

OldPublish(r) ==
    /\ pc[r] = "opub"
    /\ \/ /\ status # "CLOSED"
          /\ IF status = "CONNECTED"
             THEN /\ published' = published \cup {tok(r)} /\ UNCHANGED pending
             ELSE /\ pending' = pending \cup {tok(r)} /\ UNCHANGED published
          /\ pc' = [pc EXCEPT ![r] = "owait"]
          /\ UNCHANGED result
       \/ /\ status = "CLOSED" \/ WithPubFail
          \* return err; deferred s.Unsubscribe() still runs (nats.go:4955)
          /\ result' = [result EXCEPT ![tok(r)] = "err"]
          /\ pc' = [pc EXCEPT ![r] = "ounsub"]
          /\ UNCHANGED <<published, pending>>
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, ch, sends, delivered, pList, hpc, htgt, hmsg,
                   att, style, oldSid, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* s.NextMsg(timeout) nats.go:5622-5690; 503 -> ErrNoResponders in
\* processNextMsgDelivered (nats.go:5842).
OldRecv(r) ==
    /\ pc[r] = "owait"
    /\ \/ /\ ch[tok(r)].buf # <<>>
          /\ LET m == Head(ch[tok(r)].buf) IN
             /\ ch' = [ch EXCEPT ![tok(r)].buf = Tail(@)]
             /\ got' = [got EXCEPT ![tok(r)] = m.tok]
             /\ result' = [result EXCEPT ![tok(r)] = IF m.kind = "503" THEN "noresp" ELSE "ok"]
       \/ /\ ch[tok(r)].buf = <<>> /\ ch[tok(r)].closed
          /\ result' = [result EXCEPT ![tok(r)] = "closed"]
          /\ UNCHANGED <<ch, got>>
       \/ /\ WithTimeouts
          /\ result' = [result EXCEPT ![tok(r)] = "timeout"]
          /\ UNCHANGED <<ch, got>>
    /\ pc' = [pc EXCEPT ![r] = "ounsub"]
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, att, style, oldSid, disconnects, panicked, blocked, unsubs, postUnsub>>

\* deferred s.Unsubscribe() -> removeSub (nats.go:5237), which closes the
\* sync sub's mch (nats.go:5243).
OldUnsub(r) ==
    /\ pc[r] = "ounsub"
    /\ subs' = subs \ {oldSid[tok(r)]}
    /\ srvSubs' = srvSubs \ {oldSid[tok(r)]}
    /\ ch' = IF oldSid[tok(r)] \in subs THEN [ch EXCEPT ![tok(r)].closed = TRUE] ELSE ch
    /\ pc' = [pc EXCEPT ![r] = "done"]
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, oldSubs, nextSid,
                   sends, published, pending, delivered, pList, hpc, htgt, hmsg, att,
                   style, oldSid, result, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* Next request on the same goroutine.
NextAttempt(r) ==
    /\ pc[r] = "done"
    /\ IF att[r] < MaxAttempts
       THEN /\ att' = [att EXCEPT ![r] = @ + 1]
            /\ pc' = [pc EXCEPT ![r] = "idle"]
       ELSE /\ pc' = [pc EXCEPT ![r] = "fin"]
            /\ UNCHANGED att
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, ch, sends, published, pending, delivered, pList,
                   hpc, htgt, hmsg, style, oldSid, result, got, disconnects,
                   panicked, blocked, unsubs, postUnsub>>

----------------------------------------------------------------------------
(* respHandler on the mux subscription's goroutine (waitForMsgs           *)
(* nats.go:3786, respHandler nats.go:4799-4836).                          *)
----------------------------------------------------------------------------

MuxLive == respMux # 0 /\ respMux \in subs

\* Pop a message (waitForMsgs, nats.go:3804-3816) and run the nc.mu section
\* of respHandler (nats.go:4800-4826). Popping and locking are merged: only
\* the delivery path touches pList, so nothing observable can interleave.
HandlerLookup ==
    /\ hpc = "idle" /\ MuxLive /\ pList # <<>>
    /\ LET m == Head(pList)
           rt == IF m.prefixed THEN m.tok ELSE NoTok   \* respToken nats.go:5041
       IN
       /\ pList' = Tail(pList)
       /\ hmsg' = m
       /\ CASE status = "CLOSED" ->              \* nats.go:4802
                 /\ hpc' = "idle" /\ htgt' = NoTok /\ UNCHANGED respMap
            [] rt # NoTok ->                       \* nats.go:4812-4815
                 /\ htgt' = IF rt \in respMap THEN rt ELSE NoTok
                 /\ respMap' = respMap \ {rt}
                 /\ hpc' = "send"
            [] Cardinality(respMap) = 1 ->         \* fallback nats.go:4816-4824
                 /\ htgt' = CHOOSE t \in respMap : TRUE
                 /\ respMap' = {}
                 /\ hpc' = "send"
            [] OTHER ->
                 /\ htgt' = NoTok /\ hpc' = "send" /\ UNCHANGED respMap
    /\ UNCHANGED <<status, respInit, respMux, muxCreated, subs, srvSubs, oldSubs,
                   nextSid, ch, sends, published, pending, delivered, pc, att, style,
                   oldSid, result, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* select { case mch <- m: default: } after nc.mu is released (nats.go:4831).
\* A nil mch makes the send case never ready.
HandlerSend ==
    /\ hpc = "send"
    /\ IF htgt = NoTok
       THEN UNCHANGED <<ch, sends, panicked, blocked>>
       ELSE IF ch[htgt].closed
       THEN /\ panicked' = TRUE /\ UNCHANGED <<ch, sends, blocked>>
       ELSE IF Len(ch[htgt].buf) < ChanCap
       THEN /\ ch' = [ch EXCEPT ![htgt].buf = Append(@, hmsg)]
            /\ sends' = [sends EXCEPT ![htgt] = @ + 1]
            /\ UNCHANGED <<panicked, blocked>>
       ELSE /\ blocked' = TRUE /\ UNCHANGED <<ch, sends, panicked>>
    /\ hpc' = "idle" /\ htgt' = NoTok /\ hmsg' = NoMsg
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, published, pending, delivered, pList, pc, att,
                   style, oldSid, result, got, disconnects, unsubs, postUnsub>>

----------------------------------------------------------------------------
(* Environment: responders, the server, the network, the user.           *)
----------------------------------------------------------------------------

OwnerStyle(t) == style[t]

\* A responder (or the server's no-responders 503, or a subject-rewriting
\* hop when prefixed = FALSE) sends a message to t's reply subject; the
\* server routes it to the matching client subscription and the client's
\* readLoop runs processMsg (nats.go:3888): drop when the sid is not in
\* nc.subs, otherwise push on the async pList or the sync sub's mch.
Deliver(t, kind, prefixed) ==
    /\ status = "CONNECTED"
    /\ t \in published
    /\ delivered[t] < MaxReplies
    /\ prefixed \/ WithForeign
    /\ delivered' = [delivered EXCEPT ![t] = @ + 1]
    /\ LET m == [tok |-> t, kind |-> kind, prefixed |-> prefixed] IN
       IF OwnerStyle(t) = "new"
       THEN /\ respMux \in srvSubs /\ respMux \in subs
            /\ pList' = Append(pList, m)
            /\ UNCHANGED ch
       ELSE \* old style: the inbox sub, server-side max 1 (AutoUnsubscribe(1))
            /\ prefixed
            /\ oldSid[t] \in srvSubs /\ oldSid[t] \in subs
            /\ delivered[t] = 0
            \* sync sub, pMsgsLimit = cap(ch) = RequestChanLen: never full here
            /\ ch' = [ch EXCEPT ![t].buf = Append(@, m)]
            /\ UNCHANGED pList
    /\ UNCHANGED <<status, respInit, respMap, respMux, muxCreated, subs, srvSubs,
                   oldSubs, nextSid, sends, published, pending, hpc, htgt, hmsg, pc,
                   att, style, oldSid, result, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* The connection drops: the server forgets our subscriptions.
Disconnect ==
    /\ status = "CONNECTED" /\ disconnects < MaxDisconnects
    /\ status' = "RECONNECTING"
    /\ srvSubs' = {}
    /\ disconnects' = disconnects + 1
    /\ UNCHANGED <<respInit, respMap, respMux, muxCreated, subs, oldSubs, nextSid, ch,
                   sends, published, pending, delivered, pList, hpc, htgt, hmsg, pc,
                   att, style, oldSid, result, got, panicked, blocked, unsubs, postUnsub>>

\* doReconnect nats.go:3588-3591: resendSubscriptions, then the pending
\* buffer is flushed - SUBs reach the server before the buffered PUBs.
Reconnect ==
    /\ status = "RECONNECTING"
    /\ status' = "CONNECTED"
    /\ srvSubs' = subs
    /\ published' = published \cup pending
    /\ pending' = {}
    /\ UNCHANGED <<respInit, respMap, respMux, muxCreated, subs, oldSubs, nextSid, ch,
                   sends, delivered, pList, hpc, htgt, hmsg, pc, att, style, oldSid,
                   result, got, disconnects, panicked, blocked, unsubs, postUnsub>>

\* nc.close nats.go:6202-6297: one nc.mu critical section.
\* clearPendingRequestCalls closes every chan still in respMap (6190-6197);
\* the subs loop closes every sync sub's mch and marks subs closed (6247-6278).
Close ==
    /\ WithClose /\ status # "CLOSED"
    /\ status' = "CLOSED"
    /\ ch' = [t \in Tokens |->
                IF t \in respMap
                   \/ (OwnerStyle(t) = "old" /\ oldSid[t] \in subs)
                THEN [ch[t] EXCEPT !.closed = TRUE]
                ELSE ch[t]]
    /\ respMap' = {}
    /\ subs' = {}
    /\ srvSubs' = {}
    /\ pList' = <<>>
    /\ pending' = {}
    /\ UNCHANGED <<respInit, respMux, muxCreated, oldSubs, nextSid, sends, published,
                   delivered, hpc, htgt, hmsg, pc, att, style, oldSid, result, got,
                   disconnects, panicked, blocked, unsubs, postUnsub>>

\* REQ-2 environment: the user holds a reply whose Msg.Sub is nc.respMux
\* (processMsg sets m.Sub = sub, nats.go:3951) and calls Unsubscribe on it
\* (Subscription.Unsubscribe nats.go:5441 -> unsubscribe 5551 -> removeSub
\* 5237). nc.respMux is never reset.
UserUnsubMux ==
    /\ WithUserUnsubMux /\ status # "CLOSED" /\ unsubs = 0
    /\ MuxLive
    /\ \E t \in Tokens : OwnerStyle(t) = "new" /\ result[t] = "ok"
    /\ subs' = subs \ {respMux}
    /\ srvSubs' = srvSubs \ {respMux}
    /\ pList' = <<>>          \* waitForMsgs exits on s.closed (nats.go:3843)
    /\ respMux' = IF FixMuxReset THEN 0 ELSE respMux
    /\ unsubs' = unsubs + 1
    /\ postUnsub' = {}
    /\ UNCHANGED <<status, respInit, respMap, muxCreated, oldSubs, nextSid, ch, sends,
                   published, pending, delivered, hpc, htgt, hmsg, pc, att, style,
                   oldSid, result, got, disconnects, panicked, blocked>>

----------------------------------------------------------------------------

Requester(r) ==
    \/ NewCreate(r) \/ NewPublishOk(r) \/ NewPublishErr(r) \/ NewRecv(r)
    \/ NewRecvClosed(r) \/ NewTimeout(r) \/ NewTimeoutDelete(r)
    \/ OldSubscribe(r) \/ OldPublish(r) \/ OldRecv(r) \/ OldUnsub(r)
    \/ NextAttempt(r)

Env ==
    \/ \E t \in Tokens, k \in Kinds, p \in BOOLEAN : Deliver(t, k, p)
    \/ Disconnect \/ Reconnect \/ Close \/ UserUnsubMux

Next ==
    \/ \E r \in Req : Requester(r)
    \/ HandlerLookup \/ HandlerSend
    \/ Env

\* Fairness: every goroutine keeps running; responders eventually answer a
\* request the server can route (weak fairness on prefixed replies); the
\* connection comes back. Close, cuts, duplicates, foreign subjects and
\* unsubscribes are never forced.
Fairness ==
    /\ \A r \in Req : WF_vars(Requester(r))
    /\ WF_vars(HandlerLookup) /\ WF_vars(HandlerSend)
    /\ WF_vars(Reconnect)
    /\ \A t \in Tokens : WF_vars(Deliver(t, "reply", TRUE))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Requesters are interchangeable (safety-only configs).
Symmetry == Permutations(Req)

----------------------------------------------------------------------------
(* Invariants                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ status \in {"CONNECTED", "RECONNECTING", "CLOSED"}
    /\ respMap \subseteq Tokens
    /\ pc \in [Req -> {"idle", "pub", "wait", "tdel", "opub", "owait", "ounsub", "done", "fin"}]
    /\ result \in [Tokens -> Results]
    /\ hpc \in {"idle", "send"}

\* The reply handler never sends on a closed channel (would panic the
\* subscription goroutine).
NoSendOnClosed == ~panicked

\* The reply handler never finds its target channel full (it would drop the
\* reply in its select default) and never sends twice on one channel.
HandlerNeverBlocks == ~blocked
OneSendPerRequest == \A t \in Tokens : sends[t] <= 1

\* A request that returns a message got the message addressed to it.
NoMisdelivery ==
    \A t \in Tokens : result[t] \in {"ok", "noresp"} => got[t] = t

\* Every exit path removes the request's respMap entry (no leak). Leftovers
\* on a CLOSED connection are unreachable garbage and are not counted.
NoLeak ==
    status # "CLOSED" =>
        \A t \in Tokens : result[t] # "none" => t \notin respMap

\* After Close, every waiting requester is released: its channel is closed
\* or holds a message, or the handler is about to hand it one.
CloseReleasesWaiters ==
    status = "CLOSED" =>
        \A r \in Req :
            /\ pc[r] = "wait" =>
                 \/ ch[tok(r)].closed
                 \/ ch[tok(r)].buf # <<>>
                 \/ (hpc = "send" /\ htgt = tok(r))
            /\ pc[r] = "owait" =>
                 ch[tok(r)].closed \/ ch[tok(r)].buf # <<>>

\* The first request(s) - also concurrent ones - create the mux exactly once.
MuxCreatedOnce == muxCreated <= 1

\* While the connection is usable, nc.respMux, once set, names a live
\* subscription (otherwise every later new-style request is blackholed).
MuxValid == (status # "CLOSED" /\ respMux # 0) => respMux \in subs

----------------------------------------------------------------------------
(* Liveness                                                                *)
----------------------------------------------------------------------------

\* Every request goroutine finishes all its requests.
AllRequestsReturn == <>(\A r \in Req : pc[r] = "fin")

\* With no deadline (WithTimeouts = FALSE), a request that was sent while a
\* responder is present eventually gets its reply.
WaitersServed == \A r \in Req : (pc[r] = "wait") ~> (pc[r] # "wait")

\* REQ-2: the same, for requests issued AFTER the user unsubscribed the mux
\* (requests in flight at that moment may legitimately lose their reply).
LaterWaitersServed ==
    \A r \in Req : (pc[r] = "wait" /\ tok(r) \in postUnsub) ~> (pc[r] # "wait")

=============================================================================
