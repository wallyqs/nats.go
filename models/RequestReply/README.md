# RequestReply — core request/reply in nats.go

A TLA+ model of request/reply in `nats.go` and `context.go`, and the Go
tests in [`modeltest/reqrep`](../../modeltest/reqrep) that check its
invariants against the real client.

## Scope

| Covered | Where |
|---|---|
| New-style multiplexed requests: `createNewRequestAndSend`, `newRespInbox` / `initNewResp`, the `respMux` subscription, `respMap`, `respToken`, `respHandler` | `nats.go:4796-5046` |
| `Request`, `RequestMsg`, `RequestWithContext`, `RequestMsgWithContext` | `nats.go:4867-4940`, `context.go:21-84` |
| Old-style requests (`UseOldRequestStyle`): `oldRequest`, `oldRequestWithContext` | `nats.go:4945-4962`, `context.go:87-103` |
| No-responders (503) detection | `nats.go:4909`, `context.go:80`, `nats.go:5842` |
| Timeouts / context cancelation, late and duplicate replies | `nats.go:4921-4939`, `context.go:65-77` |
| Requests across a disconnect and reconnect: SUB suppressed while reconnecting, `resendSubscriptions` before the pending buffer is flushed | `nats.go:5220`, `3588-3591` |
| `Close` while requests are in flight: `clearPendingRequestCalls`, the sync-subscription loop | `nats.go:6190-6197`, `6247-6278` |
| Losing the mux subscription (`Msg.Sub` of a reply is the mux) | `nats.go:3951`, `5237`, `5441`, `5527` |

There is no `RequestMany` in this checkout. The `micro` package (the
responder side) is **not** covered.

## Model

[`RequestReply.tla`](RequestReply.tla). Processes: requester goroutines
(each makes `MaxAttempts` requests one after another, choosing the new or
the old style per request), the mux subscription's delivery goroutine
running `respHandler`, and the environment (responders, server, network,
user).

Every `nc.mu` critical section on these paths is free of blocking
operations (no channel sends, no I/O waits), so each critical section is
one atomic action. The interleavings that matter are where a goroutine
*releases* `nc.mu` and later takes it again, and those are separate
actions: register-then-publish in `createNewRequestAndSend`, the select
and the delete on timeout, and the lookup (under the lock) and the send
(after the lock) in `respHandler`.

| Action | Code |
|---|---|
| `NewCreate(r)` — `newRespInbox` (lazy `initNewResp`), `respMap[token] = mch`, create the mux on first use; `subscribeLocked` fails when CLOSED | `nats.go:4840-4857`, `5011-5013`, `5152-5168` |
| `NewPublishOk(r)` / `NewPublishErr(r)` — `publish` succeeds (into the pending buffer while RECONNECTING) or returns an error; `newRequest` returns it at once | `nats.go:4860-4862`, `4681-4727`, `4916-4919`, `context.go:60-63` |
| `NewRecv(r)` — `case msg, ok = <-mch` with `ok`, then the 503 check | `nats.go:4924`, `4909`; `context.go:67`, `80` |
| `NewRecvClosed(r)` — `!ok`: `ErrConnectionClosed` | `nats.go:4925`, `context.go:68` |
| `NewTimeout(r)` — `case <-t.C` / `case <-ctx.Done()` (may win even when `mch` is ready) | `nats.go:4931`, `context.go:72` |
| `NewTimeoutDelete(r)` — re-lock, `delete(respMap, token)` | `nats.go:4932-4935`, `context.go:73-75` |
| `OldSubscribe(r)` — `nc.subscribe(inbox, …, ch, isSync)` + `AutoUnsubscribe(1)` | `nats.go:4946-4954` |
| `OldPublish(r)`, `OldRecv(r)` (message / closed / timeout), `OldUnsub(r)` (deferred `Unsubscribe` → `removeSub` closes the sync `mch`) | `nats.go:4955-4962`, `5622-5690`, `5237-5244` |
| `HandlerLookup` — `waitForMsgs` pops; `respHandler` under `nc.mu`: closed check, `respToken`, lookup + delete, single-entry fallback | `nats.go:3804-3816`, `4800-4826`, `5041-5046` |
| `HandlerSend` — `select { case mch <- m: default: }` after unlocking | `nats.go:4831-4835` |
| `Deliver(t, kind, prefixed)` — a responder's reply / the server's 503 / a reply whose subject lacks the mux prefix; `processMsg` drops it when the sid is not in `nc.subs`, else appends to the mux `pList` or the sync sub's `mch` | `nats.go:3888-3905`, `4052-4070` |
| `Disconnect`, `Reconnect` — server forgets the subscriptions; `resendSubscriptions` then flush pending | `nats.go:3588-3591`, `6141` |
| `Close` — one `nc.mu` section: `clearPendingRequestCalls`, close sync subs, `subs = nil` | `nats.go:6202-6297` |
| `UserUnsubMux` — the caller unsubscribes `reply.Sub`, which is `nc.respMux` | `nats.go:3951`, `5441` → `5551` → `5237` |

The constants `FixLeak` and `FixMuxReset` switch on the fixes for REQ-1 and
REQ-2 (applied on branch `fix/conn`) so the same model checks them. They only
make a difference when publish errors (`WithPubFail`) or the mux unsubscribe
(`WithUserUnsubMux`) are enabled: `leak_fixed.cfg`, `muxunsub_fixed.cfg` and
`muxunsub_fixed_live.cfg` run with the fix on and pass; the baseline configs
do not enable those actions.

### Invariants and properties

| Name | Meaning |
|---|---|
| `NoSendOnClosed` | `respHandler` never sends on a closed channel (that would panic the delivery goroutine) |
| `HandlerNeverBlocks`, `OneSendPerRequest` | the target channel is never full when the handler sends, and each request's channel is sent on at most once |
| `NoMisdelivery` | a request that returns a message got the one addressed to it |
| `NoLeak` | every exit path of a request removes its `respMap` entry (on a CLOSED connection leftovers are unreachable and not counted) |
| `CloseReleasesWaiters` | after `Close` every waiting requester can return: its channel is closed or holds a message, or the handler is about to hand it one |
| `MuxCreatedOnce` | the first requests, including concurrent ones, create the mux exactly once |
| `MuxValid` | while the connection is open, a non-nil `nc.respMux` is a live subscription |
| `AllRequestsReturn` (liveness) | with timeouts, every requester goroutine finishes |
| `WaitersServed` (liveness) | with no deadline (`RequestWithContext(context.Background())`) and a responder that answers, every waiting request is eventually served, across a connection drop |
| `LaterWaitersServed` (liveness) | the same for requests issued after `UserUnsubMux` |

Fairness: weak fairness for every goroutine, for `Reconnect`, and for a
responder's prefixed reply to every request the server can route. Close,
cuts, duplicates, 503s, foreign subjects and unsubscribes are never forced.

### Abstractions and limitations

- `RequestChanLen` is 8 (`nats.go:66`); the model uses `ChanCap = 1`.
  Because `respHandler` deletes the entry under `nc.mu` before sending,
  `OneSendPerRequest` holds, so any capacity of at least 1 behaves the same;
  `HandlerNeverBlocks` would catch the contrary at capacity 1.
- Tokens are unique per request (`<<goroutine, attempt>>`). The real token
  is 8 base-62 digits from a per-connection `math/rand` (`nats.go:5020`);
  a collision between two in-flight requests (≈ 1 in 2·10¹⁴ per pair) would
  overwrite the first request's channel. Not modeled.
- `RequestWithContext` and `Request` share the same structure (select on
  `mch` vs. timer/`ctx.Done()`, then delete); one set of actions covers
  both. The early `ctx.Err()` check (`context.go:49`) is not modeled.
- The server is lossless while connected: a request it has received is
  answered after a reconnect if the responder still replies. In reality a
  request in the connection being cut can be lost; with a deadline that is
  a timeout, without one it is a hang by design (no retransmission), so the
  liveness configs leave out what is out of the client's hands.
- Server-side max for `AutoUnsubscribe(1)` is folded into the old-style
  subscribe. Slow-consumer drops on the mux (512k-message default limit)
  are not modeled.
- `Conn.Drain` is not in the TLA+ model (it is in the ConnLifecycle model);
  its interaction with REQ-2 is shown by a Go test.
- With `FixMuxReset`, a single variable models the handler goroutine, so an
  old mux goroutine finishing a send and the new mux goroutine are
  serialized — a slight under-approximation of the fixed code.

## TLC results

`models/tlc.sh RequestReply`; TLC 2026.10.06, 4 workers, measured on this
machine (12 configs, about 6.5 minutes in total). Re-run on `fix/conn`: the
same results and state counts, 2m21s in total. Passing configs use
`SYMMETRY` over the requesters where only safety is checked.

| Config | What | Result |
|---|---|---|
| `RequestReply.cfg` | new style, 2 goroutines × 2 requests, replies + 503s, 1 drop, Close | pass — 554,586 distinct states, depth 35, 44 s |
| `oldstyle.cfg` | old + new style mixed, duplicates, 1 drop, Close | pass — 83,531 distinct states, depth 25, 12 s |
| `dup.cfg` | duplicate replies, reply + 503, late replies, 1 drop, Close | pass — 49,695 distinct states, depth 25, 9 s |
| `termination.cfg` | `AllRequestsReturn` with timeouts and Close | pass — 37,440 distinct states, depth 19, 19 s |
| `liveness.cfg` | `WaitersServed`, `AllRequestsReturn`, no deadline, 1 drop, both styles | pass — 112,254 distinct states, depth 31, 1 min 26 s |
| `leak_fixed.cfg` | REQ-1 fix, all invariants, publish errors + drop + Close | pass — 699,676 distinct states, depth 35, 55 s |
| `muxunsub_fixed.cfg` | REQ-2 fix, safety | pass — 1,177,124 distinct states, depth 35, 1 min 36 s |
| `muxunsub_fixed_live.cfg` | REQ-2 fix, `LaterWaitersServed` | pass — 6,032 distinct states, depth 30, 7 s |
| `leak_bug.cfg` | REQ-1 | `NoLeak` violated (3-state trace), 1 s |
| `muxunsub_bug.cfg` | REQ-2 | `MuxValid` violated, 2 s |
| `muxunsub_hang_bug.cfg` | REQ-2 | `LaterWaitersServed` violated, 4 s |
| `foreign_bug.cfg` | REQ-3 | `NoMisdelivery` violated (11-state trace), 2 s |

Every invariant except the three the findings below break holds in every
configuration: the handler never sends on a closed channel and never finds
a full one, Close releases every waiter (new and old style), concurrent
first requests create one mux, and no request receives another's reply
unless the subject-rewrite fallback is involved.

## Findings

### REQ-1 — `respMap` entry leaks when a request fails before it is sent

Severity: low–medium (unbounded memory growth on a long-lived connection;
also disables the single-entry fallback of REQ-3 while any leaked entry
exists). Status: **confirmed by test; fixed on `fix/conn` in 2582083** (the
proposed diff below, as is).

`createNewRequestAndSend` registers `respMap[token]` (`nats.go:4846`)
before it subscribes the mux and publishes. When `subscribeLocked`
(`nats.go:4851-4855`) or `publish` (`nats.go:4860-4862`) fails, it returns
the error; `newRequest` (`nats.go:4916-4919`) and `requestWithContext`
(`context.go:60-63`) return it too, and nobody deletes the entry. Only
`Close` ever removes it. Errors that take this path: `ErrBadSubject`,
`ErrMaxPayload`, `ErrHeadersNotSupported`, `ErrReconnectBufExceeded`,
`ErrConnectionDraining` (while draining pubs), `ErrConnectionClosed`.

TLC trace (`leak_bug.cfg`): `NewCreate(r1)` → `NewPublishErr(r1)`:
`r1` returned `err` and `<<r1,1>>` is still in `respMap`.

The realistic case is an application retrying a request during an outage:
once the reconnect buffer is full every attempt fails instantly with
`ErrReconnectBufExceeded` and leaves an entry behind, and reconnecting does
not clean them up. `TestREQ1RespMapLeaksWhileReconnectBufferFull`: 1000
failed attempts leave 1000 entries after the reconnect. Measured cost:
about 258 bytes of heap per leaked entry (100,000 failed requests grew the
heap by 25.8 MB, `go test` without `-race`).

Tests: `TestREQ1RespMapLeaksOnPublishError`,
`TestREQ1RespMapLeaksWhileReconnectBufferFull`,
`TestStatefulRequestReplyREQ1` (zizek shrinks to one step:
`request_pub_error`).

Proposed fix (checked by `leak_fixed.cfg`, and the three tests pass with it
applied to a scratch copy):

```diff
--- a/nats.go
+++ b/nats.go
@@ func (nc *Conn) createNewRequestAndSend(subj string, hdr, data []byte) (chan *Msg, string, error) {
 		s, err := nc.subscribeLocked(nc.respSub, _EMPTY_, nc.respHandler, nil, nil, false, nil)
 		if err != nil {
+			delete(nc.respMap, token)
 			nc.mu.Unlock()
 			return nil, token, err
 		}
@@
 	nc.mu.Unlock()
 
 	if err := nc.publish(subj, respInbox, false, hdr, data); err != nil {
+		nc.mu.Lock()
+		delete(nc.respMap, token)
+		nc.mu.Unlock()
 		return nil, token, err
 	}
```

### REQ-2 — unsubscribing a reply's `Msg.Sub` breaks every later request

Severity: medium (an innocent-looking call on a value the API hands out
silently blackholes all later new-style requests on the connection until
it is closed; `Conn.Drain` then always runs to `DrainTimeout`). Status:
**confirmed by test; fixed on `fix/conn` in 4aa8101** (the proposed diff
below, as is).

`processMsg` sets `m.Sub = sub` (`nats.go:3951`), so the reply a request
returns carries the connection-wide mux subscription. Calling
`Unsubscribe`, `AutoUnsubscribe(n)` or `Drain` on it removes the
subscription (`removeSub`, `nats.go:5237`) but `nc.respMux` is never
reset, so `createNewRequestAndSend` (`nats.go:4847`) never re-creates it.
Every later request is published with a reply subject nobody subscribes
to:

- `Request` / `RequestMsg` return `ErrTimeout` although the responder
  answers;
- `RequestWithContext` with a context without deadline never returns;
- `Conn.Drain` waits for the subscription count to reach 1 because
  `respMux != nil` (`nats.go:6393-6406`); it is 0, so drain always lasts
  the full `DrainTimeout` (30 s by default) and then reports the
  `respMux.Drain()` error.

There is no hint in the API that `reply.Sub` is shared; with
`UseOldRequestStyle` the same call is harmless (`ErrBadSubscription`, the
per-request subscription is already gone).

TLC traces: `muxunsub_bug.cfg` — `NewCreate(r1)`, `NewPublishOk(r1)`,
`Deliver`, `HandlerLookup`, `HandlerSend`, `NewRecv(r1)`, `UserUnsubMux` →
`MuxValid` violated. `muxunsub_hang_bug.cfg` — after the
unsubscribe a new request is created, published and waits forever while
the server has nowhere to route its reply (`LaterWaitersServed`).

Tests: `TestREQ2RequestAfterReplySubUnsubscribe` (Unsubscribe,
AutoUnsubscribe, Drain: the next request times out),
`TestREQ2RequestWithContextHangs`, `TestREQ2DrainRunsOutTimeout` (an idle
connection takes 3.0 s to drain with `DrainTimeout(3s)`),
`TestStatefulRequestReplyREQ2` (zizek shrinks to one step:
`unsub_reply_sub`).

Proposed fix (checked by `muxunsub_fixed.cfg` and
`muxunsub_fixed_live.cfg`; the tests pass with it applied to a scratch
copy). Every caller of `removeSub` holds `nc.mu`:

```diff
--- a/nats.go
+++ b/nats.go
@@ func (nc *Conn) removeSub(s *Subscription) {
 	nc.subsMu.Lock()
 	delete(nc.subs, s.sid)
 	nc.subsMu.Unlock()
+	// The shared response subscription is gone (e.g. a caller unsubscribed
+	// a reply's Msg.Sub): let the next request create a new one.
+	if s == nc.respMux {
+		nc.respMux = nil
+	}
 	s.mu.Lock()
 	defer s.mu.Unlock()
```

The next request subscribes `respSub` again (same prefix), so replies to
requests still registered in `respMap` are delivered too. Requests that
were in flight at the moment of the unsubscribe can still lose their reply;
that is the caller's doing.

With both fixes applied, the request/drain/context tests of the nats.go
suite (`go test -run 'Request|Drain|Context|Mux|Resp|NoResponders' . ./test/`)
pass except `TestDrainQueueSub`, which fails identically on the unpatched
tree (4095 of 4096 messages) and is unrelated.

### REQ-3 — the single-entry fallback can hand a stale reply to another request

Severity: low (needs a message on the mux whose subject does not start
with the mux prefix). Status: **design hazard, confirmed with a scripted
server; not fixed**. We did not find a nats-server configuration that
produces such a subject. The fallback exists for subjects rewritten by the
server (the code comment names JetStream), so removing it is a behavior
decision for the maintainers; `TestREQ3FallbackHandsStaleReplyToOtherRequest`
stays gated by `harness.KnownBug` and `foreign_bug.cfg` still reproduces.

When the subject of a message delivered to the mux does not start with
`respSubPrefix`, `respHandler` gives it to the only entry of `respMap` if
there is exactly one (`nats.go:4816-4824`; the comment says the server may
have rewritten the subject, "could be the case with JetStream"). Nothing
ties that message to that request. TLC trace (`foreign_bug.cfg`, 11
states): `r1` times out and deletes its entry; `r2` registers and
publishes; `r1`'s reply arrives with a rewritten subject; the fallback
hands it to `r2`, which returns it as its own reply.

Test: `TestREQ3FallbackHandsStaleReplyToOtherRequest` (scripted server):
request B returns `"reply-for-A"`.

Proposed change: drop the fallback, or make it opt-in for the server path
that needs it once that path is identified:

```diff
--- a/nats.go
+++ b/nats.go
@@ func (nc *Conn) respHandler(m *Msg) {
 	rt := nc.respToken(m.Subject)
 	if rt != _EMPTY_ {
 		mch = nc.respMap[rt]
 		// Delete the key regardless, one response only.
 		delete(nc.respMap, rt)
-	} else if len(nc.respMap) == 1 {
-		// If the server has rewritten the subject, the response token (rt)
-		// will not match (could be the case with JetStream). If that is the
-		// case and there is a single entry, use that.
-		for k, v := range nc.respMap {
-			mch = v
-			delete(nc.respMap, k)
-			break
-		}
 	}
```

## Tests derived from the model

`modeltest/reqrep` (`cd modeltest && go test -race ./reqrep/...`; also
builds with `-tags=internal_testing`):

- `reqrep.go` — `Peek(nc)`: reads `nc.respMap` / `nc.respMux` through
  `reflect` + `unsafe` while holding `nc.mu`, so the model's state-based
  invariants can be checked on the real connection without touching the
  library.
- `fakeserver_test.go` — a scripted NATS server (raw TCP) that records
  SUB/PUB and writes MSG/HMSG frames on cue: replies to unknown or expired
  tokens, duplicates, a 503 to a chosen inbox, a reply with a foreign
  subject.
- `invariants_test.go` — deterministic checks of the invariants TLC proved:
  duplicate/unknown/late replies dropped and not misdelivered
  (`dup.cfg`), 503 → `ErrNoResponders` for both styles, 64 concurrent first
  requests create one mux SUB (`MuxCreatedOnce`), Close releases new-style,
  context and old-style waiters with `ErrConnectionClosed`
  (`CloseReleasesWaiters`), requests across reconnects including a mux
  first created while RECONNECTING.
- `stateful_test.go` — the zizek machine. Rules ↔ model actions:
  `request` (all three entry points, both styles), `request_dup`,
  `request_noresp`, `request_timeout`, `late_reply`, `burst` (2–8
  concurrent requesters on both connections plus late replies), `cut`
  (proxy cut + reconnect), `close_inflight` (Close with 1–4 waiters, then
  redial). Invariants: `no_leak` (`NoLeak`), `mux_valid` (`MuxValid`),
  `one_mux` (`MuxCreatedOnce`: the new-style connection holds exactly the
  mux, the old-style one none); `NoMisdelivery` and `CloseReleasesWaiters`
  are checked inside the rules. `TestStatefulRequestReply`: 200 examples,
  seed 7, about 35 s, passes. Since the fixes it includes the rules that
  triggered REQ-1 and REQ-2 (`request_pub_error`, `unsub_reply_sub`, each
  run about 370 times per 200 examples); before them, the separate
  `TestStatefulRequestReplyREQ1` / `REQ2` added one of those rules each and
  failed with one-step shrunk counterexamples. `REQREP_COUNT=1` logs how
  often each rule ran.
- `bugs_test.go` — the reproducers above. Those of REQ-1 and REQ-2 run as
  regression tests; REQ-3's is still gated by `harness.KnownBug`.

`MODELTEST_KNOWN_BUGS=1 go test -race ./reqrep/...` also runs the REQ-3
reproducer, which fails as described. On the unfixed code the REQ-1 and
REQ-2 tests and `TestStatefulRequestReply` fail.

### Notes on zizek

No zizek bugs found. One remark: `RunSteps`' per-step "stop here?" draw
makes the average run about 20 steps for a 20-step bound, which was fine
here; rules that block on the network (`cut`, `close_inflight`) made
`SuppressTooSlow` necessary.
