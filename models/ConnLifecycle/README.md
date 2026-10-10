# ConnLifecycle — connection lifecycle of `nats.go`

TLA+ models of the core connection in `nats.go`: the
CONNECTED → RECONNECTING → CONNECTED/CLOSED state machine, `Close`,
`ForceReconnect`, `Flush`/PING-PONG tracking (`nc.pongs`), the reconnect
pending buffer, the ping timer, the async callback queue (`nc.ach`),
subscription re-send on reconnect with `AutoUnsubscribe`, `Subscription.Drain`,
and connection status listeners. Tests derived from the models are in
[`modeltest/conn`](../../modeltest/conn).

Line numbers refer to `nats.go` at the base of this branch (upstream main
including #2169, `b84f342`).

**Result:** 11 findings (CONN-1 … CONN-11). Each one has a TLC
counterexample (`*_bug.cfg`) and a Go reproducer that failed against the
library the model was written against on every run (13 tests, 5/5 runs each).

**Status: all 11 are fixed on branch `fix/conn`** (one commit per finding or
tight group, listed under each finding). The 13 reproducers now run as
regression tests, both zizek state machines check every invariant by default,
and nats.go's own suites show no new failure (details under
[Validation of the fixes](#validation-of-the-fixes)). The `*_bug.cfg` configs
keep encoding the old code and still reproduce their violations.

## Files

| File | What |
|------|------|
| `ConnLifecycle.tla` | Connection: readLoop, doReconnect, ping timer, dispatcher, Flush, Close, ForceReconnect, server/network |
| `SubResend.tla` | One subscription and the server's view of it, across reconnects: AutoUnsubscribe, Unsubscribe, Drain/checkDrained, async and sync delivery |
| `StatusListener.tla` | A `StatusChanged` channel: `sendStatusEvent`, a slow reader, `RemoveStatusListener`, a user closing the channel |
| `ConnLifecycle.cfg`, `PingTimer.cfg`, `Lifecycle.cfg`, `Liveness.cfg` | ConnLifecycle with every proposed fix on: must pass |
| `SubResendAsync.cfg`, `SubResendSync.cfg`, `StatusListener.cfg` | Same for the other two specs |
| `*_bug.cfg` | The code before the fixes: TLC must report the named violation |
| `PendingFlushStallLive_bug.cfg` | CONN-5 as a temporal property (`tlc.sh` accepts TLC's `Temporal property ... was violated` as a reproduced violation). |

Each fix is a `Fix*` constant (`FixStalePong`, `FixPendingFail`, `FixRqch`,
`FixHsClose`, `FixCloseDisc`, `FixDiscPush`, `FixDrainResend`, `FixAutoBase`,
`FixRemoveOnce`, `FixProbe`, `FixRemove`). A passing config turns all of them
on: the code as fixed on `fix/conn`. A bug config leaves them off: the code
before the fixes. Exception:
`PendingFlushLost_bug.cfg` and `StalePong_bug.cfg` keep the *other* finding's
fix on so that TLC reports the intended trace and not the other path to the
same `FlushIntegrity` violation. I also ran each passing config with only one
fix switched off, and it reproduced the matching violation.

## Granularity and abstractions

* **One action per critical section of `nc.mu`.** The code holds the lock
  across `processOpErr`, `close`, `ForceReconnect`, one whole reconnect attempt
  (`createConn` + `processConnectInit` + `resendSubscriptions` +
  `flushReconnectPendingItems`, nats.go:3544-3627), and so on.
* **Lock-free steps are separate actions:**
  * readLoop's `Read` (`RLRead`) and `parse` (`RLParsePong`), because parse
    works without the lock on bytes that an earlier Read returned;
  * doReconnect's `waitForExits` (`DRStart`, `DRWait2`);
  * the back-off `select` on `rqch` versus the timer (`DRSleepRqch`,
    `DRSleepTimer`);
  * the dispatcher (`Dispatch`).
* **The flusher goroutine is folded into the writers.** Every model write is
  the in-place flush that `sendPing` does (nats.go:6033). The partial-write
  path of the flusher (#2169) is exercised by the Go tests instead.
* **Wire contents.** Client→server carries PING waiter ids. A user's PUB
  rides in front of its own PING, so "the server got PING f" stands for "the
  server got the message published before Flush f". Server→client carries
  PONGs only, so it is a count. A socket has two flags: `sockOpen` (the client
  has not closed it) and `peerAlive` (the server is serving it).
* **Scope limits.** One server URL. No TLS, websocket, or auth.
  `RetryOnFailedConnect` and `initc` are not modeled: the models start
  CONNECTED. The connection-level `Drain` is not in `ConnLifecycle.tla`; it is
  covered by reading the code (see "Checked, no finding").
* **`SubResend`.** It models one subscription. `checkDrained`'s `nc.Flush()`
  is over-approximated: it may return at any time (success, timeout, or
  error). Chan subscriptions are not modeled. The server follows
  nats-server's `processUnsub`: `max > nm` sets the max, anything else
  unsubscribes, and the subscription is removed once `nm >= max`.
* **Liveness.** It is checked in `Liveness.cfg` under weak fairness of the
  client's own goroutines and the back-off timer. Users and the environment
  get no fairness.

### Action → code (ConnLifecycle.tla)

| Action | Code |
|--------|------|
| `RLRead`, `RLErr`, `RLParsePong` | `readLoop` nats.go:3736-3784, `processPong` 4272 |
| `ProcessOpErr` (used by `RLErr`, `PingTimer`) | `processOpErr` 3640-3670 |
| `PingTimer` | `processPingTimer` 6043-6064 (stale → `processOpErr` from the timer goroutine) |
| `FlushStart`, `FlushTimeout` | `FlushTimeout` 6067-6105, `sendPing` 6033, `removeFlushEntry` 6017 |
| `UserClose` / `CloseConn` | `Close` 6308 → `close` 6204-6304 |
| `ForceReconnect` | 2638-2673 |
| `DRStart` | `doReconnect` 3359-3402: `waitForExits`, DisconnectedErrCB, `rqch := nc.rqch` |
| `DRTop`, `DRGo`, `DRSleepTimer`, `DRSleepRqch` | loop head 3422-3489, back-off `select` 3492-3535 |
| `DRWait2` | `waitForGoRoutines` 3537-3540 |
| `DRAttemptClosed`, `DRDialFail` | 3544-3561 (`createConn` assigns `nc.conn`, 2531) |
| `DRHsFail` | `processConnectInit` failure 3569-3576 |
| `DRConnect(ok, k)` | 3578-3623; `ok=FALSE` is the `flushReconnectPendingItems` failure at 3591-3600 with `flushPendingBuffer` resetting the buffer at 2408 |
| `DRClose` | after the loop: `close(CLOSED, true, nil)` 3633 |
| `Dispatch` | `asyncCBDispatcher` 3673-3698 |
| `ServerProcess`, `ServerDrop` | environment |

### Action → code (SubResend.tla)

| Action | Code |
|--------|------|
| `Publish`, `SrvProcess` | nats-server delivery and `processSub`/`processUnsub` |
| `MsgArrive` | `processMsg` 3888-4130 |
| `AsyncCbStart`, `AsyncCbEnd`, `AsyncExit` | `waitForMsgs` 3786-3880 |
| `SyncNext` | `NextMsg` → `processNextMsgDelivered` 5805-5845 |
| `AutoUnsubscribe(n)`, `Unsubscribe`, `Drain` | `AutoUnsubscribe` 5527 / `Unsubscribe` 5441 / `Drain` 5326 → `unsubscribe` 5551-5620 |
| `CDFlushReturns`, `CDRemove` | `checkDrained` 5468-5520 |
| `RemoveSub` | `removeSub` 5237-5280 |
| `Disconnect`, `Reconnect` (`Resend`) | `resendSubscriptions` 6141-6176 |

### Invariants and properties

| Name | Spec | Meaning |
|------|------|---------|
| `FlushIntegrity` | ConnLifecycle | `Flush` returns nil only if the server processed its PING, and so the messages published before it |
| `WaitForExitsUnblocked` | ConnLifecycle | When doReconnect waits for the old goroutines, every readLoop it waits for will exit without help from the server: it is parsing, or its socket is closed or dead |
| `NoSocketWhileBackingOff` | ConnLifecycle | doReconnect holds no open socket while it sleeps |
| `NoLeakedSocket` | ConnLifecycle | Every open socket is `nc.conn` |
| `BackoffRespected` | ConnLifecycle | Back-off sleeps skipped through a closed `rqch` ≤ ForceReconnect calls made while RECONNECTING |
| `NoCallbackAfterClosed`, `ClosedOnce` | ConnLifecycle | No callback runs after ClosedCB; ClosedCB runs at most once |
| `NoDoubleDisconnect` | ConnLifecycle | No two DisconnectedErrCB without a ReconnectedCB between them |
| `DisconnectReportedBeforeClosed` / `DisconnectNotLost` | ConnLifecycle | When ClosedCB runs, every lost established connection has been reported (= / ≥) |
| `StatusTransitions` (action property) | ConnLifecycle | Only the documented transitions (CONNECTING lives inside one critical section) |
| `CloseTerminates`, `ReconnectNotStuck` (temporal) | ConnLifecycle | After Close, doReconnect ends and the dispatcher delivers ClosedCB and exits; doReconnect never waits forever |
| `MaxRespected` | SubResend | `AutoUnsubscribe(n)` hands at most n messages to the user, across reconnects |
| `NoLeakedInterest` | SubResend | Once the client has removed the subscription and the server has caught up, the server has no interest left |
| `ClosedHandlerOnce` | SubResend | The subscription's closed handler runs at most once |
| `NoEventStolen`, `StaysRegistered`, `RemoveCloses`, `NoLossBelowCap` | StatusListener | The library never takes events off the user's channel; the listener stays registered; `RemoveStatusListener` closes the channel; with room in the channel, the user sees every event in order |

## TLC results (measured)

TLC 2026.10.06, `-workers auto` on a 4-core VM, `models/tlc.sh ConnLifecycle`:
all 19 configs behave as expected in 1m45s wall time.

| Config | Spec | Result | States generated / distinct | Time |
|--------|------|--------|-----------------------------|------|
| `ConnLifecycle.cfg` | ConnLifecycle | pass | 4,618,613 / 1,429,864 | 16s |
| `PingTimer.cfg` | ConnLifecycle | pass | 2,220,207 / 699,706 | 9s |
| `Lifecycle.cfg` | ConnLifecycle | pass | 363,947 / 125,158 | 2s |
| `Liveness.cfg` (FairSpec) | ConnLifecycle | pass | 367,015 / 130,622 | 10s |
| `SubResendAsync.cfg` | SubResend | pass | 590,616 / 161,541 | 3s |
| `SubResendSync.cfg` | SubResend | pass | 200,090 / 60,880 | 3s |
| `StatusListener.cfg` | StatusListener | pass | 9,273 / 4,823 | 1s |
| `StatusSteal_bug.cfg` | StatusListener | `NoLossBelowCap` violated (CONN-1) | 25 / 20 | 2s |
| `StatusRemove_bug.cfg` | StatusListener | `RemoveCloses` violated (CONN-2) | 67 / 56 | 1s |
| `DrainResend_bug.cfg` | SubResend | `NoLeakedInterest` violated (CONN-3) | 1,064 / 485 | 1s |
| `AutoUnsubAcrossReconnect_bug.cfg` | SubResend | `NoLeakedInterest` violated (CONN-4) | 2,329 / 1,087 | 1s |
| `PendingFlushStall_bug.cfg` | ConnLifecycle | `WaitForExitsUnblocked` violated (CONN-5) | 33,951 / 15,943 | 3s |
| `PendingFlushLost_bug.cfg` | ConnLifecycle | `FlushIntegrity` violated (CONN-5) | 2,379,039 / 828,185 | 14s |
| `PendingFlushStallLive_bug.cfg` | ConnLifecycle | `ReconnectNotStuck` violated (CONN-5) | 39,691 / 16,176 | 4s |
| `StalePong_bug.cfg` | ConnLifecycle | `FlushIntegrity` violated (CONN-6) | 3,506 / 2,018 | 2s |
| `Backoff_bug.cfg` | ConnLifecycle | `BackoffRespected` violated (CONN-7) | 3,871 / 2,071 | 2s |
| `HsLeak_bug.cfg` | ConnLifecycle | `NoSocketWhileBackingOff` violated (CONN-8) | 1,014 / 562 | 2s |
| `DoubleDisc_bug.cfg` | ConnLifecycle | `NoDoubleDisconnect` violated (CONN-9) | 1,187 / 675 | 1s |
| `LostDisc_bug.cfg` | ConnLifecycle | `DisconnectNotLost` violated (CONN-11) | 50 / 45 | 1s |
| `ClosedTwice_bug.cfg` | SubResend | `ClosedHandlerOnce` violated (CONN-10) | 416 / 212 | 1s |

### Modeling artifacts found and fixed along the way

* **State-space blow-up.** The first `ConnLifecycle` kept the full callback
  log and channel ids. It ran past 11M distinct states without finishing.
  Counters for the callback properties and a three-valued `nc.rqch`
  (`nil` / the channel doReconnect selects on / another one) replaced them
  without losing any of the properties.
* **Missing case in the first `SubResend` fix.** That fix
  (`max - delivered - pending`) still violated `NoLeakedInterest`. TLC showed
  a case I had not considered: `AutoUnsubscribe` called *after* a reconnect
  sends the subscription-wide max, while the server counts from its new SUB.
  That turned CONN-4 into its broader form, and the fix became "count against
  what the server sent since the last (re)SUB".
* **Go invariant too strong.** The first Go `flushIntegrity` invariant flagged
  this sequence: the flusher writes a PUB to a socket that then dies, and a
  Flush issued after the disconnect is answered on the next connection. That
  loss is core NATS at-most-once, not a bug, and the TLA+ model cannot produce
  it because it glues the PUB to its PING. The Go invariant now skips a pair
  when the status changed between Publish and Flush, or when the message died
  in a failed write that carried no PING.

## Findings

Severity is my estimate of the impact on applications.

### CONN-1 — `sendStatusEvent`'s "is it closed?" probe steals events and unregisters live listeners (Medium) — confirmed

**Fixed** on `fix/conn` in d7c509c (together with CONN-2, PUB-7 and PULL-2).

Same root cause as **PUB-7** in [`../PublishAsync/README.md`](../PublishAsync/README.md),
found independently. It is modeled here because it breaks the status-transition
invariants of this area.

`sendStatusEvent` (nats.go:6833-6851) checks whether a listener channel is
closed with `select { case <-ch: delete(...); continue; default: }`. On an
*open* channel holding an event the user has not read yet, this receives that
event and drops the channel from `statListeners[s]`.

**Trace** (`StatusSteal_bug.cfg`):
1. `Emit(CONNECTED)` → the event is buffered.
2. `Emit(CONNECTED)` → the probe takes the buffered event and unregisters the
   listener.

**In the client:** `ch := nc.StatusChanged()`, then a cut and a reconnect,
then `ch` is read. RECONNECTING was buffered. The CONNECTED event's probe
removes it and unregisters `ch` for CONNECTED. The user receives nothing, and
receives no CONNECTED event ever again. A reader that is merely slower than
two consecutive status changes is enough to hit this.

**Test:** `TestConn1StatusListenerLosesEvents`. The zizek lifecycle machine
also finds it in one step (`cut`) with the `lazyListener` invariant.

**Fix:** send without receiving, and recover from a send on a user-closed
channel.
```diff
 func (nc *Conn) sendStatusEvent(s Status) {
-Loop:
 	for ch := range nc.statListeners[s] {
-		// make sure channel is not closed
-		select {
-		case <-ch:
-			// if chan is closed, remove it
+		if !trySendStatus(ch, s) {
+			// the user closed the channel, remove it
 			delete(nc.statListeners[s], ch)
-			continue Loop
-		default:
 		}
-		// only send event if someone's listening
-		select {
-		case ch <- s:
-		default:
+	}
+}
+
+// trySendStatus sends s on ch if there is room, without receiving from it.
+// It reports false if ch is closed.
+func trySendStatus(ch chan Status, s Status) (open bool) {
+	defer func() {
+		if recover() != nil {
+			open = false
 		}
+	}()
+	select {
+	case ch <- s:
+	default:
 	}
+	return true
 }
```

### CONN-2 — `RemoveStatusListener` does not close a channel that holds an event (Low/Medium) — confirmed

**Fixed** on `fix/conn` in d7c509c. `RemoveStatusListener` now always closes the channel (a reader still receives a buffered event first).

Also part of **PUB-7**. `RemoveStatusListener` (nats.go:6799-6806) does
`select { case <-ch: default: close(ch) }`. When an event is buffered it
consumes the event and leaves the channel open, despite its doc comment ("If
the channel is not closed, it will be closed"). A goroutine doing
`for range ch` never exits.

**Trace:** `StatusRemove_bug.cfg`.
**Test:** `TestConn2RemoveStatusListenerDoesNotClose`.

**Fix:**
```diff
-	select {
-	case <-ch:
-	default:
-		close(ch)
-	}
-
 	for _, listeners := range nc.statListeners {
-		for range listeners {
-			delete(listeners, ch)
-		}
+		delete(listeners, ch)
 	}
+	// Close it unless the user already did.
+	func() {
+		defer func() { recover() }()
+		close(ch)
+	}()
```

### CONN-3 — A subscription drained across a reconnect keeps its interest on the server (Medium) — confirmed

**Fixed** on `fix/conn` in 187d13b.

`Subscription.Drain` leaves the subscription in `nc.subs` (with
`draining = true`) until `checkDrained` removes it.
* If the drain starts while RECONNECTING, the UNSUB is suppressed
  (nats.go:5592).
* If a reconnect happens before `checkDrained` finishes, the UNSUB is lost
  with the old connection.

In both cases `resendSubscriptions` (nats.go:6141) does not look at
`draining` and re-sends the SUB. `checkDrained` then calls `removeSub`
(nats.go:5503), which is client-side only. The server keeps the subscription
for the life of the connection:
* it sends every matching message, and the client drops them;
* for a queue subscription, that member keeps receiving its share of the
  group's messages, and all of them are lost.

**Trace** (`DrainResend_bug.cfg`): `Disconnect`, `Drain` (UNSUB suppressed),
`Reconnect` (SUB re-sent), `SrvProcess`, `CDFlushReturns`, `CDRemove` →
`NoLeakedInterest` fails.

**Tests:**
* `TestConn3DrainDuringReconnectLeaksServerInterest` checks the server's Connz.
* The zizek subscription machine finds it with `noLeakedInterest`. Shrunk
  case: `publish, outageStart, next, drain, outageEnd`; another seed gave
  `publish, publish, drain, outageStart, next, outageEnd, next`, which is the
  variant where the drain starts while connected.

**Fix** (in `resendSubscriptions`, together with CONN-4):
```diff
+		// CONN-3: a draining subscription has already removed its interest.
+		if s.draining {
+			s.mu.Unlock()
+			continue
+		}
```

### CONN-4 — `AutoUnsubscribe` max is counted differently by client and server after a reconnect (Medium) — confirmed

**Fixed** on `fix/conn` in 467e6ae.

The client counts `max` over the whole life of the subscription (`delivered`).
After a reconnect, the server counts only from the SUB that
`resendSubscriptions` sent. Two consequences:

* **(a) AutoUnsubscribe called after a reconnect.** `unsubscribe` sends
  `UNSUB sid max` (nats.go:5555-5559). Messages delivered before the reconnect
  count toward max on the client, but not on the server. Example: 3 messages
  received, reconnect, `AutoUnsubscribe(5)`, 2 more messages. The client
  removes the subscription, and the server keeps it for 3 more messages.
* **(b) Messages pending in the client at reconnect.** `resendSubscriptions`
  re-arms `max - delivered` (nats.go:6156). `delivered` does not include
  messages already received but still queued in the client, so the server
  again ends up with more interest than the client.

In both cases the server keeps an interest the client has dropped. For a
queue subscription, the next messages routed to that member are lost.

**Trace** (`AutoUnsubAcrossReconnect_bug.cfg`, variant (a)): SUB processed →
`Publish`, `MsgArrive` (1 pending) → `Disconnect`/`Reconnect` (SUB re-sent,
server `nm = 0`) → `AutoUnsubscribe(1)` (UNSUB 1) → the client delivers the
pending message and removes the sub → the server still has max 1, nm 0.

**Tests:** `TestConn4AutoUnsubscribeAfterReconnect` (a) and
`TestConn4AutoUnsubscribeResendIgnoresPending` (b).

**Fix:**
* count arrivals per subscription (`received`, which includes slow-consumer
  drops, because the server counted those too);
* remember the count at the last (re)SUB (`receivedBase`);
* send the server `max - receivedBase`;
* re-arm with `max - received`;
* remove the subscription only when `delivered >= max`; if `received >= max`
  already, stop the server with a plain UNSUB and let the pending messages be
  delivered.

```diff
+	received     uint64
+	receivedBase uint64
 ...  processMsg, after the closed check:
+	sub.received++
 ...  unsubscribe:
-		if sub.delivered < sub.max {
-			maxStr = strconv.Itoa(max)
+		reached = sub.delivered >= sub.max
+		if sub.received < sub.max {
+			maxStr = strconv.FormatUint(sub.max-sub.receivedBase, 10)
 		}
 ...
-	if maxStr == _EMPTY_ && !drainMode {
+	if (max == 0 || reached) && !drainMode {
 		nc.removeSub(s)
 ...  resendSubscriptions:
+		s.receivedBase = s.received
 		if s.max > 0 {
-			if s.delivered < s.max {
-				adjustedMax = s.max - s.delivered
+			if s.received < s.max {
+				adjustedMax = s.max - s.received
```
`FixAutoBase` in the model. `SubResendAsync.cfg` and `SubResendSync.cfg`
pass with it. As applied, `unsubscribe` computes "remove now" as
`max <= 0 || delivered >= max`, so `AutoUnsubscribe(n)` with `n <= 0` still
behaves like `Unsubscribe`, as before.

### CONN-5 — A failed write of the reconnect pending buffer stalls the reconnect and lets a Flush succeed for lost messages (High) — confirmed

**Fixed** on `fix/conn` in a280cc5.

A sibling of #2169 that #2169 did not cover. `doReconnect` writes the pending
buffer with `flushReconnectPendingItems` (nats.go:3591). On failure:
`flushPendingBuffer` has already reset the buffer (nats.go:2408), status goes
back to RECONNECTING, `waitForGoRoutines = true`, `continue`. Two things are
missing: the socket is not closed (`nc.conn` stays set), and `nc.pongs` is not
cleared.

* **(a) Stall.** On the next iteration `waitForExits` waits for the readLoop
  of a socket that is still open. Nothing makes that readLoop exit until the
  server drops the connection: with default server settings that takes
  minutes (2 missed 2-minute pings). Meanwhile the client stays RECONNECTING
  and makes no new connection attempt.
  * When it happens: a write failure with the socket left open. The default
    `FlusherTimeout` is 1 minute (`timeoutWriter`), so a slow or stalled
    server during reconnect produces exactly that.
  * Model: `PendingFlushStall_bug.cfg` (`WaitForExitsUnblocked`) and
    `PendingFlushStallLive_bug.cfg` (`ReconnectNotStuck`).
* **(b) Wrong Flush.** The Flush waiters whose PINGs were in the discarded
  buffer stay in `nc.pongs`. On the next connection, the first PONG (the one
  for doReconnect's own `nc.Flush()`, nats.go:3623) completes the oldest stale
  waiter. That `Flush` returns nil although its message and its PING never
  reached the server. Waiters after it are off by one.
  * Model: `PendingFlushLost_bug.cfg`. Trace: ForceReconnect, `FlushStart`
    while RECONNECTING, `DRConnect(FALSE,0)` (pending write fails, socket kept,
    `pongs` kept), the server drops the socket, `DRConnect(TRUE,0)`, then a
    PONG completes Flush 1, which the server never saw.

**Tests:**
* `TestConn5PendingFlushFailureStallsReconnect`: still RECONNECTING after 3s
  with ReconnectWait 20ms; zero dials after the failure.
* `TestConn5FlushSucceedsForLostMessage`: Flush returns nil and the
  subscriber never gets the message.
* The zizek lifecycle machine finds both. Shrunk counterexamples:
  `refuse, publishFlush, armWriteFault, allow` for `reconnects`, and
  `refuse, publishFlush (while RECONNECTING), …, armWriteFault, …, allow` for
  `flushIntegrity`.

**Fix:**
```diff
 		if nc.err != nil {
 			nc.changeConnStatus(RECONNECTING)
 			// Stop the ping timer (if set)
 			nc.stopPingTimer()
+			// The PINGs of pending flush calls were in the discarded
+			// buffer, and the socket may hold a truncated op: release the
+			// waiters and drop the socket so the readLoop exits.
+			nc.clearPendingFlushCalls()
+			nc.conn.Close()
+			nc.conn = nil
```

### CONN-6 — A PONG parsed by an abandoned readLoop completes a Flush issued during the reconnect (Medium) — confirmed

**Fixed** on `fix/conn` in ea8415e.

Several paths clear `nc.pongs` and give up the connection while its readLoop
may still be parsing bytes it Read before the socket was closed:
* `ForceReconnect`;
* `processOpErr` from the ping timer (stale connection) or from the flusher;
* `-ERR 'Stale Connection'` handled in the middle of a parse buffer
  (`processErr` → `processOpErr`, after which parse continues).

`processPong` (nats.go:4272) pops whatever is at the head of the *new*
`nc.pongs`. A `Flush` issued after the reconnect started therefore returns nil
without any round trip. Its PING is still in the pending buffer, and when that
PING is answered later, the answer completes the next waiter, so the error
cascades.

**Trace** (`StalePong_bug.cfg`, via the ping timer):
1. `PingTimer` sends PING (pout = 1); the server answers.
2. `RLRead` takes the PONG.
3. `PingTimer` fires again before parse: pout 2 > MaxPingsOut 1, stale →
   `processOpErr` clears `pongs`.
4. `FlushStart(1)` puts PING 1 in the pending buffer.
5. `RLParsePong` (old readLoop) completes Flush 1, which the server never saw.

**Test:** `TestConn6StalePongCompletesNewFlush`.
1. A wrapper conn holds a Read that has already taken a PONG off the socket.
2. `ForceReconnect`, then the proxy blocks reconnects.
3. A second `Flush` is issued.
4. The held Read is released → that Flush returns nil while the server is
   unreachable.

The zizek machine includes the `holdPong`/`releasePong` rules, but no shrunk
counterexample through this path was observed: with real timing, CONN-5 is
found first.

**Fix.** The minimal one: after a reconnect has started, no PONG belongs to a
live waiter, and doReconnect only connects after every old readLoop has
exited (`waitForExits`).
```diff
 	nc.mu.Lock()
+	// A PONG parsed after the connection was given up (ForceReconnect, stale
+	// connection, -ERR) belongs to a PING whose waiter was already released.
+	if !nc.isConnected() {
+		nc.mu.Unlock()
+		return
+	}
 	if len(nc.pongs) > 0 {
```

### CONN-7 — `ForceReconnect` while RECONNECTING disables the back-off for the rest of the episode and exhausts MaxReconnects (High) — confirmed

**Fixed** on `fix/conn` in dad4d6c.

`doReconnect` takes a local copy `rqch := nc.rqch` (nats.go:3393).
`ForceReconnect` while RECONNECTING closes `nc.rqch` and sets it to nil. The
sleep that is cut short sets `nc.rqch = make(...)` (nats.go:3497, 3532), but
the *local* `rqch` keeps the closed channel. From then on, every back-off
`select` returns immediately: the client retries in a tight loop and burns
through `MaxReconnects` (default 60) in milliseconds, then closes for good. The
API promises one immediate attempt ("bypassing the current reconnect delay").

**Trace** (`Backoff_bug.cfg`): ServerDrop, readLoop error, `ForceReconnect`,
`DRStart`, sleep bypassed, dial fails, sleep bypassed again →
`BackoffRespected` fails (2 > 1).

**Test:** `TestConn7ForceReconnectDisablesBackoff`. Setup: server down,
`ReconnectWait(250ms)`, `MaxReconnects(8)`, then one `ForceReconnect`.
Result: 7 attempts in 600ms and the connection is CLOSED.

**Fix** (both `select` sites):
```diff
 				case <-rqch:
 					rt.Stop()
 					nc.mu.Lock()
-					nc.rqch = make(chan struct{})
+					rqch = make(chan struct{})
+					nc.rqch = rqch
 					nc.mu.Unlock()
```

### CONN-8 — The socket of a failed reconnect handshake is left open (Low) — confirmed

**Fixed** on `fix/conn` in 835f8e4.

When `processConnectInit` fails on a freshly dialed socket during a reconnect
(-ERR, timeout, bad INFO), `doReconnect` sets RECONNECTING and continues
(nats.go:3569-3576) without closing `nc.conn`. Compare `connect()`, which
closes it on the same failure. The socket stays open through the back-off.
When the next dial overwrites `nc.conn`, it becomes unreferenced and lingers
until the GC finalizer closes it.

**Trace:** `HsLeak_bug.cfg` (`NoSocketWhileBackingOff`).

**Test:** `TestConn8FailedReconnectHandshakeLeaksSocket`. A scripted server
answers the reconnect's CONNECT with `-ERR` and sees no EOF within 1s.

**Fix:**
```diff
 		if nc.err = nc.processConnectInit(); nc.err != nil {
+			nc.conn.Close()
+			nc.conn = nil
```

### CONN-9 — Close during a reconnect runs DisconnectedErrCB a second time (Low) — confirmed

**Fixed** on `fix/conn` in 7585d35 (with CONN-11; the handshake path also by CONN-8's 835f8e4).

`close()` queues DisconnectedErrCB whenever `nc.conn != nil`
(nats.go:6287). During a reconnect `nc.conn` is non-nil in two cases:
* after a failed handshake (CONN-8);
* after `ForceReconnect`, which closes the socket but does not nil it
  (nats.go:2668).

doReconnect has already queued the disconnect callback at that point, so the
application gets `disconnected, disconnected, closed` for one disconnect.

**Trace** (`DoubleDisc_bug.cfg`): `ForceReconnect`, readLoop exits,
`DRStart` queues `disc`, `UserClose` queues `disc` again.

**Tests:**
* `TestConn9DoubleDisconnectCallback` (handshake-failure path):
  `[disconnected disconnected closed]`.
* The zizek lifecycle machine finds it with `noDoubleDisconnect`; shrunk to
  `cut, block, close`.

**Fix:**
```diff
+	// A reconnecting connection has already reported its disconnect.
+	wasReconnecting := nc.isReconnecting()
 	nc.status = CLOSED
 ...
-		if nc.conn != nil {
+		if nc.conn != nil && !wasReconnecting {
```

### CONN-10 — The closed handler of a sync subscription runs twice when Unsubscribe races with Drain (Low) — confirmed

**Fixed** on `fix/conn` in 961414c.

`checkDrained` calls `removeSub` when `closed` is already true
(nats.go:5503-5506). For non-async subscriptions `removeSub` calls the closed
handler on every call (nats.go:5262-5266). So
`sub.Drain(); sub.Unsubscribe()` runs the handler twice; so does Drain
combined with reaching an AutoUnsubscribe max in `NextMsg`.

**Trace:** `ClosedTwice_bug.cfg`.

**Tests:**
* `TestConn10ClosedHandlerCalledTwice`.
* The zizek subscription machine finds it with `closedHandlerOnce`; shrunk to
  `publish, drain, unsubscribe`.

**Fix:**
```diff
-	if s.typ != AsyncSubscription {
+	if s.typ != AsyncSubscription && !s.closed {
 		done := s.pDone
```

### CONN-11 — Close right after a disconnect can drop DisconnectedErrCB (Low) — confirmed

**Fixed** on `fix/conn` in 7585d35 (with CONN-9).

`doReconnect` queues DisconnectedErrCB only after `waitForExits`
(nats.go:3359-3382). If `Close()` runs in between, the following happens:
1. `processOpErr` has already set `nc.conn = nil`.
2. `close()` therefore queues only ClosedCB and the dispatcher's close marker.
3. The DisconnectedErrCB that doReconnect queues afterwards is never run.

The application sees `closed` without the `disconnected` for a connection it
did lose.

**Trace** (`LostDisc_bug.cfg`): ServerDrop, `RLErr` (processOpErr), `UserClose`,
`Dispatch` → ClosedCB has run with 0 of 1 disconnects reported.

**Test:** `TestConn11CloseLosesDisconnectCallback`. The ping timer declares the
connection stale while the readLoop holds unparsed bytes; then `Close` runs.
Callbacks: `[closed]`.

**Fix:** queue the callback where the status changes (`processOpErr`,
`ForceReconnect`), under the same lock, and not in doReconnect:
```diff
+// pushDisconnectedCB queues the disconnect callback. Lock held.
+func (nc *Conn) pushDisconnectedCB(err error) {
+	if nc.initc {
+		return
+	}
+	if disconnectedErrCB := nc.Opts.DisconnectedErrCB; disconnectedErrCB != nil {
+		nc.ach.push(func() { disconnectedErrCB(nc, err) })
+	} else if disconnectedCB := nc.Opts.DisconnectedCB; disconnectedCB != nil {
+		nc.ach.push(func() { disconnectedCB(nc) })
+	}
+}
 ... processOpErr, before `go nc.doReconnect(err, forceReconnect)`:
+		nc.pushDisconnectedCB(err)
 ... ForceReconnect, before `go nc.doReconnect(nil, true)`:
+	nc.pushDisconnectedCB(nil)
 ... doReconnect:
-	if !nc.initc {
-		if disconnectedErrCB := ...; disconnectedErrCB != nil { ... }
-	} else if nc.Opts.RetryOnFailedConnect && nc.initc && err != nil {
+	if nc.Opts.RetryOnFailedConnect && nc.initc && err != nil {
```

### Checked, no finding

* **No callback after ClosedCB, ClosedCB at most once.** This holds in every
  config. Callbacks queued after the close marker are silently dropped, which
  is what CONN-11 runs into.
* **Close terminates doReconnect** (`CloseTerminates` under fairness). Holds:
  `close()` closes `nc.rqch` and the socket, so every wait inside doReconnect
  ends.
* **`StatusTransitions`.** Holds.
* **`MaxRespected` (AutoUnsubscribe never over-delivers).** Holds in the model
  for async and sync subscriptions, and in the zizek machine. The client
  checks `delivered <= max` itself (nats.go:3848, 5832).
* **Messages in `bw.bufs` when `processOpErr` switches to pending mode.**
  `bindToNewConn` drops them on reconnect. Bytes still in the dead socket are
  lost the same way, so this is at-most-once and not a finding. A Flush whose
  PING was among them is released with an error by `clearPendingFlushCalls`.
* **Connection `Drain` with a disconnect in the middle.** `processOpErr` sees
  DRAINING and closes instead of reconnecting (nats.go:3666-3668). That is
  terminal by design; no further modeling.
* **Not modeled, noted from reading.** In `doReconnect`,
  `resendSubscriptions` and the pending flush write under `nc.mu` with no
  deadline unless `FlusherTimeout` is set. The deadline of
  `processConnectInit` is cleared on return. A server that stops reading right
  after the handshake can therefore block `Close()` until `FlusherTimeout`.
  That is the same design trade-off as the flusher, so it is not listed as a
  finding.

## Tests derived from the models (`modeltest/conn`)

* **`bugs_test.go`:** 13 reproducers, one or two per finding. All 13 failed 5
  out of 5 runs on the unfixed code
  (`MODELTEST_KNOWN_BUGS=1 go test -race -count=5 -run TestConn ./conn/`); with
  the fixes on `fix/conn` they are regression tests, no longer gated by
  `harness.KnownBug`. Each takes 0.1–3s.
* **`helpers_test.go`:**
  * `faultDialer`: a `nats.CustomDialer` whose sockets can
    * fail one write half way, keeping or closing the socket (the model's
      `DRConnect(FALSE, k)`, and #2169's partial write);
    * hold a Read that has already taken bytes off the socket (the split
      between the model's `RLRead` and `RLParsePong`);
    * refuse dials (`DRDialFail`).
  * `fakeServer`: a scripted raw-TCP NATS server (handshake, then `-ERR`, no
    EOF, …).
  * `serverSubs`: counts the server's subscriptions on a subject through Connz.
* **`machine_test.go`: two zizek state machines.**
  * Lifecycle machine. Rules: `publishFlush`, `cut`, `block`/`unblock`
    (handshake failures), `refuse`/`allow` (dial failures), `forceReconnect`,
    `close`, `armWriteFault`, `holdPong`/`releasePong`. Invariants:
    `callbacks`, `flushReturns`, `reconnects`, `flushIntegrity`,
    `noDoubleDisconnect`, `lazyListener`.
  * Subscription machine. Rules: `publish`, `next`, `autoUnsubscribe`,
    `unsubscribe`, `drain`, `cut`, `outageStart`/`outageEnd`. Invariants:
    `maxRespected`, `noLeakedInterest`, `closedHandlerOnce`. Each example
    draws whether the subscription is a queue subscription.
  * `TestLifecycleMachine` and `TestSubscriptionMachine` (12 examples) use
    every rule and check every invariant except `flushIntegrity`, which
    needs extra settling. Before the fixes they used only the rules and
    invariants that held then.
  * `Test*MachineFocused/<invariant>` (60 examples) enforces one invariant
    with the rules that reach its bug (`Test*MachineKnownBugs`, gated, before
    the fixes). Both machines run by default; the whole package takes about
    two minutes.
  * After the fixes, `flushIntegrity` turned out to be flaky (5 of 10 runs)
    for a harness reason: a Flush goroutine scheduled after later steps
    queued its PING behind a reconnect the "split" check had not seen, so
    the Flush was answered on the next connection while its message died
    with the old one (at-most-once, see "Go invariant too strong" above).
    `publishFlush` now returns only once the Flush has queued its PING, and
    the invariant skips a message after a status change when no PING followed
    it on its socket. 10 of 10 runs pass; against the unfixed library the
    focused subtest still fails (CONN-5b).

Measured on the unfixed code (60 examples, seed 7, ≤ 10 shrinks):

| Subtest | Finds | Time |
|---------|-------|------|
| lifecycle `lazyListener` | CONN-1, shrunk to `cut` | 1s |
| lifecycle `flushIntegrity` | CONN-5(b): `refuse, publishFlush, …, armWriteFault, allow` | 22–28s |
| lifecycle `reconnects` | CONN-5(a): `refuse, publishFlush, armWriteFault, allow` | ~60–80s (each shrink candidate waits 2s) |
| lifecycle `noDoubleDisconnect` | CONN-9: `cut, block, close` | 2s |
| subscription `noLeakedInterest` | CONN-3: `publish, outageStart, next, drain, outageEnd` | 8–130s |
| subscription `closedHandlerOnce` | CONN-10: `publish, drain, unsubscribe` | 9–90s |

With the fixes, every one of these subtests passes. Re-run of the new tests
against the unfixed library (a worktree of the base commit with these test
files copied in): all 13 reproducers, both default machines and all six
focused subtests fail.

## Validation of the fixes

The fixes on `fix/conn` follow the diffs above, with small deviations noted
per finding (CONN-4: `max <= 0`; CONN-8: a nil check before closing
`nc.conn`; CONN-10: the handler guard only, as described below). Measured on
`fix/conn`:

* `cd modeltest && ./run.sh ./conn/... ./reqrep/...`: pass; `./conn/...` 3 of
  3 runs pass (about 114s per run).
* `models/tlc.sh ConnLifecycle`: all 19 configs as expected (passing configs
  pass, every `*_bug.cfg` reproduces its violation), 2m29s.
* `go test -modfile=go_test.mod -race -tags=internal_testing .`: pass.
* `go test ... -p=1 ./test/...` (full suite, about 230s): two runs; each had
  one failure, both flaky on the base commit too:
  `TestKeyValueDuplicatesWindow` ("error creating store for stream" from the
  server; 1 of 60 runs failed on the base commit, 0 of 65 isolated runs on
  `fix/conn`) and
  `TestJetStreamOrderedConsumerSlowConsumer/HeartbeatRecovery` (6 of 12
  isolated runs failed on the base commit, 4 of 12 on `fix/conn`).
* `./jetstream/test/... -run 'Status|Reconnect|Consume|Messages|Publish'`:
  only `TestConsumerPrioritized` fails, 5 of 5 runs on the base commit and on
  `fix/conn` alike.

Earlier validation of the proposed diffs on a scratch copy (kept for
reference):

* all 13 reproducers pass;
* both zizek machines pass with every known-bug invariant on (77s);
* nats.go's root package tests pass
  (`go test -modfile=go_test.mod -tags=internal_testing -race .`);
* the `./test` subset matching
  `Reconnect|Status|Drain|Flush|AutoUnsub|Close|Disconnect|Ping|Stale|Callback|PartialWrite|Unsubscribe|Subscri`
  passes (80.6s), including the #2169 partial-write tests.
* the full `./test` suite (226s) passes except
  `TestJetStreamSubscribeContextCancel/cancel_unsubscribes_and_deletes_ephemeral`.
  That test is flaky without the patch too: it failed in 15 of 40 runs on the
  unpatched tree and in 13 of 40 on the patched copy. The callback cancels the
  context before `js.Subscribe` returns.

The model's `Fix*` switches match these code changes. One approximation:
`FixRemoveOnce` makes `removeSub` a no-op on a closed subscription, while the
code fix only guards the handler call; the two are equivalent for the
handler-count property.

## Limitations and notes

* **The Go tests use real goroutines and sockets.** The reproducers are
  deterministic in practice: they use read holds, write faults, and a
  scripted server rather than sleeps. The zizek machines settle state with
  bounded waits (≤ 2–3s) and can still be timing sensitive.
* **zizek limitation.** With a property that is not perfectly deterministic,
  a shrunk candidate may no longer fail. `Check` then returns
  `internal error: shrunk example no longer fails` with the candidate's blob,
  and the original failure and its cause are dropped (Hypothesis would report
  a "Flaky" error carrying the original). I worked around it by settling the
  machines more carefully and by logging the unshrunk failing example with
  `MODELTEST_LOG_FAILURES=1`.
* **TLC trace files.** TLC writes `*_TTrace_*.tla/.bin` files next to the
  spec whenever a bug config runs. They are generated output and should be
  git-ignored or deleted.
