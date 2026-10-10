# PublishAsync — JetStream asynchronous publishing

A TLA+ model of the JetStream async publisher in `jetstream/publish.go` (plus
`CleanupPublisher` in `jetstream/jetstream.go` and the connection status
listeners in `nats.go` it depends on), the counterexamples TLC finds in it,
and Go tests in `modeltest/pubasync/` that reproduce every finding against the
real library. The legacy `js.go` publisher is compared at the end.

Line numbers are for this branch (based on upstream `main` at `31a3ee4`):
`publish.go` unless stated otherwise.

## Scope

| In the model | Not in the model |
|---|---|
| `PublishMsgAsync` first attempt and 503 retry, `newAsyncReply`, `registerPAF` (max-pending stall), the ack timer, `handleAsyncReply` (ok / API error / 503), `handleAsyncErrAndUnlock`, `resetPendingAcksOnReconnect`, `clearPAF`, `maybeUnstall`, `Ok()`/`Err()` channel creation, `PublishAsyncComplete`, `CleanupPublisher`/`cleanupReplySub`, `nc.StatusChanged`/`sendStatusEvent`/`RemoveStatusListener` (buffer 10), `nc.PublishMsg`'s failure modes (closed; reconnecting with `ReconnectBufSize(-1)`), connection CONNECTED/RECONNECTING/CLOSED | Synchronous `Publish`; header/option handling; reply-token collisions (fixed in #1812); the content of acks beyond ok/err/503; the ack handler's contract (documented as must-not-block); `Drain` |

## Model

`PublishAsync.tla`, plain TLA+. One `pc` per goroutine:

| Goroutine | Code |
|---|---|
| `<<"pub", c>>` | the caller of `PublishMsgAsync` for future `c` |
| `<<"tmo", c>>` | `paf.timeout` = `time.AfterFunc(ackTimeout, …)` |
| `<<"rty", c>>` | the 503 retry `time.AfterFunc(paf.retryWait, …)` |
| `<<"disp", 0>>` | the reply subscription's callback, `handleAsyncReply` (serial) |
| `<<"rst", l>>` | `resetPendingAcksOnReconnect`, one per status channel `l` ever created |
| `<<"cln", 0>>` | a caller of `CleanupPublisher` |

`mu` is `js.publisher`'s `RWMutex` (read locks treated as exclusive: a
re-entrant `RLock` under `Lock` deadlocks just the same). Critical sections
that cannot block are single atomic steps; every point where a goroutine can
block while holding `mu` (a send into the cap-1 `paf.errCh`, calling user code)
is its own step. Channels with identity (`stallCh`, `doneCh`, the status
channel) are modeled by id, so "closes the channel a waiter captured" is
precise.

### Action → code

| Action | Code |
|---|---|
| `PubStart(c)` | `newAsyncReply` (438-462): reply sub + `StatusChanged(RECONNECTING, CLOSED)` + `go resetPendingAcksOnReconnect()`; `nc.Subscribe` fails when closed (362-364) |
| `PubReg(c)` | `registerPAF` (613-663): stall at `len(acks) >= maxpa` (626-640), else insert under `replyPrefix` (657-660); the ack timer is armed right after (372-406) |
| `PubWake(c)` / `PubStallTimeout(c)` | `select { case <-stc: continue; case <-timer.C: ErrTooManyStalledMsgs }` (635-640) |
| `PubSend(c)` | `js.conn.PublishMsg` (422); `nats.go` `publish`: `ErrConnectionClosed` (4707), `ErrReconnectBufExceeded` (4727) |
| `PubClear(c)` | `clearPAF` (683-688) on a failed publish (423-424) |
| `TmoFire/TmoLock/TmoSend/TmoDone` | timer callback (373-405); `TmoSend` = `errCh <-` (390-393) **and the error handler, under the lock** (396-398) |
| `DispTake/DispLock/DispSend/DispCb/DispClose` | `handleAsyncReply` (465-559): `getPAF` (473), `timeout.Stop` (496-498), 503 retry scheduling (501-521) or final error (523-527), delete + `maybeUnstall` + capture `doneCh` (531-533, 479-494), ack (550-558), `handleAsyncErrAndUnlock` (561-571) |
| `RtyFire/RtyGet/RtyPub/RtyClear/RtyELock/RtySend/RtyCb` | retry closure (504-519): `getPAF` then Unlock (505-510), retry branch of `PublishMsgAsync` incl. `timeout.Reset` (407-414), `PublishMsg` (422), `clearPAF` (423), then **unconditionally** `handleAsyncErrAndUnlock` (515-518) |
| `RstRecv/RstLock/RstIter/RstEnd/RstCb/RstClose` | `resetPendingAcksOnReconnect` (573-606); returns on `CLOSED` (579-581) |
| `ClnStart/ClnSub/ClnLock/ClnIter/ClnEnd/ClnCb` | `CleanupPublisher` / `cleanupReplySub` (`jetstream.go` 1295-1331) |
| `UserErrCall(c)` | `paf.Err()` (704-716) before resolution (the user does not read it in the model) |
| `UserComplete` | `PublishAsyncComplete` (739-751) |
| `Respond(c, k)` | server answers `ok` / `err` / `nr` (503); delivered only if the reply sub for `c`'s prefix is live |
| `Disconnect` | `processOpErr` → `changeConnStatus(RECONNECTING)`; in-flight replies lost |
| `Reconnect` | `doReconnect` success; reconnect buffer flushed |
| `Close` | `nc.Close()` → `changeConnStatus(CLOSED)`; subscriptions die |
| `SendStatus(s)` (inside the above) | `sendStatusEvent` (`nats.go` 6833): probe `case <-ch` (consumes a buffered event, unregisters), then non-blocking send into the cap-10 channel (6786) |
| `ClnSub`'s listener removal | `RemoveStatusListener` (`nats.go` 6799): `select { case <-ch: default: close(ch) }` |

`FIX_*` constants switch in the proposed fixes below; all `FALSE` = today's code.

### Invariants and properties

| Name | Meaning |
|---|---|
| `MaxPendingRespected` | `len(acks) <= maxpa` (the #2126 guarantee) |
| `ResolvedAtMostOnce` | each future is resolved (ack or error) at most once |
| `ErrCbAtMostOnce` | the error handler runs at most once per message |
| `SingleReport` | a failure is reported through one channel: `PublishMsgAsync`'s return value or the future/handler, not both |
| `NoBlockedSendUnderLock` | no goroutine is about to send into a full `paf.errCh` while holding the publisher mutex |
| `NoSelfDeadlock` | the timer goroutine never calls back into the publisher while holding its mutex |
| `NoLostStallWakeup` | between critical sections, a caller parked on an open stall channel implies the publisher is at the cap |
| `NoLostCompletion` | between critical sections, an open `doneCh` implies something is pending |
| `NoOrphanedFuture` | every pending future is registered under the live reply subscription |
| `ListenerSubscribed` | the publisher's status channel stays registered for RECONNECTING |
| `NoLeakedResetGoroutine` | no reconnect worker waits on a channel nobody will send on or close |
| `NoStrandedFuture` | safety form of liveness: never CLOSED + all goroutines at rest + a future pending with no timer armed |
| `EventuallyResolved` (temporal) | every registered future is eventually resolved |
| `CompleteCloses` (temporal) | every `PublishAsyncComplete` channel is eventually closed |

`FairSpec` adds weak fairness to every goroutine step, timer firing, server
answer and reconnect (not to user calls, disconnects, close, cleanup or the
stall timeout).

### Abstractions and limitations

- Calls are bounded (1–2 futures), so are disconnects (≤2), cleanups (≤1),
  `PublishAsyncComplete` calls (≤1) and retries (≤1).
- A timer that is `Reset` while its previous callback is still waiting for the
  lock is modeled as not firing again until that callback finishes (a
  slight under-approximation).
- Reading `paf.Err()` is not modeled: a read only frees the buffer, and leaving
  it out makes every double resolution visible.
- Reply messages already queued for the reply subscription survive
  `Unsubscribe` in the model (an over-approximation; they are ignored by
  `getPAF` anyway).
- Only the `RECONNECTING`/`CLOSED` status events the publisher listens to are
  modeled; a failed `processConnectInit`'s extra `RECONNECTING` is not.
- `tlc.sh` (shared) prints the *first* "distinct states found" line, which is
  usually a progress line; the numbers below come from the final TLC line.
  It also leaves `PublishAsync_TTrace_*` files behind for `_bug` configs.

## TLC results (measured)

TLC 1.8.0, `-workers 4`, on a shared 4-core box (other models were running).

| Config | Result | Distinct states | Time |
|---|---|---|---|
| `Baseline.cfg` (today's code: 2 calls, max pending 1, ok/503, retry, ack timer, 1 reconnect, Complete; symmetry) | pass, all 12 invariants | 525,144 | 39 s |
| `BaselineErrCh.cfg` (same + early `Err()`) | pass | 1,952,352 | 3 m 59 s |
| `BaselineLiveness.cfg` (today's code, 1 call, retry, ack timer, reconnect, Complete; no close/cleanup; `FairSpec`) | pass (`EventuallyResolved`, `CompleteCloses`) | 4,120 | 20 s |
| `FixedRetry.cfg` (all fixes: 2 calls, 503 retry, ack timer, re-entrant handler, no reconnect buffer, reconnect, Complete; symmetry) | pass, all 12 invariants | 1,272,617 | 2 m 9 s |
| `FixedCleanup.cfg` (all fixes: 2 calls, max-pending stall, cleanup, reconnect, close, Complete; symmetry) | pass, all 12 invariants | 647,871 | 1 m 22 s |
| `FixedLiveness.cfg` (all fixes, 1 call, `FairSpec`: 503 retry, reconnect without buffer, close, cleanup, Complete) | pass (`EventuallyResolved`, `CompleteCloses`) | 61,613 | 3 m 0 s |

Counterexample reproducers (each also passes with only its `FIX_*` flag
turned on, checked separately):

| Config | Violates | States | Trace length |
|---|---|---|---|
| `PUB1_timeout_reentrant_bug.cfg` | `NoSelfDeadlock` | 13 | 6 |
| `PUB2_cleanup_stall_bug.cfg` | `NoLostStallWakeup` | 577 | 10 |
| `PUB3_first_publish_complete_bug.cfg` | `NoLostCompletion` | 132 | 7 |
| `PUB3_retry_complete_bug.cfg` | `NoLostCompletion` | 122 | 13 |
| `PUB4_retry_double_cb_bug.cfg` | `ErrCbAtMostOnce` | 339 | 20 |
| `PUB4_retry_double_resolve_bug.cfg` | `NoBlockedSendUnderLock` | 553 | 18 |
| `PUB4_retry_timeout_double_bug.cfg` | `ResolvedAtMostOnce` | 283 | 18 |
| `PUB5_closed_never_resolved_bug.cfg` | `NoStrandedFuture` | 41 | 6 |
| `PUB6_cleanup_orphan_bug.cfg` | `NoOrphanedFuture` | 81 | 7 |
| `PUB7_status_probe_deaf_bug.cfg` | `ListenerSubscribed` | 38 | 5 |
| `PUB7_remove_listener_leak_bug.cfg` | `NoLeakedResetGoroutine` | 59 | 5 |
| `PUB8_double_report_bug.cfg` | `SingleReport` | 103 | 10 |

## Tests (`modeltest/pubasync`)

- `bugs_test.go`, `legacy_test.go`, `pub6_test.go`: one `KnownBug` reproducer per
  finding. Windows the model found inside a single goroutine are opened
  deterministically: a `CustomDialer` that parks the reconnect dial (nats.go
  dials *holding `nc.mu`*, `doReconnect` → `createConn`, `nats.go` 3553), so a
  publish blocks on `nc.mu` between `registerPAF` and its failure; and an error
  handler that blocks, which keeps the reconnect worker busy so status events
  queue up. Goroutine positions are confirmed from `runtime.Stack`, not sleeps,
  except where a 503 must have arrived (100 ms on loopback).
  All 13 reproducers failed 5/5 runs with `-race -count=5`.
- `machine_test.go`: zizek stateful tests. Rules are the model's actions
  (`publish`, `ack`, `error_ack`, `no_responders` → `Respond`; `late_reply`;
  `observe` → `UserErrCall`; `complete`; `disconnect`; `cleanup`; and for the
  known-bug machines `stalled_publish`, `close`); invariants are
  `pending_agrees` (incl. `MaxPendingRespected`), `futures_agree`
  (`ResolvedAtMostOnce`, `EventuallyResolved` for settled state),
  `callbacks_agree` (`ErrCbAtMostOnce`, ack handler once), `complete_agrees`
  (`NoLostCompletion`, no early completion), `afterRelease` (`NoLostStallWakeup`).
  The server side is a scripted responder on `ctl.<run>.>` that answers only
  when a rule says so (503 is a reply with a `Status: 503` header and no body,
  exactly what `handleAsyncReply` checks), so outcomes depend on the drawn
  sequence, not timing.
  - `TestPublishAsyncMachine` (Seed 7, 80 examples × ≤25 steps): passes, ~8 s;
    a run applies ~1,900 rules (e.g. `ack:23 cleanup:331 complete:396
    disconnect:422 error_ack:43 late_reply:229 no_responders:44 observe:35
    publish:348`).
  - `TestPublishAsyncMachineStallCleanup` (KnownBug PUB-2): finds the lost
    stall wakeup, ~55-70 s including shrinking.
  - `TestPublishAsyncMachineClose` (KnownBug PUB-5): finds and shrinks to
    `publish, close` (~40 s).

Run: `cd modeltest && go test -race ./pubasync/...` (green, known bugs
skipped); `MODELTEST_KNOWN_BUGS=1 go test -race ./pubasync/...` reproduces.
No `internal_testing` hooks are needed.

**Fix validation.** The diffs below were applied to a scratch copy of the
repository (not to this checkout) and the package was run against it with a
temporary modfile: every `KnownBug` test and the three machines pass, except
`PUB-L1` (legacy, fix not drafted). Upstream tests also pass on that copy:
`./jetstream` (white-box, incl. `TestRegisterPAFUnstallOnClear`),
`./jetstream/test -run 'Publish|Cleanup|Async|Reconnect'`,
`./test -run 'Status|Listener'` and the root package with `internal_testing`.

## Findings

Severity is impact × likelihood as we judge it; "confirmed" means a Go test
reproduces it against the real library.

### PUB-1 — Ack-timeout callback runs the error handler under the publisher lock (deadlock)

- **Severity:** high (permanent deadlock of the publisher) for any user whose
  `MsgErrHandler` touches the `JetStream` it is handed — which the handler's
  own doc invites ("the original message … for possible retransmitting").
- **Trace** (6 states): `PubStart, PubReg` (timer armed) → `TmoFire` →
  `TmoLock` (holds `mu`) → `TmoSend`: `aecb(js, msg, ErrAsyncPublishTimeout)`
  is called at 396-398 inside `defer js.publisher.Unlock()`; a handler calling
  `js.PublishAsyncPending()` or `js.PublishMsgAsync(msg)` blocks on that mutex
  forever, and so does every later publisher call and ack. Every other
  resolution path calls the handler after `Unlock`
  (`handleAsyncErrAndUnlock`, the reconnect worker since #2150, `CleanupPublisher`'s deferred calls).
- **Status:** confirmed — `TestPUB1TimeoutErrHandlerReentry`; legacy
  `js.go` 1127 has the same code: `TestPUB1LegacyTimeoutErrHandlerReentry`.
- **Fix:**

```diff
@@ PublishMsgAsync (publish.go:373)
 			paf.timeout = time.AfterFunc(js.publisher.ackTimeout, func() {
 				js.publisher.Lock()
-				defer js.publisher.Unlock()
-
 				if _, ok := js.publisher.acks[id]; !ok {
 					// paf has already been resolved
 					// while waiting for the lock
+					js.publisher.Unlock()
 					return
 				}
-
 				// ack timed out, remove from pending acks
 				delete(js.publisher.acks, id)
-
 				// check on anyone stalled and waiting.
 				js.maybeUnstall()
-
-				// send error to user
-				paf.err = ErrAsyncPublishTimeout
-				if paf.errCh != nil {
-					paf.errCh <- paf.err
-				}
-
-				// call error callback if set
-				if js.publisher.asyncPublisherOpts.aecb != nil {
-					js.publisher.asyncPublisherOpts.aecb(js, paf.msg, ErrAsyncPublishTimeout)
-				}
-
-				// check on anyone one waiting on done status.
-				if js.publisher.doneCh != nil && len(js.publisher.acks) == 0 {
-					close(js.publisher.doneCh)
-					js.publisher.doneCh = nil
+				dch := js.takeDoneChIfEmpty()
+				// send error to user; the error callback runs after Unlock
+				js.handleAsyncErrAndUnlock(paf, ErrAsyncPublishTimeout)
+				if dch != nil {
+					close(dch)
 				}
 			})
@@ (new helper, next to maybeUnstall)
+// takeDoneChIfEmpty detaches doneCh when nothing is pending; the caller
+// closes it after reporting. Lock should be held.
+func (js *jetStream) takeDoneChIfEmpty() chan struct{} {
+	if js.publisher.doneCh != nil && len(js.publisher.acks) == 0 {
+		dch := js.publisher.doneCh
+		js.publisher.doneCh = nil
+		return dch
+	}
+	return nil
+}
```

(The same change in `js.go`'s timer callback: capture `dch` and `aecb`,
`Unlock`, call, close.)

### PUB-2 — CleanupPublisher does not wake publishers stalled on max pending (lost wakeup)

- **Severity:** medium-low. A caller parked in `registerPAF` (626-640) because
  `WithPublishAsyncMaxPending` was reached stays parked after
  `CleanupPublisher` empties the publisher, and fails with
  `ErrTooManyStalledMsgs` when its stall wait (default 200 ms) runs out,
  although there is room. Bounded by the stall wait, but the error is spurious.
- **Trace** (10 states): two `PubStart`, `PubReg` fills the cap, the second
  `PubReg` stalls on `stallCh`; `ClnSub, ClnLock, ClnIter, ClnEnd` delete
  every future (`jetstream.go` 1299-1309) without `maybeUnstall`, leaving
  `len(acks)=0 < maxpa` with the stall channel open. Every other removal path
  calls `maybeUnstall` (387, 524, 532, 595, 686).
- **Status:** confirmed — `TestPUB2CleanupDoesNotWakeStalledPublisher`,
  `TestPUB2LegacyCleanupDoesNotWakeStalledPublisher` (`js.go` too), and found
  by zizek in `TestPublishAsyncMachineStallCleanup` (shrunk to `publish,
  publish, error_ack, publish, stalled_publish, cleanup`; the shrinker did not
  remove the `error_ack, publish` pair, see Notes).
- **Fix** (`jetstream.go`; mirror in `js.go` by closing `js.stc`). Note it
  must land together with PUB-6's fix: once woken, the caller registers right
  after `CleanupPublisher` removed the reply subscription — exactly the PUB-6
  window, made deterministic.

```diff
@@ func (js *jetStream) CleanupPublisher() (jetstream.go:1308)
 		delete(js.publisher.acks, id)
 	}
+	// wake callers stalled on max pending: there is room now
+	js.maybeUnstall()
 	if js.publisher.doneCh != nil {
```

### PUB-3 — A publish that fails synchronously never closes PublishAsyncComplete (lost completion)

- **Severity:** medium. `clearPAF` (683-688) deletes the future and calls
  `maybeUnstall` but never looks at `doneCh`. If that future was the last one
  pending, a channel returned by `PublishAsyncComplete` while it was
  registered is never closed (until some later future resolves). Two paths:
  - **retry** (`PUB3_retry_complete_bug.cfg`, 13 states): 503 → retry
    scheduled (504) → `PublishAsyncComplete()` → `nc.Close()` → the retry's
    `PublishMsg` fails with `ErrConnectionClosed` → `clearPAF` +
    `handleAsyncErrAndUnlock` (515-518): the future gets its error, pending is
    0, Complete stays open forever. (#1719 fixed this only for the
    retries-exhausted path, 523-526.)
  - **first attempt** (`PUB3_first_publish_complete_bug.cfg`, 7 states):
    `registerPAF` inserts, `PublishAsyncComplete()`, the connection is
    reconnecting with `ReconnectBufSize(-1)`, `PublishMsg` fails with
    `ErrReconnectBufExceeded`, `clearPAF`.
- **Status:** confirmed — `TestPUB3RetryFailureLeavesCompleteOpen`
  (deterministic: 503 → Complete → Close), `TestPUB3FirstPublishFailureLeavesCompleteOpen`
  (dial gate holds `nc.mu` so the publish waits between `registerPAF` and its
  failure).
- **Fix:** see the combined PUB-3/4/8 fix below.

### PUB-4 — 503-retry error path double-resolves a future (second send can wedge the publisher)

- **Severity:** high impact (publisher-wide deadlock or duplicate error
  reports), low likelihood (needs a 503 retry racing a reconnect, a timeout or
  a close). The retry closure checks the future under the lock (505-510),
  *releases it*, re-publishes, and on a `PublishMsg` error calls
  `handleAsyncErrAndUnlock` unconditionally (515-518). If the reconnect worker
  (585-594), the ack timer (373-405, re-armed by `Reset` at 410-412) or
  `CleanupPublisher` resolved the future in between, it is resolved a second
  time: `paf.err` is overwritten, the error handler runs twice for one message,
  and if the caller already holds `Err()` (cap 1) without having read the
  first error, `paf.errCh <- paf.err` (563-565) **blocks forever while holding
  `js.publisher`'s mutex** — every `PublishAsync`, `PublishAsyncPending`,
  `Ok()`/`Err()` call and every incoming ack then hangs.
- **Traces:** `PUB4_retry_double_cb_bug.cfg` (20 states): `Respond nr` →
  `DispLock` schedules the retry → `RtyFire, RtyGet` (future still pending) →
  `Disconnect` → `RstRecv … RstCb` (ErrDisconnected, handler #1) → `RtyPub`
  fails (no reconnect buffer) → `RtyClear, RtyELock, RtySend, RtyCb` (handler
  #2). `PUB4_retry_double_resolve_bug.cfg`: same with `UserErrCall` early →
  `RtyELock` reaches the send with a full `errCh`. `PUB4_retry_timeout_double_bug.cfg`:
  the resolver is the re-armed ack timer, and `PublishMsg` fails on a closed
  connection.
- **Status:** confirmed — `TestPUB4RetryErrorAfterReconnectReportsTwice`
  (handler gets `[server is disconnected, outbound buffer limit exceeded]`)
  and `TestPUB4RetryErrorAfterReconnectWedgesPublisher` (`PublishAsyncPending`
  never returns). Deterministic: the reconnect worker is kept busy in a blocked
  error handler from an earlier disconnect so the next RECONNECTING event
  waits in its channel; the dial gate holds `nc.mu` so the retry's
  `PublishMsg` waits; both are released in the model's order.
- **Fix:** below.

### PUB-8 — A message is reported both to the error handler and as PublishMsgAsync's error

- **Severity:** low. Between `registerPAF` and a failing `PublishMsg` the future
  can be failed by the ack timer, a reconnect or `CleanupPublisher` (error
  handler called); `PublishMsgAsync` then *also* returns the `PublishMsg` error
  (424). A caller that retransmits on both paths sends the message twice.
- **Trace** (10 states): `PubReg` → `Disconnect` → reconnect worker fails it
  (handler) → `PubSend` fails → `PubClear` → returns error.
- **Status:** confirmed — `TestPUB8SyncErrorAfterHandlerReport` (ack timer
  fires while the dial gate holds the publish; handler gets
  `timeout waiting for ack`, `PublishAsync` returns `outbound buffer limit
  exceeded`).

**Fix for PUB-3, PUB-4 and PUB-8** (one place: the `PublishMsg` failure branch
owns the outcome only if the future is still pending; the retry closure's
`if err != nil` branch at 515-518 becomes unreachable and can be dropped):

```diff
@@ func (js *jetStream) PublishMsgAsync (publish.go:422)
 	if err := js.conn.PublishMsg(pubMsg); err != nil {
-		js.clearPAF(id)
-		return nil, err
+		js.publisher.Lock()
+		if _, ok := js.publisher.acks[id]; !ok {
+			// Already resolved -- and reported -- by the ack timer, a
+			// reconnect or CleanupPublisher while we were publishing.
+			js.publisher.Unlock()
+			return paf, nil
+		}
+		delete(js.publisher.acks, id)
+		js.maybeUnstall()
+		dch := js.takeDoneChIfEmpty()
+		if o.pafRetry == nil {
+			js.publisher.Unlock()
+			if dch != nil {
+				close(dch)
+			}
+			return nil, err
+		}
+		// A retry owns the future's resolution.
+		js.handleAsyncErrAndUnlock(paf, err)
+		if dch != nil {
+			close(dch)
+		}
+		return paf, nil
 	}
```

### PUB-5 — Futures pending when the connection closes are never resolved

- **Severity:** medium (liveness). `resetPendingAcksOnReconnect` returns on
  `CLOSED` (578-581) without failing what is pending; after `nc.Close()` the
  reply subscription is gone, so without `WithPublishAsyncTimeout` those
  futures' `Ok()`/`Err()` never fire and `PublishAsyncComplete` never closes —
  `select { case <-js.PublishAsyncComplete(): … }` with no timeout hangs.
  A connection that goes CONNECTED → CLOSED directly (`nc.Close()`, or no
  reconnect allowed) never sends RECONNECTING, so nothing fails them.
  (`CleanupPublisher` would, but that is not documented as required.)
- **Trace** (6 states): `PubStart, UserComplete, PubReg, PubSend, Close,
  RstRecv` (exits). Also violated as `EventuallyResolved` under `FairSpec`.
- **Status:** confirmed — `TestPUB5CloseStrandsPendingFutures`, and zizek
  `TestPublishAsyncMachineClose` (shrunk to `publish, close`). Same in `js.go`
  (762-764). Arguably a design choice, but nothing documents it and the
  reconnect path already does the right thing.
- **Fix:**

```diff
@@ func (js *jetStream) resetPendingAcksOnReconnect() (publish.go:577)
 	for {
 		newStatus, ok := <-connStatusCh
-		if !ok || newStatus == nats.CLOSED {
+		if !ok {
 			return
 		}
+		failErr := nats.ErrDisconnected
+		if newStatus == nats.CLOSED {
+			failErr = nats.ErrConnectionClosed
+		}
 		js.publisher.Lock()
 		errCb := js.publisher.asyncPublisherOpts.aecb
 		var msgs []*nats.Msg
 		for id, paf := range js.publisher.acks {
-			paf.err = nats.ErrDisconnected
+			paf.err = failErr
@@
 		for _, msg := range msgs {
-			errCb(js, msg, nats.ErrDisconnected)
+			errCb(js, msg, failErr)
 		}
 		if dch != nil {
 			close(dch)
 		}
+		if newStatus == nats.CLOSED {
+			return
+		}
 	}
```

### PUB-6 — CleanupPublisher racing PublishMsgAsync orphans a future under a dead reply prefix

- **Severity:** medium-low (needs `CleanupPublisher` concurrent with
  publishing; the doc says the context stays usable afterwards). `newAsyncReply`
  (362) and `registerPAF` (367) are separate lock holds. A cleanup in between
  unsubscribes the reply subscription and removes the status listener but keeps
  `js.publisher.replyPrefix` (`jetstream.go` 1317-1331); `registerPAF` inserts
  the future under that prefix (657), the publish goes out with a reply subject
  nobody listens on, and the future is never resolved (no ack timeout) —
  `PublishAsyncComplete` hangs and the slot counts against max pending.
- **Trace** (7 states): `PubStart` (sub gen 1) → `ClnStart, ClnSub` (sub
  removed) `ClnLock, ClnEnd` → `PubReg` under prefix 1 with no sub.
- **Status:** confirmed (stress) — `TestPUB6CleanupRaceOrphansFuture` races
  `PublishAsync` against `CleanupPublisher`; it orphaned a future at round 248
  of 20,000 (2.1 s) on the first run. With the PUB-2 fix alone this becomes
  deterministic (a stalled caller woken by cleanup always registers in the
  window), so the two fixes go together.
- **Fix** (cleanup clears the prefix; `registerPAF` re-creates the subscription
  under the insert lock when it sees that):

```diff
@@ func (js *jetStream) cleanupReplySub() (jetstream.go:1322)
 		js.publisher.replySub.Unsubscribe()
 		js.publisher.replySub = nil
+		js.publisher.replyPrefix = ""
 	}
@@ func (js *jetStream) newAsyncReply() error (publish.go:438)
 	js.publisher.Lock()
 	defer js.publisher.Unlock()
+	return js.ensureAsyncReplyLocked()
+}
+
+// ensureAsyncReplyLocked is newAsyncReply with the publisher lock held.
+func (js *jetStream) ensureAsyncReplyLocked() error {
 	if js.publisher.replySub == nil {
@@ func (js *jetStream) registerPAF (publish.go:622)
 			js.publisher.acks = make(map[string]*pubAckFuture)
 		}
+		// CleanupPublisher ran since newAsyncReply (it clears the prefix):
+		// never register under a prefix nobody listens on.
+		if js.publisher.replyPrefix == "" {
+			if err := js.ensureAsyncReplyLocked(); err != nil {
+				js.publisher.Unlock()
+				return "", err
+			}
+		}
 		maxpa := js.publisher.asyncPublisherOpts.maxpa
```

### PUB-7 — Status listeners eat buffered events: the publisher goes deaf to reconnects, or its worker leaks

Root cause in core `nats.go`, observed through the publisher (sibling of
#1993, which switched `cleanupReplySub` to `RemoveStatusListener`).

- **PUB-7a, deaf** (`PUB7_status_probe_deaf_bug.cfg`, 5 states):
  `sendStatusEvent` (6833) checks "is the channel closed?" with
  `select { case <-ch: delete…; continue }`, which also succeeds when the
  channel merely holds an *unread* event — it consumes that event and
  unregisters the listener. So if the reconnect worker has not yet read one
  RECONNECTING when the next one is sent (it is busy, e.g. in a slow error
  handler, or simply not scheduled during a fast flap), the publisher is
  silently unsubscribed from RECONNECTING for good; later reconnects no longer
  fail pending futures (which then hang without an ack timeout) and do not
  close `PublishAsyncComplete`.
- **PUB-7b, leak** (`PUB7_remove_listener_leak_bug.cfg`, 5 states):
  `RemoveStatusListener` (6799) does `select { case <-ch: default: close(ch) }`:
  with an event buffered it consumes it and does *not* close. `CleanupPublisher`
  at that moment unregisters the channel without closing it, and the reconnect
  worker blocks on it forever (one goroutine leaked per occurrence).
- **Severity:** medium (7a: silent loss of reconnect handling; needs a busy
  worker or a fast flap), low (7b: a goroutine leak).
- **Status:** confirmed — `TestPUB7PublisherGoesDeafToReconnects` (error
  handler blocked during three disconnects; a future pending across the
  fourth is never failed) and `TestPUB7CleanupLeaksReconnectWorker`
  (`resetPendingAcksOnReconnect` goroutine still running 2 s after cleanup).
  The same `sendStatusEvent` is used by every `StatusChanged` user (pull
  consumers, ordered consumers, `js.go`) — we only exercised the publisher.
- **Fix** (`nats.go`). It changes `RemoveStatusListener` to always close (a
  receiver still drains a buffered event first, then sees the close); the
  upstream `test/` status-listener tests pass with it. If that semantic change
  is unwanted, the publisher alone can be fixed by giving
  `resetPendingAcksOnReconnect` its own stop channel, but 7a would remain for
  every listener.

```diff
@@ func (nc *Conn) RemoveStatusListener(ch chan (Status)) (nats.go:6799)
 	nc.mu.Lock()
 	defer nc.mu.Unlock()
 
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
+	// Close it even if an event is buffered: a receiver drains the event
+	// and then sees the close. Tolerate a channel the caller closed.
+	defer func() { _ = recover() }()
+	close(ch)
 }
@@ func (nc *Conn) sendStatusEvent(s Status) (nats.go:6833)
 func (nc *Conn) sendStatusEvent(s Status) {
-Loop:
 	for ch := range nc.statListeners[s] {
-		// make sure channel is not closed
-		select {
-		case <-ch:
-			// if chan is closed, remove it
+		// Never receive from ch here: that would consume an event the
+		// listener has not read yet. Drop closed channels instead.
+		if !trySendStatus(ch, s) {
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
+// trySendStatus sends s on ch without blocking (dropping it if ch is full)
+// and reports false if ch has been closed.
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

### PUB-L1 — Legacy `js.go`: PublishAsyncMaxPending is exceeded

- **Severity:** low (deprecated API; documented limit not enforced). `js.go`
  `PublishMsgAsync` registers the future first (`registerPAF`, 828-840) and
  only then stalls if `numPending > maxPending` (1091), so `PublishAsyncPending`
  reads `maxpa+1` (and more with more stalled callers) during the stall; a
  woken caller does not re-check the cap. #2126 fixed exactly this in
  `jetstream/` only.
- **Status:** confirmed — `TestPUBL1LegacyMaxPendingExceeded` (pending 2 with
  `PublishAsyncMaxPending(1)`). Not modeled separately (the model is of the
  new code, where `MaxPendingRespected` holds in every config).
- **Fix:** port #2126's `registerPAF` (stall loop before insert, under one
  lock hold) to `js.go`; not drafted here.

## Legacy `js.go` vs `jetstream/` (same design, compared by reading + tests)

| Finding | `js.go` |
|---|---|
| PUB-1 | same (timer calls `aecb` under `js.mu`, 1127) — confirmed |
| PUB-2 | same (`CleanupPublisher` never closes `js.stc`) — confirmed |
| PUB-3, PUB-4, PUB-8 | same code shape (`clearPAF` without `dch`; retry `doErr` unconditional) — by reading, not tested |
| PUB-5 | same (`resetPendingAcksOnReconnect` returns on CLOSED) — by reading |
| PUB-6 | different: `newAsyncReply` draws the token and `registerPAF` inserts in separate holds, same window — by reading |
| PUB-7 | 7a applies; for 7b `cleanupReplySub` still does `close(js.connStatusCh)` directly (the pre-#1993 code), which can race `sendStatusEvent`'s send on the same channel ("send on closed channel" panic) — by reading only |
| PUB-L1 | legacy only — confirmed |

## Things checked that hold

- `MaxPendingRespected` (the #2126 fix) holds in every configuration,
  including 503 retries (a retry never re-inserts) and stalls.
- Ack vs. timer race: `handleAsyncReply` and the timer callback both re-check
  `acks` under the lock, so an ack arriving while the timer fires resolves the
  future exactly once (Baseline, and `late_reply` in the zizek machine).
- #2150's ordering (handler calls, then `doneCh` close, both after `Unlock`)
  holds on the reconnect path; `PublishAsyncComplete` never closes early in any
  explored state.
- Duplicate / late replies and replies for a previous 503 attempt are ignored
  once the future is resolved.

## Notes

- Modeling artifact found and fixed: the first `FixedLiveness` run violated
  `EventuallyResolved` because the property demanded that a future whose
  *first* publish failed synchronously be "resolved"; such a call returns the
  error from `PublishMsgAsync` instead (the caller never gets the future). The
  property now accepts either.
- Larger variants (`FixedRetry` with `close` and early `Err()`, 2.18 M states,
  5 m 46 s; `FixedCleanup` with two disconnects and early `Err()`, > 2.8 M
  states, stopped) also found no violation in what they explored but were cut
  to stay under ~5 minutes.

- zizek: in `TestPublishAsyncMachineStallCleanup` the shrinker stopped at six
  steps (`publish, publish, error_ack, publish, stalled_publish, cleanup`)
  though `error_ack, publish` can be dropped (`publish, publish,
  stalled_publish, cleanup` fails too); rejected leading steps also stay in
  the trace. The property is wall-clock dependent (a 1 s wake budget), which
  may limit the shrinker; not investigated further, and not claimed as a zizek
  bug.
- The tests open each window with a gate (a parked dial, a blocked error
  handler) rather than sleeps. Those gates are legitimate client behaviour —
  a slow `CustomDialer`, a slow error handler — but in production the same
  interleavings depend on scheduling.
