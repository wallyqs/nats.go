# OrderedConsumer — TLA+ models of the JetStream ordered consumer

Models of `jetstream/ordered.go` (the `jetstream` package's ordered consumer)
and the parts of `jetstream/pull.go` it is built on, with tests derived from
them in `modeltest/ordered/`. Line numbers refer to upstream main at
`31a3ee4` (the base of this branch), which is the code the findings and the
`*_bug.cfg` reproducers describe. The fixes are on `fix/ordered`; the
models can check either version (see "Fixes in the models").

| Spec | Covers |
|------|--------|
| `OrderedConsumer.tla` | `Consume`: the reset goroutine, `reset()`, `errHandler`, the ordered `internalHandler`, `Stop`, over inner pull `Consume` subscriptions (dispatcher, status goroutine, cleanup) |
| `OrderedMessages.tla` | `Messages`: `orderedSubscription.Next`, `resetMessages`, `Stop`, over inner pull iterators (`pullSubscription.Next`, status goroutine, per-call heartbeat timer) |
| `OrderedFetch.tla` | `Fetch` / `FetchBytes` / `Next` / `FetchNoWait`: cursor hand-over between successive fetches, `runningFetch`, the mutex |

Run: `TLA2TOOLS=/path/to/tla2tools.jar models/tlc.sh OrderedConsumer`.
Tests: `cd modeltest && ./run.sh ./ordered/...`.

**ORD-12** is fixed on the merged `claude/magical-lovelace-my0imq-fixes`
branch by a pull.go change (the inner `Next` returns `ErrNoResponders` to an
ordered `Messages`, the only user of `notifyOnReconnect`, which then
resets); its reproducer runs as a regression test there.

## Findings

| Id | Severity | What | Status |
|----|----------|------|--------|
| ORD-1 | high | `Fetch`/`FetchBytes` return `ErrOrderedConsumerConcurrentRequests` with the consumer mutex still held; every later call on the consumer blocks forever | confirmed (deterministic); **fixed on fix/ordered** |
| ORD-2 | high | `FetchNoWait` neither applies nor records the cursor; the next fetch re-delivers the same messages | confirmed (deterministic), also found by zizek; **fixed on fix/ordered** |
| ORD-3 | high | `ConsumeErrHandler` runs with the consumer mutex held; calling `Stop()` on the ordered `ConsumeContext`, or `CachedInfo()`/`Info()`, from it deadlocks | confirmed (deterministic); **fixed on fix/ordered** |
| ORD-4 | high | Across a reset, the `Consume` handler runs concurrently for two messages and messages can be handled out of order | confirmed (deterministic), also found by zizek; **fixed on fix/ordered** |
| ORD-5 | medium | `Messages`: `Stop()` during a reset leaves a live inner iterator; `Next` after `Stop` keeps returning messages, and the inner subscription leaks | confirmed (scheduler-dependent, ~1 in 20–100 attempts; test repeats up to 600); **fixed on fix/ordered** |
| ORD-6 | medium | `Consume`: `Stop()` during a reset; the reset goroutine still starts a new inner subscription (and pull) after `Stop` returned, whose messages can reach the handler | confirmed (new pull after Stop: deterministic; late handler call: scheduler-dependent); **fixed on fix/ordered** |
| ORD-7 | high | Connection closed while `reset()` retries: the consume never ends / `Next` never returns; `ErrConnectionClosed` is never reported | confirmed (deterministic); **fixed on fix/ordered** |
| ORD-8 | medium | `Next(NextMaxWait/NextContext)` ignores its deadline while resetting | confirmed (deterministic); **fixed on fix/ordered** |
| ORD-9 | low | StopAfter reached in a narrow window around a reset: the next inner `Consume` gets `StopAfter(0)`, and the user's error handler gets `ErrInvalidOption` with a **nil** `ConsumeContext` | model only (window of a few instructions); **fixed on fix/ordered** (checked by the model) |
| ORD-10 | low (limitation) | A lost message is only detected when a later message arrives; if the lost message was the last one, the consumer stalls until the next publish | model only; open (limitation) |
| ORD-11 | low (inherited) | A message dequeued after `Stop()` returned still reaches the handler (`Stop` returns before the inner subscription is unsubscribed; no layer checks closed) | model only; pull.go behavior. **Closed for the ordered consumer on fix/ordered** by the ORD-6 handler check (checked by the model); the pull.go part is left to the pull.go fixes |
| ORD-12 | high | `Messages` never recovers if its server-side consumer disappears while no pull waits (deleted, or removed after `InactiveThreshold` while the app paused) and `Next` is polled with a deadline below two heartbeats | confirmed (deterministic, both triggers); fixed by a pull.go change on the merged fixes branch (see ORD-12) |
| ORD-13 | low | The ordered default heartbeat (5s) never reaches the inner pull; a stalled pull is detected after 30s, not 10s | confirmed (deterministic); **fixed on fix/ordered** |

Details, traces and proposed fixes are below. Cross-reference: PUB-7
(`models/PublishAsync/README.md`), status-listener event loss, discussed at the
end.

## Scope and abstractions

* **Goroutines** modeled individually, at the granularity of `c.Mutex`
  acquire/release, channel sends and receives, and atomics.
  * `Consume`: the reset goroutine, the user calling `Stop`, and for each inner
    subscription its message dispatcher (nats.go `waitForMsgs` → pull.go
    `internalHandler` → ordered `internalHandler`), its status goroutine and
    its `pullMessages` cleanup.
  * `Messages`: the user's `Next` loop, the user calling `Stop`, and for each
    inner iterator its status goroutine, its cleanup, and the per-call
    heartbeat timer.
  * `Fetch`: the user and each fetch goroutine.
* **Channels with their real capacities**: `c.doReset` (1); `errs` (10);
  status listener channels (10). The nats.go async pending queue and the
  `Messages` channel are capped at 2 entries in the model. A full queue only
  delays the server here; drops are modeled separately as `Lose`.
* **Environment**: a stream of `N` messages that all match the filter.
  Server-side consumers deliver (consumer seq, stream seq) from their start
  sequence. Fault actions, each with a budget:
  * `Lose`: a message is lost on the way.
  * `HbFault`: spurious missed heartbeat.
  * `ExtDelete`: the consumer is deleted while a pull waits, so a 409 is sent.
  * `SilentLoss` (Messages only): the consumer is deleted while no pull
    waits, so nothing is sent (the InactiveThreshold case).
  * `Expire`: a pull request expires with a 408.
  * `Disconnect` / `Reconnect` / `Close` of the connection.
  * Consumer create failures.
  * The async `DeleteConsumer` of `reset()`, which succeeds when connected and
    fails otherwise.
  * A missed heartbeat is *forced* (weakly fair) when nothing can send
    heartbeats: the consumer is gone or the connection is not up. In Consume,
    the timer lives as long as the subscription. In Messages, it is created
    per inner `Next` call and never fires under a short deadline
    (`ShortDeadline`).
* **Not modeled**:
  * Byte limits (`MaxBytes`), priority groups and pinning.
  * `Drain` of a `Consume` (Messages `Drain` shares `Stop`'s structure).
  * `Info()` and heartbeats' wall-clock values.
  * `serialNumberFromConsumer` parsing, which is assumed correct.
  * The nats.go status-listener quirk of PUB-7 (see the end): the model
    delivers every status event.
  * Timing is untimed. A "deadline" or "heartbeat timer" is an event that
    may fire.
  * Liveness assumes weak fairness of all goroutines and of the forced
    environment events, plus strong-enough fairness for Go's random `select`
    (the deadline case of `pullSubscription.Next` has its own WF).
* **Bounds**: `N = 1..2` messages (Fetch: 5), at most 3–5 inner
  subscriptions, and one or two faults per run. `BoundOK` checks that the
  subscription table was large enough. `MaxResetAttempts` (`MaxReset`) is
  modeled, but every passing config uses the default (unlimited).

## Action → code map

### Consume (`OrderedConsumer.tla`)

| Action | Code |
|--------|------|
| `Init` | `OrderedConsumer()` creates serial 1 (jetstream.go:917-929); `Consume()` reuses it (ordered.go:85-160) |
| `DPop` | nats.go:3806-3849 pop unless unsubscribed; pull.go:246-247 timer stop; 409 → pull.go:262-283; 503 → pull.go:264-274 |
| `DH1` | ordered.go:121-148 (serial check, dseq check, StopAfter guard, cursor update) under `c.Mutex` |
| `DRun` / `DEnd` | `handler(msg)` after `c.Unlock()` (ordered.go:148-149); `last` → `sub.Stop()` (150-152) |
| `DSt1` / `DSt2`, `USt1` / `USt2`, `SCst*`, `RGSa*` | `orderedSubscription.Stop` CAS (ordered.go:359) / locked part (362-367) |
| `DPost` | pull.go:290-301 (inner delivered++, inner StopAfter, timer re-arm) |
| `DEh`, `SEh`, `RGErr`, `RGCErr` | `errHandler` (ordered.go:204-233): user handler under the lock (206-209), close → Stop (211-219), reset trigger (221-231) |
| `DEhBlk`, `SEhBlk` | blocking `c.doReset <-` while holding the lock (229) |
| `SEv` / `SErr` / `SPost` / `SExit` | status goroutine pull.go:348-428 |
| `Cleanup` | pull.go:1093-1094, 1120-1150 |
| `RGSel` | select on `doReset` / `sub.done` (ordered.go:163-165, 190) |
| `RGChk` | StopAfter check (166-172) |
| `RGR1` | `reset()`: lock, stop current sub, async delete, `deliverSeq = 0`, `serial++` (552-571, 604) |
| `RGR2` / `RGR3` | `retryWithBackoff` attempt: closed check (584-587), `CreateOrUpdateConsumer` with the lock held (588-594), retries (753-812) |
| `RGOpts` / `RGCons` / `RGAsg` | ordered.go:179-189 (`innerOpts`, inner `Consume` without lock or closed check, assign) |
| `RGDn` | ordered.go:190-197 |
| `Deliver`, `Lose`, `ExtDelete`, `DelDone`/`DelFail`, `Disconnect`/`Reconnect`/`Close`, `HbFault`/`HbForced` | environment; `Broadcast` = nats.go:6833-6850; timer = pull.go:1106-1118 |

### Messages (`OrderedMessages.tla`)

| Action | Code |
|--------|------|
| `NCall` | user calls `Next`; per-call heartbeat timer (pull.go:659-664) |
| `NInner` | `pullSubscription.Next` (pull.go:648-739): closed/StopAfter checks, `checkPending` pull, msg / 409 (termErr) / 503 (swallowed, 697-702) / 408; errs: hb, connected, disconnected |
| `NTimeout` | deadline case (pull.go:740-743) |
| `NMeta` | ordered.go:305-323 |
| `NCst1`/`NCst2` | `s.Stop()` on `ErrMsgIteratorClosed` (ordered.go:295-298) |
| `NReset`, `NR1`-`NR3`, `NMsgs`/`NMsgsOK`, `NAsg` | `resetMessages` (ordered.go:331-356) and `reset()` |
| `SEv`/`SExit`/`Cleanup` | pull.go:572-609, 554-565, 1093-1150 |
| `SilentLoss`, `Expire` | consumer removed with no waiting pull; 408 |

### Fetch (`OrderedFetch.tla`)

| Action | Code |
|--------|------|
| `F1` | ordered.go:415-434 (incl. the early return at 421-423 that keeps the lock) |
| `NW` | ordered.go:496-508 |
| `Reset` | `reset()` + `currentConsumer.Fetch` / `FetchNoWait`; `runningFetch` set at 443 only for Fetch |
| `Deliver`/`Finish` | fetch goroutine pull.go:966-1034 (`res.sseq`, `done`) |

## Properties

| Name | Spec | Meaning |
|------|------|---------|
| `InOrder` | all | the user gets stream seqs 1, 2, 3, … (no gap, no duplicate, in order) |
| `NoDupNoGap` | Consume | weaker: no duplicate, and nothing skipped (allowing a message whose handler call is in flight) |
| `SerialHandler` | Consume | the handler never runs for two messages at once |
| `StopAfterBound` | Consume, Messages | at most StopAfter messages |
| `NoSelfDeadlock` | Consume | no goroutine waits for `c.Mutex` while holding it |
| `NoSpuriousInvalid` | Consume | the user's error handler never gets `ErrInvalidOption` from the consumer's own inner Consume |
| `NoResurrectedDelivery` / `NoMsgAfterStop` | Consume / Messages | no message from an inner subscription created after `Stop` returned / no `Next` begun after `Stop` returns a message |
| `NoDeliveryAfterStop` | Consume | strict: no message dequeued after `Stop` returned reaches the handler |
| `AtMostOneLive` | Consume, Messages | at most one inner subscription not stopped |
| `LockFree`, `CallsReturn` | Fetch | a returned call left the mutex free; no call blocks forever |
| `AllDelivered` | Consume, Messages | (liveness) every message (or StopAfter of them) reaches the user |
| `StopAfterCloses` | Consume | (liveness) StopAfter closes the consume |
| `CloseEndsConsume` / `CloseEndsNext` | Consume / Messages | (liveness) a closed connection ends the consume / makes `Next` return |
| `DeadlineHonored` | Messages | (liveness) a `Next` whose deadline passed returns |
| `StopReturns`, `NoLeakedSub` | Consume | (liveness) Stop returns; every inner subscription is eventually unsubscribed |

## Fixes in the models

Each spec defines `Fixes == {}`, the code at `31a3ee4`, and `AllFixes`, the
fixes on `fix/ordered` that the spec models. A cfg switches them on by
overriding the definition: `CONSTANT Fixes <- AllFixes`. The `*_bug.cfg`
reproducers leave it alone and keep checking the old code.

| Spec | `AllFixes` | Modeled as |
|------|------------|------------|
| `OrderedFetch` | ORD-1, ORD-2 | the rejected call leaves `lock = "none"`; `FetchNoWait` takes the `Fetch` path (`F1`) and its result is recorded as `runningFetch` |
| `OrderedConsumer` | ORD-3, ORD-4, ORD-6, ORD-7, ORD-9 | ORD-3: the user's `Stop()` from the error handler is its CAS plus a locked step (`DUStop`/`SUStop`/`RGUStop`), after which the handler continues (`eh2`). ORD-4: a new variable `hmu` (holder of `handlerMu`) taken in `DH1`, released in `DPost`. ORD-6: `RGOpts` and `RGAsg` check `oclosed`; `DH1` drops messages once `odone` (the `stopped` flag; Drain is not modeled). ORD-7: `RGR2`/`RGR3`/`RGCons` on a closed connection go to `RGCl` (report, then `Stop`). ORD-9: `RGOpts` re-checks StopAfter |
| `OrderedMessages` | ORD-5, ORD-7, ORD-8 | ORD-5: `NAsg` stops the new iterator when `oclosed`. ORD-7: `NR2`/`NR3`/`NMsgs` on a closed connection go to `NCst1`. ORD-8: `NR2` returns once the deadline passed and sets `nx.pend`, which the next `NCall` turns into a reset; the deadline is not restarted after a reset |

Every passing cfg runs the fixed code, and each `*_bug.cfg` of a fixed
finding has a `*_fixed.cfg` counterpart: the same scenario with the fixes,
expected to pass. Two of them use smaller bounds than their `_bug` twin,
because the full state space with liveness was too large (both still
violate their property without the override, measured):
`CloseDuringReset_fixed.cfg` has `N = 0` (with `N = 1` the space is 7.6M
states before the liveness check), and `OrderedMessages_timeout_fixed.cfg`
triggers the reset with the forced missed heartbeat instead of a deletion.
One abstraction was added for ORD-8: a reset retried after the deadline
interrupted it reuses the serial (`NR1`), which keeps the state space finite
when every `Next` call times out; the interrupted reset created no consumer
in the model, so the label is unused.

ORD-12 (pull.go) is not modeled as fixed: `OrderedMessages_noresp_bug.cfg`
still fails with the ordered fixes, as expected. ORD-10 is open and ORD-13
is timing, which the models do not represent.

## TLC results (measured)

TLC 2026.10.06, `-workers auto` on 4 cores shared with other runs (load
average 8–10 during the measurement), one `models/tlc.sh` call per cfg on
fix/ordered. `*_bug.cfg` check the code at `31a3ee4`; every other cfg checks
the fixed code (`Fixes <- AllFixes`).

| Config | Spec | Checks | Result | Distinct states | Time |
|--------|------|--------|--------|-----------------|------|
| `CloseDuringReset_bug.cfg` | Consume | ORD-7 `CloseEndsConsume` | violated | — | 4s |
| `CloseDuringReset_fixed.cfg` | Consume | ORD-7 fixed: `CloseEndsConsume` (N = 0) | pass | 372,676 | 2m02s |
| `ConcurrentHandler_bug.cfg` | Consume | ORD-4 `SerialHandler` | violated | — | 2s |
| `ConcurrentHandler_fixed.cfg` | Consume | ORD-4 fixed: `SerialHandler` | pass | 168,418 | 6s |
| `DeliveryAfterStop_bug.cfg` | Consume | ORD-11 `NoDeliveryAfterStop` | violated | — | 1s |
| `DeliveryAfterStop_fixed.cfg` | Consume | ORD-11 (ORD-6 fix): `NoDeliveryAfterStop` | pass | 486 | 1s |
| `ErrHandlerStop_bug.cfg` | Consume | ORD-3 `NoSelfDeadlock` | violated | — | 1s |
| `ErrHandlerStop_fixed.cfg` | Consume | ORD-3 fixed: `NoSelfDeadlock` | pass | 180,444 | 6s |
| `ExtDelete.cfg` | Consume | safety incl. `InOrder`, `SerialHandler`; ExtDelete + Lose | pass | 1,366,669 | 39s |
| `Live.cfg` | Consume | AllDelivered; HbFault | pass | 11,181 | 4s |
| `Live_del.cfg` | Consume | AllDelivered; ExtDelete | pass | 44,028 | 7s |
| `Live_disc.cfg` | Consume | AllDelivered; reconnect | pass | 168,418 | 23s |
| `Live_stop.cfg` | Consume | StopReturns, NoLeakedSub, no delivery after Stop; Stop + HbFault | pass | 462,837 | 2m16s |
| `OrderedConsumer.cfg` | Consume | safety incl. `InOrder`, `SerialHandler`; Lose + HbFault | pass | 332,352 | 12s |
| `OutOfOrder_bug.cfg` | Consume | ORD-4 `InOrder` | violated | — | 3s |
| `OutOfOrder_fixed.cfg` | Consume | ORD-4 fixed: `InOrder` | pass | 168,418 | 5s |
| `Reconnect.cfg` | Consume | safety incl. `InOrder`, `SerialHandler`; reconnect | pass | 168,418 | 6s |
| `StopAfter.cfg` | Consume | StopAfter = 1 incl. `InOrder`; HbFault | pass | 47,779 | 9s |
| `StopAfterInvalid_bug.cfg` | Consume | ORD-9 `NoSpuriousInvalid` | violated | — | 2s |
| `StopAfterInvalid_fixed.cfg` | Consume | ORD-9 fixed: `NoSpuriousInvalid` | pass | 641,520 | 15s |
| `StopResurrect_bug.cfg` | Consume | ORD-6 `NoResurrectedDelivery` | violated | — | 2s |
| `StopResurrect_fixed.cfg` | Consume | ORD-6 fixed: `NoResurrectedDelivery` | pass | 462,837 | 12s |
| `TailLoss_bug.cfg` | Consume | ORD-10 `AllDelivered` (open) | violated | — | 2s |
| `OrderedMessages.cfg` | Messages | safety; Lose + HbFault | pass | 11,753 | 2s |
| `OrderedMessages_close_bug.cfg` | Messages | ORD-7 `CloseEndsNext` | violated | — | 2s |
| `OrderedMessages_close_fixed.cfg` | Messages | ORD-7 fixed: `CloseEndsNext` | pass | 5,127 | 2s |
| `OrderedMessages_live.cfg` | Messages | AllDelivered + safety; HbFault + ExtDelete | pass | 142,999 | 14s |
| `OrderedMessages_live_del.cfg` | Messages | AllDelivered + safety; ExtDelete | pass | 2,521 | 2s |
| `OrderedMessages_live_disc.cfg` | Messages | AllDelivered + safety; reconnect | pass | 8,756 | 2s |
| `OrderedMessages_noresp_bug.cfg` | Messages | ORD-12 `AllDelivered` | violated | — | 1s |
| `OrderedMessages_resurrect_bug.cfg` | Messages | ORD-5 `NoMsgAfterStop` | violated | — | 2s |
| `OrderedMessages_resurrect_fixed.cfg` | Messages | ORD-5 fixed: `NoMsgAfterStop` | pass | 48,847 | 2s |
| `OrderedMessages_stopafter.cfg` | Messages | StopAfter = 1; HbFault + ExtDelete | pass | 251,917 | 24s |
| `OrderedMessages_timeout_bug.cfg` | Messages | ORD-8 `DeadlineHonored` | violated | — | 4s |
| `OrderedMessages_timeout_fixed.cfg` | Messages | ORD-8 fixed: `DeadlineHonored` (reset by missed heartbeat) | pass | 182,226 | 17s |
| `OrderedFetch.cfg` | Fetch | `InOrder`, `LockFree`, `CallsReturn`; Fetch + FetchNoWait, calls may overlap | pass | 2,544 | 1s |
| `OrderedFetch_locked_bug.cfg` | Fetch | ORD-1 `LockFree` | violated | — | 1s |
| `OrderedFetch_locked_fixed.cfg` | Fetch | ORD-1 fixed: `LockFree`, `CallsReturn` | pass | 92 | 1s |
| `OrderedFetch_nowait_bug.cfg` | Fetch | ORD-2 `InOrder` | violated | — | 1s |
| `OrderedFetch_nowait_fixed.cfg` | Fetch | ORD-2 fixed: `InOrder` | pass | 77 | 1s |

`tlc.sh` does not print the state count of a violation, hence "—"; TLC stops
at the first one. Total wall time for all 40 configs: 9m17s (a second run
of `models/tlc.sh OrderedConsumer` passed in 15m29s under heavier load).
Times vary with the load: `CloseDuringReset_bug.cfg` took 1m05s in the timed
loop and 4s when re-run at load average 3 (the 4s is in the table). Bigger fault combinations with liveness ran out of the
15-minute budget when the models were written: Messages with HbFault +
ExtDelete + reconnect, and Consume with every fault at once. Those
combinations are split across the configs above.

`models/tlc.sh` matches both "is violated" and "was violated", so the
liveness reproducers (`CloseDuringReset_bug`, `TailLoss_bug`,
`OrderedMessages_close_bug`, `OrderedMessages_timeout_bug`,
`OrderedMessages_noresp_bug`) are reported as reproduced.

Each bug still lives in its own config, but the baselines now run the fixed
code, so the Consume safety baselines check `InOrder` and `SerialHandler`
(ORD-4) in addition to `NoDupNoGap`. Liveness configs still leave out
`Lose`, because of ORD-10.

## Tests (`modeltest/ordered`)

* `bugs_test.go`: one reproducer per confirmed finding. Each asserts the
  correct behavior. On fix/ordered they are regression tests that run by
  default, except ORD-12, which stayed behind `harness.KnownBug` until its
  pull.go fix landed on the merged fixes branch.
  The ORD-7 and ORD-8 tests also check the error `Next` returns, and ORD-8
  checks that the next call completes the interrupted reset.
* `stateful_test.go`: zizek state machines whose rules are the model's actions.
  Rules: publish, cut connection (Disconnect + Reconnect), restart server,
  delete server-side consumer (ExtDelete), steal a message with a separate pull
  (Lose), "slow handler + cut" (Consume, ORD-4), `next`, `fetch`,
  `fetchBytes`, `fetchNoWait` (ORD-2), and `settle` (AllDelivered, 45s
  timeout). The invariants are the model's: `InOrder`, and for Consume also
  `SerialHandler`. The rules that hit ORD-4 and ORD-2 used to live in
  `*KnownBugs` variants, where zizek shrank them to a single "slow handler +
  cut" step (`user got stream sequences [2 1]`) and to publish → fetch →
  fetchNoWait (`[1 1]`); they are part of the default machines now. The
  Messages machine still issues the first pull before any rule runs
  (ORD-12).
* `helpers_test.go`: two environment tools.
  * **JetStream API relay.** The client uses API prefix `RLY.`, and a second
    connection forwards each request to `$JS.API.` with the client's reply
    subject. A test can *hold* the next request matching a prefix (e.g.
    `CONSUMER.CREATE.`). That parks `reset()` exactly at the model's "create
    in flight" step.
  * **`blockedIn`.** It inspects goroutine stacks to know when a goroutine is
    blocked on a mutex inside a given function (e.g. `Stop`).
  * **Relay caveat.** A request through the relay always has a responder, so
    a pull on a missing consumer gets no 503. The tests that depend on 503
    (ORD-12, the stateful tests) do not use the relay.

Measured on fix/ordered (`ZIZEK_DIR=… modeltest/run.sh ./ordered/...`, `-race`,
4 shared cores): the default run passes in 88s (`TestStatefulConsume` 13s,
`TestStatefulMessages` 9s, `TestStatefulFetch` 22s, ORD-5 17s for its 600
attempts, ORD-13 10s). With `MODELTEST_KNOWN_BUGS=1` only
`TestORD12MessagesStuckAfterSilentConsumerLoss` fails ("no message within 15s
of polling", both variants). ORD-5 passed all 600 attempts in each of 11
runs (8 with `-race`, 3 without). On the base commit (re-run with
`MODELTEST_KNOWN_BUGS=1`) every reproducer fails as described below.

---

## ORD-1 — Fetch leaks the consumer mutex on concurrent use (high)

```go
func (c *orderedConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	c.Lock()
	...
	if c.runningFetch != nil {
		if !c.runningFetch.closed() {
			return nil, ErrOrderedConsumerConcurrentRequests   // ordered.go:422, c still locked
		}
```

`FetchBytes` has the same early return (ordered.go:461-462). The lock was
moved from `currentConsumer` to `c` in #1743, and the `Unlock` on this path was
lost. The existing test (`jetstream/test/ordered_test.go`, "concurrent
requests") stops after receiving the error, so it never touches the consumer
again.

*Trace* (`OrderedFetch_locked_bug.cfg`):
1. `Fetch` starts a fetch goroutine that is still running.
2. A second `Fetch` returns the error with the lock held (`lock = "leaked"`).
3. Every later `Fetch`, `Next`, `FetchNoWait`, `CachedInfo` or `Info` blocks
   forever.

*Test*: `TestORD1FetchConcurrentRequestsLeaksLock`. After the rejected second
`Fetch` (or `FetchBytes`), `CachedInfo()` never returns.

*Fix*:
```diff
 	if c.runningFetch != nil {
 		if !c.runningFetch.closed() {
+			c.Unlock()
 			return nil, ErrOrderedConsumerConcurrentRequests
 		}
```
(in both `Fetch` and `FetchBytes`).

*Fixed on fix/ordered* as proposed. `Fetch`, `FetchBytes` and `FetchNoWait`
also assign `runningFetch` under the mutex. `OrderedFetch_locked_fixed.cfg`
passes.

## ORD-2 — FetchNoWait re-delivers messages (high)

`FetchNoWait` (ordered.go:495-513) has three problems:
* It does not copy `runningFetch.sseq` into the cursor, as `Fetch` does at
  424-426.
* It does not record its own result as `runningFetch`.
* It reads `runningFetch.done` without the lock, which is a data race.

The next reset therefore starts the new consumer at the stale cursor. Both
`FetchNoWait` → `FetchNoWait` and `Fetch` → `FetchNoWait` hand out messages
1, 2, 3 and then 1, 2, 3 again.

*Trace* (`OrderedFetch_nowait_bug.cfg`): Fetch delivers 1, 2; FetchNoWait
starts the consumer at `curS + 1 = 1`; `obs = <<1, 2, 1, …>>`.

*Test*: `TestORD2FetchNoWaitRedelivers`, which got `[1 2 3 1 2 3]`. zizek
(`TestStatefulFetchKnownBugs`) finds it unaided.

*Fix*: give `FetchNoWait` the same bookkeeping as `Fetch`.
```diff
 func (c *orderedConsumer) FetchNoWait(batch int) (MessageBatch, error) {
+	c.Lock()
 	if c.consumerType == consumerTypeConsume {
+		c.Unlock()
 		return nil, ErrOrderConsumerUsedAsConsume
 	}
-	if c.runningFetch != nil && !c.runningFetch.done {
-		return nil, ErrOrderedConsumerConcurrentRequests
+	if c.runningFetch != nil {
+		if !c.runningFetch.closed() {
+			c.Unlock()
+			return nil, ErrOrderedConsumerConcurrentRequests
+		}
+		if c.runningFetch.sseq != 0 {
+			c.cursor.streamSeq = c.runningFetch.sseq
+		}
 	}
 	c.consumerType = consumerTypeFetch
 	sub := orderedSubscription{
 		consumer: c,
 		done:     make(chan struct{}),
 	}
 	c.subscription = &sub
+	c.Unlock()
 	err := c.reset()
 	if err != nil {
 		return nil, err
 	}
-	return c.currentConsumer.FetchNoWait(batch)
+	msgs, err := c.currentConsumer.FetchNoWait(batch)
+	if err != nil {
+		return nil, err
+	}
+	c.Lock()
+	c.runningFetch = msgs.(*fetchResult)
+	c.Unlock()
+	return msgs, nil
 }
```
(`Fetch`/`FetchBytes` should also assign `runningFetch` under the lock; today
line 443 and line 483 race with a concurrent caller's read at 420.)

*Fixed on fix/ordered* as proposed. `fetchNoWait` is now a rule of the
default `TestStatefulFetch` machine; `OrderedFetch_nowait_fixed.cfg` and
`OrderedFetch.cfg` (Fetch and FetchNoWait, fixed code) pass.

## ORD-3 — ConsumeErrHandler runs under the consumer mutex (high)

`errHandler` locks `c` and calls `c.userErrHandler(cc, err)` while holding it
(ordered.go:205-209). This deadlocks if the user's handler does any of the
following:
* calls `Stop()` or `Drain()` on the ordered `ConsumeContext` returned by
  `Consume` (it takes `c` at ordered.go:362/374);
* calls `Consumer.CachedInfo()` or `Info()`.

The first is a natural reaction to `ErrConsumerDeleted` or
`ErrConnectionClosed`. The handler's goroutine stops forever. That goroutine is
the inner subscription's dispatcher or status goroutine, or the reset
goroutine, and every later operation on the consumer blocks with it. This is
the ordered sibling of #2059, which fixed the same pattern in `pull.go`.

*Trace* (`ErrHandlerStop_bug.cfg`, 3 steps): `ExtDelete` sends a 409; the
dispatcher calls `errHandler`; the user handler calls `Stop`, whose CAS
succeeds; then `Lock` → `NoSelfDeadlock` violated.

*Test*: `TestORD3ErrHandlerCallingStopDeadlocks`, with Stop and CachedInfo
variants. The server-side consumer is deleted and the error handler never
returns.

*Fix*: call the user's handler outside the lock.
```diff
 	return func(cc ConsumeContext, err error) {
 		c.Lock()
-
-		if c.userErrHandler != nil && !errors.Is(err, errOrderedSequenceMismatch) && !errors.Is(err, errConnected) {
-			c.userErrHandler(cc, err)
-		}
+		userErrHandler := c.userErrHandler
+		c.Unlock()
+		if userErrHandler != nil && !errors.Is(err, errOrderedSequenceMismatch) && !errors.Is(err, errConnected) {
+			userErrHandler(cc, err)
+		}
+		c.Lock()
 		if errors.Is(err, ErrConnectionClosed) {
```
Separately, the `cc` passed to the user is usually the *inner*
`pullSubscription`. Calling `cc.Stop()` on it stops only the current inner
subscription and not the ordered consume, whose `Closed()` then never fires.
At ordered.go:184 it is even a nil `ConsumeContext` (see ORD-9). Passing the
ordered subscription would be less surprising.

*Fixed on fix/ordered* as proposed: the handler is read under the lock and
called without it. The `cc` the inner subscriptions pass is unchanged; the
errors the reset goroutine reports (a failed reset, a failed inner
`Consume`) now carry the ordered `ConsumeContext` instead of the stopped
inner subscription or nil (see ORD-9). `ErrHandlerStop_fixed.cfg` passes.

## ORD-4 — Consume handler runs concurrently / out of order across a reset (high)

The ordered `internalHandler` advances the cursor under `c.Mutex` and calls
`handler(msg)` after unlocking (ordered.go:144-149). A reset can be triggered
by a goroutine other than the dispatcher that is running the handler. For
example, a reconnect makes the status goroutine send `errConnected`
(pull.go:365-371). The reset goroutine then starts the next inner subscription
(ordered.go:162-189) without waiting for the old dispatcher. The new
subscription's messages are dispatched by a *different* goroutine, so:
* the user handler runs for message k+1 while message k is still being
  handled;
* if the old dispatcher is between `c.Unlock()` and `handler(msg)`, message
  k+1 enters the handler before k.

The same happens with a missed-heartbeat error that races a message arrival.
An ordered consumer's main promise is handling messages one by one, in order.

*Trace* (`OutOfOrder_bug.cfg`): reconnect → `errConnected` → reset; the old
dispatcher is at `hrun` with message 1; the new subscription delivers message 2
and its handler runs first, giving `obs = <<2, …>>`. In
`ConcurrentHandler_bug.cfg`, `active = {1, 2}`.

*Test*: `TestORD4HandlerRunsConcurrentlyAcrossReset`. The handler blocks on
message 1 and the proxy cuts the connection. The test observed "handler ran
for 2 messages at once; handler completion order [2 3 1]". zizek shrinks the
"slow handler + cut" rule to a single step: `[2 1]`.

*Fix* (minimal): serialize handler invocations across inner subscriptions,
holding a handler mutex across the whole check-and-call.
```diff
 	orderedConsumer struct {
 		...
+		handlerMu         sync.Mutex // serializes Consume handler calls across inner subscriptions
 		sync.Mutex
 	}
 ...
 	internalHandler := func(serial int) func(msg Msg) {
 		return func(msg Msg) {
+			c.handlerMu.Lock()
+			defer c.handlerMu.Unlock()
 			c.Lock()
```
A stale dispatcher then waits for the running handler, re-checks the serial
and drops its message. `Stop()` from inside the handler takes only `c`, so it
is unaffected.

*Fixed on fix/ordered* as proposed (`handlerMu`, taken before `c.Mutex`;
nothing takes it while holding `c.Mutex`). The "slow handler + cut" rule is
part of the default `TestStatefulConsume` machine. `ConcurrentHandler_fixed.cfg`
and `OutOfOrder_fixed.cfg` pass, and the Consume safety baselines
(`OrderedConsumer.cfg`, `ExtDelete.cfg`, `Reconnect.cfg`) now also check
`InOrder` and `SerialHandler`.

## ORD-5 — Messages: Next returns messages after Stop (medium)

Sequence of events:
1. `Next` is in `resetMessages` → `reset()`, holding `c` across the consumer
   create (ordered.go:583-595).
2. `Stop()` CASes `closed` after the attempt's closed check (584) and waits for
   `c`.
3. The reset succeeds.
4. If `Stop` gets the mutex before line 352, it stops the *old* inner iterator
   and closes `done`.
5. `resetMessages` then installs the new iterator without checking `closed`
   (348-354).
6. `Next` (which never checks `closed` itself) returns messages from it. So
   does every later `Next`, contradicting "Calling Next after calling Stop
   will return ErrMsgIteratorClosed". The iterator, its status goroutine and
   its pull keep running.

*Trace* (`OrderedMessages_resurrect_bug.cfg`): ExtDelete → missed heartbeat → `Next`
resets → create OK → `USt1`/`USt2` → `NAsg` → `NInner` returns a message. A
`Next` call begun after Stop returns a message (`lateMsg`).

*Test*: `TestORD5MessagesNextAfterStopDuringReset`. The relay holds the
consumer create; `Stop` is called and the test waits until it is blocked on the
mutex; then the create is released. Which goroutine takes the mutex first is up
to the scheduler, so the test repeats this, observed at attempts 0–167 (about
1 in 20 with `-race`, 1 in 100 without). It fails with "Next after Stop
returned stream seq N".

*Fix*:
```diff
 	cc, err := c.currentConsumer.Messages(opts...)
 	if err != nil {
 		return err
 	}
 	c.Lock()
+	if s.closed.Load() == 1 {
+		c.Unlock()
+		cc.Stop()
+		return ErrMsgIteratorClosed
+	}
 	c.currentSub = cc.(*pullSubscription)
 	c.Unlock()
```
`Stop` CASes before locking. If the CAS comes after this check, `Stop` waits
for the mutex and then stops the new iterator.

*Fixed on fix/ordered* as proposed. `OrderedMessages_resurrect_fixed.cfg`
passes; the test passed all 600 attempts in each of 11 runs (8 with
`-race`, 3 without).

## ORD-6 — Consume: new inner subscription after Stop (medium)

This is the Consume analogue of ORD-5. `reset()` succeeds after `Stop` CASed.
After `Stop` returns, the reset goroutine creates the next inner `Consume`
(ordered.go:179-189) without checking `closed`, and only stops it on its next
`select` (190-197). Meanwhile:
* a pull request is sent to the newly created server-side consumer;
* messages that arrive before the unsubscribe reach the user handler, because
  the serial matches and nothing checks `closed`;
* `Closed()` may already have fired, if it looked at the old `currentSub`.

`Live_stop.cfg` shows the subscription is eventually stopped (no permanent
leak).

*Trace* (`StopResurrect_bug.cfg`): reset → `RGR3` ok → user `Stop` →
`RGCons` (new sub, `born`) → `Deliver` → handler (`lateNew`).

*Test*: `TestORD6ConsumeSubscribesAfterStop`. It deterministically observes
`CONSUMER.MSG.NEXT.S.<prefix>_2` relayed after `Stop()` returned, and logs any
late handler calls (scheduler-dependent).

*Fix*: check `closed` before creating and before installing the new inner
subscription, and let the handler drop messages once the ordered subscription
is closed. The handler check also covers ORD-11 for the ordered consumer.
```diff
 				c.Lock()
+				if sub.closed.Load() == 1 {
+					c.Unlock()
+					continue // done is closed (or about to be): the select returns
+				}
 				serial := c.serial
 				consumeOpts := innerOpts()
 				c.Unlock()
 				if cc, err := c.currentConsumer.Consume(internalHandler(serial), consumeOpts...); err != nil {
 					c.errHandler(serial)(cc, err)
 				} else {
 					c.Lock()
+					if sub.closed.Load() == 1 {
+						c.Unlock()
+						cc.Stop()
+						continue
+					}
 					c.currentSub = cc.(*pullSubscription)
 					c.Unlock()
 				}
 ...
 			c.Lock()
 			// handler is a noop if message was delivered for a consumer with different serial
-			if serial != c.serial {
+			if serial != c.serial || sub.closed.Load() == 1 {
 				c.Unlock()
 				return
 			}
```

*Fixed on fix/ordered*, with one change to the proposal: the handler check
uses a new `stopped` flag, set by `Stop` under `c.Mutex`, instead of
`closed`. `Drain` sets `closed` too, and a drained consume must keep handing
buffered messages to the handler (`jetstream/test` "wait for closed after
drain"). The reset goroutine also reads `currentSub` under the mutex in its
`done` case. `StopResurrect_fixed.cfg` and `DeliveryAfterStop_fixed.cfg`
(ORD-11) pass, and `Live_stop.cfg` now also checks `NoResurrectedDelivery`,
`NoDeliveryAfterStop` and `AtMostOneLive`.

## ORD-7 — Connection closed during a reset: never ends (high)

`reset()` first stops the current inner subscription, whose status goroutine
is the only listener for `CLOSED` (Consume; pull.go:351/391). It then retries
`CreateOrUpdateConsumer`. `MaxResetAttempts` defaults to unlimited
(ordered.go:620-622), and every attempt on a closed connection fails, so it
retries forever with up to 10s backoff, holding the mutex during each attempt.

* In Consume, `ErrConnectionClosed` is never reported and the ordered
  subscription never closes; this is the case #1885 fixed when no reset is in
  flight.
* In Messages, `Next` blocks forever; its own inner channel is not consulted
  until the reset returns.

The user can still call `Stop()`, which returns after the current attempt.

*Traces*: `CloseDuringReset_bug.cfg` (lasso `RGR2 ↔ RGR3` with
`conn = "closed"`) and `OrderedMessages_close_bug.cfg`.

*Test*: `TestORD7CloseDuringResetNeverEnds`, Consume and Messages. The relay
holds the create, then `nc.Close()`; after 15s `Closed()` has not fired and
`Next` has not returned.

*Fix*: treat a closed connection as terminal in `reset()`, which then reaches
the existing `ErrConnectionClosed` handling: `errHandler` stops the ordered
subscription, and `Next` returns the error.
```diff
 		cons, err = c.js.CreateOrUpdateConsumer(ctx, c.stream, *consumerConfig)
 		if err != nil {
+			if errors.Is(err, nats.ErrConnectionClosed) || c.js.conn.IsClosed() {
+				return false, err
+			}
 			return true, err
 		}
```
In the Consume reset goroutine, add `if errors.Is(err, ErrConnectionClosed) {
continue }` after reporting, so it does not try `Consume` on the closed
connection.

*Fixed on fix/ordered.* `reset()` returns `jetstream.ErrConnectionClosed`
(the error `errHandler` and the inner status goroutine use) when the
connection is closed before an attempt or the attempt fails with
`nats.ErrConnectionClosed`. The Consume reset goroutine reports it and
`continue`s; an inner `Consume` that fails on a connection closed right
after the reset is reported as `ErrConnectionClosed` too. Messages' `Next`
stops the iterator and returns `ErrMsgIteratorClosed` wrapping
`ErrConnectionClosed`, as pull.go does. `Fetch` on a closed connection now
returns `ErrConnectionClosed` instead of retrying forever.
`CloseDuringReset_fixed.cfg` and `OrderedMessages_close_fixed.cfg` pass.

## ORD-8 — Next deadline ignored during a reset (medium)

`Next(NextMaxWait(d))` and `Next(NextContext(ctx))` bound only the inner
`pullSubscription.Next`. `resetMessages` → `reset()` uses its own 10s
per-attempt timeout and an unlimited backoff, and nothing ties them to the
caller's deadline.

*Trace* (`OrderedMessages_timeout_bug.cfg`): the deadline passes while `Next`
is in `r2`/`r3` with the connection down for good.

*Test*: `TestORD8NextDeadlineIgnoredDuringReset`. With a 1s deadline and the
create held for 4s, `Next` returned after 5.0s (NextMaxWait) and 4.0s
(NextContext). If the server stays unreachable, it never returns.

`NextMaxWait` makes it worse: ordered `Next` passes the same option to every
inner `Next` call, and each call starts a fresh timer (pull.go:635-640). The
wait therefore restarts after every reset; that is the extra 1s above. The
model mirrors this in `NAsg`.

*Fix* (sketch): pass the call's deadline into the reset.
```diff
-func (c *orderedConsumer) reset() error {
+func (c *orderedConsumer) reset(ctx context.Context) error {
 ...
-		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
+		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
```
* `Next` builds `ctx` from `NextMaxWait`/`NextContext` (or
  `context.Background()`) and passes it through `resetMessages`.
* `retryWithBackoff` also selects on `ctx.Done()`.
* On `ctx` expiry, `Next` returns `nats.ErrTimeout`/`ctx.Err()` and remembers
  that a reset is pending, so the next call retries it.

*Fixed on fix/ordered* along the sketch:
* `reset(ctx)`: each attempt's 10s timeout derives from `ctx`, no attempt
  starts once `ctx` is done, and `ctx.Done()` is the backoff's `cancel`
  (replacing `c.subscription.done`, which cannot close while `reset()`
  holds the mutex). `retryWithBackoff` returns `nil` when canceled, so
  `reset()` then returns `ctx.Err()`; this removes the latent nil-consumer
  panic noted under "Other observations".
* `Next` computes the deadline of `NextMaxWait` once and gives every inner
  `Next` the time remaining; `NextContext` is passed through. On expiry
  during a reset it returns `nats.ErrTimeout` (or the context's error) and
  sets `resetPending`; the next call completes the reset before reading.
* Consume and Fetch pass `context.Background()`.

The test also checks the error and that the next call delivers. In the
model, `OrderedMessages_timeout_fixed.cfg` passes. It triggers the reset
with the forced missed heartbeat rather than a deletion, because with both
the full state space is too large for the liveness check; without the
`Fixes` override it still violates `DeadlineHonored`.

## ORD-9 — StopAfter(0) after a reset; nil ConsumeContext to the error handler (low, model only)

The reset goroutine checks `deliveredMessages >= stopAfter` (ordered.go:166-172)
and then calls `reset()`. Until `reset()` takes the lock and increments the
serial, the old inner subscription can still deliver. After a reconnect, the
old subscription itself re-pulls (pull.go:373). If it delivers the last
message in that window:
1. `innerOpts()` produces `StopAfter(0)` (110), which is rejected
   (jetstream_options.go:433-436).
2. `Consume` fails, and `c.errHandler(serial)(cc, err)` passes the **nil**
   `cc` and `ErrInvalidOption` to the user (184).
3. A handler that calls `consumeCtx.Stop()` panics.
4. The new server-side consumer is orphaned until its InactiveThreshold.

*Trace*: `StopAfterInvalid_bug.cfg`.

Not tested: the window is a few instructions wide and has no hook to hold it
open.

*Fix*: re-check under the same lock as `innerOpts`.
```diff
 				c.Lock()
+				if c.stopAfter > 0 && c.deliveredMessages >= c.stopAfter {
+					c.Unlock()
+					sub.Stop()
+					return
+				}
 				serial := c.serial
```

*Fixed on fix/ordered* as proposed. The errors the reset goroutine reports
also carry the ordered `ConsumeContext` (never nil). `StopAfterInvalid_fixed.cfg`
passes, and `StopAfter.cfg` now also checks `InOrder` and
`NoSpuriousInvalid`.

## ORD-10 — a lost tail message stalls the consumer (low / limitation, model only)

Gaps are detected only by the consumer sequence of the *next* message
(ordered.go:135; 314). Pull idle heartbeats carry no sequence, unlike the push
heartbeats the legacy ordered consumer uses. A message lost on the client is
therefore unnoticed while nothing follows it. Ways to lose one:
* nats.go async pending limits (64MB) under a 500-message batch of large
  messages;
* the `Messages` channel overflowing after a reconnect, when two pulls are
  outstanding.

The consumer then waits until the next publish. Its server-side consumer has
already advanced, so nothing re-delivers the message.

*Trace*: `TailLoss_bug.cfg`. Not tested; reachability is argued from the
limits above, not demonstrated.

*Possible direction*: treat the slow-consumer error of the inner subscription
as a reset trigger, or lift the inner subscription's pending limits, since pull
batching already bounds the backlog.

## ORD-11 — delivery after Stop returned (low, inherited from pull.go)

`pullSubscription.Stop` only closes `done`; `pullMessages` unsubscribes later
(pull.go:1093-1094, 1147). Messages dequeued by `waitForMsgs` in between reach
`pull.go internalHandler` → ordered `internalHandler` → user handler. Neither
layer checks `closed`. This is pull.go behavior (see the PullConsume model);
the handler check in the ORD-6 fix closes it for the ordered consumer.

*Trace*: `DeliveryAfterStop_bug.cfg`. Not separately tested.

*Status on fix/ordered*: closed for the ordered consumer by the ORD-6
handler check (`stopped`); `DeliveryAfterStop_fixed.cfg` passes. The
pull.go behavior itself is left to the pull.go fixes.

## ORD-12 — Messages stuck after its consumer is lost silently (high)

The scenario, step by step:
1. The server-side consumer disappears while no pull request waits, so no 409
   is sent. This happens if it is deleted, or if the server removes it after
   `InactiveThreshold` (5 min by default) because the application paused
   between `Next` calls. It cannot happen to `Consume`, which always keeps a
   pull waiting.
2. The next pull is answered with 503 No Responders, and
   `pullSubscription.Next` swallows it (pull.go:697-702: non-terminal,
   `continue`).
3. The pending count is not reset, so no new pull is sent.
4. The other way out, the missed-heartbeat timer, is created *per Next call*
   and stopped when the call returns (pull.go:659-664). It never fires if
   `Next` is called with a deadline shorter than two heartbeats, which is
   30s (ORD-13).
5. Polling with e.g. `NextMaxWait(500ms)` never recovers, and every call
   returns `nats.ErrTimeout`.

`Consume` treats `ErrNoResponders` as a reset trigger (ordered.go:225) and
recovers within a millisecond (measured). With an unbounded `Next`, recovery
takes 30s.

*Trace* (`OrderedMessages_noresp_bug.cfg`): `Expire` → `SilentLoss` → 408 →
re-pull → 503 swallowed → deadline → repeat.

*Test*: `TestORD12MessagesStuckAfterSilentConsumerLoss`, with two variants:
* `deleted`: the consumer is deleted before the first pull;
* `inactive`: `InactiveThreshold: 1s`, `PullExpiry(2s)`, then a pause.

In both, no message arrives within 15s of polling.

*Fix*: let the ordered iterator see the 503 (only the ordered consumer sets
`notifyOnReconnect`). `ErrNoResponders` is not in the direct-return list of
ordered `Next`, so it resets.
```diff
 				if termErr, _ := s.handleStatusMsg(msg, msgErr); termErr != nil {
 					s.Stop()
 					return nil, termErr
 				}
+				if errors.Is(msgErr, nats.ErrNoResponders) && s.consumeOpts.notifyOnReconnect {
+					return nil, msgErr
+				}
 				continue
```

*Not fixed on fix/ordered.* The fix is in pull.go, which is outside this
branch's scope; the ordered side needs no change, because `ErrNoResponders`
is not in the direct-return list of ordered `Next` and therefore resets.
`TestORD12MessagesStuckAfterSilentConsumerLoss` stays behind
`harness.KnownBug`, and `OrderedMessages_noresp_bug.cfg` still violates
`AllDelivered` with the ordered fixes switched on.

## ORD-13 — ordered default heartbeat not applied (low)

`setDefaults(ordered=true)` picks a 5s heartbeat for ordered consumers
(pull.go:1245-1251). This has been dead code since #1425. The ordered layer's
parse is used only for its own fields, and the inner `pullConsumer.Consume`
and `Messages` re-parse the same options with `ordered=false` (pull.go:206,
512), which gives `Expires/2 = 15s`. A stalled pull (consumer gone, network
black-holed) is detected after 30s instead of 10s. That also lengthens ORD-12
and every heartbeat-driven recovery.

*Test*: `TestORD13OrderedDefaultHeartbeatNotApplied`. The relay stalls every
pull, and no reset occurs within 15s.

*Fix*: pass the ordered defaults down explicitly.
```diff
 	consumeOpts, err := parseConsumeOpts(true, opts...)
 	...
-	opts = append(opts, consumeReconnectNotify())
+	opts = append(opts, consumeReconnectNotify(), PullHeartbeat(consumeOpts.Heartbeat))
```
(likewise in `Messages`).

*Fixed on fix/ordered*, with a private option instead of `PullHeartbeat`
that sets the heartbeat the ordered parse resolved (and already validated).
A user's `PullHeartbeat` passes through unchanged. Behavior change: ordered `Consume`/`Messages` pull requests carry a 5s idle
heartbeat by default (was 15s). Measured: the test's reset happens after
10.0s. Not modeled (timing).

## Other observations (not findings)

* **`Stop()` latency.** `reset()` holds `c.Mutex` across
  `CreateOrUpdateConsumer` and its backoff, so `Stop()` during a reset waits
  for the current attempt (≤ 10s request + ≤ 10s backoff). `backoffOpts.cancel
  = c.subscription.done` can never fire inside `retryWithBackoff`, because
  `done` is closed only under the same mutex. If it did fire,
  `retryWithBackoff` would return `nil` and `cons.(*pullConsumer)` would panic
  on a nil interface (ordered.go:599). That is latent, not reachable today.
  On fix/ordered the latency remains (bounded by `Next`'s deadline for
  Messages, ORD-8); `cancel` is the reset's context and `reset()` returns its
  error when canceled, so the nil-interface panic is gone.
* **Messages vs Consume on 503.** `Messages` resets on no-responders only
  through the heartbeat (ORD-12); `Consume` resets immediately.
* **`Consume` after `Stop`.** A second `Consume` on the same ordered consumer
  returns `ErrOrderedConsumerConcurrentRequests` even after the first was
  stopped (ordered.go:90-91), so an ordered consumer cannot be restarted. This
  looks intended.

## Cross-reference: PUB-7 status-listener event loss

`nats.go sendStatusEvent` (nats.go:6833-6850) checks whether a listener
channel is closed with a non-blocking receive. On an open channel that still
holds an unread event, that receive *consumes* the event and removes the
listener for that status. `RemoveStatusListener` (6799-6807) similarly drains
an event instead of closing. See PUB-7 in `models/PublishAsync/README.md`.

**Impact on the ordered consumer (analysis only, not modeled or tested).**
The inner `Consume` status goroutine reads its listener channel only between
`ErrHandler` calls. The ordered `errHandler` blocks on `c.Mutex`, which can be
held for a long time:
* by `reset()` during a create (up to 10s per attempt);
* by `Info()` across its API request.

If the connection flaps while that goroutine is blocked, an unread
`RECONNECTING` or `CONNECTED` is swallowed and the subscription stops
receiving that status for good. The consequences:
* **Missing `CONNECTED`.** No `errConnected` reset after that reconnect. If
  the server restarted, the pull request was lost, and recovery falls back to
  the missed heartbeat (30s, ORD-13), on every later reconnect too.
* **Missing `CLOSED`.** The ordered consume never learns of the close, with
  the same outcome as ORD-7.

The inner `Messages` iterator's status goroutine forwards events into `errs`
with a blocking send that `Next` drains, so it is affected only while no
`Next` runs. The model assumes events are delivered (`Broadcast`). A faithful
model would make the listener channel's receive in `sendStatusEvent`
destructive, as PUB-7 describes.

## Zizek notes

No zizek bugs found. A few practical points:
* Running real servers per example is slow, so the stateful tests use
  `SuppressTooSlow` and `MaxShrinks: 20`.
* Shrinking reached the minimal sequence for both known-bug machines.
* Per-rule application counts are logged so a run shows what it exercised.
  For example, for Consume: cut 21, delete 20, publish 32, restart 23,
  settle 23, steal 23.
