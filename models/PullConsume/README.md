# PullConsume — TLA+ models of the JetStream pull consumer

Models of `jetstream/pull.go` as of this branch's base (upstream `main` after
#2175): `Consume()` (`PullConsume.tla`) and `Messages()` / `Next()`
(`PullMessages.tla`), plus the parts of `nats.go` they depend on: the async
and channel subscription delivery, `Subscription.Drain`, and the
`StatusChanged` listener machinery. `Fetch()` is covered by review in the
H5 section; it has no TLA+ model. The model-derived Go tests live in
`modeltest/pullconsume/`.

**Status on `fix/pull`:** PULL-1, 3, 4, 5, 6, 7, 8, 9, 10 and O-1 are fixed
in `jetstream/pull.go` (see "Fixed on fix/pull" below and each finding).
PULL-2's root cause is in `nats.go` (`sendStatusEvent`) and is fixed by the
nats.go change on `fix/conn`; on the merged
`claude/magical-lovelace-my0imq-fixes` branch its reproducers run as
regression tests too, and the stateful Messages test no longer limits
reconnects between `Next` calls. The `pull.go:<line>` references in this README
and in the specs are to the base commit, before the fixes.

Run:

```sh
TLA2TOOLS=/path/to/tla2tools.jar models/tlc.sh PullConsume      # every cfg below
cd modeltest && ./run.sh ./pullconsume/...                      # default: green
cd modeltest && MODELTEST_KNOWN_BUGS=1 ./run.sh ./pullconsume/...  # green on the merged fixes branch
```

## What is modeled

Both specs are PlusCal (the translation is checked in). There is one process
per goroutine, `sub.Lock` is the variable `lock`, and channels are sequences
with their real capacities: `fetchNext` 1, `errs` 10, the `StatusChanged`
channel 10, and the `Messages()` channel `MaxMessages`. A `select` with
several ready cases is an `either` over the ready branches. Every labeled
step is one atomic step of the real code. Labels carry `pull.go:<line>`
comments, and the table maps them.

### PullConsume.tla (Consume)

| Process | Action (labels) | Code |
|---|---|---|
| `h` handler goroutine | `h_wait` pop from pending list / exit when sub closed | `nats.go:3786-3841` (waitForMsgs) |
| | `h_disp` heartbeat: Stop+Reset timer | `pull.go:246-255` |
| | `h_user` user callback | `pull.go:289` |
| | `h_lk1` lock, `pending--`, `delivered++`, `cp_*` checkPending, `h_rs1` reset timer, unlock | `pull.go:290-297` |
| | `h_sa` StopAfter → Stop | `pull.go:299-301` |
| | `h_lk2` 408: lock, `pending -= Nats-Pending-Messages`, checkPending, unlock | `pull.go:262-270`, `768-781` |
| | `h_ch..h_chul` subscription closed handler | `pull.go:313-324`, `nats.go:3857-3859` |
| `cp_*` checkPending | `cp_eval` threshold + `fetchInProgress` test, batch size; `cp_send` blocking `fetchNext <-`; `pending = MaxMessages` | `pull.go:461-504` |
| `s` status goroutine | `s_top` `closed` check | `pull.go:351` |
| | `s_sel` select: status / errs / done | `pull.go:354-426` |
| | RECONNECTING: stop timer | `pull.go:360-364` |
| | `s_clk`/`s_csend` CONNECTED: lock, `fetchNext <-` MaxMessages, reset pending | `pull.go:365-389` |
| | `s_clsend` CLOSED: `errs <-` (self-send) | `pull.go:391-392` |
| | `s_elk`/`s_esend` ErrNoHeartbeat: lock, `fetchNext <-` min(Max, StopAfter-delivered), reset | `pull.go:394-417` |
| | `s_eh` ErrHandler, Stop on ErrConnectionClosed | `pull.go:418-423` |
| `p` pullMessages | `p_sel` select fetchNext / done; `fetchInProgress = 1` | `pull.go:1076-1078`, `1093` |
| | `p_pull` pull(): closed → cleanup; batch < 1 → error; publish (or reconnect buffer) | `pull.go:1154-1173`, `1080-1083` |
| | `p_err` `select{errs <- err; <-done}` | `pull.go:1087-1090` |
| | `p_fip` `fetchInProgress = 0` | `pull.go:1092` |
| | `p_cl1`/`p_cl2` cleanup: RemoveStatusListener, hbMonitor.Close, Drain/Unsubscribe, closed=1 | `pull.go:1120-1150`, `nats.go:6799-6814` |
| `t` heartbeat timer | arm/fire; fired goroutine `select{errs <- ErrNoHeartbeat; <-done}` | `pull.go:1106-1118`, `785-811` |
| `u` user | Stop; Drain (CAS, then `draining`, `close(done)`); Closed() | `pull.go:816-851` |
| `lib` nats.go | checkDrained: Flush, then `pMsgs == 0` → removeSub | `nats.go:5468-5507` |
| `env` server/network | deliver to the oldest waiting pull; 408 expiry with pending count; idle heartbeat; disconnect (`sendStatusEvent(RECONNECTING)`); reconnect (pending buffer replayed, `sendStatusEvent(CONNECTED)`); nc.Close | `nats.go:6833-6851`, `6204-6286` |

### PullMessages.tla (Messages / Next)

Same `p`, `t`, `env` and `cp_*` as above, plus:

| Process | Action | Code |
|---|---|---|
| `n` user calling Next (MaxNext times) | `n_lock` lock for the whole call; closed && !draining → ErrMsgIteratorClosed | `pull.go:648-658` |
| | `n_hb` new heartbeat timer; StopAfter reached → Stop | `pull.go:659-669` |
| | `n_loop` checkPending, `n_sel` select msgs / errs / timeout | `pull.go:671-745` |
| | closed channel → Stop, ErrMsgIteratorClosed | `pull.go:675-688` |
| | ErrNoHeartbeat / errConnected → `pending = 0`; errDisconnected → stop timer | `pull.go:710-739` |
| `s` status goroutine | RECONNECTING → `select{errs <- errDisconnected; <-done}`; CONNECTED after RECONNECTING → errConnected | `pull.go:572-609` |
| `lib` | chan subscription drained → closed handler closes `msgs` | `pull.go:554-565`, `nats.go:5263-5268`, `5468-5507` |
| `env` deliver | into `msgs` if it has room, else dropped (slow consumer) | `nats.go:4013-4022` |

### Environment assumptions (constants)

* `ReqsOnDisconnect` (`keep` / `drop` / `either`): whether the server still
  holds the waiting pulls after a disconnect. A quick reconnect keeps them,
  while a server restart drops them.
* `LateHeartbeats`: if TRUE, the 2×heartbeat timer may fire while the server
  still holds a pull, i.e. heartbeats were delayed by the network. If FALSE,
  it fires only when the consuming goroutine is idle *and* no pull is at the
  server or on its way there (or there is no connection). The model has no
  clock, so this is how "2×heartbeat of silence" is expressed.
* `FastPull`: if TRUE, the server never answers a pull before `pullMessages`
  has run the statement after `pull()` returned (`fetchInProgress = 0`).
  `PullMessages_fip_bug.cfg` drops it to show PULL-10, and
  `PullConsume_fip.cfg` / `PullMessages_fip.cfg` drop it to check the fix.
* Bounds: `StreamMsgs`, `MaxReconnects`, `MaxExpires`, `MaxHBMsgs`,
  `MaxHBFires`, `MaxNext`.
* `Fixes`: the fixes below, one name per finding. Each `*_bug.cfg` is a
  passing configuration with exactly one fix removed, i.e. it encodes the
  old code for that finding and must keep reproducing. `fip` (PULL-10) was
  added with the fix on `fix/pull`; the `_bug.cfg`s predate it and run
  without it, which is the old code.

### Invariants

| Invariant | Meaning |
|---|---|
| `StopAfterRespected` | the user handler / Next return at most StopAfter messages |
| `BatchWithinMax`, `NoInvalidPull` | every pull asks for 1..MaxMessages |
| `NoOrphanedFetchSend` | no goroutine is parked on a full `fetchNext` after `pullMessages` returned. Nothing would ever read it, and the parked goroutine holds `sub.Lock` |
| `NoStuckSelfSend` | the status goroutine never self-sends on a full `errs` (it is the only reader) |
| `NoDeadlock` | when nothing can move any more and Stop/Drain was called, every goroutine has returned and a `Closed()` channel handed out is closed. Stated with `~ENABLED Next`, since TLC's own deadlock check is off |
| `ClosedMeansDone` | `Closed()` is closed only when no callback is running |
| `DrainNotDowngraded`, `DrainProcessesBuffered`, `DrainKeepsBuffered` | Drain hands every buffered message to the handler / Next |
| `NoSlowConsumerDrop`, `NoStatusDrop` | the `Messages()` channel never overflows |
| `NoSilentStall` | never idle on a healthy connection with no pull anywhere while work remains; only the heartbeat timer could restart it (after 2×heartbeat, with a spurious ErrNoHeartbeat) |
| `NoPermanentStall` | as above with the heartbeat timer not armed either, i.e. stuck for good |

### Abstractions and limitations

* Message-count mode only. `PullMaxBytes` / `LimitSize` byte accounting is
  not modeled.
* Priority groups, pin IDs and 409s (leadership change, consumer deleted)
  are not modeled.
* There is no time. Timers fire nondeterministically, restricted by
  `LateHeartbeats` and `FastPull`, and expiry is "the oldest pull expires",
  which is exact for equal `Expires`.
* `pullConsumer.Lock` (taken inside `pull()` and `getPinID`) is not a
  separate lock. Any delay inside `pull()` is covered by interleaving.
* nats.go's async pending list is unbounded. Its slow-consumer limits
  (512k messages) are not modeled.
* `ordered.go` is out of scope (another agent's model). The
  `notifyOnReconnect` path is not modeled.
* `Fetch` is reviewed in the H5 section, not modeled.

## TLC results (measured)

TLC 2026.10.06 via `models/tlc.sh` (`-workers auto`), on a 4-core machine
shared with other agents' model checkers and test runs, after the fixes
landed on `fix/pull` (passing configs now include `fip`). "States" means
distinct states, and each passing run explored its state space to the end
(0 states left on queue). The whole directory takes 451 s.

| Config | Spec | Result | States | Time |
|---|---|---|---|---|
| `PullConsume.cfg` | Consume, all fixes, Stop+Closed, reconnect (either), late HB | pass | 3,074,862 | 88 s |
| `PullConsume_drain.cfg` | Consume, all fixes, Drain+Closed, reconnect (keep) | pass | 2,507,954 | 73 s |
| `PullConsume_stopafter.cfg` | Consume, all fixes, StopAfter 1 < MaxMessages 2, Stop | pass | 355,115 | 18 s |
| `PullConsume_fip.cfg` | Consume, all fixes, `FastPull = FALSE`, StopAfter 1, reconnect (either), Stop+Closed | pass | 761,655 | 24 s |
| `PullMessages.cfg` | Messages, all fixes, 3×Next, Drain, reconnect (either), late HB | pass | 2,692,188 | 97 s |
| `PullMessages_stopafter.cfg` | Messages, all fixes, StopAfter 1 | pass | 339,499 | 18 s |
| `PullMessages_fip.cfg` | Messages, all fixes, `FastPull = FALSE`, 3×Next, Drain, reconnect (either) | pass | 2,368,527 | 91 s |
| `PullConsume_orphan_bug.cfg` | PULL-1 | `NoOrphanedFetchSend` violated | | |
| `PullMessages_orphan_bug.cfg` | PULL-1 (Messages) | `NoOrphanedFetchSend` violated | | |
| `PullConsume_steal_bug.cfg` | PULL-2 | `NoPermanentStall` violated | | |
| `PullConsume_stopafter_bug.cfg` | PULL-3 | `StopAfterRespected` violated | | |
| `PullConsume_invalidpull_bug.cfg` | PULL-3 (side path) | `NoInvalidPull` violated | | |
| `PullConsume_pendingreset_bug.cfg` | PULL-4 | `NoSilentStall` violated | | |
| `PullMessages_pendingreset_bug.cfg` | PULL-4 (Messages) | `NoSilentStall` violated | | |
| `PullMessages_overflow_bug.cfg` | PULL-5 | `NoSlowConsumerDrop` violated | | |
| `PullConsume_clamp_bug.cfg` | PULL-6 | `BatchWithinMax` violated | | |
| `PullConsume_closedch_bug.cfg` | PULL-7 | `ClosedMeansDone` violated | | |
| `PullConsume_drainrace_bug.cfg` | PULL-8 | `DrainNotDowngraded` violated | | |
| `PullMessages_drainrace_bug.cfg` | PULL-8 (Messages) | `DrainKeepsBuffered` violated | | |
| `PullMessages_fip_bug.cfg` | PULL-10 | `NoSilentStall` violated | | |

Every `_bug` configuration reports its violation within 2-9 s (measured
again after the fixes: the specs changed only by the `fip` switch, which
the `_bug` configs leave off). The
counterexamples are 7 to 27 steps long; TLC's breadth-first search makes
each the shortest one under that configuration.

## Findings

Severity: High = hang/stall forever or a goroutine/lock leak in ordinary use;
Medium = wrong results or documented-contract violations users will hit;
Low = narrow races, performance, cosmetic.

Every "confirmed" finding has a test in `modeltest/pullconsume/bugs_test.go`
that asserts the correct behavior. The failure counts below were measured
on the base commit (`-race -count=3` or `-count=5`). On `fix/pull` every
one of them is a regression test that passes, except PULL-2's, which pass
once the nats.go fix from `fix/conn` is merged (they do on the merged
fixes branch).

### PULL-1 — Stop/Drain while a pull is queued deadlocks the consume; Next hangs during Drain (High, confirmed)

**Fixed on `fix/pull`** (dcae0d3, and for `checkPending` also b1b3c75):
the status goroutine's two sends go through `sendFetchNext`, which selects
on `done`; `checkPending`'s send no longer blocks at all (PULL-10). Both
tests are regression tests now.

`fetchNext` has capacity 1 and is written with a blocking send while
holding `sub.Lock` in three places: `checkPending` (`pull.go:488`), the
CONNECTED path (`pull.go:373`) and the ErrNoHeartbeat path (`pull.go:401`).
Its only reader, `pullMessages`, returns on `done` (`pull.go:1093`)
without draining it. The writers never look at `done`, and `checkPending`
does not even look at `closed`. After Stop/Drain, the first late send fills
the buffer and the second blocks forever, holding `sub.Lock`.

* Consume trace (`PullConsume_orphan_bug.cfg`, 17 steps). The heartbeat
  timer fires and the status goroutine queues a re-pull. The user calls
  Stop, and `pullMessages` picks `done` over the ready `fetchNext` and
  returns. A 408 for the old pull, still in flight, reaches the handler,
  which drops `pending` below the threshold and blocks in `checkPending`
  holding `sub.Lock`. The subscription closed handler then blocks on
  `sub.Lock` too, so `Closed()` never fires and `Closed()` itself blocks.
  The status goroutine and the handler goroutine leak.
* Messages trace (`PullMessages_orphan_bug.cfg`). After Drain, `Next` keeps
  running `checkPending` (`pull.go:672`). Each queued `errConnected` (or
  ErrNoHeartbeat) resets `pending` to 0, the first resulting send fills
  `fetchNext`, and the second blocks. `Next` never returns, and it holds
  `sub.Lock`, so every later `Next` blocks too.
* Tests:
  * `TestPULL1StopWhileRepullQueuedDeadlocks`: the proxy drops heartbeats
    and holds the 408, and `sub.Lock` is held only to park the two
    goroutines where the trace has them. 6/6 failures.
  * `TestPULL1MessagesNextHangsDuringDrain`: two reconnects with no `Next`
    running, then Drain with the flush's PONG held, then `Next`. Fully
    deterministic, 5/5.
* The stateful `TestModelMessages` in known-bugs mode finds it
  independently ("Next did not return 5s after Drain").

Fix (verified in the model as `orphan`): every `fetchNext` send also
selects on `done`.

```diff
--- a/jetstream/pull.go
+++ b/jetstream/pull.go
@@ checkPending
-			s.fetchNext <- &pullRequest{
+			req := &pullRequest{
 				Expires:       s.consumeOpts.Expires,
 				...
 			}
-
-			s.pending.msgCount = s.consumeOpts.MaxMessages
-			s.pending.byteCount = s.consumeOpts.MaxBytes
+			select {
+			case s.fetchNext <- req:
+				s.pending.msgCount = s.consumeOpts.MaxMessages
+				s.pending.byteCount = s.consumeOpts.MaxBytes
+			case <-s.done:
+			}
@@ status goroutine, CONNECTED (pull.go:373) and ErrNoHeartbeat (pull.go:401)
-						sub.fetchNext <- &pullRequest{...}
+						select {
+						case sub.fetchNext <- &pullRequest{...}:
+						case <-sub.done:
+						}
```

### PULL-2 — `StatusChanged` loses events and unregisters itself; Consume can go silent forever (High, confirmed; root cause in nats.go core)

**Not fixed here: the root cause is in `nats.go`**, fixed by the nats.go
change on `fix/conn` (the same fix as PUB-7/CONN-1). Both tests stay gated
by `harness.KnownBug`, and the stateful Messages test keeps its reconnects
below the count that would fill `errs` (see below). No `pull.go`-side
hardening is proposed: the status goroutine cannot tell a lost event from
one that never happened.

Same root cause as PUB-7 in `models/PublishAsync/README.md`, found
independently by that model. This section covers what it does to pull
consumers.

`sendStatusEvent` (`nats.go:6833-6851`) probes "is this listener closed?"
with `select { case <-ch: ...delete...; continue }`. A buffered,
not-yet-read event satisfies that receive just as well. So when CONNECTED
is sent while the listener has not read RECONNECTING yet:

1. RECONNECTING is consumed and lost;
2. the channel is unregistered for CONNECTED, for good;
3. CONNECTED is not sent.

(`RemoveStatusListener`, `nats.go:6803-6807`, has the same pattern and
does not close a channel that holds a value.)

Consume's status goroutine then never sees CONNECTED again. On the next
reconnect it handles RECONNECTING, which stops the heartbeat timer
(`pull.go:360-364`), but it never re-pulls and never re-arms the timer. If
the server lost the waiting pull (restart, long disconnect), the consumer
stays silent on a healthy connection, forever, with no error.
`PullConsume_steal_bug.cfg` shows this in 7 steps. Messages is affected the
same way: its status goroutine stops seeing one of the two statuses, so the
#2135 reconnect accounting is lost again.

All the status goroutine has to do is not read the channel between two
status changes:

* Consume: it is inside a slow `ConsumeErrHandler`, blocked on `sub.Lock`,
  or blocked on `fetchNext`.
* Messages: `errs` is full because nobody called `Next` during five
  reconnects.

Tests:

* `TestPULL2StatusChangedLosesEvents`: two reconnects observe
  `[RECONNECTING]` instead of the four events. Deterministic, 3/3.
* `TestPULL2ConsumeStallsForeverAfterLostConnected`: a slow ErrHandler
  during one reconnect, then a server restart. 0 of 3 messages are
  delivered in 10s with expiry 1s, and no pull is ever sent again. 3/3.

Fix: do not use a receive as a closed-probe.

```diff
--- a/nats.go
+++ b/nats.go
 func (nc *Conn) sendStatusEvent(s Status) {
-Loop:
 	for ch := range nc.statListeners[s] {
-		// make sure channel is not closed
-		select {
-		case <-ch:
-			// if chan is closed, remove it
-			delete(nc.statListeners[s], ch)
-			continue Loop
-		default:
-		}
-		// only send event if someone's listening
-		select {
-		case ch <- s:
-		default:
-		}
+		if !trySendStatus(ch, s) {
+			// closed by its owner without RemoveStatusListener
+			delete(nc.statListeners[s], ch)
+		}
 	}
 }
+
+// trySendStatus sends s on ch without blocking; false if ch is closed.
+func trySendStatus(ch chan Status, s Status) (ok bool) {
+	defer func() {
+		if recover() != nil {
+			ok = false
+		}
+	}()
+	select {
+	case ch <- s:
+	default:
+	}
+	return true
+}
```

### PULL-3 — StopAfter exceeded: re-pulls ignore it, and the handler is not guarded (Medium, confirmed)

**Fixed on `fix/pull`** (f090934): the reconnect re-pull is capped by
StopAfter, a re-pull with batch 0 is not sent, and the handler discards
messages once Stop was called (`stopped()`, which also covers PULL-8) or
StopAfter was reached. `TestPULL3...` is a regression test and
`TestModelConsume` draws StopAfter by default.

* After a reconnect, the status goroutine re-pulls `MaxMessages`
  (`pull.go:375`) with no StopAfter cap.
* The ErrNoHeartbeat re-pull uses `StopAfter - delivered`
  (`pull.go:397-400`). That ignores the pulls still alive, and `delivered`
  is stale while a callback runs: `delivered++` happens after the callback.
* `internalHandler` calls the user handler without checking `closed`
  (`pull.go:286-289`). Messages already queued when StopAfter stops the
  consume still reach the handler until `pullMessages` gets round to
  Unsubscribe.

TLC's shortest trace (`PullConsume_stopafter_bug.cfg`, 23 steps): the
heartbeat timer fires during the StopAfter-th callback, the re-pull asks
for one more message, and the handler runs StopAfter+1 times.

Side path (`PullConsume_invalidpull_bug.cfg`, model-only): the same
ErrNoHeartbeat re-pull, processed between `delivered++` and the handler's
`Stop()`, asks for 0 messages. `pull()` rejects it and the user's
ErrHandler gets "invalid argument: batch size must be at least 1".

Tests:

* `TestPULL3StopAfterExceededAfterReconnect`: StopAfter(3), one reconnect,
  a handler taking 10ms. The wire shows pulls `[3 10]` and the handler is
  called 4 times, 8/8. With an instant handler the extra callback did not
  show in 8 runs (messages 4+ were not queued yet when Stop ran), but the
  over-pull on the wire still did.
* `TestModelConsume` in known-bugs mode shrinks to "StopAfter=1,
  MaxMessages=3: publish 1, cut → pull for 3".

Fix (model: `stopafter`; it also needs `pendingreset` below):

```diff
@@ internalHandler (pull.go:286)
+		if (sub.closed.Load() == 1 && sub.draining.Load() == 0) ||
+			(sub.consumeOpts.StopAfter > 0 && sub.delivered >= sub.consumeOpts.StopAfter) {
+			return // stopped, or StopAfter reached: never hand it to the user
+		}
 		if pinId := msg.Header.Get("Nats-Pin-Id"); pinId != "" {
@@ status goroutine, CONNECTED (pull.go:373)
+						batchSize := sub.consumeOpts.MaxMessages
+						if sub.consumeOpts.StopAfter > 0 {
+							batchSize = min(batchSize, sub.consumeOpts.StopAfter-sub.delivered)
+						}
+						if batchSize > 0 {
 						sub.fetchNext <- &pullRequest{
-							Batch:         sub.consumeOpts.MaxMessages,
+							Batch:         batchSize,
 							...
+						}
@@ status goroutine, ErrNoHeartbeat (pull.go:401)
-					sub.fetchNext <- &pullRequest{
+					if batchSize > 0 {
+						sub.fetchNext <- &pullRequest{
```

(`sub.delivered` is written only by the handler goroutine, so the unlocked
read in the handler is race-free.)

### PULL-4 — StopAfter < MaxMessages: after a pull expires nothing is pulled until the heartbeat times out (Medium, confirmed)

**Fixed on `fix/pull`** (d259848): `resetPendingMsgs(batch)` counts the
batch actually requested, and `checkPending` adds its batch to what is
outstanding (`min(pending+batch, MaxMessages)`, which is MaxMessages as
before without StopAfter). Both tests are regression tests.

`resetPendingMsgs` / `checkPending` set `pending.msgCount = MaxMessages`
(`pull.go:328`, `500`, `387`, `415`), but with StopAfter the pull asks for
fewer messages (`pull.go:329-332`, `480-482`, `397-400`). The counter then
over-states what is outstanding by `MaxMessages - batch`. When the pull
expires partly unfilled, the 408 subtracts the true remainder and the count
stays at or above the threshold, so no new pull is sent. The consumer waits
2×heartbeat (by default the full `Expires`, 30s) with nothing pulled:

* Messages: `Next` returns `ErrNoHeartbeat` (the default
  `ReportMissingHeartbeats`) instead of the next message.
* Consume: `ErrNoHeartbeat` is reported to the ErrHandler and new messages
  wait for the timeout.

This repeats after every expiry. Traces: `PullConsume_pendingreset_bug.cfg`
(8 steps), `PullMessages_pendingreset_bug.cfg`.

Tests (deterministic, 3/3 each):

* `TestPULL4MessagesStopAfterStallsAfterExpiry`: `StopAfter(5)`, expiry 1s.
  The second `Next` fails with "no heartbeat received", and the wire shows
  a single pull `[5]`.
* `TestPULL4ConsumeStopAfterStallsAfterExpiry`: the message is delivered
  after 1.0s and ErrNoHeartbeat is reported on a healthy connection.

Fix (model: `pendingreset`): count what was actually asked for.

```diff
@@ Consume initial pull (pull.go:328)
-	sub.resetPendingMsgs()
 	batchSize := sub.consumeOpts.MaxMessages
 	if sub.consumeOpts.StopAfter > 0 {
 		batchSize = min(batchSize, sub.consumeOpts.StopAfter-sub.delivered)
 	}
+	sub.resetPendingMsgs()
+	sub.pending.msgCount = batchSize
@@ checkPending (pull.go:500)
-			s.pending.msgCount = s.consumeOpts.MaxMessages
+			s.pending.msgCount = s.consumeOpts.MaxMessages
+			if s.consumeOpts.StopAfter > 0 {
+				s.pending.msgCount = prevMsgCount + batchSize // value before the send
+			}
@@ status goroutine (pull.go:387, 415): after resetPendingMsgs()
+						sub.pending.msgCount = batchSize
```

### PULL-5 — Messages(): the channel overflows after a reconnect or a missed heartbeat, and messages are dropped (Medium, confirmed)

**Fixed on `fix/pull`** (1d90d57), as the model's `unbounded` +
`buffered`: `Messages()` subscribes with an async handler that appends to
an unbounded `msgQueue`, which `Next` pops from; the pending count on
ErrNoHeartbeat / errConnected is reset to what is still queued. Drain keeps
its semantics (the closed handler closes the queue once drained). One
visible difference: `Next` now returns buffered messages before it looks
at queued status notifications (before, `select` picked at random).
`TestPULL5...` is a regression test (it now also checks that all 30
messages arrive), and `TestModelMessages` reconnects and misses heartbeats
by default.

`Messages()` buffers in a `ChanSubscribe` channel of exactly `MaxMessages`
(`pull.go:534`). nats.go never blocks on it: what does not fit is dropped
as a slow consumer (`nats.go:4013-4022`). On `errConnected` or
ErrNoHeartbeat, `Next` resets `pending` to 0 (`pull.go:712`, `729`) and
pulls a full `MaxMessages`, ignoring three things:

* the messages still in the channel;
* pulls that survived at the server or in nats.go's reconnect buffer;
* that the notification may be stale. A `Next` that started after the
  reconnect has already pulled once, and then pulls again when it reads the
  queued `errConnected`.

The extra messages are dropped: with AckExplicit they come back only after
`AckWait` (30s), and with AckNone they are lost. Dropped 408s also corrupt
the pending count. Trace: `PullMessages_overflow_bug.cfg`.

Tests:

* `TestPULL5MessagesOverflowAfterReconnect`: MaxMessages 10, a backlog of
  9, one reconnect. Pulls are `[10 10 6 6 6]`, a slow-consumer drop occurs,
  and only 22-25 of 30 messages arrive. 5/5.
* `TestModelMessages` found the stale-notification variant on its own in
  default mode (one message, a reconnect with no backlog, pulls `[2 2]`,
  one drop). That is why the default mode excludes reconnects for Messages.

Fix (model: `unbounded`; `buffered` alone is not enough because surviving
and replayed pulls are invisible to the client): do not use a fixed-size
channel. Deliver into an async subscription with its own unbounded list,
the way `Consume` does, and let `Next` pop from it. A cheaper partial
mitigation for the backlog part is shown below; it is not enough on its
own:

```diff
@@ Next, ErrNoHeartbeat and errConnected (pull.go:712, 729)
-				s.pending.msgCount = 0
+				s.pending.msgCount = len(s.msgs) // buffered messages still count
```

### PULL-6 — `pending.msgCount` goes negative and a pull asks for more than MaxMessages (Low, model-only)

**Fixed on `fix/pull`** (034713b): `decrementPendingMsgs` floors both
counts at 0. With StopAfter drawn by default, `TestModelConsume` hit this
against the real server before the fix (MaxMessages 2, StopAfter 0, a
reconnect the old pull survived, a pull for 3), so it is no longer
model-only.

`decrementPendingMsgs` (`pull.go:446`) has no floor, unlike the 408 path
(`pull.go:773-775`), and `checkPending` computes
`batch = MaxMessages - pending` (`pull.go:470`).
`PullConsume_clamp_bug.cfg` gives the sequence:

1. A missed heartbeat re-pulls while the old pull is alive, and the counter
   is reset.
2. The old pull's 408 arrives while `fetchInProgress = 1`, so
   `checkPending` is skipped and the counter reaches 0.
3. The new pull's messages drive it to -1.
4. The next pull asks for `MaxMessages + 1`.

A deterministic Go reproduction would need `pullMessages` parked inside
`pull()` while the handler runs. The only handle on that is
`pullConsumer.Lock`, which also blocks the handler (`getPinID`), so the
finding stays model-only.

Fix (model: `clamp`): `if s.pending.msgCount > 0 { s.pending.msgCount-- }`,
or `batchSize = min(batchSize, s.consumeOpts.MaxMessages)`.

### PULL-7 — `Closed()` fires while the message handler is still running (Low, confirmed)

**Fixed on `fix/pull`** (ff49354): the closed handlers set `handlerDone`
and `Closed()` uses it. If the subscription was closed (with the
connection) before the closed handler was set, nats.go may never call it,
so Consume/Messages then run it directly (it is idempotent).
`TestPULL7...` is a regression test.

`Closed()` returns an already-closed channel when
`!s.subscription.IsValid()` (`pull.go:846-849`). Stop's Unsubscribe makes
that true right away, while the waitForMsgs goroutine may still be inside
the user's callback. The documented contract is "processing is complete".
Trace: `PullConsume_closedch_bug.cfg`.

`TestPULL7ClosedBeforeHandlerReturns` shows it deterministically: the
handler is blocked, Stop is called, and `Closed()` is closed. 3/3.

The same shortcut made Closed() an unreliable witness in the PULL-1 test.
That test now checks goroutine exit instead.

Fix (model: `closedch`): remember that the closed handler ran, and use
that.

```diff
@@ SetClosedHandler (pull.go:317)
 			sub.Lock()
+			sub.closedHandlerDone = true
 			if sub.closedCh != nil {
@@ Closed (pull.go:846)
-	if !s.subscription.IsValid() {
+	if s.closedHandlerDone {
```

### PULL-8 — Drain/close race: `closed` is set before `draining` (Low, model-only)

**Fixed on `fix/pull`** (9f1fe39) as proposed: `stopped()` waits for
`done` before it reads `draining`; `Next` and the Consume handler use it,
and `cleanup` waits for `done` the same way.

`Drain()` does `closed.CompareAndSwap(0,1)`, *then*
`draining.Store(1)` (`pull.go:828-831`). Two readers can land in that
window:

* `pullMessages`: inside `pull()` it sees `closed` and cleans up
  (`pull.go:1081-1083`, `1143`) with `Unsubscribe` instead of `Drain`, so
  the buffered messages are discarded
  (`PullConsume_drainrace_bug.cfg`).
* A concurrent `Next`: it returns `ErrMsgIteratorClosed` with messages
  still buffered (`pull.go:650-658`; `PullMessages_drainrace_bug.cfg`).

The window is a couple of instructions wide, so no Go test was attempted.

Fix (model: `drainrace`): readers that find `closed` set wait for `done`
before reading `draining`. `Drain` stores `draining` before
`close(done)`, so it is visible after `<-s.done`.

```diff
@@ Next (pull.go:650) and cleanup (pull.go:1143)
-	drainMode := s.draining.Load() == 1
+	if s.closed.Load() == 1 {
+		<-s.done // Stop/Drain finished publishing their mode
+	}
+	drainMode := s.draining.Load() == 1
```

### PULL-9 — FetchBytes allocates ~30 MB of channels per call (Low/performance, confirmed; found while transcribing channel capacities, not through a TLC trace)

**Fixed on `fix/pull`** (1094885), differently from the proposal below:
`FetchBytes` asks for at most `maxBytes / minSize + 1` messages (capped at
1,000,000), where `minSize = len("$JS.ACK.<stream>.<consumer>.") + 1`. The
server counts at least a message's subject and ack reply subject against
max_bytes (`consumer.go`, `sz = len(subj) + len(ackReply) + len(hdr) +
len(msg)`), so no more messages can arrive, and `fetch`'s channels are
sized by that. A server counting less would only return fewer messages
than fit, which FetchBytes allows. `Messages(PullMaxBytes)` lost its 8 MB
channel with PULL-5. `TestPULL9...` checks both (4 MB budget) and is a
regression test.

`FetchBytes` sets `Batch = 1_000_000` (`pull.go:890`), and `fetch` sizes
both channels by it (`pull.go:935-937`): 16 MB for `chan Msg` plus 16 MB
for `chan *nats.Msg`, whatever `maxBytes` is. `Messages(PullMaxBytes(...))`
similarly allocates an 8 MB channel (`pull.go:534` with
`MaxMessages = 1_000_000`, `pull.go:1222`).

`TestPULL9FetchBytesAllocatesChannelsForAMillionMessages`: `FetchBytes(1024)`
allocates 30 MB, 3/3.

Fix: the channels cannot simply shrink, because the fetch goroutine sends to
`res.msgs` while holding `res.Lock` (`pull.go:972-996`). A full channel
would deadlock `Messages()`/`Error()`. So either send outside the lock and
size the channels with a small bound, or grow them on demand.

### PULL-10 — `fetchInProgress` lost wake-up (Low, model-only, timing-dependent)

**Fixed on `fix/pull`** (b1b3c75), not as sketched below: `checkPending`
no longer looks at `fetchInProgress` (the field is gone) and its send is
non-blocking. A request is queued whenever `fetchNext` has room, also while
the previous one is being sent (the pending count already includes it);
when `fetchNext` is full, the queued request brings the next message or
status, which runs the check again. Re-checking from `pullMessages` would
need `sub.Lock`, which `Next` holds while it waits, so that was not done.
In the specs this is the `fip` fix: `PullConsume_fip.cfg` and
`PullMessages_fip.cfg` pass with `FastPull = FALSE`, and
`PullMessages_fip_bug.cfg` (without `fip`) still reproduces the stall.

`checkPending` does nothing while `fetchInProgress == 1` (`pull.go:467`).
Nothing re-runs it when `pullMessages` clears the flag (`pull.go:1092`).
Suppose the last event a pull will ever produce arrives while the flag is
still 1, i.e. the server answered before `pullMessages` executed its next
statement. Then the consumer is idle with no pull until the heartbeat
timeout. `PullMessages_fip_bug.cfg` shows it with every other fix applied.

In practice this needs `pullMessages` descheduled for a full network round
trip (or for `Expires`, in the 408 case), so all other configurations
assume it away (`FastPull = TRUE`).

Fix sketch: when `checkPending` skips because of `fetchInProgress`, set a
`recheck` flag. `pullMessages`, after clearing `fetchInProgress`, re-runs
the threshold check under `sub.Lock` and pulls directly.

### O-1 — Fetch leaves its heartbeat timer armed (observation, code review)

**Fixed on `fix/pull`** (6b2a6f1) as proposed.

The fetch goroutine (`pull.go:966-1034`) returns without stopping `hbTimer`
(`pull.go:958`). For up to 2×heartbeat (10s by default) after a fetch
completes, the timer keeps the subscription, and through it the connection,
reachable. Consume's equivalent was fixed in #2164. Fix:
`if hbTimer != nil { defer hbTimer.Stop() }` at the top of the goroutine.

## The coordinator's hypotheses

* **H1**: confirmed, as PULL-1. The model also shows the handler, not only
  the status goroutine, can be the writer that is left blocked, and that
  `Messages().Next` has the same problem during Drain.
* **H2** (handler blocked on `fetchNext` ← `pullMessages` blocked on a full
  `errs` ← status goroutine blocked on `sub.Lock`): not reached. With the
  real `errs` capacity of 10 the bounded model never fills `errs`, and
  `NoStuckSelfSend` held in every run. `pullMessages`' `errs` send selects
  on `done` (`pull.go:1087-1090`), so Stop breaks that cycle. Without Stop
  it needs ten unread pull errors, which in Consume only come from a closed
  connection (then no messages flow) or batch < 1 (the PULL-3 side path).
  I consider it practically unreachable, but have not proven it.
* **H3**: several confirmed accounting problems.
  * Stall: PULL-4 (heartbeat-timeout stall), PULL-2 (permanent stall),
    PULL-10 (theoretical).
  * Over-pull: PULL-3 (exceeds StopAfter), PULL-6 (exceeds MaxMessages),
    PULL-5 (overflows the channel).
  * Batch < 1: PULL-3 side path.
  * Reconnect resets assume the old pulls are gone. When they are not,
    Consume buffers up to 2×MaxMessages. That is harmless for Consume, which
    has an unbounded list, except under StopAfter (PULL-3); it is the root of
    PULL-5 in Messages.
* **H4**:
  * Drain then Next: buffered messages are delivered and then
    `ErrMsgIteratorClosed`, except under PULL-8 (race) and PULL-1 (hang).
  * Stop then Next: `ErrMsgIteratorClosed`, never violated.
  * Next concurrent with a reconnect: PULL-5, PULL-2.
* **H5** (Fetch): no deadlock or leak. The goroutine sends at most `Batch`
  user messages into `res.msgs` (capacity `Batch`) and returns when
  `receivedMsgs == Batch` (`pull.go:1009-1013`), so the send never blocks.
  The inner channel (2×Batch) is drained continuously. After expiry the
  goroutine returns at most `Expires+1s` after the last message
  (`pull.go:1021`), and Unsubscribe is deferred. Findings in this area:
  PULL-9 and O-1.

## Model-derived Go tests (`modeltest/pullconsume`)

* `natsproxy_test.go`: a NATS-protocol-aware proxy. It plays the model's
  environment actions on a real nats-server: it can drop or hold frames
  (heartbeats, 408s, PONGs), cut connections, and record every pull
  request's batch from the wire. The file also has goroutine-stack waiters
  that park on a function (by name, so that edits to `pull.go` do not
  break them) and a `len(chan)` peek, so tests can wait for settled states
  instead of sleeping.
* `bugs_test.go`: one reproducer per confirmed finding (PULL-1, 2, 3, 4, 5,
  7, 9). On `fix/pull` all are regression tests except PULL-2's two; on the
  merged fixes branch those are regression tests too.
* `stateful_test.go`: zizek state machines, `TestModelConsume` and
  `TestModelMessages`.
  * Rules are the models' environment actions: publish, cut, late
    heartbeats, wait for expiry, a settle/liveness probe, and Next for
    Messages.
  * Invariants are the models' invariants checked on the wire and at the
    API: `StopAfterRespected`, `BatchWithinMax`, `NoInvalidPull`,
    `NoSlowConsumerDrop`, `NoSilentStall` as a liveness probe, and
    `NoDeadlock`/`DrainKeepsBuffered` on teardown.
  * Before the fixes the default draws avoided the known-bug triggers (no
    StopAfter for Consume; no reconnect or late heartbeat for Messages).
    With `MODELTEST_KNOWN_BUGS=1` they explored everything and failed on
    known findings: `TestModelConsume` shrank to the 2-step PULL-3 trace
    above, and `TestModelMessages` reported the PULL-1 Messages hang (and,
    in an earlier revision of its default mode, the stale-`errConnected`
    variant of PULL-5).
  * On `fix/pull` the default run explores the fixed findings: Consume
    draws StopAfter, Messages reconnects and misses heartbeats. On
    `fix/pull` alone Messages made at most four reconnects in a row without
    Next, to stay clear of PULL-2; the merged fixes branch drops that
    limit. Both pass, about 23 s and 29 s,
    6 and 8 examples, fixed seeds (also 25 examples each with other seeds,
    82 s and 101 s, green).
  * Rules that cut the connection first wait (up to 500 ms, without
    asserting) for messages already on their way to the client. The proxy
    loses what is in flight and the server redelivers it only after
    AckWait (30 s); once StopAfter was drawn by default, that environment
    loss showed up as a spurious NoSilentStall.

Verification of the fixes (measured on `fix/pull`):

* `./run.sh ./pullconsume/...`: green (the two PULL-2 tests skip).
  With `MODELTEST_KNOWN_BUGS=1` only the two PULL-2 tests fail.
* The regression tests, copied onto the base commit and run with
  `MODELTEST_KNOWN_BUGS=1`, all fail there, each with the failure described
  under its finding (e.g. PULL-5: pulls `[10 10 6 6 6]`, 26 of 30
  received; PULL-9: 30 MB).
* `./run.sh ./ordered/...` (the ordered consumer builds on `pull.go`):
  green.

Notes on zizek:

* These properties run against a real server and are not perfectly
  replayable. When a failing example stops failing on replay, zizek since
  `12359a5` (wallyqs/zizek) returns `*zizek.Flaky` with the original error
  and blob (before that it reported `internal error: shrunk example no
  longer fails` and dropped the cause). The tests also log the first
  failure's notes themselves (`logFirst`), which `Flaky` does not carry.
* Shrinking is expensive here (each example takes seconds), so
  `MaxShrinks`/`MaxStall` are kept small. Even so, the Messages failure in
  known-bugs mode is reported long-ish (~16 steps).
* Rules that `Assume(false)` show up as "rule rejected by assumption" steps
  in shrunk traces. That is noise in the notes, but it does no harm.
