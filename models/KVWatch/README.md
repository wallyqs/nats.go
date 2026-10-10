# KVWatch — JetStream KeyValue watchers and key listing

TLA+ model of the KeyValue watcher in `jetstream/kv.go` (`WatchFiltered` and
everything built on it: `Watch`, `WatchAll`, `Keys`, `ListKeys`,
`ListKeysFiltered`, `History`; options `UpdatesOnly`, `IncludeHistory`,
`IgnoreDeletes`, `MetaOnly`, `ResumeFromRevision`) running over the legacy
ordered push consumer of `js.go` (`kv.pushJS.Subscribe(..., nats.OrderedConsumer())`),
against a stream that other clients write to concurrently. Model-derived Go
tests live in [`modeltest/kvwatch`](../../modeltest/kvwatch).

## Scope

In: the watcher's setup goroutine and `w.mu`; the "initial values done" nil
marker (`initPending` from `NumPending + Delivered` at consumer creation,
`received`, `delta == 0`); the `w.updates` channel with its real blocking
semantics (capacity 256 abstracted to `ChanCap`); `Stop()` / context
cancellation and the closed handler that closes `w.updates`; `Keys()`,
`ListKeys()` (with its lister goroutine and the 256-slot `kl.keys`),
`History()`; the ordered consumer: gap detection on data messages and on
idle heartbeats, `activityCheck`, `resetOrderedConsumer` with its goroutine,
sid generations dropping messages for the old consumer; the server consumer:
DeliverLastPerSubject skip list, DeliverAll / DeliverNew / start sequence,
`num_pending` in every ack reply (`delta`), heartbeats with
`Nats-Last-Consumer`; environment: Put / Delete / Purge (rollup) with the
bucket `History` limit, removal of stored messages without a new message
(PurgeDeletes, stream purge by subject, MaxAge), loss of a delivered message
(reconnect, test hook), server restart (R1 memory consumer lost), context
cancellation.

Out: flow control and pending limits (slow-consumer drops are covered as
"message loss"), clustering, the JSON/wire layer, `MetaOnly` (it only changes
payloads; delete detection uses headers that stay), mirrors/sources,
per-message TTL markers (handled like deletes by `kv.go:1298-1304`).

## Files

| File | What |
|---|---|
| `KVWatch.tla` | the spec |
| `KVWatch.cfg` | baseline: every mode, `IgnoreDeletes` on/off, Watch user, concurrent Put/Delete/Purge, no faults — passes |
| `KVWatch_fixed_lps.cfg`, `_remove`, `_restart`, `_keys`, `_listkeys`, `_stop` | all proposed fixes (`Fixes` constant) under message loss / removals / a server restart / ctx cancel / early Stop — pass |
| `KVWatch_history_faults.cfg` | today's code (+ only the KV-8 fix), IncludeHistory watcher under message loss and a server restart — passes |
| `KV*_bug.cfg` | counterexample reproducers, one per finding — must violate |

Run: `TLA2TOOLS=/path/to/tla2tools.jar models/tlc.sh KVWatch`.

## Action → code map

Line numbers are for this checkout (`jetstream/kv.go`, `js.go`, `nats.go`).

| TLA+ action | Code |
|---|---|
| `SetupStart` | `kv.go:1351` `w.mu.Lock()`; `kv.go:1355-1360` `pushJS.Subscribe`; `js.go:1983-1990` SUB sent, consumer created (`upsertConsumer`); server start set: LPS skip list `nats-server consumer.go:6406-6427`, DeliverNew/All/ByStartSequence; options `kv.go:1334-1347` (`StartSequence` overrides LPS/New since it is appended last) |
| `SetupInfo` | `js.go:2032` `jsi.pending = info.NumPending + info.Delivered.Consumer` (info built after the consumer started delivering) |
| `SetupClosedH` | `kv.go:1364-1368` `SetClosedHandler` (runs after `js.go:2068-2073` started the ctx goroutine) |
| `SetupPending` | `kv.go:1372-1385` `InitialConsumerPending` (`js.go:2081-2088`, errors while `jsi.consumer == ""`), marker if 0, `initPending`; return, `w.mu` released (deferred) |
| `ServerStep` | `consumer.go:5025-5040` skip-list pop / next message; ack reply carries `num_pending` (`delta`, `kv.go:1306`) |
| `ServerHB` | idle heartbeat with `Nats-Last-Consumer` (only gap-revealing ones are generated) |
| `ClientRecv` | `nats.go:3897-3966` sid lookup / closed check; `js.go:2179-2207` `checkOrderedMsgs` / `commitOrderedMsg`; `js.go:2559-2585` heartbeat gap check; `js.go:2233-2274` `tryResetOrderedConsumer` → `resetOrderedConsumer(jsi.sseq+1)` |
| `HbTimeout` | `js.go:2501-2525` `activityCheck` (2 × 5s without heartbeats while connected → reset) |
| `ResetBegin` / `ResetCreate` | `js.go:2309-2435`: new sid at once (`applyNewSID`), goroutine: `jsi.dseq = 1`, `jsi.consumer = ""` (`:2397`), `DeliverByStartSequencePolicy`, `OptStartSeq = sseq` (`:2381-2382`), `jsi.consumer = name` (`:2433`) |
| `CbPop` | `nats.go:3801-3815` pop + `mcb(m)` (`:3849`); `kv.go:1279-1307` parse, `w.mu.Lock()` |
| `CbSend` | `kv.go:1309-1320` `w.updates <- entry` (blocks while full), then `kv.go:1322-1330` accounting |
| `CbNil` | `kv.go:1328` `w.updates <- nil` |
| `CbExit` / `CbClose` | `nats.go:3843` closed → leave loop, `:3872` call `pDone`; `kv.go:1364-1368` close `w.updates` under `w.mu` |
| `WatchRead` / `WatchStop` | user draining `Updates()`; `kv.go:1235-1240` `Stop()` → `Unsubscribe` (`nats.go:5551-5620`, `removeSub` `:5237-5276`) |
| `KeysRead` / `KeysReturn` | `kv.go:1402-1424` (user `Keys` with mode LPS + IgnoreDeletes); `History` `kv.go:1499-1519` is the same loop over an IncludeHistory watch (user `Keys` with mode ALL) |
| `ListerRecv` / `ListerSend` / `ListerStop` | `kv.go:1440-1455` lister goroutine (`kl.keys <- entry.Key()` at `:1449`) |
| `ListUserRead` / `ListUserStop` | `kl.Keys()` reader; `KeyLister.Stop()` (`kv.go:1494-1496`) |
| `Put` / `Del` | `kv.Put`, `kv.Delete`, `kv.Purge` (rollup), with `MaxMsgsPerSubject = History` |
| `RemoveKey` / `Expire` | `PurgeDeletes()` (`kv.go:1567-1580`) / stream purge by subject; MaxAge |
| `NetDrop` | a delivered message lost before `processMsg` (reconnect; `AddMsgFilter` in tests) |
| `ServerRestart` | R1 memory ordered consumer lost, in-flight bytes lost, client re-SUBs |
| `CtxCancel` | `js.go:2068-2073` `<-ctx.Done(); sub.Unsubscribe()` |
| `IdleMarker` | fix for KV-3 only (not in today's code) |

## Invariants

Safety (recorded when the step that breaks them happens, `bad` set):
`MarkerAtMostOnce`; `NoMarkerUpdatesOnly`; `MarkerComplete` — when the marker
is sent, every value the initial phase owes (LPS: latest per key at creation;
IncludeHistory / Resume: every stored revision in range; minus deletes under
IgnoreDeletes) was delivered, or no longer exists, or a newer revision of
that key was delivered; `RevisionOrder` — revisions strictly increase;
`NoStaleEntry` — nothing below the mode's floor (LPS: latest at creation;
UpdatesOnly: last seq at creation + 1; Resume: the revision); `NoLostUpdate`
— no stored message written after creation is skipped; `NoSendOnClosed`,
`CloseOnce`; `KeysSound` — `Keys`/`ListKeys` return only keys that were
live at some point during the call; `KeysNoSilentTruncation`.

Quiescent (environment stopped and no process can move — TLC's deadlock
check is off, these name the stuck states): `NoStuckInit` — a live watcher
has sent its marker; `Converged` — a live watcher delivered the latest
revision of every watched key; `NoLeakAfterStop` — after Stop / ctx cancel
the callback goroutine exited, `Updates()` is closed and the ListKeys lister
exited and closed `Keys()`.

## Abstractions and limitations

- `w.updates` and `kl.keys` have capacity 1 instead of 256: blocking is
  what matters; the Go tests show where 256 changes the likelihood (KV-6).
- The server's consumer-create response is folded into one client step
  that reads `NumPending + Delivered` at some point after creation (data
  messages may be processed before it); a gap or heartbeat that would
  start a reset waits until that step (a reset before the response would
  also race `jsi.consumer = info.Name` and `jsi.pending`, not modeled —
  TLC flagged this as an artifact when it was allowed).
- `ResetBegin` clears `jsi.consumer` at once; the code does it a little
  later inside the goroutine (`js.go:2397`). The KV-8 window exists in the
  code too, the model only makes it slightly wider.
- At most one reset goroutine in flight (the code tolerates concurrent ones
  via `rewireOrderedSub` returning false; not explored).
- Heartbeats are generated only when they reveal a gap (others are no-ops
  for the client). `HbTimeout` is enabled only when the consumer serving the
  current sid is gone (connected, no heartbeats).
- Flow control is not modeled; its effect (the server holding back part of
  the initial set while the reader is slow) is just one interleaving of
  `ServerStep` with environment actions.
- Fixes are modeled at the level of what they decide (e.g. KV-3's check is
  an action enabled when ConsumerInfo would report nothing pending and
  everything delivered handled), not their Go code.

## TLC results

Measured on this machine (4 cores shared with other jobs), TLC 2026.10.06,
`-workers auto`, `models/tlc.sh` settings.

| Config | Result | Distinct states | Depth / trace | Time |
|---|---|---:|---:|---:|
| `KVWatch.cfg` (baseline, 4 modes × IgnoreDeletes, MaxSeq 3, History 2) | pass | 4,812,460 | depth 26 | 142 s |
| `KVWatch_history_faults.cfg` (today's code + KV-8 fix, IncludeHistory, 1 drop + 1 restart, MaxSeq 2) | pass | 414,936 | depth 28 | 21 s |
| `KVWatch_fixed_lps.cfg` (all fixes, LPS, 1 drop) | pass | 3,874,704 | depth 33 | 118 s |
| `KVWatch_fixed_remove.cfg` (all fixes, LPS × IgnoreDeletes, 1 removal) | pass | 7,867,476 | depth 27 | 225 s |
| `KVWatch_fixed_restart.cfg` (all fixes, New/Resume/All, 1 restart) | pass | 7,763,272 | depth 29 | 224 s |
| `KVWatch_fixed_keys.cfg` (all fixes, Keys()/History(), 1 ctx cancel) | pass | 12,937,360 | depth 30 | 275 s |
| `KVWatch_fixed_listkeys.cfg` (all fixes, ListKeys + early Stop, 1 ctx cancel) | pass | 16,239,638 | depth 37 | 320 s |
| `KVWatch_fixed_stop.cfg` (all fixes, Watch + early Stop, 1 ctx cancel) | pass | 10,411,406 | depth 30 | 280 s |
| `KV1_stale_reset_bug.cfg` | `NoStaleEntry` violated | 1,353,016 | 16 states | 24 s |
| `KV1_keys_deleted_bug.cfg` | `KeysSound` violated | 3,457,469 | 20 states | 62 s |
| `KV2_reset_from_one_bug.cfg` | `NoStaleEntry` violated | 58,730 | 14 states | 4 s |
| `KV3_marker_stuck_bug.cfg` | `NoStuckInit` violated | 9,129 | 8 states | 2 s |
| `KV4_keys_ctx_bug.cfg` | `KeysNoSilentTruncation` violated | 16,210 | 11 states | 3 s |
| `KV5_listkeys_leak_bug.cfg` | `NoLeakAfterStop` violated | 81,924 | 17 states | 6 s |
| `KV6_keys_stop_leak_bug.cfg` | `NoLeakAfterStop` violated | 626,391 | 17 states | 17 s |
| `KV7_closed_handler_bug.cfg` | `NoLeakAfterStop` violated | 1,092 | 9 states | 2 s |
| `KV8_pending_err_bug.cfg` | `MarkerComplete` violated | 1,294,893 | 17 states | 35 s |

Times are wall clock on a loaded machine; the fixed configs with users are
the slowest (~5 min). Total for the directory: about 30 minutes.

## Findings

Severity: High = wrong data or a hang in normal operation after a common
fault; Medium = wrong result / leak under a plausible condition; Low = narrow
race, hard to hit.

### KV-1 — reset during the initial phase replays history: stale values, deleted keys, early marker (High)

A gap while a DeliverLastPerSubject watcher is still delivering initial
values makes the ordered consumer restart with
`DeliverByStartSequence(jsi.sseq+1)` (`js.go:2274, 2381-2382`). For a bucket
with `History > 1` that replays every revision after the last delivered
one, not the last per subject. The watcher hands out superseded revisions as
initial values, `received` counts them against `initPending` (taken from the
LPS consumer) so the marker arrives before the real latest values, and with
`IgnoreDeletes` (`Keys`, `ListKeys`) the old Put of a deleted key is
reported as a live key. With `History == 1` only one revision per key exists
and the replay is harmless.

Trace (`KV1_stale_reset_bug.cfg`, 16 states): b@1, b@2 written; watch
(LPS set = {2}); the delivery of b@2 is lost; the idle heartbeat reports
`Nats-Last-Consumer 1 ≥ jsi.dseq 1` → reset from seq 1 → b@1 delivered,
marker. `KV1_keys_deleted_bug.cfg`: a put@1, a del@2, gap → `Keys() = [a]`.

Triggers: any lost message during the initial phase — a reconnect while a
large watch/Keys() is streaming, a slow-consumer drop.

Status: **confirmed** — `TestKV1ResetDuringInitialPhaseDeliversStaleRevision`
(`[a@1 b@2=b1 c@3 <nil>]`: stale b@2, marker before b@4),
`TestKV1KeysReturnsDeletedKeyAfterReset` (`Keys() = [a b c]`, b deleted),
and found independently by the zizek machine `TestKVWatchModelFaults`
(shrunk: put b, delete b, drop next b, `Keys() = [b]`).

Proposed fix (sketch; js.go owns the reset, kv.go owns the count): while an
ordered consumer created with DeliverLastPerSubject has not finished its
initial set, recreate it with DeliverLastPerSubject again and skip what was
already handed over; tell the watcher how many entries are still owed.

```diff
--- a/js.go
+++ b/js.go
@@ type jsSub struct {
 	sseq    uint64
+	// Ordered LPS consumer still in its initial set: a reset recreates it
+	// with DeliverLastPerSubject and drops sseq <= skipTo.
+	lpsInit bool
+	skipTo  uint64
@@ func (sub *Subscription) checkOrderedMsgs(m *Msg) (bool, orderedSeqs) {
 	if dseq != jsi.dseq {
 		return true, orderedSeqs{}
 	}
+	if sseq <= jsi.skipTo {
+		// Re-sent by an LPS restart; already delivered. Advance, don't queue.
+		jsi.dseq++
+		return false, orderedSeqs{skip: true}
+	}
 	return false, orderedSeqs{dseq: dseq, sseq: sseq}
@@ func (sub *Subscription) resetOrderedConsumer(sseq uint64) {
-		cfg.DeliverPolicy = DeliverByStartSequencePolicy
-		cfg.OptStartSeq = sseq
+		if jsi.lpsInit {
+			cfg.DeliverPolicy = DeliverLastPerSubjectPolicy
+			cfg.OptStartSeq = 0
+			jsi.skipTo = sseq - 1
+		} else {
+			cfg.DeliverPolicy = DeliverByStartSequencePolicy
+			cfg.OptStartSeq = sseq
+		}
@@ (after upsertConsumer of the reset)
+		if jsi.lpsInit {
+			jsi.pending = cinfo.NumPending + cinfo.Delivered.Consumer // re-read by kv.go
+			jsi.resets++
+		}
--- a/jetstream/kv.go
+++ b/jetstream/kv.go
@@ update := func(m *nats.Msg) {
+		// An LPS restart re-counts what is still owed (InitialConsumerPending
+		// returns the new value, minus entries skipped as already delivered).
+		if !w.initDone && sub.OrderedResets() != w.resets { ... recompute w.initPending ... }
@@ after the marker is sent
+		sub.EndOrderedLPSInit() // lpsInit = false: later resets resume by sequence
```

Trade-off (also in the model, ghost `snap.collapse`): an LPS restart
collapses updates written during the initial phase to their latest
revision, as LPS already does for writes made before the watch.
`KVWatch_fixed_lps.cfg` checks the fixed behavior, including the re-count
(without it the marker comes early — TLC found that in a first version of
the fix).

### KV-2 — a reset before the first delivered message starts at sequence 1 (High)

`jsi.sseq` starts at 0 and only advances when a message is handed over, so
a reset before that (`activityCheck` after a server restart, or a heartbeat
gap) uses `OptStartSeq = 1`. An `UpdatesOnly()` watcher then replays every
stored value as an update; a `ResumeFromRevision(r)` watcher gets revisions
below `r`; an LPS watcher whose first message was lost gets full history
(that is KV-1's 16-state trace). The slow-consumer path special-cases this
(`nats.go:4115-4117`) but the other reset paths do not.

Trace (`KV2_reset_from_one_bug.cfg`, 14 states): a@1; UpdatesOnly watch;
server restart; heartbeat timeout → reset from 1 → a@1 delivered.

Status: **confirmed, no test hooks needed** —
`TestKV2UpdatesOnlyReplaysHistoryAfterServerRestart` (embedded server
restart, ~15 s: delivers `[a@1 b@2 c@3]`, want `[c@3]`); fast variants
`TestKV2UpdatesOnlyReplaysHistoryAfterLostFirstUpdate`,
`TestKV2ResumeFromRevisionReplaysOlderRevisions`. This is an ordered
consumer (`js.go`) issue; by code reading it affects every legacy ordered
consumer created with DeliverNew / a start sequence / a start time that is
reset before its first message, not only KV.

Proposed fix: seed the tracker with the consumer's start floor.

```diff
--- a/js.go
+++ b/js.go
@@ func (js *js) subscribe(...)
 			sub.jsi.dc = true
 			sub.jsi.pending = info.NumPending + info.Delivered.Consumer
+			// Resume point for ordered resets before the first message:
+			// where the consumer started, not the start of the stream.
+			if o.ordered && sub.jsi.sseq == 0 && info.Delivered.Consumer == 0 {
+				sub.jsi.sseq = info.Delivered.Stream
+			}
```

(`Delivered.Stream` is the start floor when nothing was delivered yet:
DeliverNew → last seq, ByStartSequence(r) → r-1. If messages were already
delivered when the info was built, the first message handed over sets
`sseq` as today.)

### KV-3 — the marker is never sent when owed initial values are removed (High)

The watcher ends the initial phase when `received >= initPending` or a
message carries `delta == 0`. If initial values that were not delivered yet
disappear without a new message — `PurgeDeletes()` (removes old delete
markers with keep=0), a stream purge by subject, MaxAge expiry — the server
has nothing more to send, `received` never reaches `initPending` and no
message carries `delta == 0`. `Watch` users never see "initial values done";
`Keys()`/`History()` block until their context ends (and then hit KV-4).
A later write to a watched key unblocks it (its `delta` is 0). The legacy
`kv.go` has an `initDoneTimer` that sends the marker with
`ErrKeyWatcherTimeout` after the JS timeout; `jetstream/kv.go` has nothing.

Trace (`KV3_marker_stuck_bug.cfg`, 8 states): a@1; watch (pending 1); a
removed before delivery; nothing is ever delivered.

Status: **confirmed** — `TestKV3MarkerNeverSentWhenPendingInitialValuesAreRemoved`:
280 keys, the reader is slow so flow control stalls the server with 12
values undelivered; those are purged; 268 entries arrive and no marker.

Proposed fix (sketch): when the watcher is idle in its initial phase, ask
the server.

```diff
--- a/jetstream/kv.go
+++ b/jetstream/kv.go
@@ type watcher struct {
 	received    uint64
+	idleCheck   *time.Timer
@@ WatchFiltered, after initPending is set
+		w.idleCheck = time.AfterFunc(idleInitCheck, w.checkIdleInit)
+
+// checkIdleInit ends a stuck initial phase: nothing pending on the server
+// and every delivered message handled means nothing more is owed.
+func (w *watcher) checkIdleInit() {
+	ci, err := w.sub.ConsumerInfo()
+	w.mu.Lock()
+	defer w.mu.Unlock()
+	if w.initDone || w.closed {
+		return
+	}
+	if err == nil && ci.NumPending == 0 && ci.Delivered.Consumer == w.handledFromConsumer() {
+		w.initDone = true
+		w.updates <- nil
+		return
+	}
+	w.idleCheck.Reset(idleInitCheck)
+}
```

The `Delivered.Consumer` comparison matters: TLC showed that a check on
`NumPending == 0` alone fires while a lost message is still undetected
(marker before the lost initial value is redelivered). A plain timer like
legacy's would also fire early while flow control stalls a slow reader.

### KV-4 — Keys()/History() return a truncated result with no error when the context ends (Medium)

`Keys()` and `History()` range over `Updates()` until the marker. When their
context is cancelled or times out, `js.go:2068-2073` unsubscribes, the
closed handler closes `w.updates`, the `range` ends normally and the
function returns the keys collected so far with a nil error (or
`ErrNoKeysFound` / `ErrKeyNotFound` when none arrived). `ListKeys` closes
`Keys()` the same way, indistinguishable from a complete listing.

Trace (`KV4_keys_ctx_bug.cfg`, 11 states).

Status: **confirmed** — `TestKV4KeysContextCancelSilentlyTruncates`: 50
keys, cancel after 25 are read → `Keys()` returns 25 keys, nil.

Proposed fix:

```diff
@@ func (kv *kvs) Keys(ctx context.Context, opts ...WatchOpt) ([]string, error) {
 	var keys []string
+	complete := false
 	for entry := range watcher.Updates() {
 		if entry == nil {
+			complete = true
 			break
 		}
 		keys = append(keys, entry.Key())
 	}
+	if !complete {
+		if err := ctx.Err(); err != nil {
+			return nil, err
+		}
+		return nil, nats.ErrConnectionClosed
+	}
```

(same for `History`; `ListKeys` needs an error surface, e.g. the `Error()`
the legacy `KeyLister` gained in #1889.)

### KV-5 — KeyLister.Stop() leaks the lister goroutine and the watcher callback (Medium)

`KeyLister` documents `Stop()` for stopping before all keys were read, but
the lister goroutine sends with a bare `kl.keys <- entry.Key()`
(`kv.go:1449`, `:1480`). Once the caller stops reading, it blocks forever;
`Keys()` is never closed and the watcher callback stays blocked on the full
`w.updates` holding `w.mu`. Stopping the watcher does not help (it can only
take effect between callbacks), and `ctx.Done()` is only selected on the
receive side.

Trace (`KV5_listkeys_leak_bug.cfg`).

Status: **confirmed** — `TestKV5ListKeysStopLeaksGoroutines`: 1000 keys,
read one, `Stop()`: `ListKeys.func1` and `WatchFiltered.func1` are still
blocked 3 s later.

Proposed fix:

```diff
@@ ListKeys / ListKeysFiltered lister goroutine
-				kl.keys <- entry.Key()
+				select {
+				case kl.keys <- entry.Key():
+				case <-ctx.Done():
+					return
+				case <-kl.stop: // closed by keyLister.Stop()
+					return
+				}
```

plus KV-6's fix so the callback blocked on `w.updates` is released.

### KV-6 — Keys()/ListKeys()/History() can leak the watcher callback after Stop (Low)

These helpers stop reading `Updates()` at the marker and then `Stop()` the
watcher. If more than 256 updates are delivered between the marker and the
unsubscribe, the callback blocks on the full `w.updates` while holding
`w.mu`; after `Stop()` nobody reads, so the subscription's `waitForMsgs`
goroutine and the 256 buffered entries leak (the closed handler never
runs). Same for a `Watch` user who calls `Stop()` and stops reading.

Trace (`KV6_keys_stop_leak_bug.cfg`, 17 states, `ChanCap = 1`).

Status: **model-only**. A Go reproduction attempt (20 000 keys, 600 updates
queued behind the last initial value) did not hit it: flow control keeps
the server from running ahead, and only ~10 updates arrived in the ~1 ms
between the marker and `Stop()`. It needs a sustained write rate well above
what flow control lets through in that window, so it is real but unlikely.

Proposed fix: make the callback's sends abandonable.

```diff
@@ type watcher struct {
+	stopped chan struct{} // closed by Stop() (once)
@@ update callback
-			w.updates <- entry
+			select {
+			case w.updates <- entry:
+			case <-w.stopped:
+				return
+			}
```

(same for the marker send; `Stop()` closes `w.stopped` before
`Unsubscribe`.)

### KV-7 — Updates() is never closed if the context ends right after Subscribe returns (Low)

`js.subscribe` starts the goroutine that unsubscribes on `ctx.Done()`
(`js.go:2068-2073`) before `WatchFiltered` installs the closed handler
(`kv.go:1364`). If the context ends in between, `waitForMsgs` exits with
`pDone == nil` and nobody ever closes `w.updates`: a `for range
w.Updates()` blocks forever.

Trace (`KV7_closed_handler_bug.cfg`, 9 states).

Status: **model-only** (the window is a few instructions; not reproduced).

Proposed fix: after `SetClosedHandler` (still under `w.mu`), if
`!sub.IsValid()`, close `w.updates` and mark the watcher closed; the
handler and the callback check that flag (close once, no sends after).
Checked as fix `closedh` in `KVWatch_fixed_stop.cfg` and `KVWatch_fixed_keys.cfg`.

### KV-8 — a reset racing InitialConsumerPending sends the marker after the first entry (Low)

While a reset goroutine recreates the consumer, `jsi.consumer` is `""`
(`js.go:2397` → `:2433`) and `InitialConsumerPending` returns an error
(`js.go:2084`). `WatchFiltered` then leaves `initPending = 0` and
`initDone = false`, so the first entry satisfies `received >= initPending`
and the marker follows it.

Trace (`KV8_pending_err_bug.cfg`, with KV-1/2 fixed to isolate it; 17
states): a gap detected by a heartbeat between `SetClosedHandler` and
`InitialConsumerPending`.

Second flavor (found by `KVWatch_history_faults.cfg` before `pendingerr`
was added to it, 9 states): on an empty selection (`NumPending == 0`) the
creation-time marker is skipped the same way, and as no message ever
arrives the marker is never sent (`NoStuckInit`) until a write comes in.
Trigger: a server restart / heartbeat gap reset in flight while
`WatchFiltered` reads the pending count.

Status: **model-only** (needs a reset within the first round trip).

Proposed fix: `InitialConsumerPending` should not depend on the consumer
name (`jsi.pending` is set once at creation); check `sub.jsi == nil` only.

### KV-9 — nats-server file store: purge by subject + purge by sequence hide stored values (server, High; not nats.go)

Found by the zizek machine (`TestKVWatchModel`, rules `removeKey` +
`expire`): on a file-backed KV stream, `a@1 c@2`, purge subject c, purge
below seq 2, `a@3 b@4 b@5`, purge below seq 4 leaves stream state
`Msgs:0 FirstSeq:6 LastSeq:5` while `b@5` is stored (`Get(b)` returns it).
Consumers start from that state: `WatchAll()` delivers only the marker,
`History(b)` returns `ErrKeyNotFound`, `Keys()` returns no keys. With
`History=2` the state is `Msgs:1` and `History(b)` misses `b@5`.
MemoryStorage is correct. This is the embedded `nats-server v2.15.0`
(`modeltest/go.mod`); not checked against the upstream issue tracker.

Status: **confirmed** — `TestKV9FileStorePurgeAccountingHidesValues`. The
default zizek machine leaves out `expire` so it tests the client.

### Checked and not a bug

- Send on a closed `w.updates` / double close: the closed handler runs on
  the subscription's own `waitForMsgs` goroutine after its last callback,
  and the creation-time marker send holds `w.mu` (`NoSendOnClosed`,
  `CloseOnce` hold in every configuration). The #1965 race (closing without
  `w.mu`) is not reachable any more.
- Concurrent writes between consumer creation and the info response
  (`NumPending + Delivered` over-counts): the marker comes after the extra
  update, never before an initial value (`MarkerComplete` holds in the
  baseline). This is the source of the duplicates `ListKeys` documents.
- `ResumeFromRevision` beyond the last revision: `NumPending == 0`, marker
  at once.
- A delete/purge as the last initial entry with `IgnoreDeletes`: still
  counted, marker sent.
- IncludeHistory watchers recover from message loss and a server restart
  with today's code apart from KV-8 (`KVWatch_history_faults.cfg` passes): DeliverAll
  resumed from `jsi.sseq+1` is exactly right, even from sequence 1.

## Tests derived from the model (`modeltest/kvwatch`)

| Test | Kind |
|---|---|
| `TestKVWatchModel` | zizek state machine (rules = model actions: put, del/purge, removeKey, watch in every mode/filter with writes racing the initial delivery, stop, keys, listKeys/listKeysFiltered, history), invariants = the model's, against a reference stream; passes |
| `TestKVWatchModelFaults` (`internal_testing`) | the same plus `dropNext` (NetDrop via `AddMsgFilter`); KnownBug KV-1/KV-2 |
| `TestKV1*`, `TestKV2*`, `TestKV3*`, `TestKV4*`, `TestKV5*`, `TestKV9*` | deterministic reproducers, `harness.KnownBug` |

```
cd modeltest
go test -race ./kvwatch/...                                   # passes, KnownBug tests skipped
go test -race -tags=internal_testing ./kvwatch/...            # same
MODELTEST_KNOWN_BUGS=1 go test -race -tags=internal_testing ./kvwatch/...  # reproduces
```

zizek note: `RunSteps` uses swarm-style rule subsets and a per-step stop
probability, so a 30-step bound gives ~10-30 applied rules per example; the
test logs rule counts (about 2 800 applications for 100 examples).
