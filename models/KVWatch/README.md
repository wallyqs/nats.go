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
| `KVWatch_fixed_lps.cfg`, `_remove`, `_restart`, `_keys`, `_listkeys`, `_stop` | the fixes as applied on `fix/kv` (`Fixes` constant, `InfoAtCreate = TRUE`) under message loss / removals / a server restart / ctx cancel / early Stop — pass |
| `KVWatch_history_faults.cfg` | today's code (+ only the KV-8 fix), IncludeHistory watcher under message loss and a server restart — passes |
| `KV*_bug.cfg` | counterexample reproducers, one per finding, encoding the code before the fixes — must violate |

Run: `TLA2TOOLS=/path/to/tla2tools.jar models/tlc.sh KVWatch`.

## Action → code map

Line numbers are for the code before the fixes (the models commit); the
fixes on `fix/kv` shift them. The `Fixes` constant names what each fix
changes: `reset` and `recount` (KV-1, with the `js.go` start floor of KV-2
in `reset`), `marker` (KV-3), `keysctx` (KV-4), `lister` (KV-5),
`stopdrain` (KV-6), `closedh` (KV-7), `pendingerr` (KV-8).

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
| `IdleMarker` | fix `marker`: `jetstream/kv.go` `watcher.checkInit` (timer while the initial phase is open and idle) |
| `TrackLPS` | fix `reset`: `js.go` `commitOrderedMsg`, end of the ordered consumer's last-per-subject phase (`jsi.lps`) |

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
- Fixes are modeled at the level of what they decide, following the Go
  code on `fix/kv`: `IdleMarker` merges `checkInit`'s ConsumerInfo /
  `sub.Pending()` snapshot and its check under `w.mu` into one step (a
  message delivered in between is newer than the snapshot, so a marker
  before it is still complete).
- `InfoAtCreate`: current servers build the consumer-create response from
  the info taken at creation, before the delivery goroutine starts
  (nats-server `consumer.go:1909-1910`, v2.15.0), so `Delivered.Consumer`
  is 0 there. The KV-2 fix relies on that; with `InfoAtCreate = FALSE`
  (older servers, the response built at any point later) a response that
  already counts a delivered message seeds nothing, and if that message is
  lost a reset still starts at sequence 1, as before. The fixed
  configs set it TRUE, everything else FALSE (as the model was).

## TLC results

Measured on this machine (4 cores shared with other jobs), TLC 2026.10.06,
`-workers auto`, `models/tlc.sh` settings, with the fixes as applied on
`fix/kv`.

| Config | Result | Distinct states | Depth | Time |
|---|---|---:|---:|---:|
| `KVWatch.cfg` (baseline, 4 modes × IgnoreDeletes, MaxSeq 3, History 2) | pass | 4,812,460 | 26 | 183 s |
| `KVWatch_history_faults.cfg` (code before the fixes + KV-8 fix, IncludeHistory, 1 drop + 1 restart, MaxSeq 2) | pass | 433,872 | 28 | 17 s |
| `KVWatch_fixed_lps.cfg` (all fixes, LPS, 1 drop) | pass | 2,116,668 | 33 | 74 s |
| `KVWatch_fixed_remove.cfg` (all fixes, LPS × IgnoreDeletes, 1 removal) | pass | 3,805,624 | 27 | 79 s |
| `KVWatch_fixed_restart.cfg` (all fixes, New/Resume/All, 1 restart) | pass | 2,927,236 | 29 | 54 s |
| `KVWatch_fixed_keys.cfg` (all fixes, Keys()/History(), 1 ctx cancel) | pass | 5,578,984 | 30 | 150 s |
| `KVWatch_fixed_listkeys.cfg` (all fixes, ListKeys + early Stop, 1 ctx cancel) | pass | 7,066,390 | 37 | 171 s |
| `KVWatch_fixed_stop.cfg` (all fixes, Watch + early Stop, 1 ctx cancel) | pass | 5,065,454 | 30 | 141 s |
| `KV1_stale_reset_bug.cfg` | `NoStaleEntry` violated | 1,403,390 | 16 | 38 s |
| `KV1_keys_deleted_bug.cfg` | `KeysSound` violated | 3,330,161 | 21 | 80 s |
| `KV2_reset_from_one_bug.cfg` | `NoStaleEntry` violated | 59,050 | 13 | 6 s |
| `KV3_marker_stuck_bug.cfg` | `NoStuckInit` violated | 9,093 | 8 | 3 s |
| `KV4_keys_ctx_bug.cfg` | `KeysNoSilentTruncation` violated | 23,262 | 10 | 4 s |
| `KV5_listkeys_leak_bug.cfg` | `NoLeakAfterStop` violated | 82,827 | 18 | 6 s |
| `KV6_keys_stop_leak_bug.cfg` | `NoLeakAfterStop` violated | 629,194 | 19 | 22 s |
| `KV7_closed_handler_bug.cfg` | `NoLeakAfterStop` violated | 1,056 | 9 | 2 s |
| `KV8_pending_err_bug.cfg` | `MarkerComplete` violated | 1,690,788 | 17 | 41 s |

Total for the directory: 16.5 minutes (`models/tlc.sh KVWatch`, wall
clock). The fixed configs explore fewer states than the earlier proposed
fixes did (the KV-3 check and the KV-1 phase now follow the Go code), the
baseline is unchanged.

Ablation (`KVWatch_fixed_lps.cfg`, measured): without `reset` `Safety` is
violated (617,470 distinct states), without `recount` too (1,341,122);
without `marker` it still passes (no removals in that config).

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

Status: **fixed on `fix/kv`** (`1cdd710`); was **confirmed** — `TestKV1ResetDuringInitialPhaseDeliversStaleRevision`
(`[a@1 b@2=b1 c@3 <nil>]`: stale b@2, marker before b@4),
`TestKV1KeysReturnsDeletedKeyAfterReset` (`Keys() = [a b c]`, b deleted),
and found independently by the zizek machine `TestKVWatchModelFaults`
(shrunk: put b, delete b, drop next b, `Keys() = [b]`).

Fix on `fix/kv` (no new API):

- `js.go`: an ordered consumer created with DeliverLastPerSubject tracks
  whether it may still be delivering the last message per subject
  (`jsi.lps`). While it may, a reset recreates it with
  DeliverLastPerSubject and drops what the new consumer sends again up to
  the last delivered stream sequence (`jsi.lpsSkip`, still advancing the
  delivery sequence). The phase ends when an upper bound of what is left
  of the set reaches 0: the num_pending of the first message of each
  consumer, counted down per delivered message and capped by each
  message's own num_pending. Both only over-estimate, so it never ends
  early; it ends late by the messages written between the consumer's
  creation and its first delivery, or by initial values removed before
  delivery, or as soon as a message reports num_pending 0. A create
  response with nothing pending ends it at once. After it, resets resume
  by sequence as before. This is the legacy ordered consumer, so it also
  applies to `js.Subscribe(..., nats.OrderedConsumer(),
  nats.DeliverLastPerSubject())` (`TestKV1LegacyOrderedLastPerSubjectReset`).
- `jetstream/kv.go` and legacy `kv.go`: the watcher re-counts what is
  owed when the consumer changes. On the first message of a recreated
  consumer (consumer token of the reply subject) `initPending = received +
  num_pending`; on the very first message `initPending = max(initPending,
  received + num_pending)`, which also covers a consumer recreated before
  anything was delivered.

Model: fixes `reset` (js.go) and `recount` (kv.go). `KVWatch_fixed_lps.cfg`
passes with both and fails without either (`Safety`: NoStaleEntry without
`reset`, MarkerComplete without `recount`, measured).

Trade-off (also in the model, ghost `snap.collapse`): an LPS restart
collapses updates written during the initial phase to their latest
revision, as LPS already does for writes made before the watch.

Residual (two faults at once, not in the fixed configs): a reset in the
initial phase after which every remaining initial value is removed without
a new message. The new consumer then delivers only messages that are
dropped as already delivered, so the KV-3 check never sees its delivery
sequence handled, and the marker waits for the next write.

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

Status: **fixed on `fix/kv`** (`a143322`); was **confirmed, no test hooks needed** —
`TestKV2UpdatesOnlyReplaysHistoryAfterServerRestart` (embedded server
restart, ~15 s: delivers `[a@1 b@2 c@3]`, want `[c@3]`); fast variants
`TestKV2UpdatesOnlyReplaysHistoryAfterLostFirstUpdate`,
`TestKV2ResumeFromRevisionReplaysOlderRevisions`. This is an ordered
consumer (`js.go`) issue; by code reading it affects every legacy ordered
consumer created with DeliverNew / a start sequence / a start time that is
reset before its first message, not only KV.

Fix on `fix/kv`, as proposed: `js.subscribe` seeds `jsi.sseq` with the
create response's `Delivered.Stream` when its `Delivered.Consumer` is 0
(current servers, see `InfoAtCreate`). For every legacy ordered consumer a
reset before the first message then resumes where the consumer started:
DeliverNew → the last sequence at creation + 1, ByStartSequence(r) → r,
ByStartTime → the sequence found for that time, DeliverLast → the last
message, DeliverAll → the first sequence, DeliverLastPerSubject → the
first sequence of the skip list (it resets with KV-1's fix anyway). Older
servers that build the response after delivery started are unchanged
(the first message seeds it, as before).

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

Status: **fixed on `fix/kv`** (`88fb7fc`); was **confirmed** — `TestKV3MarkerNeverSentWhenPendingInitialValuesAreRemoved`:
280 keys, the reader is slow so flow control stalls the server with 12
values undelivered; those are purged; 268 entries arrive and no marker.

Fix on `fix/kv` (`jetstream/kv.go`; legacy `kv.go` keeps its
`initDoneTimer`): while the initial phase is open, a timer
(`kvWatchInitCheck`, 2 s, restarted by every message) runs
`watcher.checkInit`: `sub.ConsumerInfo()` and `sub.Pending()`, then, under
`w.mu`, the marker if NumPending is 0, nothing is queued, and
`Delivered.Consumer` is 0 or equals the delivery sequence of the last
message the callback handled from that same consumer. Otherwise it checks
again later. Cost: one ConsumerInfo request per 2 s while a watcher waits
for initial values without receiving any.

The `Delivered.Consumer` comparison matters: TLC showed that a check on
`NumPending == 0` alone fires while a lost message is still undetected
(marker before the lost initial value is redelivered). "Nothing queued"
keeps it from firing while entries of a replaced consumer still wait for
the callback (`Delivered.Consumer` of the new one is then 0). A plain timer like legacy's would also fire
early while flow control stalls a slow reader.

### KV-4 — Keys()/History() return a truncated result with no error when the context ends (Medium)

`Keys()` and `History()` range over `Updates()` until the marker. When their
context is cancelled or times out, `js.go:2068-2073` unsubscribes, the
closed handler closes `w.updates`, the `range` ends normally and the
function returns the keys collected so far with a nil error (or
`ErrNoKeysFound` / `ErrKeyNotFound` when none arrived). `ListKeys` closes
`Keys()` the same way, indistinguishable from a complete listing.

Trace (`KV4_keys_ctx_bug.cfg`, 11 states).

Status: **fixed on `fix/kv`** (`c46d166`) for `Keys()` and `History()`; was **confirmed** — `TestKV4KeysContextCancelSilentlyTruncates`: 50
keys, cancel after 25 are read → `Keys()` returns 25 keys, nil.

Fix on `fix/kv` (both packages): when `Updates()` is closed before the
marker, `Keys()` and `History()` return the context's error, else
`ErrConnectionClosed` if the connection is closed, else
`ErrConsumerDeleted`. **Behavior change:** they used to return the partial
result with a nil error, or `ErrNoKeysFound` / `ErrKeyNotFound`. In the
legacy package the context is the one given with `nats.Context`.

Not fixed: `ListKeys` / `ListKeysFiltered` in `jetstream` still just close
`Keys()`; reporting it needs an API addition (an `Error()` on
`jetstream.KeyLister`, like `KeyValueLister` has), left to the
maintainers.

### KV-5 — KeyLister.Stop() leaks the lister goroutine and the watcher callback (Medium)

`KeyLister` documents `Stop()` for stopping before all keys were read, but
the lister goroutine sends with a bare `kl.keys <- entry.Key()`
(`kv.go:1449`, `:1480`). Once the caller stops reading, it blocks forever;
`Keys()` is never closed and the watcher callback stays blocked on the full
`w.updates` holding `w.mu`. Stopping the watcher does not help (it can only
take effect between callbacks), and `ctx.Done()` is only selected on the
receive side.

Trace (`KV5_listkeys_leak_bug.cfg`).

Status: **fixed on `fix/kv`** (`461e8ca`); was **confirmed** — `TestKV5ListKeysStopLeaksGoroutines`: 1000 keys,
read one, `Stop()`: `ListKeys.func1` and `WatchFiltered.func1` are still
blocked 3 s later.

Fix on `fix/kv` (both packages): `KeyLister.Stop()` closes a `stop`
channel that the lister goroutine selects on next to its send (and, in
`jetstream`, next to `ctx.Done()`); with KV-6's fix the callback blocked on
`w.updates` gives up, so `Updates()` and `Keys()` are closed.
`TestKV5ListKeysStopLeaksGoroutines` checks both goroutines are gone.

### KV-6 — Keys()/ListKeys()/History() can leak the watcher callback after Stop (Low)

These helpers stop reading `Updates()` at the marker and then `Stop()` the
watcher. If more than 256 updates are delivered between the marker and the
unsubscribe, the callback blocks on the full `w.updates` while holding
`w.mu`; after `Stop()` nobody reads, so the subscription's `waitForMsgs`
goroutine and the 256 buffered entries leak (the closed handler never
runs). Same for a `Watch` user who calls `Stop()` and stops reading.

Trace (`KV6_keys_stop_leak_bug.cfg`, 17 states, `ChanCap = 1`).

Status: **fixed on `fix/kv`** (`461e8ca`); was **model-only**. A Go reproduction attempt (20 000 keys, 600 updates
queued behind the last initial value) did not hit it: flow control keeps
the server from running ahead, and only ~10 updates arrived in the ~1 ms
between the marker and `Stop()`. It needs a sustained write rate well above
what flow control lets through in that window, so it is real but unlikely.

Fix on `fix/kv` (both packages): every send of the update callback (and
of the marker, also from the legacy `initDoneTimer` and the KV-3 check)
selects on a `stopped` channel closed by `Stop()` before it unsubscribes,
and on the watch context (which unsubscribes too). Model: fix
`stopdrain`.

### KV-7 — Updates() is never closed if the context ends right after Subscribe returns (Low)

`js.subscribe` starts the goroutine that unsubscribes on `ctx.Done()`
(`js.go:2068-2073`) before `WatchFiltered` installs the closed handler
(`kv.go:1364`). If the context ends in between, `waitForMsgs` exits with
`pDone == nil` and nobody ever closes `w.updates`: a `for range
w.Updates()` blocks forever.

Trace (`KV7_closed_handler_bug.cfg`, 9 states).

Status: **fixed on `fix/kv`** (`461e8ca`); was **model-only** (the window is a few instructions; not reproduced).

Fix on `fix/kv` (both packages), as proposed: after the closed handler is
installed (legacy: `sub.pDone`, under `sub.mu`), if the subscription is
already closed, `Updates()` (and legacy `Error()`) is closed there; a
`closed` flag under `w.mu` makes the close happen once and stops later
sends. Checked as fix `closedh` in `KVWatch_fixed_stop.cfg` and
`KVWatch_fixed_keys.cfg`.

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

Status: **fixed on `fix/kv`** (`69365b6`); was **model-only** (needs a reset within the first round trip).

Fix on `fix/kv`, as proposed: `InitialConsumerPending` checks only
`sub.jsi == nil` (`jsi.pending` is set once, at creation). KV-1's re-count
on the first message covers the first flavor as well.
`KV8_pending_err_bug.cfg` keeps `Fixes = {"reset"}` (the `js.go` half of
KV-1 only) and still reproduces it.

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
| `TestKVWatchModelFaults` (`internal_testing`) | the same plus `dropNext` (NetDrop via `AddMsgFilter`); found KV-1/KV-2, passes since the fixes (40 examples, about 5 minutes: a lost last message is noticed at the next 5 s heartbeat) |
| `TestKV1*`, `TestKV2*`, `TestKV3*`, `TestKV4*`, `TestKV5*` | deterministic regression tests from the counterexamples (failed before the fixes) |
| `TestKV1LegacyOrderedLastPerSubjectReset` (`internal_testing`) | KV-1's fix for a plain legacy ordered consumer with DeliverLastPerSubject: no replay during the initial set, resume by sequence after it |
| `TestKV9*` | nats-server reproducer, `harness.KnownBug` |

```
cd modeltest
go test -race ./kvwatch/...                                   # passes, KV-9 skipped
go test -race -tags=internal_testing ./kvwatch/...            # same, plus the fault machine
MODELTEST_KNOWN_BUGS=1 go test -race -tags=internal_testing ./kvwatch/...  # KV-9 fails
```

zizek note: `RunSteps` uses swarm-style rule subsets and a per-step stop
probability, so a 30-step bound gives ~10-30 applied rules per example; the
test logs rule counts (about 2 800 applications for 100 examples).
