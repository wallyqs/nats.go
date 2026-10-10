# TLA+ models of nats.go

Executable TLA+ specifications of the concurrency-heavy parts of this client,
and the tests derived from them. The point is to find bugs, not to document
the design: each model is written at the granularity of the real code
(goroutines, mutexes, channel operations with their real buffer capacities,
`select` nondeterminism, atomic flags, timers), its actions are mapped to
`file.go:line`, and every violation TLC reports was either traced back to a
modeling mistake and fixed, or confirmed against the real library by a Go test
in [`../modeltest`](../modeltest).

| Model | Code under test | Confirmed by test | Model-only |
|-------|-----------------|-------------------|------------|
| [`PullConsume`](PullConsume) | `jetstream/pull.go` — `Consume`, `Messages`, `Fetch` | 7 (PULL-1…5, 7, 9) | 3 |
| [`OrderedConsumer`](OrderedConsumer) | `jetstream/ordered.go` | 10 (ORD-1…8, 12, 13) | 3 |
| [`PublishAsync`](PublishAsync) | `jetstream/publish.go`, legacy `js.go` | 9 (PUB-1…8, PUB-L1) | — |
| [`ConnLifecycle`](ConnLifecycle) | `nats.go` — reconnect, close, drain, flush, auto-unsubscribe, status listeners | 11 (CONN-1…11) | — |
| [`KVWatch`](KVWatch) | `jetstream/kv.go` watchers and key listing, legacy `js.go` ordered push consumer | 6 (KV-1…5, KV-9 is a nats-server bug) | 3 |
| [`RequestReply`](RequestReply) | `nats.go`, `context.go` — request/reply mux | 3 (REQ-1…3; REQ-3 only with a scripted server) | — |

Each model's README has the action→code mapping, the invariants, the
abstractions, measured TLC results, and for every finding its trace, its
status and a proposed fix as a diff.

## Fix status

The fixes live on `claude/magical-lovelace-my0imq-fixes`, which merges one
branch per area (`fix/conn`, `fix/pull`, `fix/pub`, `fix/ordered`,
`fix/kv`). On it every confirmed reproducer is a regression test that runs
by default, and each model has passing configs with its fixes switched on
next to the `*_bug.cfg` configs that keep reproducing the old code.

| Area | Fixed | Not fixed |
|------|-------|-----------|
| PullConsume | PULL-1…10, O-1 | — |
| OrderedConsumer | ORD-1…9, 11, 12, 13 | ORD-10 (a lost last message is only noticed on the next one; design limitation) |
| PublishAsync | PUB-1…8, PUB-L1 | legacy `js.go` siblings of PUB-3/4/5/8 |
| ConnLifecycle | CONN-1…11 | — |
| KVWatch | KV-1…8 | KV-9 (nats-server); `ListKeys` still ends without an error on context end (needs a `KeyLister` API change) |
| RequestReply | REQ-1, REQ-2 | REQ-3 (the single-entry reply fallback; a maintainer decision) |

The behavior changes these fixes make are listed in each model's README.

## Most important findings

Deadlocks and hangs:

- **PUB-1** — the async-publish ack-timeout callback runs the user's error
  handler under the publisher lock; a handler that publishes or calls
  `PublishAsyncPending` deadlocks (`jetstream/publish.go`, legacy `js.go`).
- **ORD-1** — ordered `Fetch`/`FetchBytes` return
  `ErrOrderedConsumerConcurrentRequests` without unlocking the consumer;
  every later call on it blocks forever (`jetstream/ordered.go:421`).
- **ORD-3** — the ordered consumer's `ConsumeErrHandler` runs under the
  consumer mutex; calling `Stop()`/`Info()` from it deadlocks.
- **PULL-1** — `Stop`/`Drain` racing a refill, reconnect or missed heartbeat
  leaves a goroutine blocked on `fetchNext` holding `sub.Lock`: `Consume`
  leaks and `Closed()` never fires; `Messages().Next` hangs during `Drain`.
- **ORD-7** — closing the connection while an ordered consumer is resetting
  makes the reset retry forever; `Next` never returns.
- **KV-3** — purging or expiring not-yet-delivered initial values means a
  watcher's "initial values done" marker never arrives, so `Keys()` and
  `History()` block until their context ends.

Lost or wrong data:

- **CONN-5** — a failed write of the reconnect buffer is not treated as a
  disconnect: reconnect stalls on the old socket, and a later PONG makes a
  `Flush` succeed for data that was discarded.
- **KV-1 / KV-2** — the legacy ordered push consumer's reset drops
  last-per-subject delivery (stale revisions, deleted keys in `Keys()`), or
  restarts from sequence 1 (`UpdatesOnly` watchers replay history).
- **ORD-2** — `FetchNoWait` on an ordered consumer never advances the
  cursor, so the next fetch re-delivers the same messages.
- **ORD-4** — after a reset, the ordered `Consume` handler can run for two
  messages at once and complete them out of order.
- **PULL-5** — `Messages()` re-pulls a full batch after a reconnect into a
  buffer that still holds messages; the overflow is dropped as a slow
  consumer.
- **CONN-3 / CONN-4** — subscriptions drained or auto-unsubscribed across a
  reconnect keep their interest on the server.
- **REQ-2** — unsubscribing a reply's `Msg.Sub` (the shared request mux)
  breaks every later request on the connection.

Reconnect handling:

- **CONN-7** — `ForceReconnect` during a reconnect disables the back-off for
  the rest of that reconnect; `MaxReconnects` is spent in milliseconds.
- **PUB-7 = CONN-1 = PULL-2** — `sendStatusEvent` decides whether a listener
  channel is closed by *receiving* from it, which swallows an unread event
  and unregisters a live listener. Every `StatusChanged` user is exposed;
  pull `Consume` stops pulling after a later reconnect, and the async
  publisher stops reacting to reconnects.

## Running

```shell
models/tlc.sh                    # every config of every model
models/tlc.sh PullConsume        # one model
modeltest/run.sh                 # the model-derived Go tests (-race)
MODELTEST_KNOWN_BUGS=1 modeltest/run.sh   # plus the bug reproducers
```

`tlc.sh` downloads `tla2tools.jar` into `models/.tools/` on first use (or set
`TLA2TOOLS`). Configs named `*_bug.cfg` are counterexample reproducers: they
encode the code as it is today and TLC must report a violation for them; the
script fails if it does not. Every other config must pass.

## How the tests relate to the models

The tests in `modeltest/` live in a nested Go module so that the root module
gains no dependencies. They run nats.go against an embedded nats-server through
a TCP proxy (`modeltest/harness`), which turns the models' environment actions
— connection cut, server restart, consumer deleted, message dropped — into
operations a test can perform.

Two kinds of test come out of each model:

- **Stateful property tests** with [zizek](https://github.com/wallyqs/zizek)
  (`zizek.Machine`): the model's actions become rules, its invariants become
  machine invariants checked against the real client after every step, and
  zizek's shrinking reduces a failing run to the few steps that matter.
- **Reproducers** for each confirmed finding. They assert the correct
  behavior, so they fail until the bug is fixed; `harness.KnownBug` skips them
  unless `MODELTEST_KNOWN_BUGS=1`.
