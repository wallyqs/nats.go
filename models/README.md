# TLA+ models of nats.go

Executable TLA+ specifications of the concurrency-heavy parts of this client,
and the tests derived from them. The point is to find bugs, not to document
the design: each model is written at the granularity of the real code
(goroutines, mutexes, channel operations with their real buffer capacities,
`select` nondeterminism, atomic flags, timers), its actions are mapped to
`file.go:line`, and every violation TLC reports was either traced back to a
modeling mistake and fixed, or confirmed against the real library by a Go test
in [`../modeltest`](../modeltest).

| Model | Code under test | Findings |
|-------|-----------------|----------|
| [`PullConsume`](PullConsume) | `jetstream/pull.go` — `Consume`, `Messages`, `Fetch` | see README |
| [`OrderedConsumer`](OrderedConsumer) | `jetstream/ordered.go` | see README |
| [`PublishAsync`](PublishAsync) | `jetstream/publish.go` | see README |
| [`ConnLifecycle`](ConnLifecycle) | `nats.go` — reconnect, close, drain, flush, auto-unsubscribe | see README |
| [`KVWatch`](KVWatch) | `jetstream/kv.go` — watchers and key listing | see README |
| [`RequestReply`](RequestReply) | `nats.go`, `context.go` — request/reply mux | see README |

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
