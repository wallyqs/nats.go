# Review of commit ada86e76bfffdc46d4a212b6e0eb67a4bcf57cea

**Commit:** `[IMPROVED] Add JetStream API migration guide`
**Author:** Piotr Piotrowski
**File:** `jetstream/MIGRATION.md` (+743 lines)

## Overall Assessment

Well-structured, comprehensive migration guide covering the transition from the
legacy `nats.JetStreamContext` API to the new `jetstream` package. The guide is
well-organized with a clear table of contents, side-by-side legacy/new
comparisons, and practical code examples. The technical content is largely
accurate against the current `jetstream` package on `main`.

**Recommendation: Approve with minor suggestions.**

## Issues

### 1. Object Store Management table is missing listing methods

The Object Store Management Methods table omits `ObjectStoreNames` and
`ObjectStores`, while the equivalent KV table correctly includes
`KeyValueStoreNames` and `KeyValueStores`.

Missing rows:

| Legacy                         | New                                |
|--------------------------------|------------------------------------|
| `js.ObjectStoreNames()`        | `js.ObjectStoreNames(ctx)`         |
| `js.ObjectStores()`            | `js.ObjectStores(ctx)`             |

### 2. Consider mentioning `CreateOrUpdateStream` more prominently

`CreateOrUpdateStream` is only briefly noted as "Also: `CreateOrUpdateStream()`"
in the Stream Management table. Given that `CreateOrUpdateConsumer` gets
substantial coverage and its own examples, `CreateOrUpdateStream` might warrant
more visibility since it's commonly used in practice.

### 3. Missing mention of `Consumer.Next()` method

The guide covers `Consume()`, `Messages()`, `Fetch()`, `FetchNoWait()`, and
`FetchBytes()`, but the `Consumer` interface also has a standalone `Next()` method
for fetching a single message. This is a useful convenience method that could be
documented briefly.

### 4. `PublishAsyncComplete()` behavioral differences not noted

The guide shows `<-js.PublishAsyncComplete()` in both legacy and new examples
without noting whether there are any behavioral differences (e.g., the new
`WithPublishAsyncTimeout` option).

### 5. Minor: Consume/Messages options are types, not functions

The "Consume/Messages Options" table lists options like `PullMaxMessages(n)`,
`StopAfter(n)` in function-call syntax. Several of these are actually type
conversions (e.g., `type PullMaxMessages int`), not function calls. This works
identically from the user's perspective, so this is a very minor nit.

## Verified API Accuracy

Cross-referenced the guide against the actual `jetstream` package source:

| Guide Claim | Verified |
|---|---|
| `jetstream.New/NewWithDomain/NewWithAPIPrefix` constructors | Yes |
| `CreateStream/UpdateStream/CreateOrUpdateStream/DeleteStream` | Yes |
| `Stream.Purge/GetMsg/GetLastMsgForSubject/DeleteMsg/SecureDeleteMsg` | Yes |
| `CreateConsumer/UpdateConsumer/CreateOrUpdateConsumer` | Yes |
| `CreatePushConsumer/UpdatePushConsumer/CreateOrUpdatePushConsumer` | Yes |
| Publish options (`WithMsgID`, `WithExpectStream`, etc.) | Yes |
| `DoubleAck(ctx)` replaces legacy `AckSync()` | Yes |
| `NakWithDelay(dur)` and `TermWithReason(reason)` on `Msg` | Yes |
| Error variables (`ErrConsumerDeleted`, `ErrNoHeartbeat`, etc.) | Yes |
| `WithMessagesErrOnMissingHeartbeat` option | Yes |
| KV methods (`CreateKeyValue/UpdateKeyValue/CreateOrUpdateKeyValue`) | Yes |
| ObjectStore methods (`CreateObjectStore/UpdateObjectStore/CreateOrUpdateObjectStore`) | Yes |
| `ConsumerConfig` field names (`MemoryStorage`, `Replicas`, `InactiveThreshold`) | Yes |
| `FetchMaxWait`, `FetchNoWait`, `FetchBytes` | Yes |
| `OrderedConsumer` with `OrderedConsumerConfig` | Yes |
| DeliverPolicy constant names match between packages | Yes |
| Default AckPolicy change (AckNone -> AckExplicit) | Yes |

## What's Done Well

- Clear table of contents and logical progression from initialization through
  streams, consumers, publishing, consuming, KV, and object store
- "Why Migrate?" section effectively communicates the value proposition
- Consuming Messages section thoroughly covers every legacy pattern (Subscribe,
  SubscribeSync, QueueSubscribe, ChanSubscribe, PullSubscribe)
- Error handling section clearly distinguishes terminal vs. recoverable errors
- Subscription Options Mapping table is comprehensive
- Correctly calls out the ack policy default change and consumer cleanup semantics
- Push consumer coverage includes the correct separate methods and DeliverGroup
  for queue semantics
