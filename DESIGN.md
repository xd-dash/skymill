# Skymill design invariants

This document records correctness boundaries that should be preserved across providers and service transports.

## Authority

1. The durable stream is authoritative for accepted work.
2. A successful publish response means the stream append is durable according to the configured Redis durability policy.
3. Logma hints are best-effort observations only. Hint failure never changes the result of Publish, ACK, or NACK.
4. ACK/NACK follows processing state; hints never cause ACK/NACK without checking authoritative state.

## Delivery

5. Delivery is at-least-once. Skymill does not claim exactly-once side effects.
6. Source idempotency is explicit and scoped by org, tenant, application, and stream.
7. Watermill message identity is not a substitute for source idempotency.
8. Consumer recovery belongs to the stream provider. Applications must not recreate ready/processing queues or leases.

## Retention

9. Stream retention and source-idempotency retention are separate policies.
10. An idempotency record must not outlive its referenced stream entry if callers expect duplicate responses to return a replayable entry.
11. A source reconciliation index may have a different retention horizon from both.

## Service boundary

12. Redis credentials stay inside the Fatline/Skymill placement boundary.
13. Authentication establishes caller identity; Skymill authorization checks the requested binding/action.
14. Consume authority is group-scoped. A caller must not choose arbitrary streams/groups merely by changing request JSON.
15. Delivery tokens are capabilities owned by the service instance unless/until they are made durable. Losing an instance must leave the underlying Watermill message pending so provider recovery can reclaim it.

## Long-running work

16. Do not keep an HTTP delivery token as the sole correlation for a long-running qualification.
17. Persist application correlation before relinquishing the worker.
18. A Logma completion hint wakes a controller; the controller verifies durable completion state before ACK.

## Operations

19. Redis persistence/replication policy is part of the durability contract and must be visible in Fatline placement.
20. Backlog, pending count/age, reclaim count, ACK/NACK rate, hint failures, and reconciliation lag should be observable.
21. Poison messages need an explicit bounded retry/dead-letter policy; infinite NACK loops are not an acceptable default.


## Hardened generic contract

The generic contract now has five explicit planes:

```text
authority   Binding + Authorizer / HTTP Grant
delivery    Watermill Redis Stream + source PublishOnce
workflow    durable Correlation + Settle
policy      RetentionPolicy + RetryPolicy + WorkflowPolicy
signals     HintPublisher (Logma adapter)
operations  Metrics + Pending()
```

A source application should not create its own delivery queue, retry lease, workflow token store, or stream-selection authorization. Source-specific state such as GitHub App installations and GitHub delivery reconciliation remains outside Skymill.

### Retry/dead letter

Bounded retry is configured with `RetryPolicy.MaxDeliveries` and a distinct `DeadLetterStream`. `PrepareDelivery` is the generic pre-dispatch gate. When the limit is exceeded it idempotently appends the message to the DLQ and tells the consumer to ACK the source message. DLQ publication must succeed before the source can be ACKed.

### Workflow settlement

`Correlate` durably records a long-running workflow identity before the short-lived delivery worker is released. `Settle` is an idempotent pending-to-terminal transition and emits only a best-effort `stream.workflow.settled` hint afterward. Application-specific result state remains authoritative for whether a workflow should be settled.

### Metrics

The exporter-neutral `Metrics` interface currently records publish acceptance/duplicates, ACK/NACK, dead-letter transitions, workflow correlation, and workflow settlement. `Pending()` exposes consumer-group pending count/range/consumer count for reconciliation and scraping.


## Generalization pass

The service no longer tries to compare count-based `MaxLen` with time-based idempotency TTL. `RetentionMode` makes the guarantee explicit:

- `operational`: count trimming and TTL are independent operational bounds; Fatline observes/SLOs the relationship.
- `replay-safe`: Skymill refuses count-based `MaxLen`, so it cannot itself trim an entry while retaining an idempotency pointer to it. Fatline must ensure external Redis retention obeys the same contract.

Retry authority is now Redis' PEL delivery count (`XPENDING`), not mutable Watermill metadata. This count survives process death and claiming. `PrepareDelivery(streamEntryID, message)` evaluates the configured policy from that durable state.

Long-running workflow settlement no longer requires a process-local HTTP delivery token. Correlation records carry the stream entry ID and consumer group; `SettleAndAck` atomically transitions the workflow correlation to a terminal state and `XACK`s the PEL entry. It is safe to retry after an ambiguous HTTP response.

`Status()` is the generic Fatline lifecycle/reconciliation surface: stream length, pending range/count, consumer count, and oldest pending idle age. The HTTP service exposes status and workflow correlation/settlement without exposing Redis credentials or accepting caller-selected bindings.


## Stable authorization and delivery vocabulary

Skymill now has one authorization vocabulary across its Go and HTTP boundaries: `ActionPublish`, `ActionConsume`, `ActionSettle`, and `ActionInspect`. `Authorizer.Authorize(ctx, AuthorizationRequest)` receives the immutable Binding and consumer group. HTTP grants reuse the same actions rather than maintaining a parallel operation enum. This is the intended Fatline compiled-ACL adapter point.

`Delivery` is the provider-neutral envelope for durable consumer identity. Its `ProviderDeliveryID` is opaque outside the provider adapter. Redis Streams maps it to the XID; future providers may map their own durable delivery identity without changing application contracts. Retry policy consumes this envelope/state rather than Redis-specific IDs in application APIs.

The remaining Redis-specific code is therefore an adapter concern: PEL inspection, XACK, XADD, and stream/group status. Those details should not migrate into Probot or other applications.


## Durable provider boundary

`Stream` no longer owns Redis PEL/XACK/XADD/XLEN/XPENDING mechanics. Those are behind one coherent internal `DurableProvider` boundary:

```text
Publish / PublishOnce / Subscribe
DeliveryState
DeadLetter
Ack
Status
Create/Get/SettleCorrelation
Close
```

The Redis Streams adapter is the first implementation and may use Watermill plus go-redis internally. Skymill's policy, authorization, workflow composition, metrics, and Logma hints operate only on provider-neutral `Delivery`, `ProviderStatus`, `PublishResult`, and `Correlation` values.

This boundary is intentionally coarse. New providers should implement the durability semantics as a unit rather than growing one provider-specific interface method whenever an application discovers another Redis command it needs. Application code must not depend on Redis stream IDs; `ProviderDeliveryID` is opaque.


## Durable provider boundary

The generic Stream layer no longer owns Redis transport mechanics. DurableProvider is the single internal boundary for publish, publish-once, subscription, provider delivery state, dead-letter, ACK, status, and durable workflow correlation storage. The Redis Streams adapter owns Watermill construction plus XADD/XACK/XPENDING/XLEN/XINFO and Redis correlation hashes.

Application-visible delivery identity is ProviderDeliveryID. Redis XIDs are one adapter representation, not part of the Skymill contract.

Contract tests use a fake DurableProvider to exercise bounded-retry/DLQ failure, settlement after the original worker is gone, repeated terminal settlement with ACK retry, and provider-neutral status. Provider-specific integration tests should separately exercise Redis crash/reclaim behavior.


## Durable provider boundary

The generic Stream layer no longer owns Redis PEL/XACK/XADD/XLEN/XPENDING mechanics. `DurableProvider` is the single internal boundary for durable transport operations: publish, publish-once, subscribe, delivery state, dead-letter, ACK, status, correlation persistence, and atomic settle+ACK.

The Redis Streams adapter implements that contract with Watermill and go-redis. Provider delivery identity remains opaque to Stream and applications.

Atomicity is intentionally part of the provider contract: `SettleAndAck` must make workflow terminal state and transport acknowledgement one retry-safe provider operation. For Redis Streams this is one Lua operation over the correlation hash and XACK, avoiding the earlier failure window between terminal settlement and ACK.

Tests at the generic layer use a fake DurableProvider and verify durable-attempt policy, DLQ failure behavior, and settlement delegation without importing Redis. Provider-specific crash/reclaim integration tests belong with the Redis adapter.


## Delivery identity qualification

The provider boundary now carries `Delivery` envelopes through subscription, not bare Watermill messages. This is required for durable workflow semantics: Watermill Redis Stream messages expose the Watermill UUID but the upstream subscriber keeps the Redis XID inside its message handler for ACK/NACK and does not attach that XID to the returned `message.Message`.

Accordingly, the Redis adapter uses Watermill's message type, wire marshaller, and publisher, while Skymill owns the consumer-group read/claim loop that must preserve the provider delivery identity. Redis-specific XREADGROUP/XPENDING/XCLAIM remains entirely inside the adapter. Applications see only `ProviderDeliveryID`, `Attempt`, and the Watermill message.

A NACK is defined generically as "not acknowledged; eligible for provider redelivery". On the baseline Redis adapter it leaves the entry in the PEL and reclaim performs the later delivery. This avoids requiring Redis-version-specific immediate-release commands in the generic contract.

The Redis integration qualification now covers duplicate publish-once, durable PEL attempt count across XCLAIM, DLQ failure without source ACK, repeatable settle-and-ACK after the original worker disappears, and subscription delivery identity surviving consumer cancellation/reclaim.


## Redis keyspace atomicity

Redis provider keys are derived from the authority Binding and share one Redis Cluster hash tag. The physical source stream, idempotency keys, workflow correlations, and DLQ streams for a binding therefore occupy the same hash slot. This is required because publish-once and settle-and-ACK use multi-key Lua operations; using the caller's logical stream name directly would work on standalone Redis but fail with CROSSSLOT on Redis Cluster.

`Binding.Stream` is consequently a logical Skymill stream name. Redis physical key layout is an adapter detail and must not leak into applications or Fatline ACL rules.


## Contract freeze

The Skymill durable-provider contract is frozen at qualification commit `88b8b9ce3f2d9031cb7fa7f9771b9d4d3c0dbac8` (qualification run 35556700862).

The frozen application-facing vocabulary is:
- immutable `Binding` plus action-based `AuthorizationRequest`;
- provider-neutral `Delivery` with opaque `ProviderDeliveryID`;
- `DurableProvider` as the internal durability boundary;
- durable correlation state behind that provider boundary;
- provider-atomic `SettleAndAck`;
- durable retry attempts derived from provider state;
- explicit operational versus replay-safe retention modes;
- Logma hints as best-effort observations, never correctness authority.

Changes to these semantics require a deliberate contract revision and renewed provider qualification. Provider implementations, operational tuning, additional metrics, and application adapters may evolve without expanding this vocabulary.

Qualification at the frozen commit passed from a clean checkout with Redis: module resolution, generic contract tests, Redis durability/crash-reclaim tests, and the committed module-graph reproducibility gate.
