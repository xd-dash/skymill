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
