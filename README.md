# skymill

Skymill is the durable-stream sibling to Logma.

- **Skymill / Watermill Redis Streams**: durable delivery, consumer groups, ACK/NACK, retry/reclaim, replay.
- **Logma / Redis Pub/Sub**: ephemeral low-latency hints and fan-out.
- **Fatline**: placement, Redis ownership, authority/bindings, tenancy, ACL/rate policy, and lifecycle.

The core invariant is that the stream is authoritative. Logma hints may wake or accelerate consumers, but losing a hint must never lose work.

## Boundary

Skymill intentionally depends on Watermill's message and Redis Stream implementation instead of recreating ready/processing queues, processing leases, or stale-worker reclaim logic.

Applications provide an already configured `redis.UniversalClient`. This keeps Redis placement, credentials, ACLs, DB selection, and connection lifecycle outside Skymill and lets Fatline construct the client idiomatically.

`Binding` carries authority-relevant identity:

```go
skymill.Binding{
    Org:         "xd-dash",
    Tenant:      "probot-runtime",
    Application: "github-webhooks",
    Stream:      "github.webhooks",
}
```

An optional `Authorizer` is called before publish/subscribe. Skymill does not invent an authentication system; Fatline or an application adapter supplies policy.

## GitHub / Probot target

```text
GitHub webhook
      |
      v
Probot ingress
  HMAC verification
  GitHub delivery GUID idempotency
      |
      v
Skymill Publish
      |
      v
Redis Stream (authoritative)
      |
      +------> Logma hint: stream.activity
      |
      v
consumer group: probot-runtime
      |
      v
Probot.receive()
      |
      v
ACK
```

GitHub App/installation records and the GitHub delivery GUID reconciliation index remain Probot-domain state. The old `deliveries:ready`, `deliveries:processing`, claim Lua, processing lease, and requeue bookkeeping do not: Redis Streams/Watermill own those mechanics.

For long-running qualification, a consumer may durably correlate the stream delivery with a Smoke/Huram qualification. A Logma completion hint can wake a controller, but the controller must verify authoritative completion state before ACKing.

## Provider direction

The public application boundary should remain Watermill-compatible. Redis Streams is the first provider, not a permanent requirement of application code. Future Skymill providers can use the same Watermill Publisher/Subscriber model while Fatline binds only providers whose capabilities satisfy the requested durable-stream class.
