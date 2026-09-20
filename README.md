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


## Service boundary

`httpapi.Server` exposes Skymill to non-Go runtimes while keeping Watermill and Redis credentials inside the Fatline-hosted service:

```text
POST /v1/messages
POST /v1/deliveries/receive
POST /v1/deliveries/ack
POST /v1/deliveries/nack
```

The server accepts an optional `Authenticator`, so a Fatline deployment can authenticate service/capability credentials before Skymill invokes its binding authorization. Redis credentials never need to be given to Probot.

Durable source ingress uses `Stream.PublishOnce`. It atomically checks an idempotency key and appends the Watermill-compatible Redis Stream entry in one Redis Lua operation. The resulting Redis Stream entry ID is stored against that idempotency key. Retrying the same source delivery therefore returns the original entry rather than appending another message.

For GitHub, the idempotency key and Watermill message UUID are both the GitHub delivery GUID. This closes the failure window that would exist if Probot independently wrote a GUID hash and then called a remote stream publisher.
