# @exspeed/sdk

TypeScript client for [Exspeed](https://github.com/alternayte/exspeed). It
speaks the binary client protocol v2 ([`docs/protocol.md`](../../docs/protocol.md))
over TCP or TLS. Requires Node.js 18 or later.

- One client is one connection. Requests are multiplexed, so a long pull
  never blocks other calls. Share one client across your application.
- Durable consumers with push (credit-windowed subscriptions) or pull
  delivery, acks, redelivery with backoff, and dead-lettering.
- A coalescing publisher for high-throughput, order-preserving writes.
- Automatic reconnection that re-establishes subscriptions.

## Install

```bash
npm install @exspeed/sdk
```

## Quick start

```ts
import { ExspeedClient } from "@exspeed/sdk";

const client = await ExspeedClient.connect({ host: "127.0.0.1", port: 5933 });

await client.createStream({ name: "orders", maxAgeSecs: 7 * 86400 }); // idempotent

const { offset } = await client.publish("orders", {
  subject: "orders.placed",
  value: { id: 42, total: 99.5 }, // objects are JSON-encoded
  key: "customer-7",
});

await client.createConsumer({ name: "billing", stream: "orders", filterSubjects: ["orders.placed"] });

const sub = await client.subscribe("billing", { window: 256 });
for await (const msg of sub) {
  const order = msg.json<{ id: number; total: number }>();
  await charge(order);
  msg.ack();
}

await client.close();
```

## Streams

```ts
await client.createStream({
  name: "orders",
  maxAgeSecs: 86400,       // retention by age
  maxBytes: 10 * 2 ** 30,  // retention by size
  dedupWindowSecs: 300,    // how long msgIds are remembered
  dedupMaxEntries: 1_000_000,
  compaction: false,
});
await client.updateStream({ name: "orders", maxAgeSecs: 3 * 86400 });
await client.streamInfo("orders");  // { name, earliestOffset, nextOffset, records, config, internal }
await client.listStreams();
await client.deleteStream("orders"); // 409 while consumers exist
```

Omitted numeric settings mean "server default". `createStream` is idempotent
when the stream already exists with the same settings, and fails with 409 when
they differ. `updateStream` replaces all settings, so pass every field you
want to keep.

## Publishing

```ts
// One record. `value` may be a Uint8Array (sent as-is), a string (UTF-8)
// or anything JSON-serializable.
await client.publish("orders", {
  subject: "orders.placed",
  value: { id: 1 },
  key: "customer-7",
  headers: { "trace-id": "abc" },
  msgId: newMsgId(), // idempotency key
});

// Several records in one request; one result per record.
const results = await client.publishBatch("orders", [
  { subject: "orders.placed", value: { id: 2 } },
  { subject: "orders.placed", value: { id: 3 } },
]);
```

Each publish resolves with `{ offset, duplicate }`.

**Idempotency.** When a record carries a `msgId`, the server remembers it
for the stream's dedup window. A retry with the same `msgId` and the same
body returns the original offset with `duplicate: true` and writes nothing.
Reusing a `msgId` with a different body is a bug, and fails with 409
(`detail.stored_offset`). `newMsgId()` generates a time-ordered UUIDv7.

### The coalescing publisher

For throughput, use a publisher. It gathers concurrent `publish` calls into
`PublishBatch` requests and keeps many batches in flight. Records reach the
stream in the order you called `publish`, and each call resolves with its own
record's offset.

```ts
const publisher = client.publisher({
  batchWindowMs: 0,      // 0 = batch whatever was published in the same event-loop turn
  maxBatchRecords: 512,  // records per request
  maxInFlight: 4096,     // accepted but unacknowledged records; publish() waits beyond this
});

await Promise.all(events.map((e) => publisher.publish("events", { subject: "events.raw", value: e })));
await publisher.flush(); // wait for everything accepted so far
await publisher.close(); // flush, then reject further publishes
```

If a batch fails, every record in it rejects with the same error.

## Reading without a consumer

```ts
const page = await client.read("orders", {
  from: 0,              // first offset
  maxRecords: 100,
  filter: "orders.eu.>", // NATS-style: `*` = one token, `>` = one or more
  waitMs: 5000,         // long-poll when caught up
});
for (const r of page.records) console.log(r.offset, r.subject, r.json());
// continue with { from: page.nextOffset }
```

Stateless reads keep no server-side state. Use them for replay, tools and
tailing.

## Consumers

A consumer is a named, durable cursor over one stream, kept on the server.
It tracks what has been delivered, what is acknowledged and what is due for
redelivery.

```ts
await client.createConsumer({
  name: "billing",
  stream: "orders",
  filterSubjects: ["orders.placed", "orders.eu.>"], // default: all subjects
  deliver: "all",            // "all" | "new" | { fromOffset } | { fromTime }
  ack: "explicit",           // or "none" (at-most-once)
  ackWaitMs: 30_000,         // redeliver if not acked in time
  maxDeliver: 5,             // then dead-letter (0 = retry forever)
  backoffMs: [1000, 5000, 30_000], // redelivery delays by attempt (the last repeats)
  maxAckPending: 1000,       // pause delivery at this many unacked records
  dlqStream: "orders-dlq",   // where dead letters go (unset = dropped and counted)
  ephemeral: false,          // true = deleted when this connection closes
});
```

Every field except `name` and `stream` is optional and defaults on the
server. `createConsumer` is idempotent for an identical spec and fails with
409 if the consumer exists with a different one. Also available:
`consumerInfo(name)`, `listConsumers(stream?)`, `deleteConsumer(name)` and
`seek(name, target)`:

```ts
await client.seek("billing", "earliest");          // or { earliest: true }
await client.seek("billing", "latest");
await client.seek("billing", { offset: 1000 });
await client.seek("billing", { timeMs: Date.parse("2026-10-01") }); // or a Date
```

### Push: subscriptions

```ts
const sub = await client.subscribe("billing", { window: 256 });

for await (const msg of sub) {
  // msg.offset, msg.timestamp (ms), msg.timestampNs (bigint), msg.deliveryCount, msg.subject,
  // msg.key, msg.value (Buffer), msg.headers, msg.json(), msg.text(), msg.header(name)
  msg.ack();
}
console.log(sub.endReason); // { code, message }
```

The server pushes at most `window` records ahead of your code. The SDK
returns credit as you take messages from the iterator (in batches of half the
window), so a slow handler slows delivery down instead of filling memory.

A subscription ends when:

- you call `sub.unsubscribe()` or `break` out of the `for await` loop
  (`endReason.code === 0`);
- the consumer or its stream is deleted (`404`);
- the node loses leadership (`503`, see [Errors](#errors));
- the client closes (`0`), or the connection is lost and not re-established
  (`503`).

When a subscription ends, records delivered to it and not acked are
redelivered, including any it had buffered that your code never saw.

`sub.next({ timeoutMs })` is an alternative to `for await`. It resolves with
the next message, or `null` on timeout or once the subscription has ended.

### Pull

```ts
const msgs = await client.pull("billing", { maxMessages: 100, expiresMs: 5000 });
for (const m of msgs) {
  await handle(m);
  m.ack();
}
```

`pull` long-polls for up to `expiresMs` and resolves with whatever is
available, or `[]` on timeout. It suits batch jobs and request-driven work.
Push suits steady streams.

### Work sharing

Any number of subscriptions and pullers, on any connection and in any
process, can share one consumer. Each record goes to exactly one of them at a
time, so to scale out, run more instances against the same consumer name:

```ts
// in every instance of the billing service
await client.createConsumer({ name: "billing", stream: "orders" }); // idempotent
for await (const msg of await client.subscribe("billing")) { ... }
```

For fan-out, where every reader sees every record, give each reader its own
consumer.

### Acks, redelivery and dead letters

| Call | Effect |
|------|--------|
| `msg.ack()` | Done. Fire-and-forget: no round trip; acks made in the same event-loop turn share one frame. |
| `await msg.nack(delayMs?)` | Redeliver after `delayMs`, or after the consumer's `backoffMs` when omitted. |
| `await msg.term(reason)` | Never redeliver: dead-letter now. |
| `await msg.inProgress()` | Still working: reset the ack deadline. |
| `await client.ack(consumer, offsets)` | Ack several offsets and wait for confirmation. |

An unacked record is redelivered when its `ackWaitMs` deadline passes, when
it is nacked, or when the subscription it went to ends or disconnects.
`msg.deliveryCount` is 1 on the first delivery and goes up on each
redelivery. After `maxDeliver` deliveries, or on `term`, the record goes to
`dlqStream` with these headers: `exspeed-dlq-origin`, `exspeed-dlq-stream`,
`exspeed-dlq-original-offset`, `exspeed-dlq-deliveries` and
`exspeed-dlq-reason`.

Delivery is **at-least-once**, so make handlers idempotent. Because
`msg.ack()` doesn't wait, an ack the server rejects surfaces as the client's
`"error"` event. An ack made while the connection is down is dropped, and the
record is redelivered.

## Queries and metadata

```ts
const r = await client.query(`SELECT COUNT(*) AS n FROM "orders"`);
// { columns: ["n"], rows: [[123]], rowCount: 1, executionTimeMs: 2 }

await client.metadata(); // { nodeId, isLeader, leader, serverVersion }
await client.ping();     // round-trip time in ms
client.serverInfo;       // { serverVersion, nodeId, leader } from the handshake
```

With auth enabled, `query` needs a global-admin credential, since SQL can
read any stream.

## Errors

All errors extend `ExspeedError`:

| Class | When |
|-------|------|
| `ServerError` | The server rejected the request. It has `code`, `message`, `detail` and `leaderHint`. |
| `ConnectionError` | Not connected: the connection was lost, is being re-established, or the client is closed. |
| `TimeoutError` | No response within `requestTimeoutMs` (plus a pull's or read's own wait). |
| `ProtocolError` | The server sent something this SDK can't decode. |

`ServerError.code` is HTTP-like, and `ErrorCode` names the codes:

| Code | Meaning | `detail` |
|------|---------|----------|
| 400 | Malformed request, invalid name, filter or config | |
| 401 | Not authenticated | |
| 403 | The credential lacks the needed permission | |
| 404 | Stream or consumer not found | |
| 409 | Exists with different settings; stream still has consumers; `msgId` reused with a different body | `{ stored_offset }`, `{ consumers }` |
| 429 | Retry later (dedup map full, too many concurrent waits on one connection) | `{ retry_after_secs }` |
| 500 | Internal error | |
| 503 | Not the leader, or still starting | `{ leader }` |

`detail` is passed through as the server sent it, with snake_case keys. For
503 errors, `err.leaderHint` holds the leader's address when the server knows
it. The SDK does not follow it automatically: connect a client to that
address.

```ts
try {
  await client.publish("orders", record);
} catch (err) {
  if (err instanceof ServerError && err.code === ErrorCode.Unavailable && err.leaderHint) {
    // reconnect to err.leaderHint
  }
  throw err;
}
```

## Reconnection

Reconnection is on by default. When the connection drops:

1. Pending requests reject with `ConnectionError`. They are not retried,
   because a publish may or may not have been applied. Retry publishes with
   a `msgId` to make the retry safe. New requests also fail with
   `ConnectionError` until the connection is back.
2. The client emits `"disconnect"` and reconnects with exponential backoff
   (100 ms doubling to 5 s, with jitter, unlimited attempts by default).
3. Once reconnected, it re-creates the ephemeral consumers it created
   (the server deleted them with the old connection), then re-subscribes
   every live subscription with its original window. Your `for await` loop
   keeps running and doesn't notice the gap. Records that were buffered but
   not yet handed to your code are discarded, and the server redelivers
   them, along with anything delivered but not acked. A re-subscribe that
   fails (for example, the consumer was deleted meanwhile) ends that
   subscription with the server's error code.
4. The client emits `"reconnect"`.

If the server rejects the credential (401/403) or `maxAttempts` runs out,
the client closes: subscriptions end with code 503 and `"close"` is emitted.
The first `connect()` is never retried; it rejects straight away.

```ts
const client = await ExspeedClient.connect({
  host: "exspeed.internal",
  reconnect: { maxAttempts: 30, initialDelayMs: 200, maxDelayMs: 10_000 },
  // reconnect: false  -> the client closes when the connection drops
});
client.on("disconnect", (err) => log.warn("exspeed disconnected", err));
client.on("reconnect", (info) => log.info("exspeed reconnected", info));
client.on("close", (err) => log.error("exspeed closed", err));
client.on("error", (err) => log.warn("exspeed async error", err)); // failed acks/credits
```

Durable consumers make this safe: the cursor and the unacked set live on the
server, so nothing is lost and nothing is skipped. An ephemeral consumer
created with `deliver: "new"` does skip records published while it was gone,
because the re-created consumer starts at the end of the stream.

The client pings every `keepaliveMs` (20 s) so the server, which drops
connections idle for 120 s, keeps the connection open. A ping that times out
is treated as a dead connection.

## Clusters

Against a cluster ([High availability](../../docs/high-availability.md)),
give the client some seed addresses. It connects to whichever node is the
leader, following the leader hints followers return. After a failover it
reconnects to the new leader and restores subscriptions as described above.

```ts
const client = await ExspeedClient.connect({
  servers: ["exspeed-0.exspeed:5933", "exspeed-1.exspeed:5933", "exspeed-2.exspeed:5933"],
});
```

Without `servers`, a node that names another node as leader in its handshake
is still followed, so connecting through a Service that routes to any node
works too. A write that reaches a follower anyway fails with `ServerError`
503, and `err.detail.leader` names the leader.

## TLS and authentication

```ts
import { readFileSync } from "node:fs";

const client = await ExspeedClient.connect({
  host: "exspeed.example.com",
  port: 5933,
  token: process.env.EXSPEED_TOKEN, // the server's --auth-token, or a credential token
  tls: true,                        // verify against the system CAs
  // tls: { ca: readFileSync("ca.pem") }               // private CA
  // tls: { ca, cert, key }                            // mutual TLS
  // tls: { servername: "exspeed.internal" }           // SNI / certificate name override
});
```

`tls` takes `true` or any Node
[`tls.connect` options](https://nodejs.org/api/tls.html#tlsconnectoptions-callback).
Certificates are verified by default. A wrong or missing token fails
`connect()` with `ServerError` 401. A token without permission for an
operation gets 403. With scoped credentials, `listStreams` and
`listConsumers` return only what the credential can see. See
[Security](../../docs/security.md).

## Options

| Option | Default | |
|--------|---------|---|
| `host` | `"127.0.0.1"` | |
| `port` | `5933` | |
| `servers` | none | Cluster seeds (`"host:port"`); connects to the leader. Overrides `host`/`port`. |
| `token` | none | Bearer token |
| `tls` | off | `true` or `tls.connect` options |
| `clientId` | `"exspeed-ts"` | Shown in server logs |
| `requestTimeoutMs` | `30000` | Per request, on top of a pull's or read's own wait. Also bounds connecting. |
| `keepaliveMs` | `20000` | `0` disables pings |
| `reconnect` | `true` | `false`, or `{ maxAttempts, initialDelayMs, maxDelayMs }` |

Numbers on the wire are 64-bit. The SDK represents offsets and timestamps
as JavaScript `number`s, and rejects values above `Number.MAX_SAFE_INTEGER`
(2^53 - 1) rather than rounding them.

## Development

```bash
npm ci
npm run typecheck
npm test          # unit tests, plus e2e tests when a server binary is available
npm run build
```

The end-to-end tests (`test/e2e`) start real servers. They use the binary
named by `EXSPEED_BIN`, or else `target/debug/exspeed` at the repository root
(`cargo build -p exspeed --bin exspeed`). Without one, they are skipped with
a message.
