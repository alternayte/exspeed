# @exspeed/sdk

TypeScript client SDK for Exspeed's binary wire protocol. Works with Node.js (>=18) and Bun.

## Install

```bash
npm install @exspeed/sdk
```

## Quick Start

```ts
import { ExspeedClient } from "@exspeed/sdk";

const client = new ExspeedClient({
  host: "localhost",
  port: 5933,
  clientId: "my-service",
});

await client.connect();

// Create a stream
await client.createStream("orders", { maxAgeSecs: 86400 });

// Publish (JSON auto-serialized)
const { offset } = await client.publish("orders", {
  subject: "orders.created",
  data: { orderId: 123, total: 49.99 },
  key: "customer-456",
});

// Create a consumer
await client.createConsumer({
  name: "order-processor",
  stream: "orders",
  startFrom: "earliest",
});

// Subscribe and consume
const sub = await client.subscribe("order-processor");

for await (const msg of sub) {
  console.log(msg.subject, msg.json());
  await msg.ack();
}

await client.close();
```

## API

### `new ExspeedClient(options)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `clientId` | `string` | required | Unique client identifier |
| `host` | `string` | `"localhost"` | Server host |
| `port` | `number` | `5933` | Server port |
| `auth` | `{ type: "token", token: string }` | none | Authentication |
| `reconnect` | `boolean` | `true` | Auto-reconnect on disconnect |
| `requestTimeout` | `number` | `10000` | Request timeout (ms) |

### Publishing

```ts
// JSON (auto-serialized)
await client.publish("stream", { subject: "topic", data: { key: "value" } });

// Raw bytes
await client.publish("stream", { subject: "topic", value: Buffer.from(bytes) });
```

### Subscribing

```ts
const sub = await client.subscribe("consumer-name");

sub.on("error", (err) => console.error(err));
sub.on("slow", () => console.warn("falling behind"));

for await (const msg of sub) {
  msg.json();           // parsed JSON
  msg.raw();            // Buffer
  msg.offset;           // bigint
  msg.subject;          // string
  msg.deliveryAttempt;  // number

  await msg.ack();   // or msg.nack()
}
```

If a consumer falls behind retention (its stored offset is below the stream's earliest surviving offset), the server terminates the subscription rather than silently jumping forward. The SDK surfaces this as a subscription-closed event — re-seek explicitly (e.g. `seek` to `"earliest"` or a business-meaningful time) before re-subscribing. Auto-reconnecting with the same offset will just hit the same error.

### Batch Fetch

```ts
const records = await client.fetch("stream", { offset: 0n, maxRecords: 100 });
for (const r of records) {
  console.log(r.json(), r.offset);
}
```

### Other Operations

```ts
await client.createStream("name", { maxAgeSecs: 86400 });
await client.createConsumer({ name: "c", stream: "s", startFrom: "earliest" });
await client.deleteConsumer("c");
const { offset } = await client.seek("consumer", { timestamp: BigInt(Date.now()) });
const latencyMs = await client.ping();
await client.close();
```

## Events

```ts
client.on("connected", () => {});
client.on("disconnected", ({ error }) => {});
client.on("reconnecting", ({ attempt, delay }) => {});
client.on("reconnected", ({ attempt }) => {});
client.on("close", ({ error }) => {});
```

### Idempotent publish

Pass a `msgId` to make a publish safe to retry. If the same `msgId` arrives
twice with the same body, the second call succeeds with `duplicate: true`
and returns the original offset. If the same `msgId` arrives with a
*different* body, the broker rejects the publish with `KeyCollisionError` —
treat this as a bug.

**Retry-safe app publish** (most common):

```ts
import { ExspeedClient, newMsgId } from '@exspeed/sdk'

const client = new ExspeedClient({ host: 'localhost', clientId: 'my-service' })
const msgId = newMsgId()

const result = await client.publish('orders', { subject: 'orders.created', data: order, msgId })
console.log(result)  // { offset: 42n, duplicate: false }
```

**Outbox / connector-style** (msgId from business data):

```ts
await client.publish('events', {
  subject: `${event.aggregate_type}.${event.event_type}`,
  data: event.payload,
  msgId: event.id,
})
```

**Fire-and-forget** (no idempotency, fastest):

```ts
await client.publish('logs', { subject: 'log.info', value: Buffer.from(logLine) })
```

**Handling errors:**

```ts
import { KeyCollisionError, DedupMapFullError } from '@exspeed/sdk'

try {
  await client.publish('orders', { subject: 'orders.created', data: order, msgId })
} catch (err) {
  if (err instanceof KeyCollisionError) {
    console.error(`BUG: msgId collision at offset ${err.storedOffset}`)
    throw err
  }
  if (err instanceof DedupMapFullError) {
    // Retries exhausted. The broker is overloaded or misconfigured.
    console.error(`dedup map full for ${err.stream}, retries exhausted; hint was ${err.retryAfterSecs}s`)
  }
  throw err
}
```
