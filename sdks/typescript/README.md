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
