# Getting started

> **Status:** Exspeed is pre-1.0 and not yet production-ready. See
> [REVIEW.md](REVIEW.md) for the known gaps and the plan to close them.

## Install

**Prebuilt binary** (macOS, Linux, Windows; built by cargo-dist for each release):

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/alternayte/exspeed/releases/latest/download/exspeed-installer.sh | sh
```

**Docker:**

```bash
docker run -d --name exspeed \
  -p 5933:5933 -p 8080:8080 \
  -v exspeed-data:/var/lib/exspeed \
  nayth/exspeed:latest
```

**From source** (Rust 1.94+):

```bash
git clone https://github.com/alternayte/exspeed.git
cd exspeed
cargo build --release -p exspeed
./target/release/exspeed server
```

The server listens on:

| Port | Purpose |
|------|---------|
| `5933` | Binary TCP protocol (SDK clients) |
| `8080` | HTTP API, `/metrics`, `/healthz`, `/readyz`, `/webhooks/*` |
| `5934` | Replication (multi-pod mode only) |

Data goes to `./exspeed-data` by default (`--data-dir` to change).

## First stream

```bash
# Create a stream (default retention: 7 days / 10 GB)
exspeed create orders

# Publish a few records. Subjects are dot-delimited; keys are optional.
exspeed pub orders '{"total": 99, "region": "eu"}' --subject order.eu.created --key ord-1
exspeed pub orders '{"total": 42, "region": "us"}' --subject order.us.created --key ord-2

# Read them back
exspeed tail orders --last 5 --no-follow

# Query with SQL
exspeed query "SELECT key, subject, payload->>'region' AS region FROM orders"
```

The same operations over HTTP:

```bash
curl -X POST localhost:8080/api/v1/streams -H 'Content-Type: application/json' \
  -d '{"name": "orders"}'

curl -X POST localhost:8080/api/v1/streams/orders/publish -H 'Content-Type: application/json' \
  -d '{"subject": "order.eu.created", "key": "ord-1", "data": {"total": 99}}'

curl -X POST localhost:8080/api/v1/queries -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM orders LIMIT 10"}'
```

## Consume from an application

Applications use the binary protocol through the TypeScript SDK
([`sdks/typescript`](../sdks/typescript/README.md)):

```ts
import { ExspeedClient } from "@exspeed/sdk";

const client = new ExspeedClient({ host: "localhost", port: 5933, clientId: "worker-1" });
await client.connect();

await client.createConsumer({ name: "order-processor", stream: "orders", startFrom: "earliest" });

for await (const msg of await client.subscribe("order-processor")) {
  console.log(msg.subject, msg.json());
  await msg.ack();
}
```

> ⚠️ Known issue: the current SDK drops batched deliveries (`RecordsBatch`
> frames), so a consumer that starts with a backlog will miss records. See
> [REVIEW.md §3.8](REVIEW.md#38-typescript-sdk).

## Next steps

- [Concepts](concepts.md): streams, subjects, keys, offsets, consumers
- [ExQL](exql.md): SQL over streams
- [Connectors](connectors.md): Postgres CDC, webhooks, JDBC, S3, RabbitMQ, …
- [Examples](../examples): `getting-started` (Bun) and `order-processing`
