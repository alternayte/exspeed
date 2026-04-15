# Order Processing: E-Commerce Order Pipeline

A full-featured demo showing Exspeed as the backbone of an event-driven order processing system.

## What This Demonstrates

- **SDK publishing** — Hono API server publishes order events via the TypeScript SDK
- **Consumer workers** — background process consumes and processes orders in real time
- **Postgres outbox pattern** — connector config that captures database changes via logical replication
- **Sink connectors** — high-value order notifications forwarded to an HTTP endpoint
- **ExQL queries** — ad-hoc SQL queries and materialized views over streaming data

## Architecture

```
                                +-------------------+
  curl/client                   |   Exspeed Server  |
      |                         |                   |
      v                         |  order-events     |
  +----------+  SDK publish     |  stream           |
  | Hono API | --------------> |  [0] [1] [2] ...  | -----> consumer.ts
  | :3000    |                  |                   |        (order-processor)
  +----------+                  |                   |
                                |  ExQL engine      | -----> dashboard.ts
  +----------+  pg outbox       |  (materialized    |        (order_stats view)
  | Postgres | - - - - - - - > |   views + queries) |
  | :5432    |  connector       |                   |
  +----------+                  |  HTTP sink        | -----> httpbin.org
                                |  (high-value)     |        (notifications)
                                +-------------------+
```

## Prerequisites

- **Docker** and **Docker Compose**
- **Bun** (https://bun.sh)

## Running

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts the Exspeed server and Postgres with the order schema pre-loaded.

### 2. Install dependencies

```bash
bun install
```

### 3. Start the API server

```bash
bun run start
```

### 4. Start the consumer (in another terminal)

```bash
bun run consumer
```

### 5. Create some orders

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": "cust-1", "total": 99.99, "region": "eu"}'

curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": "cust-2", "total": 750.00, "region": "us"}'

curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": "cust-3", "total": 24.50, "region": "eu"}'
```

You should see each order logged by the consumer in the other terminal.

### 6. View the dashboard

```bash
bun run dashboard
```

This creates a materialized view and queries order stats grouped by region.

## Connectors

### pg-outbox (source)

Captures rows from the `outbox_events` table in Postgres via logical replication. In a production setup you would write to both the `orders` table and the `outbox_events` table in a single transaction, guaranteeing exactly-once event publishing. See `db/init.sql` for the schema and `exspeed/connectors.d/pg-outbox.toml` for the connector config.

### high-value-notify (sink)

Forwards order events where `total > 500` to an HTTP endpoint. In this demo it posts to httpbin.org so you can see the payload. Replace the URL with your own webhook in production. See `exspeed/connectors.d/high-value-notify.toml`.

## ExQL Queries to Try

Once you have some orders in the stream, you can query them via the Exspeed HTTP API:

```bash
# Count orders by region
curl -X POST http://localhost:8080/api/v1/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT payload->>'\''region'\'' AS region, COUNT(*) FROM \"order-events\" GROUP BY region"}'

# Find high-value orders
curl -X POST http://localhost:8080/api/v1/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM \"order-events\" WHERE (payload->>'\''total'\'')::DECIMAL > 500"}'

# Latest 5 orders
curl -X POST http://localhost:8080/api/v1/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM \"order-events\" ORDER BY offset DESC LIMIT 5"}'
```
