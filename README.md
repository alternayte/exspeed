# Exspeed

A lightweight streaming platform built in Rust. One binary, zero partitions, ordered logs, SQL queries over streams.

## Table of Contents

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [ExQL (SQL Engine)](#exql-sql-engine)
- [Connectors](#connectors)
- [HTTP API Reference](#http-api-reference)
- [Configuration](#configuration)
- [Docker Deployment](#docker-deployment)
- [Architecture](#architecture)
- [Building from Source](#building-from-source)

## Key Features

- **Ordered streams** — one stream = one ordered log, no partitions
- **Binary protocol** (TCP port 5933) and **HTTP API** (port 8080)
- **Push delivery** with ACK/NACK, dead-letter queues, consumer groups
- **ExQL SQL engine** — bounded queries, continuous queries, materialized views, tumbling windows, stream-stream joins, joins with external databases (Postgres, MySQL)
- **Connectors** — HTTP webhook, HTTP sink, HTTP poller, Postgres outbox, Postgres CDC, JDBC sink, S3 sink, RabbitMQ source/sink
- **Retention policies** — time-based (default 7 days) and size-based (default 10 GB per stream)
- **Subject filtering** and key-based indexing with SEEK
- **Prometheus metrics** at `/metrics`
- **Hot-reload** connector configs from `connectors.d/` directory
- **Single binary** — `exspeed server` runs everything

## Quick Start

```bash
# Build
cargo build --release

# Start the server
./target/release/exspeed server

# In another terminal:
exspeed create orders
exspeed pub orders '{"total": 99, "region": "eu"}' --subject order.eu.created --key ord-1
exspeed pub orders '{"total": 42, "region": "us"}' --subject order.us.created --key ord-2
exspeed tail orders --last 5
exspeed query "SELECT payload->>'region' AS region, COUNT(*) FROM orders GROUP BY payload->>'region'"
```

## CLI Reference

The binary is `exspeed`. All commands accept `--server <url>` (default `http://localhost:8080`, or set `EXSPEED_URL`) and `--json` for JSON output.

### Server

```bash
# Start with defaults (TCP 0.0.0.0:5933, HTTP 0.0.0.0:8080, data in ./exspeed-data)
exspeed server

# Custom bind addresses and data directory
exspeed server --bind 0.0.0.0:5933 --api-bind 0.0.0.0:8080 --data-dir /var/lib/exspeed
```

### Stream Management

```bash
# Create a stream with default retention (7 days, 10 GB)
exspeed create orders

# Create with custom retention
exspeed create events --retention 30d --max-size 50gb

# List all streams
exspeed streams

# Show stream details (offsets, storage size, retention config)
exspeed info orders

# Delete a stream
exspeed delete orders
```

### Publishing

```bash
# Publish a record
exspeed pub orders '{"total": 99}'

# Publish with subject and key
exspeed pub orders '{"total": 99, "region": "eu"}' --subject order.eu.created --key ord-1
```

### Tailing

```bash
# Tail the last 10 records
exspeed tail orders --last 10

# Follow new records in real time
exspeed tail orders

# Tail without following
exspeed tail orders --no-follow

# Filter by subject
exspeed tail orders --subject order.eu.created

# Read from the beginning
exspeed tail orders --from-beginning
```

### Consumers

```bash
# List all consumers
exspeed consumers

# Show consumer details (offset, lag, group, subject filter)
exspeed consumer-info my-consumer
```

### Queries (ExQL)

```bash
# Run a bounded (batch) SQL query
exspeed query "SELECT * FROM orders LIMIT 10"

# Run a continuous query (stays running, writes results to an output stream)
exspeed query --continuous "SELECT payload->>'region' AS region, COUNT(*) FROM orders GROUP BY payload->>'region'"
```

### Views

```bash
# List all materialized views
exspeed views

# Show view details
exspeed view region_counts
```

### Connectors

```bash
# List running connectors
exspeed connectors

# Validate a connector config file
exspeed connector validate path/to/connector.toml

# Validate and test connectivity
exspeed connector dry-run path/to/connector.toml
```

## ExQL (SQL Engine)

ExQL lets you query streams using SQL. Streams are tables where each row has `offset`, `timestamp`, `subject`, `key`, `payload` (JSON), and `headers`.

### Bounded Queries

Standard SQL executed against the current contents of a stream:

```sql
-- Count records per region
SELECT payload->>'region' AS region, COUNT(*)
FROM orders
GROUP BY payload->>'region'

-- Filter by subject and time
SELECT payload, timestamp
FROM orders
WHERE subject = 'order.eu.created'
  AND timestamp > now() - INTERVAL '1 hour'

-- Aggregations
SELECT
  payload->>'region' AS region,
  SUM((payload->>'total')::numeric) AS revenue,
  COUNT(*) AS order_count
FROM orders
GROUP BY payload->>'region'
ORDER BY revenue DESC
```

### Continuous Queries

Continuous queries run indefinitely, processing new records as they arrive and writing results to an output stream:

```sql
-- Real-time order count per region (results written to a new stream)
SELECT payload->>'region' AS region, COUNT(*)
FROM orders
GROUP BY payload->>'region'
EMIT CHANGES
```

```bash
# Start via CLI
exspeed query --continuous "SELECT payload->>'region', COUNT(*) FROM orders GROUP BY 1 EMIT CHANGES"

# Start via HTTP API
curl -X POST http://localhost:8080/api/v1/queries/continuous \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT payload->>'\''region'\'', COUNT(*) FROM orders GROUP BY 1 EMIT CHANGES"}'
```

### Materialized Views

Materialized views maintain an always-up-to-date table from a continuous query:

```bash
# Create via HTTP API
curl -X POST http://localhost:8080/api/v1/views \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE MATERIALIZED VIEW region_counts AS SELECT payload->>'\''region'\'' AS region, COUNT(*) AS cnt FROM orders GROUP BY 1"}'

# Query the view
curl http://localhost:8080/api/v1/views/region_counts

# Lookup a single key
curl "http://localhost:8080/api/v1/views/region_counts?key=eu"
```

### Tumbling Windows

```sql
SELECT
  TUMBLE(timestamp, INTERVAL '5 minutes') AS window,
  payload->>'region' AS region,
  COUNT(*) AS cnt
FROM orders
GROUP BY window, region
EMIT CHANGES
```

### Stream-Stream Joins

```sql
SELECT o.payload, p.payload
FROM orders o
JOIN payments p ON o.key = p.key
  AND p.timestamp BETWEEN o.timestamp AND o.timestamp + INTERVAL '10 minutes'
EMIT CHANGES
```

### Joins with External Databases

Register an external database connection, then join stream data against it:

```bash
# Register a Postgres connection
curl -X POST http://localhost:8080/api/v1/connections \
  -H 'Content-Type: application/json' \
  -d '{"name": "warehouse", "driver": "postgres", "url": "postgresql://user:pass@host:5432/db"}'

# Join stream with external table
exspeed query "SELECT o.payload, c.name FROM orders o JOIN warehouse.customers c ON o.payload->>'customer_id' = c.id::text"
```

```bash
# List connections
curl http://localhost:8080/api/v1/connections

# Remove a connection
curl -X DELETE http://localhost:8080/api/v1/connections/warehouse
```

## Connectors

Connectors move data between Exspeed and external systems. Configure them as TOML files in `<data-dir>/connectors.d/` for hot-reload, or create them via the HTTP API.

### HTTP Webhook (Source)

Receives HTTP POST requests and writes them to a stream:

```toml
# connectors.d/stripe-webhook.toml
name = "stripe-webhook"
connector_type = "source"
plugin = "http_webhook"
stream = "stripe_events"

[settings]
path = "/webhooks/stripe"
auth_token = "${STRIPE_WEBHOOK_SECRET}"  # env var substitution
```

Once configured, POST to `http://localhost:8080/webhooks/stripe` and records land in the `stripe_events` stream.

### HTTP Sink

Forwards stream records to an HTTP endpoint:

```toml
# connectors.d/notify-service.toml
name = "notify-service"
connector_type = "sink"
plugin = "http_sink"
stream = "notifications"

[settings]
url = "https://api.example.com/notify"
method = "POST"
headers = { Authorization = "Bearer ${API_TOKEN}" }

# Optional: transform the payload before sending
transform_sql = "SELECT payload->>'message' AS body, payload->>'channel' AS channel"
```

### HTTP Poller (Source)

Polls an HTTP endpoint at a regular interval:

```toml
# connectors.d/weather-poller.toml
name = "weather-poller"
connector_type = "source"
plugin = "http_poll"
stream = "weather_data"

[settings]
url = "https://api.weather.com/v1/current"
interval_secs = 60
headers = { X-API-Key = "${WEATHER_API_KEY}" }
```

### Postgres Outbox (Source)

Polls an outbox table and writes new rows to a stream:

```toml
# connectors.d/pg-outbox.toml
name = "pg-outbox"
connector_type = "source"
plugin = "postgres_outbox"
stream = "domain_events"

[settings]
connection_url = "postgresql://user:pass@localhost:5432/myapp"
table = "outbox"
poll_interval_secs = 1
```

### Postgres CDC (Source)

Captures changes from Postgres logical replication:

```toml
# connectors.d/pg-cdc.toml
name = "pg-cdc"
connector_type = "source"
plugin = "postgres_cdc"
stream = "users_cdc"

[settings]
connection_url = "postgresql://user:pass@localhost:5432/myapp"
slot_name = "exspeed_users"
publication = "users_pub"
```

### JDBC Sink

Writes stream records to a database table:

```toml
# connectors.d/jdbc-sink.toml
name = "analytics-sink"
connector_type = "sink"
plugin = "jdbc"
stream = "analytics_events"

[settings]
connection_url = "postgresql://user:pass@localhost:5432/analytics"
table = "events"
```

### S3 Sink

Writes stream records to S3 (or MinIO) in batches:

```toml
# connectors.d/s3-archive.toml
name = "s3-archive"
connector_type = "sink"
plugin = "s3"
stream = "orders"

[settings]
bucket = "order-archive"
region = "us-east-1"
endpoint = "http://minio:9000"          # for MinIO
access_key = "${AWS_ACCESS_KEY_ID}"
secret_key = "${AWS_SECRET_ACCESS_KEY}"
prefix = "orders/"
```

### RabbitMQ Source

Consumes messages from a RabbitMQ queue:

```toml
# connectors.d/rabbitmq-source.toml
name = "rmq-ingest"
connector_type = "source"
plugin = "rabbitmq"
stream = "incoming_messages"

[settings]
url = "amqp://guest:guest@localhost:5672"
queue = "my-queue"
```

### RabbitMQ Sink

Publishes stream records to a RabbitMQ exchange:

```toml
# connectors.d/rabbitmq-sink.toml
name = "rmq-publish"
connector_type = "sink"
plugin = "rabbitmq"
stream = "outgoing_messages"

[settings]
url = "amqp://guest:guest@localhost:5672"
exchange = "my-exchange"
routing_key = "events"
```

### Validating Connectors

```bash
# Check config syntax, plugin, stream name, transform SQL
exspeed connector validate connectors.d/stripe-webhook.toml

# Validate + attempt a real connection
exspeed connector dry-run connectors.d/pg-outbox.toml
```

## HTTP API Reference

Base URL: `http://localhost:8080`

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |

### Streams

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/streams` | List all streams |
| `POST` | `/api/v1/streams` | Create a stream |
| `GET` | `/api/v1/streams/{name}` | Get stream details |
| `POST` | `/api/v1/streams/{name}/publish` | Publish a record |

```bash
# Create a stream
curl -X POST http://localhost:8080/api/v1/streams \
  -H 'Content-Type: application/json' \
  -d '{"name": "orders", "max_age_secs": 604800, "max_bytes": 10737418240}'

# List streams
curl http://localhost:8080/api/v1/streams

# Get stream info
curl http://localhost:8080/api/v1/streams/orders

# Publish a record
curl -X POST http://localhost:8080/api/v1/streams/orders/publish \
  -H 'Content-Type: application/json' \
  -d '{"subject": "order.eu.created", "key": "ord-1", "data": {"total": 99, "region": "eu"}}'
```

### Consumers

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/consumers` | List all consumers |
| `GET` | `/api/v1/consumers/{name}` | Get consumer details (offset, lag) |
| `DELETE` | `/api/v1/consumers/{name}` | Delete a consumer |

```bash
curl http://localhost:8080/api/v1/consumers
curl http://localhost:8080/api/v1/consumers/my-consumer
curl -X DELETE http://localhost:8080/api/v1/consumers/my-consumer
```

### Connectors

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/connectors` | List all connectors |
| `POST` | `/api/v1/connectors` | Create a connector |
| `GET` | `/api/v1/connectors/{name}` | Get connector status |
| `DELETE` | `/api/v1/connectors/{name}` | Delete a connector |
| `POST` | `/api/v1/connectors/{name}/restart` | Restart a connector |

```bash
# Create a connector via API (same fields as TOML)
curl -X POST http://localhost:8080/api/v1/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "my-webhook",
    "connector_type": "source",
    "plugin": "http_webhook",
    "stream": "events",
    "settings": {"path": "/webhooks/my-webhook"}
  }'

# List connectors
curl http://localhost:8080/api/v1/connectors

# Restart a connector
curl -X POST http://localhost:8080/api/v1/connectors/my-webhook/restart

# Delete a connector
curl -X DELETE http://localhost:8080/api/v1/connectors/my-webhook
```

### Queries

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/queries` | Execute a bounded SQL query |
| `GET` | `/api/v1/queries` | List continuous queries |
| `POST` | `/api/v1/queries/continuous` | Create a continuous query |
| `GET` | `/api/v1/queries/{id}` | Get query details |
| `DELETE` | `/api/v1/queries/{id}` | Delete a continuous query |

```bash
# Bounded query
curl -X POST http://localhost:8080/api/v1/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT payload->>'\''region'\'' AS region, COUNT(*) FROM orders GROUP BY 1"}'

# Response:
# {"columns": ["region", "count"], "rows": [["eu", 42], ["us", 17]], "row_count": 2, "execution_time_ms": 12}

# Create continuous query
curl -X POST http://localhost:8080/api/v1/queries/continuous \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT payload->>'\''region'\'', COUNT(*) FROM orders GROUP BY 1 EMIT CHANGES"}'

# List continuous queries
curl http://localhost:8080/api/v1/queries

# Delete a continuous query
curl -X DELETE http://localhost:8080/api/v1/queries/q-abc123
```

### Materialized Views

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/views` | List all materialized views |
| `POST` | `/api/v1/views` | Create a materialized view |
| `GET` | `/api/v1/views/{name}` | Get view rows (all or `?key=<k>`) |

```bash
# Create materialized view
curl -X POST http://localhost:8080/api/v1/views \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE MATERIALIZED VIEW region_counts AS SELECT payload->>'\''region'\'' AS region, COUNT(*) AS cnt FROM orders GROUP BY 1"}'

# Get all rows
curl http://localhost:8080/api/v1/views/region_counts

# Get a single row by key
curl "http://localhost:8080/api/v1/views/region_counts?key=eu"
```

### External Database Connections

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/connections` | List connections |
| `POST` | `/api/v1/connections` | Add a connection |
| `DELETE` | `/api/v1/connections/{name}` | Remove a connection |

```bash
curl -X POST http://localhost:8080/api/v1/connections \
  -H 'Content-Type: application/json' \
  -d '{"name": "warehouse", "driver": "postgres", "url": "postgresql://user:pass@host:5432/db"}'

curl http://localhost:8080/api/v1/connections
curl -X DELETE http://localhost:8080/api/v1/connections/warehouse
```

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks/{path}` | Ingest via HTTP webhook connector |

```bash
curl -X POST http://localhost:8080/webhooks/stripe \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer whsec_...' \
  -d '{"type": "payment_intent.succeeded", "data": {"amount": 2000}}'
```

### Metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metrics` | Prometheus metrics |

```bash
curl http://localhost:8080/metrics
```

Exposes: uptime, storage bytes per stream, consumer lag per stream/consumer, active connections, and more.

## Configuration

### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `0.0.0.0:5933` | TCP protocol bind address |
| `--api-bind` | `0.0.0.0:8080` | HTTP API bind address |
| `--data-dir` | `./exspeed-data` | Persistent data directory |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `EXSPEED_URL` | Server URL for CLI commands (default `http://localhost:8080`) |
| `RUST_LOG` | Log level filter (e.g. `info`, `debug`, `exspeed=debug`) |

### Retention Defaults

| Setting | Default | Description |
|---------|---------|-------------|
| `--retention` | `7d` | Maximum age of records in a stream |
| `--max-size` | `10gb` | Maximum storage size per stream |

Retention is enforced by a background task that periodically scans streams and removes expired segments.

### Connector Environment Variable Substitution

Connector TOML configs support `${ENV_VAR}` syntax for secrets:

```toml
[settings]
connection_url = "${DATABASE_URL}"
auth_token = "${WEBHOOK_SECRET}"
```

## Docker Deployment

### Build the Image

```bash
docker build -t exspeed .
```

### Run with Docker Compose

The included `docker-compose.yml` starts Exspeed alongside Postgres, RabbitMQ, and MinIO:

```bash
docker compose up -d
```

Services:

| Service | Ports | Purpose |
|---------|-------|---------|
| `exspeed` | `5933`, `8080` | Exspeed server |
| `postgres` | `5432` | For CDC, outbox, JDBC connectors |
| `rabbitmq` | `5672`, `15672` | RabbitMQ source/sink connectors |
| `minio` | `9000`, `9001` | S3-compatible object storage |

```bash
# Create a stream
curl -X POST http://localhost:8080/api/v1/streams \
  -H 'Content-Type: application/json' \
  -d '{"name": "orders"}'

# Publish
curl -X POST http://localhost:8080/api/v1/streams/orders/publish \
  -H 'Content-Type: application/json' \
  -d '{"subject": "order.created", "data": {"total": 99}}'
```

### Run Standalone

```bash
docker run -d \
  --name exspeed \
  -p 5933:5933 \
  -p 8080:8080 \
  -v exspeed-data:/var/lib/exspeed \
  exspeed
```

## Architecture

Exspeed is a Cargo workspace with 9 crates:

```
crates/
  exspeed/              Main binary — CLI parsing, server entry point
  exspeed-common/       Shared types (StreamName, Metrics)
  exspeed-protocol/     Binary TCP protocol — codec, frame format, message types
  exspeed-streams/      StorageEngine trait — append, read, create/delete streams
  exspeed-storage/      File-based storage implementation — segment files, indexes, retention
  exspeed-broker/       Consumer state, push delivery, ACK/NACK, DLQ, consumer groups
  exspeed-api/          HTTP API (Axum) — REST endpoints, webhook ingestion
  exspeed-connectors/   Connector framework — source/sink plugins, TOML config, hot-reload
  exspeed-processing/   ExQL engine — SQL parsing, bounded/continuous queries, materialized views
```

### Data Flow

```
Producers ──TCP/HTTP──> exspeed-broker ──> exspeed-storage (append-only log)
                              │
                              ├──> Push delivery to consumers (TCP)
                              ├──> exspeed-connectors (sinks: HTTP, S3, JDBC, RabbitMQ)
                              └──> exspeed-processing (ExQL continuous queries, materialized views)

External sources ──> exspeed-connectors (sources: webhook, CDC, outbox, poller, RabbitMQ)
                              │
                              └──> exspeed-storage
```

### Storage Layout

```
exspeed-data/
  streams/
    orders/
      config.json           Stream config (retention, max_bytes)
      00000000000000000000/  Segment directory
        data.log             Append-only record log
        index.dat            Offset index
      00000000000000001000/
        ...
  consumers/
    my-consumer.json        Consumer state (offset, group, filter)
  connectors.d/
    stripe-webhook.toml     Connector config (hot-reloaded)
  queries/
    ...                     Continuous query state
  views/
    ...                     Materialized view state
```

## Building from Source

Requirements:
- Rust 1.94+ (stable)

```bash
git clone https://github.com/your-org/exspeed.git
cd exspeed

# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Run the server
./target/release/exspeed server
```

The binary is at `target/release/exspeed`. Copy it anywhere — it's a single static binary (dynamically links glibc).
