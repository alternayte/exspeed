# Exspeed

A lightweight streaming platform built in Rust. One binary, zero partitions, ordered logs, SQL queries over streams.

## Table of Contents

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Benchmarks](#benchmarks)
- [CLI Reference](#cli-reference)
- [ExQL (SQL Engine)](#exql-sql-engine)
- [Connectors](#connectors)
- [HTTP API Reference](#http-api-reference)
- [Idempotent publish](#idempotent-publish)
- [Configuration](#configuration)
- [Securing Exspeed](#securing-exspeed)
- [Operations & deployment](#operations--deployment)
- [Multi-pod deployment](#multi-pod-deployment)
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
- **Idempotent publish** — retry-safe publishes with a `msg_id` field (xxhash64 collision detection, per-stream window, snapshot-based fast restart). See [Idempotent publish](#idempotent-publish).
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

## Benchmarks

On a single MacBook (8 vCPU, 16 GB RAM, APFS+NVMe), Exspeed v0.3 sustains:

| Workload                | Sync (default, durable) | Async (opt-in)         |
|-------------------------|-------------------------|------------------------|
| Publish, 1 KB payload   | ~7,406 msg/s            | ~43,382 msg/s          |
| E2E latency @ 10k/s     | p50 33 ms / p99 66 ms   | p50 41 ms / p99 87 ms  |
| Fan-out, 4 consumers    | ~97 msg/s aggregate     | ~90 msg/s aggregate    |

Sync mode beats NATS JetStream's sync mode (~6,500 msg/s) on the same
hardware. Async mode closes to within ~2.4× of NATS's async (~106k msg/s).

Numbers refreshed 2026-04-21 on git `29c50d4`.

> **ExQL note:** The binary-search found no candidate that sustained ≥95% of any target rate above the 5,000 msg/s floor on macOS. This reflects APFS write overhead under the dual publish+query load, not a bug in the query engine.

Full methodology, per-scenario tables, comparison data, and reproduction
steps in [BENCHMARKS.md](BENCHMARKS.md).
A reproducible comparison kit (Kafka included) lives in [`bench/README.md`](bench/README.md).

### Operational tuning

| Flag (env) | Default | Effect |
|---|---|---|
| `--storage-sync` (`EXSPEED_STORAGE_SYNC`) | `sync` | `sync` = group-commit + fsync per batch (durable). `async` = fsync on a timer (faster, may lose ms of data on crash). |
| `--storage-flush-window-us` (`EXSPEED_FLUSH_WINDOW_US`) | `500` | Max time the WAL appender waits to fill a batch in `sync` mode. |
| `--storage-flush-threshold-records` (`EXSPEED_FLUSH_THRESHOLD_RECORDS`) | `256` | Max records per batch before forced flush. |
| `--storage-flush-threshold-bytes` (`EXSPEED_FLUSH_THRESHOLD_BYTES`) | `1048576` | Max bytes per batch before forced flush. |
| `--storage-sync-interval-ms` (`EXSPEED_SYNC_INTERVAL_MS`) | `10` | (async mode) Fsync timer interval. |
| `--storage-sync-bytes` (`EXSPEED_SYNC_BYTES`) | `4194304` | (async mode) *Reserved for future use — timer-only in this release.* |
| `--delivery-buffer` (`EXSPEED_DELIVERY_BUFFER`) | `8192` | mpsc buffer per subscription. |

**`--storage-sync=async`** trades durability for throughput. On a crash, you may lose up to `--storage-sync-interval-ms` of acked data. This matches NATS JetStream's default behavior. Keep `sync` (the default) for production unless throughput is critical and you understand the tradeoff.

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
| `GET` | `/healthz` | Leader probe — 200 only if this pod is the cluster leader, else 503. Use for LB traffic routing. |
| `GET` | `/readyz` | Readiness probe — 200 once startup is complete and `data_dir` is writable. Use for k8s readiness gates. |

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

## Idempotent publish

Exspeed supports retry-safe (idempotent) publishes via a `msg_id` field on the TCP `PUBLISH` frame, or via the `x-idempotency-key` header for backward compatibility with existing connectors.

### Semantics

**First-body-wins.** If a client publishes `msg_id=X` with body `A`, a retry with the same `msg_id=X` and the same body `A` receives `PublishOk { duplicate: true, offset: <original> }` — the original offset is returned and no new record is written. If the retry arrives with a *different* body `B` (a likely bug in the caller), the server responds with a `KeyCollision` error that includes the offset of the first write. When the per-stream dedup map is at capacity and an eviction cannot free a slot, the server responds with `DedupMapFull { retry_after_secs }`, which is a retryable condition (try again after the window expires). Messages published without a `msg_id` bypass the dedup engine entirely — they are always written immediately and are unaffected by a full dedup map.

### CLI configuration

```bash
# Create a stream with a 10-minute dedup window and 2M-entry cap
exspeed create orders --dedup-window 10m --dedup-max-entries 2000000

# Update an existing stream's dedup window to 30 minutes
exspeed update-stream orders --dedup-window 30m
```

### Defaults

| Setting | Default | Minimum |
|---------|---------|---------|
| `dedup_window` | `5m` (300 s) | `1 s` (must be ≤ retention) |
| `dedup_max_entries` | `500_000` | `1` |

### Memory and on-disk cost

Each dedup entry stores a `msg_id` string (variable), an offset (8 bytes), a timestamp (8 bytes), and a body hash (8 bytes). At the default 500,000-entry cap with average 32-byte `msg_id` strings the in-memory footprint is approximately **150 MB per stream** worst case. On disk, each stream maintains a `dedup_snapshot.bin` file in its stream directory. The snapshot is written every 60 seconds and on graceful shutdown, and is included in `exspeed snapshot` offline backups.

Storage layout with dedup:

```
exspeed-data/
  streams/
    orders/
      config.json           Stream config (retention, dedup_window, dedup_max_entries)
      dedup_snapshot.bin    Periodic dedup-map snapshot — restored on restart
      00000000000000000000/
        data.log
        index.dat
```

### Prometheus alerts

```yaml
# Any body-collision is a producer bug — alert immediately.
- alert: ExspeedDedupCollision
  expr: rate(exspeed_dedup_collisions_total[5m]) > 0
  for: 1m
  annotations:
    summary: "Exspeed dedup key collision — same msg_id published with different body"

# Sustained cap hits mean the window is too long or throughput exceeds the cap.
- alert: ExspeedDedupMapFull
  expr: rate(exspeed_dedup_map_full_total[5m]) > 0.1
  for: 10m
  annotations:
    summary: "Exspeed dedup map full — increase dedup_max_entries or shorten dedup_window"

# Full-scan rebuilds at startup are expected only when the snapshot is missing.
# A p95 > 30s indicates abnormally large streams or slow storage.
- alert: ExspeedDedupSlowRebuild
  expr: histogram_quantile(0.95, exspeed_dedup_rebuild_duration_seconds_bucket{source="full_scan"}) > 30
  annotations:
    summary: "Exspeed dedup full-scan rebuild took > 30s"
```

### Connector compatibility

Connectors that already set an `x-idempotency-key` header (e.g. the Postgres outbox connector) continue to work unchanged — the same dedup engine processes both the header and the `msg_id` wire field. No connector reconfiguration is required to benefit from idempotent publish.

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
| `LOG_FORMAT` | `text` (default) or `json` — JSON for K8s/Loki/ELK ingestion |
| `EXSPEED_MAX_CONNS` | Cap on concurrent TCP connections (default `1024`); over-cap closes are logged and counted in `exspeed_connections_rejected_total` |

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

## Securing Exspeed

Auth and TLS are both off by default. Turn each on independently via environment
variables.

### Token authentication

```bash
export EXSPEED_AUTH_TOKEN=$(openssl rand -hex 32)
```

When set, every TCP client must include this token in the `Connect` handshake
(`AuthType::Token`) and every HTTP request to `/api/v1/*` must include
`Authorization: Bearer <token>`. The following paths always bypass auth —
they're designed to be reachable by probes, scrapers, and webhook senders:

| Path | Who uses it |
|---|---|
| `GET /healthz` | Liveness probes |
| `GET /readyz` | Readiness probes |
| `GET /metrics` | Prometheus scrape |
| `POST /webhooks/*` | External webhook senders (they carry their own per-webhook auth) |

If you need to authenticate `/metrics` as well, run a reverse-proxy sidecar
that adds Basic auth. Broker-wide bearer tokens aren't a good fit for scrapers.

### Managing credentials (multiple named identities)

`EXSPEED_AUTH_TOKEN` gives one shared admin token. For more than one app
sharing the broker, point `EXSPEED_CREDENTIALS_FILE` at a TOML file with
one entry per app. Each entry stores `sha256(token)` — never the token
itself — and a list of per-stream permissions (`publish`, `subscribe`,
`admin`).

**Generate a credential:**

```bash
$ exspeed auth gen-token
a3f7...                    # stdout: raw token — give this to the app, ONCE
b29c...                    # stderr: sha256(token) — paste into the TOML
```

**Example `credentials.toml`:**

```toml
[[credentials]]
name = "orders-service"
token_sha256 = "<the stderr output from gen-token>"
permissions = [
  { streams = "orders-*", actions = ["publish", "subscribe"] },
]

[[credentials]]
name = "ops-admin"
token_sha256 = "<another sha256>"
permissions = [
  { streams = "*", actions = ["publish", "subscribe", "admin"] },
]
```

**Run the server:**

```bash
EXSPEED_CREDENTIALS_FILE=/etc/exspeed/credentials.toml \
EXSPEED_TLS_CERT=/etc/exspeed/cert.pem \
EXSPEED_TLS_KEY=/etc/exspeed/key.pem \
  exspeed server
```

**Migration from single `EXSPEED_AUTH_TOKEN`:** the env var keeps working
as a synthetic `legacy-admin` identity (full global admin). Add scoped
credentials to the file; migrate one app at a time; unset the env var
when done. You can set both at once — the server registers `legacy-admin`
from the env var plus every entry in the TOML. (Reserved: an entry named
`legacy-admin` while the env var is set refuses to start; rename the
entry or unset the env var.)

**Rotation:** add a new credential, point the client at its new token,
remove the old entry, restart the server. Restart is required —
credentials are loaded once at boot (v1).

**Multi-pod:** each pod reads its own `EXSPEED_CREDENTIALS_FILE`.
Distribute the same file to every pod via a k8s Secret mount, Coolify
file mount, Ansible, etc. Divergent files produce inconsistent authz
across the cluster.

**Verify what your token grants:** `exspeed auth whoami` calls
`GET /api/v1/whoami` and prints the identity + permissions JSON.

**Validate the file offline (CI):** `exspeed auth lint /path/to/credentials.toml`
exits non-zero with a specific error on any parse or validation failure.

### TLS

```bash
export EXSPEED_TLS_CERT=/etc/exspeed/tls/fullchain.pem
export EXSPEED_TLS_KEY=/etc/exspeed/tls/privkey.pem
```

Both variables must be set together, or neither. When set, **both** the TCP
(5933) and HTTP (8080) listeners serve TLS using the same cert/key pair — one
cert, two ports. Make sure the cert's SAN list covers every hostname clients
will use.

TLS uses pure-Rust `rustls`. Default protocol versions: TLS 1.2 and 1.3.

#### Dev certs

For local development, generate a self-signed cert:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout dev-key.pem -out dev-cert.pem
```

Point the server at it:

```bash
EXSPEED_TLS_CERT=dev-cert.pem EXSPEED_TLS_KEY=dev-key.pem \
  exspeed server
```

Clients that don't trust your dev CA need to opt in:

- CLI: `EXSPEED_INSECURE_SKIP_VERIFY=1 exspeed streams`
- TypeScript SDK: `new ExspeedClient({ tls: { rejectUnauthorized: false } })`

### Rotation

Cert and token rotation require a server restart. Combined with Plan A's
graceful shutdown, a restart is a ~10-second blip on a single node and
zero-downtime behind a multi-pod deployment. Live SIGHUP reload is on the
roadmap but not in v1.

### What's not in v1

- mTLS (no client-cert verification)
- SASL / JWT / OAuth2
- Live SIGHUP reload of `credentials.toml` — changes need a restart
- Rate limiting on failed auth attempts — use an ingress WAF or fail2ban

The target deployment model is "trust the network boundary" (VPC, service
mesh, Hetzner private network, k8s namespace) with per-app scoped
credentials on top.

## Operations & deployment

Single-node hardening knobs and behaviors useful in production. Defaults are sensible — most deployments only touch these for K8s integration or capacity tuning.

### Structured logging

```bash
LOG_FORMAT=json   # JSON lines, one event per line — for Loki / ELK / Cloud Logging
LOG_FORMAT=text   # default; human-readable, ANSI-coloured
```

Combine with `RUST_LOG` for level/target filtering. JSON output preserves spans and fields (`request_id`, `stream`, etc.) for downstream querying.

### Connection cap

```bash
EXSPEED_MAX_CONNS=1024   # default
```

Caps concurrent TCP connections to the broker port. When the cap is reached, new connections are accepted-then-immediately-closed; each rejection is logged and increments `exspeed_connections_rejected_total`. Tune by watching that counter alongside `exspeed_active_connections`.

### Exclusive data-dir lock

`exspeed server` takes an exclusive `flock` on `{data_dir}/.exspeed.lock` at startup. A second process pointed at the same `data_dir` fails fast with a clear "already in use" error — no silent dual-writer corruption. The lock is released when the process exits (including crashes; the kernel frees the flock).

Do not delete the lockfile manually to "recover" — it's a TOCTOU footgun and not necessary. If the holding process is gone, the next start succeeds.

### Graceful shutdown

On `SIGTERM` or `SIGINT` the server stops accepting new TCP connections, waits up to **10 seconds** for in-flight connections to drain, then exits. The HTTP listener and background tasks are cancelled in the same window.

For Kubernetes, set `terminationGracePeriodSeconds: 30` (or higher) on the pod so the kubelet doesn't `SIGKILL` the process before the drain completes.

### Consumers vs. retention

If a consumer's offset falls behind the retention window (age or size), the broker returns `StorageError::OffsetOutOfRange` on its next read and **terminates the subscription** — it does not silently jump forward to the first surviving record, which would look like successful consumption of data that was actually lost.

The delivery task logs a warning with `requested` and `earliest` offsets so operators can spot it in log aggregation. To recover, the application must explicitly re-seek — typically to the earliest available offset, or to a business-meaningful point. Naïve auto-reconnect from the lost offset will hit the same error; clients should treat this signal as "your position is gone, choose where to resume."

Operationally: size your retention with your slowest expected consumer in mind. Metrics of interest are `exspeed_consumer_lag_records` and per-stream storage bytes.

### Consumer state durability

Consumer offsets are persisted atomically (tempfile + rename + parent-dir fsync). A crash at any point during a save leaves either the previous offset intact or the new offset fully durable — never a truncated file. If you see stray `*.json.tmp` files in `{data_dir}/consumers/` at startup, they're the remnants of a crashed save and are safely ignored by the loader.

### `/healthz` vs `/readyz`

| Endpoint | Returns 200 when | Recommended use |
|---|---|---|
| `/healthz` | This pod is the cluster leader | LB traffic routing (only the leader serves traffic — see [Multi-pod deployment](#multi-pod-deployment)) |
| `/readyz` | Startup complete **and** `data_dir` is writable | k8s `readinessProbe` and startup gates |

Single-node deployments still benefit from `/readyz` — it stays 503 during WAL replay and connector startup, so an LB or systemd unit knows when the broker is actually serving.

### Non-root container

The published Docker image runs as `uid 1000` (no shell, no home dir). For Kubernetes with a mounted PV:

```yaml
spec:
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000          # so the PV is writable by uid 1000
  containers:
    - name: exspeed
      image: exspeed:latest
      ...
```

Without `fsGroup`, the volume mount may be owned by root and the broker will fail at startup (the `flock` and segment writes both need write access).

## Multi-pod deployment

Exspeed supports **hot-standby multi-pod** via a single cluster-leader
lease. Running N broker pods means N identical pods; exactly one is the
**leader** at any moment and serves all traffic. Standbys are silent
until failover. If the leader crashes, a survivor takes over within the
lease TTL.

### A health-check-aware load balancer is REQUIRED

Standby pods return `503` on every `/api/v1/*` endpoint except
`/api/v1/leases`. Without a probe-aware LB, consumers connecting to a
standby will see 503s on ~(N−1)/N of their requests. **This is a
deployment misconfiguration, not a bug.**

### Requirements

1. **Shared consumer store.** `EXSPEED_CONSUMER_STORE=postgres` or `=redis`.
   The `file` backend does not support multi-pod coordination; the server
   warns loudly on every boot when file-backed.

2. **One `data_dir` per pod.** Plan A's `flock` guarantees exclusive
   access. Multi-pod does NOT mean shared storage — each pod owns its own
   streams. Typical deployment: N identical pods, each with its own PV /
   local disk.

3. **A probe-aware LB in front of both HTTP (8080) and TCP (5933).** k8s
   `Service` + `readinessProbe` handles this natively — only pods passing
   the probe receive traffic on any port.

### k8s deployment (recommended)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: exspeed
spec:
  selector: { app: exspeed }
  ports:
    - name: api
      port: 8080
      targetPort: 8080
    - name: tcp
      port: 5933
      targetPort: 5933
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: exspeed
spec:
  replicas: 2
  selector:
    matchLabels: { app: exspeed }
  serviceName: exspeed
  template:
    metadata:
      labels: { app: exspeed }
    spec:
      containers:
        - name: exspeed
          image: exspeed:latest
          ports:
            - containerPort: 8080
            - containerPort: 5933
          env:
            - name: EXSPEED_CONSUMER_STORE
              value: postgres
            - name: EXSPEED_OFFSET_STORE_POSTGRES_URL
              valueFrom: { secretKeyRef: { name: pg, key: url } }
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            periodSeconds: 5
            failureThreshold: 2
            successThreshold: 1
          volumeMounts:
            - name: data
              mountPath: /var/lib/exspeed
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: { requests: { storage: 10Gi } }
```

### nginx (active health check; nginx Plus or a compatible module)

```nginx
upstream exspeed {
    server exspeed-0:8080;
    server exspeed-1:8080;
    health_check uri=/healthz interval=5s fails=2 passes=1;
}
```

### HAProxy

```
backend exspeed
    option httpchk GET /healthz
    http-check expect status 200
    default-server check inter 5s fall 2 rise 1
    server pod0 exspeed-0:8080
    server pod1 exspeed-1:8080
```

### Failover timing

| Scenario | Time to failover |
|---|---|
| Leader crashes (SIGKILL / OOM) | ≤ TTL + TTL/3 + probe_interval ≈ **30–40s** |
| Leader SIGTERM (graceful) | ≤ TTL/3 + probe_interval ≈ **5–15s** |
| Backend partition (heartbeat fails) | ~20s to detect, then failover per above |

### Tuning

```bash
EXSPEED_LEASE_TTL_SECS=30         # default 30
EXSPEED_LEASE_HEARTBEAT_SECS=10   # default 10 (~TTL/3)
```

Shorter TTL = faster failover + more chatty backend traffic. Longer TTL
= slower failover + less traffic.

### Operator visibility

- `GET /healthz` — 200 if this pod is the leader, 503 otherwise. Public.
- `GET /metrics` — Prometheus. Public. Includes `exspeed_is_leader`,
  `exspeed_leader_transitions_total{direction}`, and the existing
  `exspeed_lease_*` series (`name="cluster:leader"`).
- `GET /api/v1/leases` — bearer-authed; returns the single
  `cluster:leader` row. Available on any pod (leader and standby) so
  operators can discover who's in charge from anywhere.
- Postgres backend: `SELECT * FROM exspeed_leases WHERE name = 'cluster:leader';`

### Replication

In multi-pod mode Exspeed runs **asynchronous follower-pull replication**:
every non-leader pod mirrors the leader's `data_dir` over a persistent
TCP session on port 5934. When the leader dies, the surviving pod that
wins the lease already has an up-to-date copy of every stream, so
failover is data-preserving (subject to the RPO below). There is no
manual operator work between failover and serving traffic — the new
leader starts accepting writes as soon as `/healthz` returns 200.

#### RPO (data loss on crash)

Writes are acknowledged when they hit the leader's local disk — the
leader does not wait for a follower to apply the record before
responding. The window between "leader acks" and "follower applies" is
reported as `exspeed_replication_lag_seconds` + `exspeed_replication_lag_records`
(the latter is best-effort; `lag_seconds` is the primary signal).
If the leader crashes with `lag > 0` at the moment of death, those
records can be lost on promotion. Mitigations:

- **Keep lag low.** Alert on `exspeed_replication_lag_seconds{stream=~".*"} > 10`.
- **Idempotent publishes.** The `x-idempotency-key` header (TCP
  publish) / `x-idempotency-key` HTTP header / `msg_id` JSON field
  deduplicates retries after a failover. Clients that retry on any
  5xx after an ambiguous ack will get exactly-once semantics across a
  failover window.

#### RTO (time-to-serve)

Unchanged from Plan E: ~30-40s for an unclean leader death (lease
TTL + retry slack + probe flip) and ~5-15s for a graceful SIGTERM. The
new leader's storage is already warm, so there's no data-reload step.

#### Required replicator credential

The follower side of the handshake authenticates with a bearer token
carrying `Action::Replicate`. Declare it in your `credentials.toml`:

```toml
[[credentials]]
name = "replicator"
token_sha256 = "<sha256 of the bearer>"
permissions = [
  { streams = "*", actions = ["replicate"] },
]
```

Then point every pod at the bearer via `EXSPEED_REPLICATOR_CREDENTIAL`.
Startup hard-fails in multi-pod mode if this env var is unset — the
follower cannot authenticate without it.

#### Tuning

| Env var | Default | Purpose |
|---|---|---|
| `EXSPEED_CLUSTER_BIND` | `0.0.0.0:5934` | Leader-side listener for follower sessions. |
| `EXSPEED_CLUSTER_ADVERTISE` | same as bind | What the leader writes into the `cluster:leader` lease row as its replication endpoint. Set when the listen address differs from what peers should dial (NAT / k8s pod-IP vs service-IP). |
| `EXSPEED_REPLICATION_BATCH_RECORDS` | 1000 | Max records per `RecordsAppended` frame. Smaller = lower latency, larger = better throughput. |
| `EXSPEED_REPLICATION_HEARTBEAT_SECS` | 5 | Leader-side keepalive cadence when no records are flowing. Paired with the 30s follower idle timeout (6× ratio) — a single dropped heartbeat does not tear a session down. |
| `EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS` | 30 | Follower tears the session down if it receives nothing for this long. |
| `EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS` | 100000 | Leader's per-follower mpsc queue capacity. Bigger = more memory per stuck follower, smaller = earlier drops under backpressure. |

#### Seeding a large initial replica

The wire protocol streams every historical record from offset 0 on
first connect, which is fine for tens of millions of small records but
slow for TB-scale datasets. For those, rsync or snapshot the leader's
`data_dir` to the new pod offline, start the new pod pointed at the
seeded dir, and let the replication session pick up from the tail.
There's no manifest-fingerprint verification — the follower's cursor
is authoritative about where it left off.

#### Metrics

- `exspeed_replication_role{role="leader|follower|standalone"}` — gauge set to 1 for the current role, 0 otherwise.
- `exspeed_replication_connected_followers` — gauge; count of active follower sessions on the leader.
- `exspeed_replication_lag_seconds{stream}` — gauge; seconds between leader's latest write and follower's last-applied record. **Primary indicator** — alert on `exspeed_replication_lag_seconds{stream=~".*"} > 10`.
- `exspeed_replication_lag_records{stream}` — gauge; same idea, in records. Best-effort — reflects offset-lag at the moment of the last applied batch, not the live tail; `lag_seconds` is the more reliable signal.
- `exspeed_replication_records_applied_total{stream}` — counter; records applied on the follower.
- `exspeed_replication_bytes_total{direction="in|out"}` — counter; bytes over the replication socket.
- `exspeed_replication_truncated_records_total{stream}` — counter; records truncated from the follower's local storage during divergent-history reconciliation.
- `exspeed_replication_reseed_total{stream}` — counter; streams wiped + rebuilt because the follower fell behind the leader's retention window.
- `exspeed_replication_follower_queue_drops_total` — counter; records dropped by the leader when a follower's queue was full.
- `exspeed_auth_denied_total{action="replicate"}` — counter; replication handshakes rejected for missing `Action::Replicate`.

> **Prometheus suffix quirk.** Scrapers observe counters here with a doubled `_total` suffix (e.g. `exspeed_replication_truncated_records_total_total`, `exspeed_replication_records_applied_total_total`). This is a known OTel-to-Prometheus exporter behaviour — it appends `_total` to counter names, including ones that already end in `_total`. Write PromQL and alert rules against the doubled name.

#### Running the ignored replication integration tests

The five Postgres-backed replication tests are `#[ignore]`d by default because they require a live Postgres. To run them locally:

```bash
docker compose up -d postgres
EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed \
  cargo test -p exspeed --ignored -- --nocapture
```

#### Operator endpoints

- `GET /api/v1/leases` — existing endpoint; now includes a
  `replication_endpoint` field on the `cluster:leader` row so operators
  (and followers) can see where to dial.
- `GET /api/v1/cluster/followers` — leader-only, admin-bearer-gated.
  Returns a list of currently-connected followers with `follower_id` +
  `registered_at`. Returns 503 on single-pod pods with an explicit
  `hint` string pointing at `EXSPEED_CONSUMER_STORE`.

#### Known limitation: TCP publish leader-gate

Clients should connect only to the leader's port 5933 via the probe-aware
Service — the readiness probe only routes to the pod whose `/healthz`
returns 200. A TCP client that bypasses the Service and connects
directly to a follower's port 5933 to publish will succeed: the write
lands on the follower's local storage and is overwritten by the
divergent-history truncation on the next replication handshake cycle.
Tracked for a future release.

#### k8s deployment with replication

Extend the Service + StatefulSet example above to expose 5934:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: exspeed
spec:
  selector: { app: exspeed }
  ports:
    - name: api
      port: 8080
      targetPort: 8080
    - name: tcp
      port: 5933
      targetPort: 5933
    - name: cluster
      port: 5934
      targetPort: 5934
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: exspeed
spec:
  replicas: 2
  selector:
    matchLabels: { app: exspeed }
  serviceName: exspeed
  template:
    metadata:
      labels: { app: exspeed }
    spec:
      containers:
        - name: exspeed
          image: exspeed:latest
          ports:
            - containerPort: 8080
            - containerPort: 5933
            - containerPort: 5934
          env:
            - name: EXSPEED_CONSUMER_STORE
              value: postgres
            - name: EXSPEED_OFFSET_STORE_POSTGRES_URL
              valueFrom: { secretKeyRef: { name: pg, key: url } }
            - name: EXSPEED_REPLICATOR_CREDENTIAL
              valueFrom: { secretKeyRef: { name: replicator, key: token } }
            - name: EXSPEED_CLUSTER_BIND
              value: "0.0.0.0:5934"
            - name: EXSPEED_CLUSTER_ADVERTISE
              value: "$(POD_NAME).exspeed.$(POD_NAMESPACE).svc.cluster.local:5934"
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            periodSeconds: 5
            failureThreshold: 2
            successThreshold: 1
```

### What's still not in v1

- **Synchronous replication.** Every ack is local-disk; the RPO window
  is non-zero on crash. There's no `wait_for_quorum` knob.
- **Per-stream replication factor.** Every stream replicates to every
  follower. You can't mark a stream as "leader-only" or "2/3 replicas".
- **Raft / consensus writes.** Lease coordination is single-key; there
  is no multi-stage write commit. A split-brain scenario with a
  partitioned lease backend is recoverable via divergent-history
  truncation, not prevented.
- **Geo / WAN replication.** The replication protocol assumes a
  low-RTT network between pods. Running followers across regions
  works mechanically but lag alerts will fire continuously.
- **Catastrophic S3-only restore.** There is no "restore from object
  storage" path independent of a live follower. Sink connectors to S3
  provide an archive, but restoring a stream from that archive into
  a new cluster is a manual operator task.

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
