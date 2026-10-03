# Concepts

Exspeed combines a Kafka-style durable log, NATS-style subjects, and a SQL
engine in one binary. This page defines the vocabulary used across the rest
of the docs.

## Streams

A **stream** is an append-only, ordered log of records. Each stream has
exactly one partition: there is no partition count to choose, and ordering
is total within the stream.

Each stream has a retention policy: `max_age` (default 7 days) and
`max_bytes` (default 10 GB). A background task deletes whole sealed segments
that exceed either limit. Segments roll at 256 MB.

## Records

| Field | Description |
|-------|-------------|
| `offset` | Monotonic `u64` position in the stream, assigned on append |
| `timestamp` | Append time (ms in bounded queries; see the ExQL caveats) |
| `subject` | Dot-delimited routing string, e.g. `order.eu.created` |
| `key` | Optional entity key (bytes). Used by SEEK, joins, and idempotent upserts |
| `payload` | Opaque bytes. ExQL treats it as JSON |
| `headers` | List of `(key, value)` string pairs |

## Subjects and filters

Subjects are dot-delimited tokens. Consumers, `tail`, and sink connectors can
filter on them using NATS-style wildcards:

| Filter | Matches | Doesn't match |
|--------|---------|---------------|
| `orders.*` | `orders.placed` | `orders.us.placed` |
| `orders.>` | `orders.placed`, `orders.us.placed` | `orders` |
| (empty) | everything | — |

`>` must be the last token. Today a non-final `>` is silently treated as
final.

## Consumers

A **consumer** is a named, durable cursor over one stream. It has:

- `stream` and an optional `subject_filter`
- `start_from`: `earliest`, `latest`, or a specific `offset`
- an optional `group` name

Consumers are created over the TCP protocol (SDK `createConsumer`). The
HTTP API can list, inspect, and delete them, but not create them. A client
**subscribes** to a consumer and receives records pushed over its TCP
connection. It then **acks** or **nacks** each record.

**Fetch** is a separate, stateless read of `N` records from an offset. It
does not move any consumer cursor.

### Current delivery semantics

Several features are not implemented yet. See [REVIEW.md §3.3](REVIEW.md#33-broker-delivery-consumers-dedup)
for details.

- **Ungrouped consumers** behave as a read cursor. Acks are cumulative: acking
  offset `N` moves the cursor to `N` and skips any unacked records before it.
  There is no ack timeout and no automatic redelivery. The committed record is
  redelivered once on resume.
- **Groups.** With a Postgres or Redis coordinator configured, records are
  shared across group members, with an ack timeout of 30 s. With the default
  single-node setup, every member currently receives every record.
- **DLQ.** A record is copied to `<stream>-dlq` after 5 nacks of the same
  offset. Ungrouped consumers never redeliver a nacked record, so in practice
  the client has to re-seek before it can nack again.
- **Retention.** If a consumer falls behind retention, the subscription
  ends. The client must re-seek.

The target model is a JetStream-style consumer with an ack floor, a pending
list, `ack_wait`, `max_deliver`, and push and pull delivery. Its design is in
[REVIEW.md §5.3](REVIEW.md#53-one-consumer-model-jetstream-style-in-the-broker).

## Idempotent publish

Publishes can carry a `msg_id`. Within the stream's dedup window, a retry
with the same `msg_id` and the same body returns the original offset. The
same `msg_id` with a different body is rejected. See
[idempotent-publish.md](idempotent-publish.md).

## Continuous queries and materialized tables

ExQL (SQL on Apache DataFusion) can run a `SELECT` once (a **bounded**
query) or continuously:

- **`CREATE STREAM <out> AS SELECT …`** (alias `CREATE VIEW`) starts a
  long-running query. It writes its results as records to the stream
  `<out>`.
- **`CREATE TABLE <t> AS SELECT … GROUP BY …`** (alias `CREATE MATERIALIZED
  VIEW`) keeps the current aggregate per key, backed by the changelog stream
  `<t>`. It is readable via `/api/v1/views/<t>` and `SELECT * FROM <t>`.

Continuous queries run on event time with watermarks, checkpoint their
state, and survive restarts without duplicating output.

See [exql.md](exql.md).

## Connectors

**Sources** pull from external systems into a stream, for example Postgres
CDC, outbox tables, webhooks, RabbitMQ, or HTTP polling. **Sinks** push a
stream out to external systems, for example JDBC databases, HTTP endpoints,
S3, or RabbitMQ. They are configured as TOML files in `connectors.d/` or via
the HTTP API. See [connectors.md](connectors.md).

## Leadership (multi-pod)

In multi-pod mode exactly one pod holds the cluster lease and serves traffic.
The others replicate asynchronously and wait. See
[high-availability.md](high-availability.md).
