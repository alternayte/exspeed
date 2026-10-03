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
| `timestamp` | Append time, stored in nanoseconds. ExQL exposes it as a millisecond timestamp |
| `subject` | Dot-delimited routing string, e.g. `order.eu.created` |
| `key` | Optional entity key (bytes). Compacted streams keep the newest record per key; ExQL exposes it as the `key` column |
| `payload` | Opaque bytes. ExQL treats it as JSON |
| `headers` | List of `(key, value)` string pairs |

Every write path checks the same limits: a subject of at most 1024 bytes, a
key of at most 64 KiB, a value of at most 8 MiB, and at most 256 headers
(64 KiB in total).

## Subjects and filters

Subjects are dot-delimited tokens. Consumers, `tail`, and sink connectors can
filter on them using NATS-style wildcards:

| Filter | Matches | Doesn't match |
|--------|---------|---------------|
| `orders.*` | `orders.placed` | `orders.us.placed` |
| `orders.>` | `orders.placed`, `orders.us.placed` | `orders` |
| (empty) | everything | — |

`>` must be the last token; a filter with a non-final `>`, an empty token or
a partial wildcard (`orders.b*`) is rejected. The same rules apply to every
filter: consumers, `tail`, sink `subject_filter` and the ExQL
`subject_matches` function. A published subject (at most 1024 bytes) may be
empty but may not contain empty tokens, `*`, `>`, whitespace or control
characters.

## Consumers

A **consumer** is a named, durable cursor over one stream that tracks which
records have been processed. Create one over TCP (SDK `createConsumer`) or
HTTP (`POST /api/v1/consumers`):

| Setting | Default | Meaning |
|---------|---------|---------|
| `stream` | — | The stream to consume |
| `filter_subjects` | all | Subject filters; a record is delivered if any filter matches |
| `deliver` | `all` | Where to start: `all`, `new`, `{from_offset}`, `{from_time}` (ms) |
| `ack` | `explicit` | `explicit`, or `none` (delivery counts as processed) |
| `ack_wait_ms` | 30000 | Redeliver if not acked within this time |
| `max_deliver` | 5 | Attempts before dead-lettering (`0` = retry forever) |
| `backoff_ms` | — | Redelivery delays by delivery count, after a nack or timeout (the last value repeats; empty = redeliver at once) |
| `max_ack_pending` | 1000 | Unacked records allowed before delivery pauses |
| `dlq_stream` | — | Where records go after `max_deliver` attempts or a `term`. Unset: they are dropped and counted |
| `ephemeral` | false | Deleted when the creating connection closes (TCP only) |

**Delivery.** Clients either **subscribe** (push, with a credit window the
SDK tops up automatically) or **pull** batches with a long-poll. Each
delivered record is then settled:

- `ack`: processed; never delivered again.
- `nack(delay)`: redeliver after `delay`, or after the consumer's backoff.
- `term(reason)`: give up now and dead-letter it.
- `in_progress`: still working; reset the ack timer.

A record that is not acked in time, or whose subscriber disconnects, is
redelivered (to any subscriber) with a higher `delivery_count`.

**Scaling out: work sharing.** Any number of subscribers and pullers can
attach to the same consumer, from any connection or any instance of your
application. Each record goes to one of them at a time. This is how you run N
replicas of a worker: give them all the same consumer name. For fan-out
(every service sees every record), give each service its own consumer.

**Durability.** Consumer state (ack floor, unacked records, delivery counts)
is stored in the internal, compacted stream `__consumers`. It survives
restarts and replicates with the log. Delivery is **at-least-once**: an
application can see a record more than once (after a crash, timeout or nack).
Make handlers idempotent, or dedup on `offset`.

**Retention.** If retention deletes records a consumer has not reached yet,
the consumer skips ahead to the earliest record still retained.

**Stateless reads** (`read` over TCP, `GET /api/v1/streams/{name}/records`
over HTTP) return records from any offset without a consumer, and can
long-poll for new data.

The wire-level details are in [protocol.md](protocol.md#consumers).

## Idempotent publish

Publishes can carry a `msg_id`. Within the stream's dedup window (default
5 minutes), a retry with the same `msg_id` and the same body returns the
original offset. The same `msg_id` with a different body is rejected. See
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

Continuous queries run on event time with watermarks and checkpoint their
state. After a restart or failover they resume from the checkpoint; output
records carry deterministic idempotency keys, so a resumed query doesn't
duplicate its output.

See [exql.md](exql.md).

## Connectors

**Sources** pull from external systems into a stream, for example Postgres
CDC or polling, outbox tables, SQL Server CDC, JDBC polling, webhooks,
RabbitMQ, or HTTP polling. **Sinks** push a
stream out to external systems, for example JDBC databases, HTTP endpoints,
S3, or RabbitMQ. They are configured as TOML files in `connectors.d/` or via
the HTTP API. See [connectors.md](connectors.md).

## Clusters

In a cluster exactly one node holds the lease: the leader takes writes and
runs consumers, connectors and continuous queries. The other nodes replicate
its whole log and take over on failure. With `acks = all` (the default),
acknowledged writes survive a failover. See [high-availability.md](high-availability.md).
