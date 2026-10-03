# Exspeed

**A streaming platform in one binary: a Kafka-style durable log, NATS-style
subjects, SQL over streams, and built-in connectors.**

```
exspeed server        # that's the whole deployment
```

Exspeed is 0.x software: the wire protocol and the on-disk format can change
between minor versions (the [changelog](CHANGELOG.md) says when and how to
upgrade).

## Why Exspeed

- **One process, no partitions.** Each stream is one totally ordered log, so
  there is no ZooKeeper or KRaft to run and no partition count to choose.
- **Subjects with wildcards.** Publish to `order.eu.created`, then consume
  `order.*.created` or `order.>`.
- **Replayable and retained.** Records are kept by time and size. You can
  seek by offset or timestamp and replay from anywhere.
- **SQL built in.** Run one-shot queries (on Apache DataFusion), continuous
  queries into new streams, materialized tables, event-time windows and
  stream joins.
- **Connectors built in.** Postgres CDC and outbox, webhooks, JDBC, S3,
  RabbitMQ and HTTP, configured with TOML files that hot-reload.
- **Simple to operate.** It is a single static binary with a Docker image,
  Prometheus metrics, health and readiness probes, a JSON log mode, token
  auth with per-stream scopes, and TLS.

## Quick start

```bash
cargo build --release -p exspeed        # or: docker run -p 5933:5933 -p 8080:8080 nayth/exspeed
./target/release/exspeed server

# in another terminal
exspeed create orders
exspeed pub orders '{"total": 99, "region": "eu"}' --subject order.eu.created --key ord-1
exspeed pub orders '{"total": 42, "region": "us"}' --subject order.us.created --key ord-2
exspeed tail orders --last 5 --no-follow
exspeed query "SELECT key, subject, payload->>'region' AS region FROM orders"
```

Applications connect through the [TypeScript SDK](sdks/typescript/README.md)
or the Rust [`exspeed-client`](crates/exspeed-client) on TCP port 5933
([protocol](docs/protocol.md)). The HTTP API is on port 8080.

## Features

| Area | What you get |
|------|--------------|
| Streams | Durable, crash-safe append-only logs with subjects, retention by time and size, compaction, and reads by offset or timestamp |
| Consumers | Push (credit flow control) and pull, per-message ack / nack / term / in-progress, ack timeout, redelivery with backoff, `max_deliver` and dead-letter streams; one consumer can be shared by many subscribers to split work across app instances. State lives in the log |
| Idempotent publish | `msg_id` deduplication on every write path, within batches, from startup and across failover ([idempotent-publish.md](docs/idempotent-publish.md)) |
| ExQL | SQL on Apache DataFusion: bounded queries (joins, aggregates, window functions, subqueries, JSON numerics, offset/time pushdown) and continuous queries (event-time windows, stream-stream and stream-table joins, durable tables, checkpointed state, effectively-once output) ([exql.md](docs/exql.md)) |
| Connectors | Postgres CDC, outbox and poll; JDBC sink and poll (Postgres, MySQL, SQL Server); SQL Server CDC; RabbitMQ source and sink; S3 sink; HTTP poll, sink and webhook. Supervised, checkpointed, at-least-once or effectively-once, each tested against the real service ([guarantees](docs/connectors.md#delivery-guarantees)) |
| High availability | Epoch-fenced leader lease (Postgres or Redis), pull replication of every stream with divergence truncation, `acks = all` / `quorum`, TLS between nodes ([high-availability.md](docs/high-availability.md)) |
| Operations | One config file, Helm chart, ordered graceful shutdown, online backup and restore, Prometheus metrics, OpenAPI spec, scoped tokens and TLS ([operations.md](docs/operations.md), [security.md](docs/security.md)) |

Each stream is a single partition, so one stream's write rate is bounded by
one node; spread load across streams and nodes. Clients speak Exspeed's own
protocol (TypeScript SDK, Rust client) or HTTP; there is no Kafka, AMQP or
NATS wire compatibility.

## Performance

One 4-vCPU cloud VM (virtio disk, ext4), broker and load generator on the same
host, every broker fsyncing before it acknowledges, 1 KiB records:

| Workload | Exspeed | Kafka 3.8 | NATS JetStream 2.10 |
|----------|--------:|----------:|--------------------:|
| Publish, 4 publishers | 51.7k msg/s | 11.4k msg/s | 2.3k msg/s (synchronous one-at-a-time publishing) |
| Drain a 1M-record backlog | 411k msg/s | 117k msg/s | 75k msg/s |
| End-to-end latency, p50 / p99 | 6.0 / 10.1 ms (at 1k msg/s) | 1 / 7 ms (one message at a time) | — |

The tools differ in how they drive each workload; the caveats, the async-mode
numbers, the machine details and the exact commands are in
[BENCHMARKS.md](BENCHMARKS.md).

## Documentation

| | |
|---|---|
| **Learn** | [Getting started](docs/getting-started.md) · [Concepts](docs/concepts.md) |
| **Use** | [CLI](docs/cli.md) · [HTTP API](docs/http-api.md) · [ExQL](docs/exql.md) · [Connectors](docs/connectors.md) · [Idempotent publish](docs/idempotent-publish.md) · [TypeScript SDK](sdks/typescript/README.md) · [Protocol](docs/protocol.md) |
| **Run** | [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Security](docs/security.md) · [High availability](docs/high-availability.md) |
| **Contribute** | [Architecture](docs/architecture.md) · [Development](docs/development.md) · [Benchmarks](BENCHMARKS.md) · [Changelog](CHANGELOG.md) · [2026-10 design review](docs/history/2026-10-review.md) |

## License

[MIT](LICENSE)
