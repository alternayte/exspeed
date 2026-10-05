# Exspeed

**A streaming platform in one binary: a Kafka-style durable log, NATS-style
subjects and request-reply, RabbitMQ-style queues, a key-value store, SQL over
streams, and built-in connectors.**

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
- **Queues when you need them.** Work-queue and interest retention, TTLs,
  delayed delivery, bounded streams that reject when full, header routing,
  priorities and single active consumers, on the same streams.
- **Messaging and key-value too.** Non-persistent pub/sub with queue groups
  and request-reply, and key-value buckets with history, compare-and-set and
  watches.
- **NATS clients work unchanged.** An optional NATS listener lets any NATS
  client library publish, subscribe and request on the same bus, and write
  durable records with JetStream's publish calls.
- **SQL built in.** Run one-shot queries (on Apache DataFusion), continuous
  queries into new streams, materialized tables, event-time windows and
  stream joins.
- **Connectors built in.** Postgres CDC and outbox, webhooks, JDBC, S3,
  RabbitMQ and HTTP, configured with TOML files that hot-reload.
- **Simple to operate.** It is a single static binary with a Docker image,
  Prometheus metrics, health and readiness probes, a JSON log mode, token
  auth with per-stream and per-subject scopes, and TLS with client
  certificates.

## Install

```bash
# Linux and macOS: prebuilt binary
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/alternayte/exspeed/releases/latest/download/exspeed-installer.sh | sh

# Docker (amd64 and arm64)
docker run -p 5933:5933 -p 8080:8080 -v exspeed-data:/var/lib/exspeed \
  ghcr.io/alternayte/exspeed:latest
```

Windows (PowerShell): `irm https://github.com/alternayte/exspeed/releases/latest/download/exspeed-installer.ps1 | iex`.
Every release also has plain archives on the
[releases page](https://github.com/alternayte/exspeed/releases), and a
[Helm chart](deploy/helm/exspeed) runs it on Kubernetes.

## Quick start

```bash
exspeed server --data-dir ./exspeed-data

# in another terminal
exspeed create orders
exspeed pub orders '{"total": 99, "region": "eu"}' --subject order.eu.created --key ord-1
exspeed pub orders '{"total": 42, "region": "us"}' --subject order.us.created --key ord-2
exspeed tail orders --last 5 --no-follow
exspeed query "SELECT key, subject, payload->>'region' AS region FROM orders"
```

Applications connect on TCP port 5933 ([protocol](docs/protocol.md)) with a
client for [TypeScript](sdks/typescript/README.md),
[Python](sdks/python/README.md), [Go](sdks/go/README.md),
[Java](sdks/java/README.md), [.NET](sdks/dotnet/README.md) or
[Rust](crates/exspeed-client), or with any NATS client when the
[NATS listener](docs/nats.md) is on. The HTTP API is on port 8080.

## Features

| Area | What you get |
|------|--------------|
| Streams | Durable, crash-safe append-only logs with subjects, retention by time and size, compaction, and reads by offset or timestamp |
| Consumers | Push (credit flow control) and pull, per-message ack / nack / term / in-progress, ack timeout, redelivery with backoff, `max_deliver` and dead-letter streams; one consumer can be shared by many subscribers to split work across app instances. State lives in the log |
| Queues | Work-queue and interest retention, per-message and stream TTLs, delayed delivery, `max_msgs` with discard old/new, last-N-per-subject, header filters, priority, single active consumers, dead-letter causes ([queues.md](docs/queues.md)) |
| Core messaging | Non-persistent publish/subscribe, queue groups, request-reply with "no responders" ([messaging.md](docs/messaging.md)) |
| Key-value | Buckets with history, compare-and-set, TTLs, keys and watch, over TCP and HTTP ([kv.md](docs/kv.md)) |
| NATS protocol | Core NATS on its own port: pub/sub, queue groups, request-reply, headers, auth and TLS; streams capture subjects with JetStream-style publish acks and `Nats-Msg-Id` dedup ([nats.md](docs/nats.md)) |
| Clients | TypeScript, Python, Go, Java, .NET and Rust, each with push and pull consumers, core messaging, KV, TLS and reconnection |
| Idempotent publish | `msg_id` deduplication on every write path, within batches, from startup and across failover ([idempotent-publish.md](docs/idempotent-publish.md)) |
| ExQL | SQL on Apache DataFusion: bounded queries (joins, aggregates, window functions, subqueries, JSON numerics, offset/time pushdown) and continuous queries (event-time windows, stream-stream and stream-table joins, durable tables, checkpointed state, effectively-once output) ([exql.md](docs/exql.md)) |
| Connectors | Postgres CDC, outbox and poll; JDBC sink and poll (Postgres, MySQL, SQL Server); SQL Server CDC; RabbitMQ source and sink; S3 sink; HTTP poll, sink and webhook. Supervised, checkpointed, at-least-once or effectively-once, each tested against the real service ([guarantees](docs/connectors.md#delivery-guarantees)) |
| High availability | Epoch-fenced leader lease (Postgres or Redis), pull replication of every stream with divergence truncation, `acks = all` / `quorum`, TLS between nodes ([high-availability.md](docs/high-availability.md)) |
| Operations | One config file, Helm chart, ordered graceful shutdown, online backup and restore, Prometheus metrics, OpenAPI spec, scoped tokens, TLS and client certificates ([operations.md](docs/operations.md), [security.md](docs/security.md)) |

Each stream is a single partition, so one stream's write rate is bounded by
one node; spread load across streams and nodes. Clients speak Exspeed's own
protocol, the core NATS protocol, or HTTP; there is no Kafka or AMQP wire
compatibility, and the JetStream management API is not implemented.

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
| **Use** | [Queues](docs/queues.md) · [Core messaging](docs/messaging.md) · [Key-value](docs/kv.md) · [CLI](docs/cli.md) · [HTTP API](docs/http-api.md) · [ExQL](docs/exql.md) · [Connectors](docs/connectors.md) · [Idempotent publish](docs/idempotent-publish.md) · [NATS](docs/nats.md) · Clients: [TypeScript](sdks/typescript/README.md), [Python](sdks/python/README.md), [Go](sdks/go/README.md), [Java](sdks/java/README.md), [.NET](sdks/dotnet/README.md) · [Protocol](docs/protocol.md) |
| **Run** | [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Security](docs/security.md) · [High availability](docs/high-availability.md) |
| **Contribute** | [Architecture](docs/architecture.md) · [Development](docs/development.md) · [Benchmarks](BENCHMARKS.md) · [Changelog](CHANGELOG.md) · [2026-10 design review](docs/history/2026-10-review.md) |

## License

[MIT](LICENSE)
