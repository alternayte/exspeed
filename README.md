# Exspeed

**A streaming platform in one binary: a Kafka-style durable log, NATS-style
subjects, SQL over streams, and built-in connectors.**

```
exspeed server        # that's the whole deployment
```

> **Status: pre-1.0, not production-ready.** The core ideas work end to end,
> but a deep review found correctness gaps in delivery, HA, ExQL and the
> connectors. Read [docs/REVIEW.md](docs/REVIEW.md) for the findings and the
> rebuild plan before you depend on Exspeed.

## Why Exspeed

- **One process, no partitions.** Each stream is one totally ordered log, so
  there is no ZooKeeper or KRaft to run and no partition count to choose.
- **Subjects with wildcards.** Publish to `order.eu.created`, then consume
  `order.*.created` or `order.>`.
- **Replayable and retained.** Records are kept by time and size. You can
  seek by offset or timestamp and replay from anywhere.
- **SQL built in.** Run one-shot queries, continuous queries into new streams,
  materialized views, tumbling windows and stream-stream joins.
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

## Feature status

| Area | Status |
|------|--------|
| Durable streams, subjects, retention, compaction, publish, read | ✅ rewritten storage engine (crash-safe, lock-free reads) |
| Consumers: push + pull, ack/nack/term, ack timeout, redelivery, backoff, DLQ | ✅ JetStream-style, state in the log |
| Work sharing across app instances (one consumer, many subscribers) | ✅ |
| Idempotent publish (`msg_id`) | ✅ single node · ⚠️ gaps in batches and on failover |
| Auth (scoped tokens) and TLS | ✅ |
| ExQL bounded queries | ✅ filter/project · ⚠️ joins, JSON numerics, many clauses ([§3.5](docs/REVIEW.md#35-exql-exspeed-processing)) |
| ExQL continuous queries, windows, joins, views | ⚠️ partial; state is not durable |
| Connectors | ⚠️ see per-plugin status in [docs/connectors.md](docs/connectors.md) |
| Multi-pod HA with replication | ❌ not safe yet ([§3.4](docs/REVIEW.md#34-ha-leadership--replication)) |

## Documentation

| | |
|---|---|
| **Learn** | [Getting started](docs/getting-started.md) · [Concepts](docs/concepts.md) |
| **Use** | [CLI](docs/cli.md) · [HTTP API](docs/http-api.md) · [ExQL](docs/exql.md) · [Connectors](docs/connectors.md) · [Idempotent publish](docs/idempotent-publish.md) · [TypeScript SDK](sdks/typescript/README.md) · [Protocol](docs/protocol.md) |
| **Run** | [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Security](docs/security.md) · [High availability](docs/high-availability.md) |
| **Contribute** | [Architecture](docs/architecture.md) · [Development](docs/development.md) · [Review & roadmap](docs/REVIEW.md) · [Benchmarks](BENCHMARKS.md) · [Changelog](CHANGELOG.md) |

## License

[MIT](LICENSE)
