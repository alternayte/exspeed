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

## Feature status

| Area | Status |
|------|--------|
| Durable streams, subjects, retention, compaction, publish, read | ✅ rewritten storage engine (crash-safe, lock-free reads) |
| Consumers: push + pull, ack/nack/term, ack timeout, redelivery, backoff, DLQ | ✅ JetStream-style, state in the log |
| Work sharing across app instances (one consumer, many subscribers) | ✅ |
| Idempotent publish (`msg_id`) | ✅ on every write path, within batches, enforced from startup, rebuilt on failover ([idempotent-publish.md](docs/idempotent-publish.md)) |
| Auth (scoped tokens) and TLS | ✅ |
| ExQL bounded queries | ✅ full SQL on DataFusion (joins, HAVING, window functions, subqueries), JSON numerics, pushdown, timeouts/limits ([exql.md](docs/exql.md)) |
| ExQL continuous queries, windows, joins, tables | ✅ event-time windows, stream-stream/stream-table joins, durable tables, checkpointed state, effectively-once output; query and connection definitions live in the replicated `__exql_queries` / `__exql_connections` streams ([exql.md](docs/exql.md#state-recovery-and-delivery-guarantees)) |
| Connectors: framework (checkpoint protocol, supervisor, typed settings, DLQ, replicated offsets) | ✅ tested with fault injection |
| Connectors: Postgres CDC, outbox, poll; JDBC sink; HTTP poll/sink/webhook | ✅ at-least-once or effectively-once, tested against real services ([guarantees](docs/connectors.md#delivery-guarantees)) |
| Connectors: RabbitMQ, S3, SQL Server CDC | ⚠️ implemented to the same protocol; unit-tested only, no service tests in CI yet |
| Operations: config file, Helm chart, graceful shutdown, online backup + restore, OpenAPI spec | ✅ ([operations.md](docs/operations.md), [http-api.md](docs/http-api.md)) |
| Multi-pod HA with replication | ❌ not safe yet ([§3.4](docs/REVIEW.md#34-ha-leadership--replication)) |

## Performance

Single node, broker and benchmark driver on one 4-vCPU cloud VM (virtio
disk, ext4), default durable mode (fsync before every acknowledgement):

| Workload | Result |
|----------|--------|
| Publish, 1 KiB records, 4 producers | ~62k msg/s (63 MB/s); ~67k msg/s with `--storage-sync async` |
| Publish, 10 KiB records | ~13k msg/s (137 MB/s) |
| Drain a 1M-record backlog | ~354k msg/s with reads, ~285k msg/s with a push consumer |
| End-to-end latency at 5k msg/s | p50 5.3 ms, p99 11.2 ms |

Machine details, all results and the exact commands are in
[BENCHMARKS.md](BENCHMARKS.md). No Kafka or NATS numbers are published; the
[comparison kit](bench/README.md#reproduce-a-comparison-with-kafka-and-nats-jetstream)
runs all three on your hardware.

## Documentation

| | |
|---|---|
| **Learn** | [Getting started](docs/getting-started.md) · [Concepts](docs/concepts.md) |
| **Use** | [CLI](docs/cli.md) · [HTTP API](docs/http-api.md) · [ExQL](docs/exql.md) · [Connectors](docs/connectors.md) · [Idempotent publish](docs/idempotent-publish.md) · [TypeScript SDK](sdks/typescript/README.md) · [Protocol](docs/protocol.md) |
| **Run** | [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Security](docs/security.md) · [High availability](docs/high-availability.md) |
| **Contribute** | [Architecture](docs/architecture.md) · [Development](docs/development.md) · [Review & roadmap](docs/REVIEW.md) · [Benchmarks](BENCHMARKS.md) · [Changelog](CHANGELOG.md) |

## License

[MIT](LICENSE)
