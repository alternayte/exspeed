# Exspeed documentation

## Start here

| Page | What it covers |
|------|----------------|
| [Getting started](getting-started.md) | Install, run, and publish, tail and query your first stream |
| [Concepts](concepts.md) | Streams, records, subjects, consumers, work sharing, retention |

## Using Exspeed

| Page | What it covers |
|------|----------------|
| [CLI reference](cli.md) | Every `exspeed` command and flag |
| [HTTP API reference](http-api.md) | Every endpoint and the permission it needs |
| [ExQL](exql.md) | SQL over streams: bounded and continuous queries, windows, joins, materialized tables, external Postgres tables |
| [Connectors](connectors.md) | Source and sink plugins, config format, delivery guarantees, retry and DLQ |
| [Idempotent publish](idempotent-publish.md) | `msg_id` dedup semantics, sizing, alerts |
| [TypeScript SDK](../sdks/typescript/README.md) | `@exspeed/sdk`: publishing, push and pull consumers, reconnection, TLS and auth |
| [Rust client](../crates/exspeed-client) | `exspeed-client` crate |
| [Client protocol](protocol.md) | Binary protocol v2 reference for SDK authors |

## Running Exspeed

| Page | What it covers |
|------|----------------|
| [Configuration](configuration.md) | `exspeed.toml`, environment variables and flags: every server setting |
| [Operations](operations.md) | Docker, Kubernetes, logging, probes, shutdown, backups, metrics |
| [Security](security.md) | Tokens, scoped credentials, TLS |
| [High availability](high-availability.md) | Clusters: leader election, replication, failover, `acks` |

## Internals and project

| Page | What it covers |
|------|----------------|
| [Architecture](architecture.md) | Crates, data flow, storage layout, startup sequence |
| [Development](development.md) | Building, testing, connector test infra, benchmarks, releasing |
| [Benchmarks](../BENCHMARKS.md) | Published numbers and methodology |
| [Changelog](../CHANGELOG.md) | Release history and upgrade notes |
| [2026-10 design review](history/2026-10-review.md) | Historical record: the October 2026 deep review, the design it proposed, and the test behind each fix |
