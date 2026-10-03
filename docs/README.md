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
| [HTTP API reference](http-api.md) | Every endpoint, plus auth and the TCP frame format |
| [ExQL](exql.md) | SQL over streams: bounded and continuous queries, views, windows, joins, indexes |
| [Connectors](connectors.md) | Source and sink plugins, config format, retry and DLQ |
| [Idempotent publish](idempotent-publish.md) | `msg_id` dedup semantics, sizing, alerts |
| [TypeScript SDK](../sdks/typescript/README.md) | `@exspeed/sdk`: publishing, push and pull consumers, reconnection, TLS and auth |
| [Rust client](../crates/exspeed-client) | `exspeed-client` crate |
| [Client protocol](protocol.md) | Binary protocol v2 reference for SDK authors |

## Running Exspeed

| Page | What it covers |
|------|----------------|
| [Configuration](configuration.md) | Server flags and every environment variable |
| [Operations](operations.md) | Docker, logging, probes, shutdown, backups, metrics |
| [Security](security.md) | Tokens, scoped credentials, TLS |
| [High availability](high-availability.md) | Multi-pod leader lease and replication (not production-safe yet) |

## Internals and project

| Page | What it covers |
|------|----------------|
| [Architecture](architecture.md) | Crates, data flow, storage layout, startup sequence |
| [Development](development.md) | Building, testing, connector test infra, benchmarks, releasing |
| [**Review and roadmap**](REVIEW.md) | October 2026 deep review: what's broken, the target architecture, and the phased plan |
| [Benchmarks](../BENCHMARKS.md) | Published numbers and methodology |
| [Changelog](../CHANGELOG.md) | Release history |
