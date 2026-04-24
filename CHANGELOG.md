# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] — 2026-04-24

ExQL query engine improvements — structured errors, predicate pushdown,
and SQL queries over the TCP wire protocol.

### ExQL improvements

- **Structured error messages.** Parse errors now include line/column
  position extracted from the SQL parser. Unsupported-feature errors
  include a hint listing what ExQL does support. All error responses
  (HTTP and TCP) return a JSON object with `error`, `code`, and
  context-specific fields (`line`, `column`, `hint`).
- **Predicate pushdown.** When a `WHERE` clause sits directly above a
  stream scan, the filter is absorbed into the scan operator. Rows are
  evaluated during storage batch reads — non-matching rows are never
  materialized, significantly improving performance on large streams
  with selective predicates.
- **TCP query protocol.** Bounded SQL queries can now be executed over
  the binary TCP protocol via `OpCode::Query` (0x20) /
  `OpCode::QueryResult` (0x85). JSON-encoded results match the HTTP
  API format.

### TypeScript SDK

- **`client.query(sql)`** — execute bounded SQL queries over TCP.
  Returns `QueryResult` with `columns`, `rows`, `rowCount`,
  `executionTimeMs`. Errors throw `QueryError` with structured `code`,
  `line`, `column`, `hint` fields.

### Documentation

- Fixed all connector TOML examples in the README to use the correct
  `[connector]` section format with `type` (not bare `connector_type`).
- Added documentation for JDBC poll source, SQLite JDBC sink, and
  SQL Server JDBC sink connectors.
- Fixed JDBC sink `connection_url` → `connection` in README example.

## [0.3.0] — 2026-04-23

Connector-framework release. Substantially extends the connector surface
with new dialects, new connectors, and cross-cutting resilience primitives.
All changes are backwards-compatible — existing connector configs continue
to parse and run unchanged.

### New connectors + dialects

- **JDBC sink: SQL Server dialect.** New `mssql://` / `sqlserver://` scheme
  support. MERGE + WITH (HOLDLOCK) upsert grammar, ISJSON check-constraint
  on JSON columns, DATETIMEOFFSET for timestamptz. Backed by `tiberius` +
  `bb8-tiberius` (sqlx does not support MSSQL). E2E coverage against
  SQL Server 2022 Developer edition.
- **JDBC sink: SQLite dialect.** New `sqlite:` scheme support via sqlx's
  native SQLite driver. Standard `ON CONFLICT ... DO UPDATE` upsert
  (SQLite 3.24+).
- **`jdbc_poll` source (new connector).** Periodically polls a SQL table
  for rows whose `tracking_column` exceeds the last-seen value, emitting
  each row as JSON. Supports Postgres, MySQL, SQLite (via sqlx), and SQL
  Server (via tiberius). Uses dialect-specific SQL (`LIMIT` vs `TOP(N)`).
- **`mssql_cdc` source (new connector).** Streams insert/update/delete
  events from SQL Server tables with Change Data Capture enabled. Reads
  from `[cdc].[fn_cdc_get_all_changes_<capture_instance>]`; persists the
  last-processed Log Sequence Number as a hex string.

### Connector resilience framework

- **`RetryPolicy` primitive.** Full-jitter exponential backoff; per-
  connector `[retry]` TOML section configures `max_retries`,
  `initial_backoff_ms`, `max_backoff_ms`, `multiplier`, `jitter`. Applied
  by the manager on whole-batch transient failures and on source-poll
  errors.
- **Dead Letter Queue (`DlqWriter`).** Routes poison records to a
  configurable exspeed stream (`dlq_stream` setting) with
  `exspeed-dlq-*` metadata headers (origin, reason, detail, original
  offset, timestamp). Original payload preserved byte-identically so the
  DLQ stream is replay-ready.
- **`on_transient_exhausted` dispatch.** New config option selects post-
  exhaustion behavior: `halt` (stop the connector), `dlq_batch` (route
  the remaining batch to DLQ), or `loop_forever` (default; preserves
  pre-0.3 behavior).
- **Enriched `WriteResult`.** Sinks now return `Poison { poison_offset,
  reason, record, … }` or `TransientFailure { error, … }` instead of
  the opaque `AllFailed`. Each built-in sink (JDBC, HTTP, RabbitMQ, S3)
  classifies its errors explicitly.
- **JDBC sink SQLSTATE classifier.** PK violations (Postgres 23505,
  MySQL 1062, MSSQL 2627) are treated as duplicate-ignored; NOT NULL
  (23502), numeric overflow (22003), invalid text (22P02) are routed as
  `Poison`; anything else is `Transient`.
- **HTTP sink: retry delegated to manager.** The inline
  `1 << attempt` backoff loop is removed; 4xx → `Poison`, 5xx/network
  → `TransientFailure`. Manager applies `RetryPolicy`. Existing
  `retry_count` setting is deprecated.

### Observability

New OpenTelemetry counters on every connector:
- `exspeed_connector_dlq_total{connector, reason}`
- `exspeed_connector_dlq_failures_total{connector}`
- `exspeed_connector_retry_attempts_total{connector, outcome}`
- `exspeed_connector_transient_exhausted_total{connector, action}`

### Tests

- 4 always-runnable DLQ + retry E2E tests (axum mock HTTP sink, TCP fetch
  protocol for DLQ stream inspection).
- 2 SQLite sink E2E tests.
- 1 SQLite `jdbc_poll` E2E test.
- 6 MSSQL sink E2E tests (DB-gated on `EXSPEED_MSSQL_URL`).
- 1 MSSQL `jdbc_poll` E2E test (DB-gated).
- 9 SQLSTATE classifier unit tests; 8 RetryPolicy unit tests; 7 DlqWriter
  and `PoisonReason` unit tests; 8 `jdbc_poll` unit tests; 7 `mssql_cdc`
  unit tests.

### Breaking (internal only)

- `WriteResult::AllFailed` removed from `exspeed-connectors` public
  types. Crate is not published to crates.io; the public surface of the
  `exspeed` binary (TCP wire protocol, HTTP API, CLI) is unchanged.

## [0.2.0] — 2026-04-21

First public release since 0.1.1. Rolls up multiple internal milestones
(perf-overhaul-v0.2, perf-round-2, profile-driven fixes, storage
unification) into a single tagged version.

### Performance

On a macbook-laptop (APFS+NVMe), relative to 0.1.1:

| Metric                       | 0.1.1          | 0.2.0           | Δ                |
|------------------------------|----------------|-----------------|------------------|
| Publish 1 KB sync msg/s      | ~225           | 7,010           | ~31×             |
| Publish 1 KB async msg/s     | n/a            | 69,728          | new mode         |
| E2E p50 @ 10k/s sync         | 250+ ms        | 15.8 ms         | −94%             |
| E2E p99 @ 10k/s sync         | 250+ ms        | 60.8 ms         | −76%             |
| E2E p99 @ 10k/s async        | n/a            | 23.3 ms         | new mode         |

Sync throughput is fsync-bound on macOS APFS (`F_FULLFSYNC` ~5 ms). The
v0.2 group-commit writer already amortized fsyncs across records, so the
storage-unification work in 0.2.0 did not change sync throughput — its
wins are elsewhere: sync p50 was halved, async throughput climbed 61%
over the previous internal milestone, and async p99 dropped 73%.

### Storage — breaking on-disk change

- The separate `wal.log` file is gone. Segments are now the sole journal.
- On startup, if a legacy `wal.log` is found in any partition directory,
  the server fails fast with remediation instructions. Wipe your data
  dir or downgrade to 0.1.1.
- Write path does one encoding, one CRC pass, one write, one fsync per
  batch (sync mode) or per timer tick (async mode).
- Crash recovery is a CRC-validating tail scan of the active segment,
  truncating the first torn or corrupt frame.

### Wire protocol

- New opcodes: `PublishBatch = 0x0A` (client → server) and
  `PublishBatchOk = 0x8A` (server → client). Per-record results.
  Legacy `Publish` / `PublishOk` still work.
- `RecordsBatch = 0x83` now used for batched push delivery.

### Client SDKs

- Rust SDK: new `Publisher` with transparent coalescing (default 100µs
  window, 256 records max) plus explicit `publish_batch(Vec<req>)`.
- TypeScript SDK: same Publisher pattern (`batchWindowMs` option,
  `publishBatch(stream, reqs)`).

### Operational tuning (new flags)

- `--storage-sync sync|async` (env `EXSPEED_STORAGE_SYNC`)
- `--storage-flush-window-us` / `--storage-flush-threshold-records` /
  `--storage-flush-threshold-bytes`
- `--storage-sync-interval-ms` (async only)
- `--delivery-buffer`

See README's "Operational tuning" section for defaults and trade-offs.

### Broker

- Per-partition group-commit writer coalesces concurrent `storage.append`
  callers into shared fsyncs.
- Consumer-store saves are debounced (100ms per-consumer, latest wins)
  and moved off the Ack hot path.
- `DashMap` replaces `RwLock<HashMap>` on partition / appender / syncer
  maps for lock-free publish-path reads.

### Testing

- Reproducible comparison kit under `bench/` (Kafka via docker-compose;
  NATS instructions in BENCHMARKS.md).
- `exspeed-bench` harness: publish, latency, fanout, continuous-query
  scenarios.

## [0.1.1] — 2026-04-20

### Fixed

- **Consumer state is now persisted atomically.** Offsets under
  `{data_dir}/consumers/` are written via tempfile + rename + parent-dir fsync
  instead of open-with-truncate + write. A crash during a save previously
  left a truncated / empty JSON file that failed to parse on next startup,
  silently losing the consumer's offset. Stray `*.json.tmp` files from a
  crashed save are safely ignored by the loader.
- **`StorageEngine::read` no longer silently skips trimmed-away history.**
  When a consumer asks for an offset that retention has already deleted,
  both `FileStorage` and `MemoryStorage` now return
  `StorageError::OffsetOutOfRange { requested, earliest }` instead of
  quietly jumping forward to the first surviving record. The broker's
  delivery task logs a warning with both offsets and terminates the
  subscription so the client can re-seek deliberately. Reading past `next`
  (normal tailing) is unchanged — only reading **below earliest** errors.

### Added

- `StorageError::OffsetOutOfRange { requested, earliest }` variant on the
  public `exspeed-streams` API. SDK callers that care about this case can
  match on it; everyone else will see it as a generic error surfaced via
  the existing error path.

### Notes for operators

Upgrading from 0.1.0 is a drop-in replacement — no data migration, no
configuration changes. The only behavior change that might surface is
that a consumer lagging past retention will now see its subscription end
with an `offset X is below earliest retained offset Y` log line, instead
of silently starting to consume from the new earliest.

## [0.1.0] — Initial release

- File-backed stream broker with custom binary wire protocol (TCP :5933)
  and HTTP management API (:8080).
- ExQL: SQL parser, logical plan, physical operators, bounded +
  continuous queries, tumbling windows, stream-stream joins with WITHIN.
- NATS-style subject filtering (`*`, `>`).
- Single-partition-per-stream model with segment rolling, WAL, offset +
  time indexes, CRC32C framing.
- Async replication (Plan G): leader/follower pull-based fan-out with
  cursor persistence and divergent-history recovery.
- Built-in source/sink connectors for Postgres (outbox + CDC), RabbitMQ,
  S3, HTTP (webhook, poller, sink), JDBC.
- TypeScript SDK (`@exspeed/sdk`) implementing the wire protocol.
