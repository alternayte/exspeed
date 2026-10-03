# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The rebuild from [docs/REVIEW.md](docs/REVIEW.md). Breaking changes
throughout. Nothing is compatible with 0.5.x: wire protocol, on-disk format,
connector config and SDK API all change.

### Storage (Phase 1)

- New file engine: a writer thread per partition with group commit, readers
  that take no locks, a high watermark (readers only see durable data),
  sparse offset/time indexes, and fencing on write/fsync errors. Sidecars and
  truncation are crash-safe.
- Compaction (`compaction = true` per stream): keeps the latest record per
  key; tombstones delete keys.
- Removed: bloom filters, secondary indexes (`.sidx`), S3 tiering and its
  `EXSPEED_STORAGE_S3_*` variables. The segment format is now version 2, and
  old data dirs are refused.

### Consumers and client protocol v2 (Phase 2)

- New binary protocol ([docs/protocol.md](docs/protocol.md)), version byte 2.
  It adds out-of-order replies, fire-and-forget acks and credits,
  `SubscriptionEnded` pushes, JSON admin replies, structured errors with
  details, and handshake and idle timeouts.
- JetStream-style consumers:
  - push (credit-based) and pull delivery
  - `ack`, `nack(delay)`, `term`, `in_progress`
  - `ack_wait`, `max_deliver`, `backoff`, `max_ack_pending`, DLQ with
    provenance headers
  - work sharing between any number of subscribers on one consumer, across
    connections and app instances
  - immediate redelivery when a subscriber goes away
  - ephemeral consumers
- Consumer state lives in the compacted internal stream `__consumers`.
  Streams named `__*` are internal.
- Removed: consumer groups, the file/Postgres/Redis/S3 consumer stores, the
  work coordinators, `Fetch`/`Seek`/`Record` opcodes, `--delivery-buffer`.
- HTTP: `POST /api/v1/consumers`, `POST /api/v1/consumers/{name}/seek`,
  `GET /api/v1/streams/{name}/records`, and `compaction` on stream creation.
- `EXSPEED_LEASE_BACKEND=postgres|redis` selects multi-pod mode.
  `EXSPEED_CONSUMER_STORE` is a deprecated alias.
- New Rust client crate `exspeed-client`, with a coalescing publisher.
  `exspeed tail` no longer goes through SQL.

### Operations (Phase 3)

- Online backup: `GET /api/v1/backup` (global admin, leader) streams a tar
  archive while writes continue. Each stream is a point-in-time copy of
  every record below its high watermark at snapshot start: sealed segments
  whole, the active segment cut after its last complete record, indexes and
  metadata regenerated to match. A manifest (`exspeed-backup.json`) lists
  each stream's `[earliest_offset, next_offset)`. Connector, connection and
  ExQL directories are included; credentials are not. Consistency is per
  stream, not across streams; internal progress streams are snapshotted
  first so a restore replays rather than skips.
- `exspeed backup --url … [--token …] --output backup.tar` downloads and
  verifies a backup; `exspeed restore --input backup.tar --data-dir DIR
  [--force]` restores it offline (takes the data-dir lock, refuses a
  non-empty dir, validates the manifest, paths and every stream's offsets
  before moving anything into place). See
  [docs/operations.md](docs/operations.md#backup-and-restore).
- OpenAPI 3.1 document for the HTTP API at `GET /api/v1/openapi.json`
  (no auth), generated with utoipa from annotations on the handlers. Tests
  keep it in sync with the router in both directions.
- HTTP responses for streams, publish and record browsing are now typed
  structs (same JSON).
- `exspeed-bench catchup`: backlog-drain throughput with stateless reads and
  with a push consumer; part of `exspeed-bench all`. Single-scenario
  commands take `--duration-secs`, `--payload-sizes`, `--rate` and
  `--catchup-records` overrides.
- BENCHMARKS.md and the README numbers re-measured on Linux (4-vCPU cloud
  VM, virtio disk; ~62k msg/s durable 1 KiB publish), with the exact
  commands and raw results in `bench/results/2026-10-03-linux-*.json`. `bench/compare/` has a docker-compose file (Kafka in
  KRaft mode, NATS JetStream) and `run.sh` to run equivalent workloads with
  each system's own perf tools; no comparison numbers are published until
  they are run on the same hardware.

### Connectors v2 (Phase 5)

- A supervisor per connector (starting/running/backoff/failed/stopped):
  restarts with backoff, catches panics, and reports status, last error,
  restart count, lag and last success over HTTP and in metrics.
- Uniform checkpoint protocol: source checkpoints are saved only after the
  append is durable; sink offsets are committed only after `flush()`.
- Typed per-plugin settings that reject unknown keys and accept native TOML
  types. Errors are classified as transient, poison or fatal.
- Connector offsets are stored in the internal `__connector_offsets` stream
  by default (`EXSPEED_CONNECTOR_OFFSET_STORE=log|file`). The Postgres, Redis
  and S3 offset stores are removed.
- Config changes:
  - `postgres` is split into `postgres_cdc` and `postgres_poll`.
  - `dlq_stream` moves to `[connector]`.
  - `http_webhook` needs an explicit `auth_type` (adds `hmac_sha256`).
  - See [docs/connectors.md](docs/connectors.md).
- Plugin fixes:
  - CDC keys and envelope, TOAST columns, LSN feedback on idle
  - poll ties, outbox id types, `mssql_cdc` cursor
  - batched JDBC inserts, HTTP status taxonomy
  - S3 sink idempotent object keys, RabbitMQ confirms

### High availability (Phase 6)

- Clusters are now safe to run: Kafka-style pull replication with leader
  epochs replaces the old push replication. Every stream replicates,
  internal ones included, with original offsets, timestamps, keys and
  headers. Stream metadata (create, config, delete) and retention trims
  replicate too. See [docs/high-availability.md](docs/high-availability.md).
- Lease records carry an epoch (fencing token), the holder's replication
  and client endpoints, and the in-sync replica set. Releasing a lease
  expires it rather than deleting it. Node ids are stable
  (`{data_dir}/node_id`). The Postgres table is now `exspeed_cluster_leases`.
  The heartbeat bounds every backend call and steps down before the lease
  can expire elsewhere.
- `cluster.acks = "all"` (default) acknowledges writes once every in-sync
  replica has them. `cluster.min_insync_replicas` rejects writes when too
  few replicas are in sync. Only ISR members can be elected unless
  `cluster.unclean_leader_election = true`.
- A deposed leader truncates writes it never replicated when it rejoins
  (per-stream epoch history, KIP-101).
- Promotion rebuilds the dedup state from the replicated log, so retried
  `msg_id` publishes stay idempotent across failover.
- Leader hints: followers name the leader in `ConnectOk`, `Metadata`, 503
  errors (TCP and HTTP) and `/healthz`. New `cluster.client_advertise`.
  The Rust client gained `Client::connect_cluster`. The TypeScript SDK takes
  `servers: [...]` and finds the leader again after a failover.
- `GET /api/v1/cluster` (any node) shows the role, epoch, ISR and follower
  progress. It replaces `/api/v1/cluster/followers`.
- New settings: `cluster.client_advertise`, `node_id`, `acks`,
  `min_insync_replicas`, `replica_lag_max_ms`, `ack_timeout_ms`,
  `unclean_leader_election`. Removed: `follower_queue_records`. Lease
  defaults are now a 15 s TTL and a 3 s heartbeat. A replicator credential is
  only required when auth is on.
- Tests: in-process multi-node clusters over an in-memory lease backend.
  They cover replication of every write kind, failover without losing
  acknowledged writes, divergent-leader truncation, follower restart and
  `min_insync_replicas`. A shared conformance suite runs against the
  memory, Postgres and Redis lease backends.
### Cluster metadata in the log (Phase 6)

- ExQL query definitions, ExQL connections and API-created connector configs
  move from node-local files into compacted internal streams written through
  the single write path: `__exql_queries`, `__exql_connections` and
  `__connectors` (key = id, value = JSON definition, delete = tombstone).
  They replicate with the log, and writes are leader-only.
- The catalogs are reloaded at the start of every leader tenure, so a
  promoted follower runs the queries and connectors the old leader had.
- Migration: on first start, the leader imports `exql/queries/*.json`,
  `connections/*.json` and `connectors/*.json` into the streams and renames
  those directories to `<dir>.migrated`.
- Connections defined in `connections.d/` or the environment can no longer
  be deleted through the API (`409`); unparseable `connections.d/` files are
  logged instead of silently skipped.

## [0.5.0] — 2026-04-24

Indexing release. Queries on timestamp, key, and payload fields are now
index-aware — orders of magnitude faster on large streams.

### Indexing

- **Timestamp seek.** `WHERE timestamp > X` uses the existing TimeIndex
  (`.tix`) on sealed segments to jump to the approximate position via
  `seek_by_time()` instead of scanning from offset 0.
- **Bloom filters on key.** Per-segment `.bloom` files are built at seal
  time. `WHERE key = 'x'` skips sealed segments whose bloom filter
  proves the key is absent.
- **Secondary indexes on payload fields.**
  `CREATE INDEX idx ON stream(payload->>'field')` builds sorted
  `(hash, offset)` index files (`.sidx`) per segment. Existing sealed
  segments are backfilled on creation. The planner automatically
  detects indexed payload predicates and uses `IndexScan` for targeted
  offset lookups instead of sequential scans.
- **Index management API.** `POST /api/v1/indexes`,
  `GET /api/v1/indexes`, `DELETE /api/v1/indexes/{name}`.
- **CLI support.** `exspeed query "CREATE INDEX ..."` and
  `exspeed query "DROP INDEX ..."` route to the indexes API.
- **Startup reload.** Saved index definitions are loaded from
  `{data_dir}/indexes/` at startup and registered on partitions.

### Connectors

- **`key_field` config.** New global connector option in `[connector]`
  section. Extracts record keys from JSON payloads for any source
  connector. Plugin-specific key logic takes precedence.

### Bug fixes

- **Timestamp cross-type comparison.** `WHERE timestamp > 1000` now
  compares numerically instead of falling through to string comparison
  (`Value::Timestamp` vs `Value::Int`).

## [0.4.1] — 2026-04-24

ExQL query engine hardening — correctness fixes, safety nets, and
ORDER BY optimizations.

### Bug fixes

- **COUNT(column) now skips NULLs.** The accumulator had a tautological
  condition that counted all rows regardless. COUNT(*) still counts
  all rows; COUNT(column) skips NULLs per SQL semantics.
- **DISTINCT aggregates now work.** The `distinct` flag on
  `SUM(DISTINCT x)`, `COUNT(DISTINCT x)`, etc. was silently discarded.
  Values are now deduplicated per group before accumulation.
- **NULL join keys no longer match.** Hash join converted NULL to the
  string "NULL", causing all NULL-keyed rows to match. NULLs are now
  excluded from the lookup (SQL semantics: NULL ≠ NULL).
- **ReDoS in LIKE patterns.** Replaced recursive backtracking
  (`like_match_recursive`) with an iterative O(n×m) two-pointer
  algorithm. Pathological patterns like `%a%b%c%d%...%` no longer
  cause exponential CPU usage.
- **`last_offset()` no longer allocates entire record vec.** Scans
  sequentially with O(1) memory instead of `read_all()`.

### Safety nets

- **Default result row limit (10,000).** Bounded queries without an
  explicit LIMIT are capped at 10,000 rows to prevent OOM. Users
  override with an explicit `LIMIT` clause.
- **GROUP BY cardinality limit (100,000).** Queries exceeding 100,000
  distinct groups return an error instead of consuming unbounded memory.
- **Hash join right-side limit (100,000).** Joins against streams
  exceeding 100,000 rows on the right side return an error.

### Performance

- **ORDER BY offset ASC sort elimination.** Since offsets are
  monotonically increasing in storage, `ORDER BY offset ASC` removes
  the Sort operator entirely.
- **ORDER BY offset DESC LIMIT N reverse scan.** Reads the last N
  records directly from the tail of the stream — no full scan or sort.
- **TopN operator.** For all other `ORDER BY ... LIMIT N` queries
  (timestamp, key, subject), replaces the full Sort with a bounded
  binary heap using O(N) memory.

## [0.4.0] — 2026-04-24

ExQL query engine improvements — structured errors, predicate pushdown,
and SQL queries over the TCP wire protocol.

### Bug fixes

- **Active segment full-scan on read.** `read_from()` on the active
  (unsealed) segment decoded the entire file then filtered, making
  every bounded query O(total_records) regardless of LIMIT. Now uses
  sequential read that stops after the requested batch size.

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
