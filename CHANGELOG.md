# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
