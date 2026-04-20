# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
