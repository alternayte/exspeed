# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Exspeed?

Exspeed is a stream processing platform written in Rust — a message broker with an integrated SQL-like query engine (ExQL) for real-time stream processing. It uses a custom binary wire protocol over TCP (port 5933) with a separate HTTP API for management.

## Build & Test Commands

### Rust (workspace root)
```bash
cargo build                              # Build all crates
cargo test                               # Run all tests (unit + integration)
cargo test -p exspeed-storage            # Test a single crate
cargo test -p exspeed -- broker_test     # Run a single integration test file
cargo test -p exspeed -- broker_test::test_name  # Run a single test
cargo clippy --workspace                 # Lint
cargo run -- server --data-dir /tmp/exspeed  # Run the server
```

### TypeScript SDK (`sdks/typescript/`)
```bash
npm run build        # Bundle with tsup (ESM + CJS)
npm run test         # Run vitest (single run)
npm run test:watch   # Vitest in watch mode
npm run typecheck    # tsc --noEmit
```

### Infrastructure (for connector integration tests)
```bash
docker-compose up -d   # Postgres (5432), RabbitMQ (5672/15672), MinIO (9000/9001)
```

### Releasing

Published artifacts: Docker Hub image, npm-registry TS SDK, GitHub release.

```bash
# 1. Version bump: workspace + all per-crate Cargo.toml + sdks/typescript/package.json
# 2. Update CHANGELOG.md; refresh BENCHMARKS.md + README numbers.
# 3. Tag + push
git tag -a vX.Y.Z -m "release notes"
git push origin main
git push origin vX.Y.Z

# 4. Docker image — multi-arch (amd64 + arm64) via cloud builder
#    Don't pipe through `tee` without `set -o pipefail` — buildx errors get swallowed
#    (exit code reflects tee, not the build). Let buildx write straight to the terminal,
#    or wrap the whole line: `set -o pipefail; docker buildx ... 2>&1 | tee build.log`.
docker buildx build --builder cloud-nayth-projects \
  --platform linux/amd64,linux/arm64 \
  -t docker.io/nayth/exspeed:X.Y.Z \
  -t docker.io/nayth/exspeed:latest \
  --push .

# 5. TS SDK
cd sdks/typescript && npm publish   # prepublishOnly runs typecheck + test + build

# 6. GitHub release — body is the CHANGELOG section for this version
awk '/^## \[X\.Y\.Z\]/{flag=1;next} /^## \[/{flag=0} flag' CHANGELOG.md > /tmp/notes.md
gh release create vX.Y.Z --title "vX.Y.Z — headline" --notes-file /tmp/notes.md
```

- Docker image: `docker.io/nayth/exspeed` — tags `latest` + `X.Y.Z`. Always publish both arches; Apple Silicon users need `arm64`.
- Default buildx builder (`cloud-nayth-projects`) has dedicated `linux-amd64` and `linux-arm64` cloud nodes — multi-arch builds run in parallel rather than emulated locally.
- TS SDK publishes as `@exspeed/sdk` on the public npm registry. `publishConfig.access: public` handles scoped-package access.

### Operator env vars (Plan A hardening)
- `LOG_FORMAT=json|text` — tracing output format (default `text`).
- `EXSPEED_MAX_CONNS` — concurrent TCP connection cap (default `1024`); rejections logged + counted in `exspeed_connections_rejected_total`.
- Server takes an exclusive `flock` on `{data_dir}/.exspeed.lock` at startup; a second process on the same dir fails fast.
- `SIGTERM`/`SIGINT` triggers graceful shutdown with a 10s drain.
- `/healthz` = leader-only (Plan E); `/readyz` = startup-complete + `data_dir` writable.
- Docker image runs as `uid 1000` — k8s pods need `fsGroup: 1000` for PV writes.

## Architecture

### Crate Dependency Graph (bottom-up)
```
exspeed-common          Shared types (StreamName, Offset), subject matching, metrics
    ↓
exspeed-streams         StorageEngine trait, Record/StoredRecord types
    ↓
exspeed-protocol        Wire protocol: Frame codec, OpCodes, ClientMessage/ServerMessage
exspeed-storage         File-based storage: segments, offset/time indexes, tail-scan recovery, retention
    ↓
exspeed-broker          Stream management, consumer state, delivery pipeline, ack/nack
exspeed-connectors      Source/sink connector framework + builtins (Postgres, RabbitMQ, S3)
exspeed-processing      ExQL engine: SQL parser → logical plan → physical operators → runtime
    ↓
exspeed-api             HTTP API (Axum): /api/v1/streams, consumers, connectors, queries, views
    ↓
exspeed                 Binary: CLI + server orchestration (TCP accept loop + HTTP server)
```

### Key Architectural Patterns

- **StorageEngine trait** (`exspeed-streams`): Sync trait with `append`, `read`, `seek_by_time`, `create_stream`. FileStorage is the real impl; MemoryStorage exists for tests. Async callers use `spawn_blocking`.
- **Wire protocol**: 10-byte frame header `[Version(1)][OpCode(1)][CorrelID(4)][PayloadLen(4)]`. Correlation IDs match request/response; push-delivered records use CorrelID 0. CRC32C framing on stored records.
- **Segment-based storage**: Log-structured append-only. Directory layout: `{data_dir}/streams/{stream}/partitions/0/`. Segments roll at 256MB. Offset and time indexes for random access.
- **Single partition per stream**: Simplifies broker logic. Single-writer semantics.
- **Delivery pipeline**: One `tokio::spawn`'d task per active subscription. Polls storage in batches, applies NATS-style subject filtering (`*` = one token, `>` = one or more), sends records via `mpsc` channel to connection handler.
- **Consumer groups**: Round-robin across group members. State is in-memory (single-instance for now).
- **ExQL execution**: Two paths — bounded (one-shot SELECT, reads entire stream) and continuous (long-lived task, outputs to target stream or materialized view). Supports EMIT CHANGES/FINAL, tumbling windows, stream-stream joins with WITHIN.
- **Connector lifecycle**: `SourceConnector`/`SinkConnector` traits with start/poll/commit/stop. ConnectorManager loads from TOML configs in `{data_dir}/connectors.d/`, supports hot-reload via filesystem watcher.

### Server Startup Sequence
1. Open FileStorage (CRC-validating tail-scan recovery of the active segment)
2. Create Broker, load persisted consumers from `{data_dir}/consumers/*.json`
3. Create ConnectorManager, load all connector configs
4. Create ExqlEngine, load query registry, resume continuous queries
5. Spawn retention enforcement background task
6. Spawn HTTP API server (Axum)
7. TCP accept loop — each connection gets a `tokio::spawn`'d handler

### TypeScript SDK
The SDK (`@exspeed/sdk`) implements the binary wire protocol over TCP. Key classes:
- **ExspeedClient**: Main interface — connect, publish, subscribe, fetch, seek, stream/consumer CRUD
- **Connection**: TCP connection with correlation-based request/response, reconnection, keepalive
- **Subscription**: AsyncIterable with Message objects providing `json<T>()`, `ack()`, `nack()`

Each subscription gets its own TCP connection. Protocol layer is in `src/protocol/` with per-operation modules.

## Integration Tests

Integration tests live in `crates/exspeed/tests/`. They spin up a real server (FileStorage + Broker + API) on a random port using `portpicker` and `tempfile` for isolation. Test files:
- `connect_test` / `broker_test` / `consumer_test` / `seek_test` — TCP protocol tests
- `exql_test` / `exql_windows_test` — query engine tests
- `connector_test` — connector lifecycle tests
- `api_test` — HTTP API endpoint tests

## Subject Filtering

NATS-style dot-delimited subjects with wildcards:
- `orders.*` — matches one token (e.g., `orders.placed`, not `orders.us.placed`)
- `orders.>` — matches one or more tokens (must be last segment)
- Empty filter matches all subjects
