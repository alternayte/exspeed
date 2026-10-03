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
cargo test -p exspeed --test it -- consumer_test           # One integration test module
cargo test -p exspeed --test it -- consumer_test::test_name # A single test
cargo clippy --workspace --all-targets -- -D warnings       # Lint (CI uses the latest stable)
cargo run -- server --data-dir /tmp/exspeed                 # Run the server
cargo run -- config print-default                           # Every server setting (exspeed.toml)
```

### TypeScript SDK (`sdks/typescript/`)
```bash
npm run build        # Bundle with tsup (ESM + CJS)
npm run test         # Run vitest (single run)
npm run test:watch   # Vitest in watch mode
npm run typecheck    # tsc --noEmit
```
E2E tests start a real server from `EXSPEED_BIN` or `target/debug/exspeed` (build it first); they are skipped if neither exists.

### Infrastructure (for connector integration tests)
```bash
docker-compose up -d   # Postgres (5432), MySQL, SQL Server, RabbitMQ (5672/15672), S3 via moto (9000)
```

### Releasing

Published artifacts: Docker Hub image, npm-registry TS SDK, GitHub release with prebuilt binaries.

**The GitHub release is driven by cargo-dist** (`.github/workflows/release.yml` + `dist-workspace.toml`). Pushing a `vX.Y.Z` tag auto-fires the workflow, which builds binaries for aarch64/x86_64 macOS, Linux, Windows, generates install scripts (`exspeed-installer.sh` / `.ps1`), computes SHA256s, and creates the GitHub Release with release notes extracted from `CHANGELOG.md`. **Do NOT `gh release create` manually — it races the workflow and causes `a release with the same tag name already exists: vX.Y.Z` at the end of the workflow.**

```bash
# 1. Version bump: workspace + all per-crate Cargo.toml + sdks/typescript/package.json
# 2. Update CHANGELOG.md (format `## [X.Y.Z] — YYYY-MM-DD` — cargo-dist extracts by this heading);
#    refresh BENCHMARKS.md + README numbers.
# 3. Tag + push — this alone triggers cargo-dist and creates the GitHub Release.
git tag -a vX.Y.Z -m "release notes"
git push origin main
git push origin vX.Y.Z
# Watch: gh run watch $(gh run list --workflow=release.yml --limit 1 --json databaseId -q '.[0].databaseId')

# 4. Docker image — multi-arch (amd64 + arm64) via cloud builder.
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
```

- Docker image: `docker.io/nayth/exspeed` — tags `latest` + `X.Y.Z`. Always publish both arches; Apple Silicon users need `arm64`.
- Default buildx builder (`cloud-nayth-projects`) has dedicated `linux-amd64` and `linux-arm64` cloud nodes — multi-arch builds run in parallel rather than emulated locally.
- TS SDK publishes as `@exspeed/sdk` on the public npm registry. `publishConfig.access: public` handles scoped-package access.
- If the release workflow does fail at the `host` step (e.g. because a manual release pre-existed or a previous run left a partial release): delete the release with `gh release delete vX.Y.Z --yes --cleanup-tag=false`, then `gh run rerun <run-id> --failed` — build artifacts are cached so only the `host` job re-runs.

### Server configuration
- Settings resolve as defaults < `exspeed.toml` (`--config` / `EXSPEED_CONFIG`) < env < flags into `ServerArgs` (`crates/exspeed/src/config.rs`); `exspeed config validate|show|print-default`. Tests and embedders build `ServerArgs { .., ..Default::default() }` directly — `run_with_shutdown` reads no env vars itself.
- `[cluster]` selects multi-pod mode (`EXSPEED_LEASE_BACKEND=postgres|redis`); see `docs/configuration.md`.
- Server takes an exclusive `flock` on `{data_dir}/.exspeed.lock` (released when `run_with_shutdown` returns); a second process on the same dir fails fast.
- `SIGTERM`/`SIGINT`: ordered shutdown — drain sessions and the HTTP API, stop connectors, stop queries, `leadership.resign_after(..)` (cancels leader work, waits for consumers to save their final state, then closes writes and releases the lease), final dedup snapshot, `FileStorage::close()`.
- `/healthz` = 200 only on the leader; `/readyz` = startup complete, both listeners bound, `data_dir` writable (reports `degraded` with `failed_streams` when a partition is fenced).
- Docker image runs as `uid 1000` — k8s pods need `fsGroup: 1000` for PV writes.

## Architecture

### Crate Dependency Graph (bottom-up)
```
exspeed-common          Shared types (StreamName, Offset), subject filters, auth, metrics
    ↓
exspeed-streams         StorageEngine trait, Record/StoredRecord, StreamConfig
    ↓
exspeed-protocol        Wire protocol: Frame codec, opcodes, client protocol v2 (client.rs)
exspeed-storage         FileStorage: writer thread per partition, lock-free readers, sparse indexes, retention, compaction
    ↓
exspeed-broker          Log (single write path), dedup, consumers, leases/leadership, replication
exspeed-connectors      Supervised source/sink connectors + builtins (Postgres CDC/poll/outbox, JDBC, HTTP, RabbitMQ, S3)
exspeed-processing      ExQL on DataFusion: bounded queries + continuous dataflow
    ↓
exspeed-api             HTTP API (Axum): streams, records, consumers, connectors, queries, views
    ↓
exspeed                 Binary: CLI, config, server bootstrap (cli/server.rs), TCP sessions (session.rs)
exspeed-client          Async Rust client for protocol v2 (used by tests and the bench)
```

### Key Architectural Patterns

- **StorageEngine trait** (`exspeed-streams`): async trait with `append`, `read`, `seek_by_time`, `create_stream`, etc. FileStorage is the real impl; MemoryStorage exists for tests.
- **Single write path**: every writer (TCP, HTTP, webhooks, connectors, ExQL, consumer state, DLQ) appends through `exspeed_broker::log::Log` (leader gate → validation → dedup → storage → replication feed → metrics). Never call `StorageEngine::append` directly.
- **Wire protocol v2** (`docs/protocol.md`, `exspeed-protocol/src/client.rs`): 10-byte frame header `[Version=2][OpCode][CorrelID u32 LE][PayloadLen u32 LE]`. Responses may arrive out of order; pushes and fire-and-forget requests use CorrelID 0. Server side is `crates/exspeed/src/session.rs`; the Rust client is `crates/exspeed-client`.
- **Segment-based storage**: Log-structured append-only. Directory layout: `{data_dir}/streams/{stream}/partitions/0/`. Segments roll at 256MB. Offset and time indexes for random access.
- **Single partition per stream**: Simplifies broker logic. Single-writer semantics.
- **Consumers** (`exspeed-broker/src/consumer/`): JetStream-style. One actor per consumer, leader only; pure state machine in `core.rs` (ack floor, in-flight with deadlines, scheduled redeliveries, DLQ). Push (credits) and pull delivery; many subscribers on one consumer share its records (work queue across app instances). State persisted to the compacted internal stream `__consumers`. Streams starting with `__` are internal.
- **ExQL** (`exspeed-processing`, `docs/exql.md`): bounded queries run on Apache DataFusion over per-stream `TableProvider`s (offset/time/LIMIT pushdown, JSON numeric coercion, timeouts, memory pool, row cap). Continuous queries are a micro-batch dataflow driven by `watch_appends`: event-time tumbling/hopping windows with watermarks, stream-stream joins `WITHIN`, stream-table joins, durable tables; state checkpointed to `__exql_ckpt_<id>`, output deduped via deterministic `x-idempotency-key`.
- **Connectors** (`docs/connectors.md`): each runs under a supervisor (backoff, panic catch, status); sources checkpoint only after the append is durable, sinks commit only after `flush()`. Typed per-plugin settings; offsets in `__connector_offsets`. TOML configs in `{data_dir}/connectors.d/` hot-reload.

### Server Startup Sequence
1. Open FileStorage (tail-scan recovery of each active segment; one writer thread per partition)
2. Build BrokerAppend (dedup maps rebuild in the background), lease, `ClusterLeadership`, `Broker` (Log + ConsumerManager)
3. Create ConnectorManager and ExqlEngine, load configs/queries
4. Leader supervisor: on promotion starts consumers (restored from `__consumers`), connectors, continuous queries, retention
5. Spawn HTTP API server (Axum)
6. TCP accept loop — each connection runs `session::run`

### TypeScript SDK
The SDK (`@exspeed/sdk`, `sdks/typescript/`) implements protocol v2 over TCP; see its README. Key pieces:
- **ExspeedClient**: connect (TLS, token, auto-reconnect with re-subscribe), streams, publish / `publishBatch` / coalescing `publisher()`, `read`, consumers CRUD + `seek`, `subscribe` (push, credit window), `pull`, `query`
- **Subscription**: `AsyncIterable<Message>`; `Message` has `json<T>()`, `text()`, `ack()`, `nack()`, `term()`, `inProgress()`
- Protocol codec in `src/protocol/`; unit tests use a scriptable fake server, e2e tests (`test/e2e/`) a real one.

## Documentation

User docs live in `docs/` (index: `docs/README.md`); the root README is a short landing page. Diagrams in any Markdown doc must be Mermaid (` ```mermaid ` blocks, rendered by GitHub), never ASCII art. Check them with `npx -y @mermaid-js/mermaid-cli -i x.mmd -o x.svg` (a `;` inside a sequence-diagram message ends the statement). Docs are evergreen: describe what the system does in the present tense, state limitations as plain facts, and keep project history (what changed, migrations, renamed settings) in `CHANGELOG.md`. `docs/architecture.md` is the design reference; `docs/history/2026-10-review.md` is the October 2026 deep review kept as a historical record (its §7 maps every finding to the test that proves the fix).

## Integration Tests

Integration tests live in `crates/exspeed/tests/`. They spin up a real server (FileStorage + Broker + API) on pre-bound listeners (`exspeed_testkit::bind_local()` passed as `ServerArgs::tcp_listener`/`api_listener`; `pick_unused_port` only where a child process must restart on the same port) with a temp data dir; most files are modules of the single `it` binary (`tests/it/main.rs`). Test files:
- `common/mod.rs` — `TestServer` harness (in-process server, `client()`, `restart()`)
- `protocol_test` / `consumer_test` / `dedup_test` — client protocol and consumer semantics via `exspeed-client`
- `exql_test` / `exql_windows_test` — query engine tests
- `connector_test` — connector lifecycle tests
- `api_test` — HTTP API endpoint tests

## Subject Filtering

NATS-style dot-delimited subjects with wildcards:
- `orders.*` — matches one token (e.g., `orders.placed`, not `orders.us.placed`)
- `orders.>` — matches one or more tokens (must be last segment)
- Empty filter matches all subjects
