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

### TypeScript SDK
The SDK (`@exspeed/sdk`, `sdks/typescript/`) implements protocol v2 over TCP; see its README. Key pieces:
- **ExspeedClient**: connect (TLS, token, auto-reconnect with re-subscribe), streams, publish / `publishBatch` / coalescing `publisher()`, `read`, consumers CRUD + `seek`, `subscribe` (push, credit window), `pull`, `query`
- **Subscription**: `AsyncIterable<Message>`; `Message` has `json<T>()`, `text()`, `ack()`, `nack()`, `term()`, `inProgress()`
- Protocol codec in `src/protocol/`. Unit tests use a scriptable fake server; e2e tests (`test/e2e/`) start a real server from `EXSPEED_BIN` or `target/debug/exspeed` (skipped if neither exists).

## Documentation

User docs live in `docs/` (index: `docs/README.md`); the root README is a short landing page. `docs/REVIEW.md` holds the October 2026 deep review, the target architecture and the phased plan — read it before making structural changes, and keep the per-feature status notes in `docs/` honest when fixing or adding features.

## Integration Tests

Integration tests live in `crates/exspeed/tests/`. They spin up a real server (FileStorage + Broker + API) on random ports (`exspeed_testkit::pick_unused_port`) with a temp data dir; most files are modules of the single `it` binary (`tests/it/main.rs`). Test files:
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
