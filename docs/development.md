# Development

## Build and test

You need Rust 1.94 or newer. Node 18+ is only needed for the TypeScript SDK.

```bash
cargo build                              # all crates
cargo build --release -p exspeed         # just the server/CLI binary
cargo test --workspace --lib             # fast unit tests
cargo test -p exspeed --test exql_test   # one integration test file
cargo clippy --workspace
```

Integration tests are compiled into **one binary per crate** (`tests/it/`),
with one module per former test file. Tests that change process-wide
environment variables still have their own binaries in `tests/*.rs`. Those
will be merged once configuration moves into a struct; until then, don't
add `set_var` calls to modules under `tests/it/`.

On a disk-constrained machine, build without debug info, as CI does:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace
```

Pick test ports with `exspeed_testkit::pick_unused_port()`. It binds
`127.0.0.1:0`, so it also works on IPv4-only hosts.

## CI

`.github/workflows/ci.yml` runs on every PR. It has four jobs:

| Job | What it runs |
|-----|--------------|
| `lint` | `cargo fmt --check` and `cargo clippy -D warnings` |
| `test` | `cargo test --workspace` |
| `test-services` | Postgres (with `wal_level=logical`), Redis and MySQL in Docker, then the gated tests with `--include-ignored` |
| `sdk` | SDK typecheck, test and build |

## TypeScript SDK

```bash
cd sdks/typescript
npm ci
npm run typecheck
npm test            # vitest, against mock sockets
npm run build       # tsup → ESM + CJS
```

## Infrastructure for connector tests

```bash
docker compose up -d postgres rabbitmq minio mysql mssql
```

| Service | Ports | Credentials |
|---------|-------|-------------|
| postgres (`wal_level=logical`) | 5432 | `testuser` / `testpass`, db `testdb` |
| rabbitmq | 5672, 15672 | `guest` / `guest` |
| minio | 9000, 9001 | `minioadmin` / `minioadmin` |
| mysql | 3306 | `exspeed` / `exspeed` |
| mssql (amd64 only) | 1433 | `sa` / `Exspeed_Test!1` |

The database-backed tests **skip silently, and report as passing,** when
their env var isn't set:

```bash
EXSPEED_POSTGRES_URL="postgres://testuser:testpass@localhost:5432/testdb" \
  cargo test -p exspeed --test jdbc_sink_postgres_test

EXSPEED_MYSQL_URL="mysql://exspeed:exspeed@localhost:3306/exspeed" \
  cargo test -p exspeed --test jdbc_sink_mysql_test

EXSPEED_MSSQL_URL="mssql://sa:Exspeed_Test!1@localhost:1433/master?trust_server_certificate=true" \
  cargo test -p exspeed --test jdbc_sink_mssql_test
```

The replication and multi-pod tests are `#[ignore]` because they need
Postgres. They are currently known to fail, and Phase 6 of the plan replaces
them:

```bash
EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://testuser:testpass@localhost:5432/testdb \
  cargo test -p exspeed -- --ignored --nocapture
```

Microsoft doesn't publish an arm64 SQL Server image. The compose file pins
`platform: linux/amd64`, so on Apple Silicon it runs under emulation and the
first boot takes about 30 s.

## Benchmarks

See [BENCHMARKS.md](../BENCHMARKS.md) and [bench/README.md](../bench/README.md).

```bash
cargo build --release -p exspeed -p exspeed-bench
./target/release/exspeed server --data-dir /tmp/exspeed-bench &
./target/release/exspeed-bench all --server localhost:5933 --api http://localhost:8080 --profile local
```

> The published numbers are from v0.2.0 on macOS. Driver and broker ran on
> the same host. Re-run them on Linux before you quote them.

## Releasing

Releases are driven by cargo-dist. See the "Releasing" section of
[CLAUDE.md](../CLAUDE.md) for the full checklist:

1. Bump versions.
2. Update `CHANGELOG.md`.
3. Tag `vX.Y.Z` and push. This alone creates the GitHub Release.
4. Build the multi-arch Docker image.
5. Run `npm publish`.

## Repository layout

```
crates/              Rust workspace (see docs/architecture.md)
sdks/typescript/     @exspeed/sdk
examples/            getting-started (Bun) and order-processing demos
bench/               comparison kit (Kafka) and stored results
docs/                this documentation
```
