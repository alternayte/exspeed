# Development

## Build and test

You need Rust 1.94 or newer. The client SDKs need their own toolchains: Node 18+ (TypeScript), Python 3.10+, Go 1.22+, JDK 17+ with Maven, and the .NET 8 SDK.

```bash
cargo build                              # all crates
cargo build --release -p exspeed         # just the server/CLI binary
cargo test --workspace --lib             # fast unit tests
cargo test -p exspeed --test it -- exql_test              # one integration test module
cargo test -p exspeed --test it -- consumer_test::test_name   # one test
cargo clippy --workspace --all-targets -- -D warnings
```

Integration tests are compiled into **one binary per crate** (`tests/it/`),
with one module per test file. Every module in that binary shares one
process environment, and tests run in parallel, so don't add `set_var`
calls to modules under `crates/exspeed/tests/it/`. Two files in
`crates/exspeed/tests/` build their own binaries: `auth_test.rs` (one of its
tests sets `EXSPEED_AUTH_TOKEN` for the CLI client) and `lifecycle_test.rs`.
In-process servers take their settings from `ServerArgs`, not from the
environment.

On a disk-constrained machine, build without debug info, as CI does:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace
```

In-process test servers get pre-bound listeners: bind
`exspeed_testkit::bind_local()` (`127.0.0.1:0`) and pass it as
`ServerArgs::tcp_listener` / `api_listener` (the `TestServer` harness does
this), so no other test can grab the port in between. Use
`exspeed_testkit::pick_unused_port()` only when the port must be known
before the server exists: a child process (`crash_test`), or a node that
restarts on the same address (`cluster_test`'s fault-proxy nodes). Both
work on IPv4-only hosts.

These environment variables change what the tests do:

| Variable | Effect |
|----------|--------|
| `EXSPEED_POSTGRES_URL`, `EXSPEED_MYSQL_URL`, `EXSPEED_MSSQL_URL`, `EXSPEED_RABBITMQ_URL`, `EXSPEED_S3_ENDPOINT` | Service URLs for the connector and JDBC tests (see below) |
| `EXSPEED_LEASE_POSTGRES_URL`, `EXSPEED_LEASE_REDIS_URL` | Lease conformance tests and the three-node `crash_test` (they also accept `EXSPEED_OFFSET_STORE_POSTGRES_URL` / `EXSPEED_OFFSET_STORE_REDIS_URL`). Skipped, and reported as passing, when unset. |
| `CI=true` | Service-backed connector and JDBC tests fail instead of skipping when their URL is missing |
| `EXSPEED_ENOSPC_DIR` | A small, empty filesystem for the disk-full tests (CI mounts a 16 MiB tmpfs). Skipped when unset. |
| `EXSPEED_CRASH_ROUNDS` | Rounds of the single-node `kill -9` test (default 5) |
| `EXSPEED_PROPTEST_CASES` | Cases for the storage property tests (default 16) |
| `EXSPEED_BIN` | Server binary for the SDKs' end-to-end tests (default `target/debug/exspeed`) |

## CI

`.github/workflows/ci.yml` runs on every pull request and every push to
`main`. Its jobs:

| Job | What it runs |
|-----|--------------|
| `lint` | `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` |
| `test` | Builds the binary, validates every example `connectors.d/*.toml` with `exspeed connector validate`, mounts a 16 MiB tmpfs for the disk-full tests, then `cargo test --workspace` |
| `test-services` | Postgres (with `wal_level=logical`), Redis, MySQL, RabbitMQ, an S3-compatible store (moto) and SQL Server (with its Agent) in Docker, then the library, binary, `it`, `leadership_test`, `postgres_lease_test` and `redis_lease_test` test targets with `--include-ignored` |
| `server-bin` | Builds the server once and uploads it for the SDK jobs' end-to-end tests |
| `sdk` | TypeScript SDK: typecheck, test, build, and the examples typecheck |
| `sdk-python` | Python SDK on 3.10 and 3.13: ruff, mypy, pytest |
| `sdk-go` | Go SDK: `go vet`, `gofmt`, `go test -race` |
| `sdk-java` | Java SDK: `mvn -B verify` |
| `sdk-dotnet` | .NET SDK: `dotnet build` and `dotnet test` |
| `helm` | `helm lint` and rendering of the single-node and multi-pod values |

## TypeScript SDK

```bash
cd sdks/typescript
npm ci
npm run typecheck
npm test            # vitest: unit tests against a scriptable fake server,
                    # e2e tests against target/debug/exspeed or $EXSPEED_BIN
npm run build       # tsup → ESM + CJS
```

The end-to-end tests (`test/e2e/`) are skipped when neither binary exists,
so build the server first (`cargo build -p exspeed --bin exspeed`).

## Other SDKs

```bash
cd sdks/python && pip install -e '.[dev]' && ruff check . && mypy src && pytest
cd sdks/go && go vet ./... && test -z "$(gofmt -l .)" && go test -race -count=1 ./...
cd sdks/java && mvn -B verify
cd sdks/dotnet && dotnet build -c Release && dotnet test -c Release --no-build
```

Each SDK has unit tests against a scriptable fake server, codec tests on
the byte fixtures generated from the Rust encoder (shared with
`sdks/typescript/test/unit/protocol.test.ts`), and end-to-end tests that
start `$EXSPEED_BIN` or `target/debug/exspeed` and skip when neither
exists.

## Infrastructure for connector tests

```bash
docker compose up -d postgres rabbitmq s3 mysql mssql
docker run -d --name redis -p 6379:6379 redis:7    # lease tests only
```

| Service | Ports | Credentials |
|---------|-------|-------------|
| postgres (`wal_level=logical`) | 5432 | `testuser` / `testpass`, db `testdb` |
| rabbitmq | 5672, 15672 | `guest` / `guest` |
| s3 (moto, S3-compatible) | 9000 | any; the tests default to `minioadmin` / `minioadmin` |
| mysql | 3306 | `exspeed` / `exspeed`, db `exspeed` |
| mssql (amd64 only, Agent enabled) | 1433 | `sa` / `Exspeed_Test!1` |

The compose file has no Redis service; the `docker run` line above starts
one for the Redis lease tests.

**Connector framework and plugin tests** live in
`crates/exspeed-connectors/tests/it/`: fake sources and sinks drive the
checkpoint protocol and the supervisor (crashes between append and ack,
flush failures, restarts, panics), and an in-process HTTP server drives
`http_poll`/`http_sink`. The service-backed modules run real plugins:
`postgres_test` (`postgres_cdc`, `postgres_outbox`, `postgres_poll`),
`rabbitmq_test`, `s3_test` and `jdbc_test` (the `jdbc` sink and `jdbc_poll`
on MySQL and SQL Server, and `mssql_cdc`). These tests are `#[ignore]`d;
with `--include-ignored` they skip when their URL is unset, and **fail**
when `CI=true` is set without it:

```bash
EXSPEED_POSTGRES_URL="postgres://testuser:testpass@localhost:5432/testdb" \
EXSPEED_RABBITMQ_URL="amqp://guest:guest@localhost:5672/%2f" \
EXSPEED_S3_ENDPOINT="http://localhost:9000" \
EXSPEED_MYSQL_URL="mysql://exspeed:exspeed@localhost:3306/exspeed" \
EXSPEED_MSSQL_URL="mssql://sa:Exspeed_Test!1@localhost:1433/exspeed?trust_server_certificate=true" \
  cargo test -p exspeed-connectors --test it -- --include-ignored
```

CDC can't be enabled on SQL Server's `master` database, so create the
`exspeed` database once:

```bash
docker exec exspeed-mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
  -U sa -P 'Exspeed_Test!1' -Q "IF DB_ID('exspeed') IS NULL CREATE DATABASE exspeed"
```

The JDBC end-to-end tests in `crates/exspeed/tests/it/` follow the same
rules (`#[ignore]`d, skip without their URL, fail under `CI=true`):

```bash
EXSPEED_POSTGRES_URL="postgres://testuser:testpass@localhost:5432/testdb" \
  cargo test -p exspeed --test it -- jdbc_sink_postgres_test --include-ignored

EXSPEED_MYSQL_URL="mysql://exspeed:exspeed@localhost:3306/exspeed" \
  cargo test -p exspeed --test it -- jdbc_sink_mysql_test --include-ignored

EXSPEED_MSSQL_URL="mssql://sa:Exspeed_Test!1@localhost:1433/exspeed?trust_server_certificate=true" \
  cargo test -p exspeed --test it -- jdbc_sink_mssql_test jdbc_poll_test --include-ignored
```

**Replication and multi-pod tests.** `cluster_test` (in-process nodes
replicating over real TCP on the in-memory lease, including the randomized
partition test) and `exspeed-broker`'s `replication_test` and
`leadership_test` run in a plain `cargo test`. The lease conformance tests
and the three-node `kill -9` test need a real lease backend:

```bash
EXSPEED_LEASE_POSTGRES_URL=postgres://testuser:testpass@localhost:5432/testdb \
EXSPEED_LEASE_REDIS_URL=redis://localhost:6379 \
  cargo test -p exspeed-broker --test postgres_lease_test --test redis_lease_test

EXSPEED_LEASE_POSTGRES_URL=postgres://testuser:testpass@localhost:5432/testdb \
  cargo test -p exspeed --test it -- crash_test::kill_9_in_a_three_node_cluster --nocapture
```

The `kill -9` tests run the server binary that Cargo builds for the test
target as separate processes. See
[high-availability.md](high-availability.md#how-it-is-tested) for what each
test proves.

Microsoft doesn't publish an arm64 SQL Server image. The compose file pins
`platform: linux/amd64`, so on Apple Silicon it runs under emulation and the
first boot takes about 30 s.

## Benchmarks

See [BENCHMARKS.md](../BENCHMARKS.md) and [bench/README.md](../bench/README.md).

```bash
cargo build --release -p exspeed -p exspeed-bench
./target/release/exspeed server --data-dir /tmp/exspeed-bench &
./target/release/exspeed-bench all --server localhost:5933 --api http://localhost:8080 \
  --profile local --output bench/results/all.json
```

> The published numbers come from a 4-vCPU Linux cloud VM with the driver
> and the broker on the same host, and BENCHMARKS.md compares them with
> Kafka and NATS JetStream measured on the same machine. Treat them as an
> order of magnitude for that hardware.

## Releasing

A release is cut by running the Release workflow (cargo-dist) from the
Actions tab with the tag `vX.Y.Z` (`dry-run` builds without publishing).
It creates the tag on `main`, builds the binaries and installers, creates
the GitHub Release with the matching `CHANGELOG.md` section as its notes,
and publishes the multi-arch image `ghcr.io/alternayte/exspeed` from those
binaries. The checklist (version bump, changelog, tag, publishing the SDKs) is in
the "Releasing" section of [CLAUDE.md](../CLAUDE.md).

## Repository layout

```
crates/              Rust workspace (see docs/architecture.md)
sdks/typescript/     @exspeed/sdk
sdks/python/         exspeed (PyPI)
sdks/go/             github.com/alternayte/exspeed/sdks/go
sdks/java/           io.github.alternayte:exspeed-client
sdks/dotnet/         Exspeed.Client (NuGet)
deploy/helm/         Helm chart
examples/            getting-started (Bun) and order-processing demos
bench/               comparison kit (Kafka, NATS JetStream) and stored results
docs/                this documentation
```
