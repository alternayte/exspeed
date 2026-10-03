# Configuration

You configure the server with command-line flags and environment variables.
There is no config file yet. A layered `exspeed.toml` is planned in
[REVIEW.md §5.7](REVIEW.md#57-operations).

## Server flags

Every flag that has an env var can be set either way.

### Listeners and data

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--bind` | — | `0.0.0.0:5933` | TCP protocol listener |
| `--api-bind` | — | `0.0.0.0:8080` | HTTP listener |
| `--data-dir` | — | `./exspeed-data` | Data directory. The server takes an exclusive `flock` on it. |

### Auth and TLS

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--auth-token` | `EXSPEED_AUTH_TOKEN` | — | Shared admin bearer token |
| `--credentials-file` | `EXSPEED_CREDENTIALS_FILE` | `{data_dir}/credentials.toml` if present | Scoped credentials ([security.md](security.md)) |
| `--tls-cert` | `EXSPEED_TLS_CERT` | — | PEM certificate chain. Set together with `--tls-key`. |
| `--tls-key` | `EXSPEED_TLS_KEY` | — | PEM private key |

### Storage tuning

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--storage-sync` | `EXSPEED_STORAGE_SYNC` | `sync` | `sync`: group commit with an fsync per batch; records become visible to readers after the fsync. `async`: records are visible once written and fsynced on a timer. |
| `--storage-flush-window-us` | `EXSPEED_FLUSH_WINDOW_US` | `500` | Longest the appender waits to fill a batch |
| `--storage-flush-threshold-records` | `EXSPEED_FLUSH_THRESHOLD_RECORDS` | `256` | Flush early at this many records |
| `--storage-flush-threshold-bytes` | `EXSPEED_FLUSH_THRESHOLD_BYTES` | `1048576` | Flush early at this many bytes |
| `--storage-sync-interval-ms` | `EXSPEED_SYNC_INTERVAL_MS` | `10` | Fsync interval in `async` mode |
| `--storage-sync-bytes` | `EXSPEED_SYNC_BYTES` | `4194304` | In `async` mode, fsync early once this many bytes are unsynced (`0` = timer only) |

With `--storage-sync=async`, a crash can lose up to
`--storage-sync-interval-ms` (or `--storage-sync-bytes`) of acknowledged
writes. This matches NATS JetStream's default. A failed background fsync
marks the stream failed (read-only until restart).

## Environment variables

### General

| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | `info` | Log filter, e.g. `exspeed=debug` |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `EXSPEED_MAX_CONNS` | `1024` | Maximum number of concurrent TCP connections |
| `EXSPEED_DEDUP_WINDOW_SECS` | `300` | Default dedup window for streams that don't set one |

### Multi-pod coordination

Single-node defaults are file-based. See [high-availability.md](high-availability.md).

**Backend selection:**

| Variable | Default | Description |
|----------|---------|-------------|
| `EXSPEED_LEASE_BACKEND` | none | `postgres` or `redis` turns on multi-pod mode: one pod holds the cluster lease and serves writes, the others follow. Unset = single node. (`EXSPEED_CONSUMER_STORE` is accepted as a deprecated alias.) |
| `EXSPEED_OFFSET_STORE` | `file` | Where connector offsets are stored (`file`, `postgres`, `redis`, `s3`, `stream`) |

Consumer state needs no backend: it lives in the internal `__consumers`
stream.

**Postgres:**

| Variable | Default | Description |
|----------|---------|-------------|
| `EXSPEED_OFFSET_STORE_POSTGRES_URL` | — | Postgres URL, used by the Postgres lease and offset store |
| `EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA` | `public` | |
| `EXSPEED_OFFSET_STORE_POSTGRES_TABLE` | `exspeed_offsets` | Connector offset table |

**Redis:**

| Variable | Default | Description |
|----------|---------|-------------|
| `EXSPEED_OFFSET_STORE_REDIS_URL` | — | Redis URL, used by the Redis lease and offset store |
| `EXSPEED_OFFSET_STORE_REDIS_KEY_PREFIX` | `exspeed:offsets:` | |
| `EXSPEED_LEASE_REDIS_KEY_PREFIX` | `exspeed:lease:` | |

**S3 (connector offsets):**

| Variable | Default | Description |
|----------|---------|-------------|
| `EXSPEED_OFFSET_STORE_S3_BUCKET` | — | |
| `EXSPEED_OFFSET_STORE_S3_REGION` | `us-east-1` | |
| `EXSPEED_OFFSET_STORE_S3_ENDPOINT` | — | |
| `EXSPEED_OFFSET_STORE_S3_ACCESS_KEY` | — | |
| `EXSPEED_OFFSET_STORE_S3_SECRET_KEY` | — | |
| `EXSPEED_OFFSET_STORE_S3_PREFIX` | `exspeed/offsets/` | |

**Lease and replication:**

| Variable | Default | Description |
|----------|---------|-------------|
| `EXSPEED_LEASE_TTL_SECS` | `30` | Leader lease TTL |
| `EXSPEED_LEASE_HEARTBEAT_SECS` | `10` | Lease refresh interval |
| `EXSPEED_REPLICATOR_CREDENTIAL` | — | Bearer token that followers use. Required in multi-pod mode. |
| `EXSPEED_CLUSTER_BIND` | `0.0.0.0:5934` | Replication listener |
| `EXSPEED_CLUSTER_ADVERTISE` | the bind address | Replication address advertised to peers |
| `EXSPEED_REPLICATION_BATCH_RECORDS` | `1000` | |
| `EXSPEED_REPLICATION_HEARTBEAT_SECS` | `5` | |
| `EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS` | `30` | |
| `EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS` | `100000` | |

### External database connections (ExQL)

| Variable | Description |
|----------|-------------|
| `EXSPEED_CONNECTION_<NAME>_DRIVER` | `postgres`, `mysql`, `sqlite` or `mssql` |
| `EXSPEED_CONNECTION_<NAME>_URL` | Connection URL |

### Client CLI

| Variable | Description |
|----------|-------------|
| `EXSPEED_URL` | HTTP base URL |
| `EXSPEED_AUTH_TOKEN` | Bearer token |
| `EXSPEED_INSECURE_SKIP_VERIFY=1` | Skip TLS verification |

## Stream defaults

| Setting | Default | Set with |
|---------|---------|----------|
| Retention (age) | `7d` | `exspeed create --retention`, or `max_age_secs` over HTTP |
| Retention (size) | `10gb` | `--max-size`, or `max_bytes` over HTTP |
| Dedup window | `5m` | `--dedup-window`, or `dedup_window_secs` |
| Dedup max entries | `500000` | `--dedup-max-entries`, or `dedup_max_entries` |
| Segment size | 256 MB | not configurable |
| Compaction | off | `compaction` in `StreamConfig` only; not exposed over HTTP or the CLI yet |
| Tombstone retention | `24h` | `tombstone_retention_secs` in `StreamConfig` |

A background task enforces retention every 60 s by deleting whole sealed
segments from the front of the log. The active segment is never deleted. A
stream whose `stream.json` is unreadable is skipped (with a warning); the
others are still enforced.

Compacted streams keep only the newest record per key in sealed segments,
and a background compactor visits them every 60 s. A record with a key and
an empty value is a tombstone that deletes the key; the tombstone itself is
removed after the tombstone retention. Offsets are preserved, so compacted
streams have offset gaps. See
[architecture.md](architecture.md#storage-layout).

## Connector env substitution

Inside connector TOML files, `${VAR}` and `${VAR:-default}` are replaced
with values from the server's environment. See [connectors.md](connectors.md).
