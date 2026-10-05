# Configuration

The server reads settings from four layers. A later layer overrides an
earlier one:

1. built-in defaults
2. a TOML config file (`--config path`, or `EXSPEED_CONFIG`)
3. environment variables
4. command-line flags

The configuration is resolved and validated before anything starts.
Unknown keys in the file, malformed numbers or a missing TLS key fail fast.

```bash
exspeed config print-default > exspeed.toml   # every key, commented, with its default
exspeed config validate -c exspeed.toml       # resolve file + env + flags and check them
exspeed config show -c exspeed.toml           # print the resolved values (secrets redacted)
exspeed server -c exspeed.toml
```

The file has these sections: `[server]`, `[auth]`, `[tls]`, `[storage]`,
`[cluster]`, `[connectors]`, `[exql]`, `[log]` and `[nats]`. The tables below give each setting
as **file key / env var / flag**.

## Server

### Listeners and data

| File key | Env | Flag | Default | Description |
|----------|-----|------|---------|-------------|
| `server.bind` | `EXSPEED_BIND` | `--bind` | `0.0.0.0:5933` | Client protocol listener |
| `server.api_bind` | `EXSPEED_API_BIND` | `--api-bind` | `0.0.0.0:8080` | HTTP listener |
| `server.data_dir` | `EXSPEED_DATA_DIR` | `--data-dir` | `./exspeed-data` | Data directory. The server takes an exclusive `flock` on it. |
| `server.max_connections` | `EXSPEED_MAX_CONNS` | `--max-connections` | `1024` | Concurrent client connections. Extra connections are refused and counted. |
| `server.drain_timeout_secs` | `EXSPEED_DRAIN_TIMEOUT_SECS` | — | `10` | Time open connections and in-flight HTTP requests get on shutdown |
| `server.handshake_timeout_secs` | `EXSPEED_HANDSHAKE_TIMEOUT_SECS` | — | `10` | A TCP client must finish TLS and send `Connect` within this, or the connection is closed |
| `server.idle_timeout_secs` | `EXSPEED_IDLE_TIMEOUT_SECS` | — | `120` | A TCP connection that sends no frame (clients ping) for this long is closed |
| `server.stop_timeout_secs` | `EXSPEED_STOP_TIMEOUT_SECS` | — | `30` | Total budget, after the drain, for stopping connectors, continuous queries and consumers (final state) and writing the dedup snapshot. Shutdown takes at most `drain_timeout_secs + stop_timeout_secs`. |
| `server.metrics_token` | `EXSPEED_METRICS_TOKEN` | — | — | When set, `GET /metrics` requires `Authorization: Bearer <token>` ([security.md](security.md#metrics-token)) |
| `nats.bind` | `EXSPEED_NATS_BIND` | `--nats-bind` | off | Serve the core NATS protocol on this address, e.g. `0.0.0.0:4222` ([nats.md](nats.md)). It uses the `[tls]` certificate and the same credentials as the client port. |

Every listener is bound before anything else starts: a port that is already
in use (or a bad TLS file, or a connector/ExQL catalog that can't be read)
makes `exspeed server` exit with an error instead of running half-started.

### Auth and TLS

| File key | Env | Flag | Default | Description |
|----------|-----|------|---------|-------------|
| `auth.token` | `EXSPEED_AUTH_TOKEN` | `--auth-token` | — | Shared bearer token with full admin rights (the `legacy-admin` identity) |
| `auth.credentials_file` | `EXSPEED_CREDENTIALS_FILE` | `--credentials-file` | `{data_dir}/credentials.toml` if present | Scoped credentials ([security.md](security.md)) |
| `tls.cert` | `EXSPEED_TLS_CERT` | `--tls-cert` | — | PEM certificate chain. Set together with the key. |
| `tls.key` | `EXSPEED_TLS_KEY` | `--tls-key` | — | PEM private key |
| `tls.client_ca` | `EXSPEED_TLS_CLIENT_CA` | `--tls-client-ca` | — | Require client certificates signed by this CA on the TCP port (mutual TLS; see [security.md](security.md#client-certificates-mutual-tls)) |

Auth is on as soon as either a token or a credentials file is configured,
including a `credentials.toml` that merely exists in the data directory.

### Logging

| File key | Env | Default | Description |
|----------|-----|---------|-------------|
| `log.format` | `LOG_FORMAT` | `text` | `text` or `json` |
| `log.level` | `RUST_LOG` | `info` | Log filter, e.g. `exspeed=debug,warn` |

### Storage tuning

File keys live under `[storage]`: `sync`, `flush_window_us`,
`flush_threshold_records`, `flush_threshold_bytes`, `sync_interval_ms`,
`sync_bytes` and `dedup_window_secs`.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--storage-sync` | `EXSPEED_STORAGE_SYNC` | `sync` | `sync`: group commit with an fsync per batch; records become visible to readers after the fsync. `async`: records are visible once written and fsynced on a timer. |
| `--storage-flush-window-us` | `EXSPEED_FLUSH_WINDOW_US` | `500` | Longest the appender waits to fill a batch |
| `--storage-flush-threshold-records` | `EXSPEED_FLUSH_THRESHOLD_RECORDS` | `256` | Flush early at this many records |
| `--storage-flush-threshold-bytes` | `EXSPEED_FLUSH_THRESHOLD_BYTES` | `1048576` | Flush early at this many bytes |
| `--storage-sync-interval-ms` | `EXSPEED_SYNC_INTERVAL_MS` | `10` | Fsync interval in `async` mode |
| `--storage-sync-bytes` | `EXSPEED_SYNC_BYTES` | `4194304` | In `async` mode, fsync early once this many bytes are unsynced (`0` = timer only) |
| — | `EXSPEED_DEDUP_WINDOW_SECS` | `300` | Fallback `msg_id` dedup window, used only for a stream whose own dedup settings aren't loaded. Each stream stores its own window (see [Stream defaults](#stream-defaults)); this setting doesn't change that default. |

With `--storage-sync=async`, a crash can lose up to
`--storage-sync-interval-ms` (or `--storage-sync-bytes`) of acknowledged
writes. NATS JetStream likewise acknowledges writes before they are fsynced
by default. A failed background fsync marks the stream failed (read-only
until restart).

### Cluster (`[cluster]`)

None of this is needed for a single node. See [high-availability.md](high-availability.md).

| File key | Env | Default | Description |
|----------|-----|---------|-------------|
| `cluster.lease` | `EXSPEED_LEASE_BACKEND` | `none` | `postgres` or `redis` turns on cluster mode: one node holds the lease and serves writes, the others replicate |
| `cluster.postgres_url` | `EXSPEED_LEASE_POSTGRES_URL` | — | Postgres for the lease (table `exspeed_cluster_leases`). Required with `lease = "postgres"`. |
| `cluster.postgres_schema` | `EXSPEED_LEASE_POSTGRES_SCHEMA` | `public` | |
| `cluster.redis_url` | `EXSPEED_LEASE_REDIS_URL` | — | Redis for the lease. Required with `lease = "redis"`. |
| `cluster.redis_key_prefix` | `EXSPEED_LEASE_REDIS_KEY_PREFIX` | `exspeed:lease:` | |
| `cluster.lease_ttl_secs` | `EXSPEED_LEASE_TTL_SECS` | `15` | A crashed leader is replaced within about this long |
| `cluster.lease_heartbeat_secs` | `EXSPEED_LEASE_HEARTBEAT_SECS` | `3` | Between 1 and `lease_ttl_secs / 3` |
| `cluster.bind` | `EXSPEED_CLUSTER_BIND` | `0.0.0.0:5934` | Replication listener |
| `cluster.advertise` | `EXSPEED_CLUSTER_ADVERTISE` | the bind address | Address peers replicate from. Set it when `bind` is a wildcard. |
| `cluster.client_advertise` | `EXSPEED_CLIENT_ADVERTISE` | `server.bind` when it is a specific address | Client-protocol address (`host:port`) sent to clients as the leader hint |
| `cluster.node_id` | `EXSPEED_NODE_ID` | generated into `{data_dir}/node_id` | Stable node identity |
| `cluster.replicator_credential` | `EXSPEED_REPLICATOR_CREDENTIAL` | — | Token followers present (needs the `replicate` action). Required when auth is on. |
| `cluster.acks` | `EXSPEED_ACKS` | `all` | `all`: acknowledge once every in-sync replica has the write. `quorum`: `all`, with at least a majority of `cluster.size` in sync. `leader`: after the leader's local write. |
| `cluster.size` | `EXSPEED_CLUSTER_SIZE` | — | Number of nodes. Required for `acks = quorum`. |
| `cluster.min_insync_replicas` | `EXSPEED_MIN_INSYNC_REPLICAS` | `1` | With `acks = all`, writes fail with 503 while fewer replicas (leader included) are in sync |
| `cluster.replica_lag_max_ms` | `EXSPEED_REPLICA_LAG_MAX_MS` | `10000` | A follower that hasn't caught up for this long leaves the ISR |
| `cluster.ack_timeout_ms` | `EXSPEED_ACK_TIMEOUT_MS` | `10000` | How long an `acks = all` write waits for replication before a retryable 503 |
| `cluster.unclean_leader_election` | `EXSPEED_UNCLEAN_LEADER_ELECTION` | `false` | `true` lets a node outside the ISR take over (availability over durability) |
| `cluster.tls` | `EXSPEED_CLUSTER_TLS` | `false` | Serve and require TLS on the cluster port, with the `[tls]` certificate (which must then be set) |
| `cluster.tls_ca` | `EXSPEED_CLUSTER_TLS_CA` | the `[tls]` cert | Trust roots for peers' certificates |
| `connectors.offset_store` | `EXSPEED_CONNECTOR_OFFSET_STORE` | `log` | `log` (the `__connector_offsets` stream; replicates with the data) or `file`. See [connectors.md](connectors.md#offsets). |

Consumer state needs no backend: it lives in the internal `__consumers`
stream.

### ExQL (`[exql]`)

| File key | Env | Default | Description |
|----------|-----|---------|-------------|
| `exql.query_timeout_secs` | `EXSPEED_QUERY_TIMEOUT_SECS` | `30` | Bounded query timeout (min 1) |
| `exql.query_max_rows` | `EXSPEED_QUERY_MAX_ROWS` | `10000` | Rows a bounded query returns before the result is marked `truncated` |
| `exql.query_memory_mb` | `EXSPEED_QUERY_MEMORY_MB` | `512` | Memory pool shared by all bounded queries, in MiB (min 16); a query that needs more fails instead of spilling |
| `exql.query_partitions` | `EXSPEED_QUERY_PARTITIONS` | `1` | DataFusion target partitions for bounded queries (1–64) |
| `exql.checkpoint_ms` | `EXSPEED_EXQL_CHECKPOINT_MS` | `5000` | How often continuous queries checkpoint their state (min 100) |
| `exql.default_grace_ms` | `EXSPEED_EXQL_DEFAULT_GRACE_MS` | `0` | Allowed lateness for continuous queries that name no `GRACE PERIOD` |
| `exql.max_event_time_skew_ms` | `EXSPEED_EXQL_MAX_EVENT_TIME_SKEW_MS` | `86400000` | A `TIMESTAMP BY` value further than this ahead of the record timestamp (or before 1970) is replaced by the record timestamp |

### External database connections (ExQL)

| Variable | Description |
|----------|-------------|
| `EXSPEED_CONNECTION_<NAME>_DRIVER` | `postgres` (the only driver external tables support) |
| `EXSPEED_CONNECTION_<NAME>_URL` | Connection URL; `${VAR}` references are resolved from the environment |

A connection is registered only when both variables are set. Its name is
`<NAME>` lowercased, with `_` turned into `-` (`EXSPEED_CONNECTION_APP_DB_URL`
names `app-db`). Environment connections override ones of the same name in
`{data_dir}/connections.d/*.toml`, which override connections created over
HTTP. See [exql.md](exql.md#external-databases).

### Client CLI

| Variable | Description |
|----------|-------------|
| `EXSPEED_URL` | HTTP base URL (same as `--server`) |
| `EXSPEED_AUTH_TOKEN` | Bearer token |
| `EXSPEED_INSECURE_SKIP_VERIFY=1` | Skip TLS verification |
| `EXSPEED_HEALTHCHECK_URL` | Probe URL for `exspeed healthcheck` |

## Stream defaults

| Setting | Default | Set with |
|---------|---------|----------|
| Retention (age) | `7d` | `exspeed create --retention`, or `max_age_secs` over HTTP |
| Retention (size) | `10gb` | `--max-size`, or `max_bytes` over HTTP |
| Dedup window | `5m` (capped at the retention age) | `--dedup-window`, or `dedup_window_secs`; must not exceed the retention age |
| Dedup max entries | `500000` | `--dedup-max-entries`, or `dedup_max_entries` |
| Segment size | 256 MB | not configurable |
| Compaction | off | `compaction: true` when creating a stream over HTTP or TCP (SDK `compaction`). The CLI has no compaction flag. |
| Tombstone retention | `24h` | Not settable through the API or CLI; stored as `tombstone_retention_secs` in the stream's `stream.json` |

A background task on the leader enforces retention every 60 s (starting
10 s after it takes over) by deleting whole sealed segments from the front
of the log; followers mirror the trims through replication. The active
segment is never deleted. A stream whose `stream.json` is unreadable is
skipped (with a warning); the others are still enforced.

Compacted streams keep only the newest record per key in sealed segments,
and a background compactor visits them every 60 s. Records without a key
are never removed. A record with a key and an empty value is a tombstone
that deletes the key; the tombstone itself is removed after the tombstone
retention. Offsets are preserved, so compacted streams have offset gaps.
See
[architecture.md](architecture.md#storage-layout).

## Connector env substitution

Inside connector TOML files, `${VAR}` and `${VAR:-default}` are replaced
with values from the server's environment. See [connectors.md](connectors.md).
