# Operations

These are the settings and behaviours that matter when you run a single node
in production. The defaults suit most deployments; usually you only change
them for Kubernetes integration or capacity tuning.

For multi-pod deployments, see [high-availability.md](high-availability.md).
For every flag and environment variable, see [configuration.md](configuration.md).

## Contents

- [Docker](#docker)
- [Kubernetes (Helm)](#kubernetes-helm)
- [Structured logging](#structured-logging)
- [Connection cap](#connection-cap)
- [Open file limit](#open-file-limit)
- [Exclusive data-dir lock](#exclusive-data-dir-lock)
- [Graceful shutdown](#graceful-shutdown)
- [Startup failures](#startup-failures)
- [Consumers vs. retention](#consumers-vs-retention)
- [Consumer state durability](#consumer-state-durability)
- [`/healthz` vs `/readyz`](#healthz-vs-readyz)
- [Non-root container](#non-root-container)
- [Backup and restore](#backup-and-restore)
- [Metrics](#metrics)

## Docker

```bash
docker build -t exspeed .        # or use the published nayth/exspeed image

docker run -d --name exspeed \
  -p 5933:5933 -p 8080:8080 \
  -v exspeed-data:/var/lib/exspeed \
  nayth/exspeed:latest
```

To use a config file, mount it and point `EXSPEED_CONFIG` at it:

```bash
docker run -d --name exspeed -p 5933:5933 -p 8080:8080 \
  -v exspeed-data:/var/lib/exspeed \
  -v $PWD/exspeed.toml:/etc/exspeed/exspeed.toml:ro \
  -e EXSPEED_CONFIG=/etc/exspeed/exspeed.toml \
  nayth/exspeed:latest
```

The image runs `exspeed server --data-dir /var/lib/exspeed` and has a
`HEALTHCHECK` that runs `exspeed healthcheck`. That command exits 0 when
`/readyz` answers 200, and you can also use it in other probes. It finds
the server the way `exspeed server` resolves its settings (config file in
`EXSPEED_CONFIG`, then the environment): `/readyz` on the `api_bind` port
over loopback, `https` when a TLS certificate is configured. Change the
listeners with `EXSPEED_API_BIND` / the config file rather than command-line
flags (the probe can't see flags), or set `EXSPEED_HEALTHCHECK_URL` (or
`--url`) to probe a specific URL. Self-signed certificates are accepted.

The repository's `docker-compose.yml` starts Exspeed together with
Postgres, RabbitMQ, an S3-compatible store (moto), MySQL and SQL Server. It
is meant for developing connectors; see [development.md](development.md).

## Kubernetes (Helm)

The chart lives in [`deploy/helm/exspeed`](../deploy/helm/exspeed). By default it deploys
a single-node StatefulSet with these settings:

- a persistent volume
- `fsGroup: 1000`
- startup, readiness and liveness probes on `/readyz`
- a 60 s termination grace period
- `LOG_FORMAT=json`

```bash
helm install exspeed deploy/helm/exspeed \
  --set persistence.size=50Gi \
  --set auth.credentialsSecret=exspeed-credentials   # Secret with credentials.toml
```

These values cover the common cases:

| Value | Purpose |
|-------|---------|
| `auth.tokenSecret` | Secret with a shared admin token (key `token`) |
| `auth.credentialsSecret` | Secret with `credentials.toml` |
| `tls.secretName` | `kubernetes.io/tls` Secret; serves TLS on both listeners |
| `env` | Any server env var from [configuration.md](configuration.md) |
| `serviceMonitor.enabled` | Prometheus Operator scraping of `/metrics` |
| `replicas` | `>1` runs a cluster (needs `cluster.postgresUrlSecret` and `cluster.replicatorTokenSecret`; see [high-availability.md](high-availability.md)) |
| `cluster.acks`, `cluster.minInsyncReplicas` | Write durability in a cluster |

## Structured logging

```bash
LOG_FORMAT=json   # JSON lines, one event per line — for Loki / ELK / Cloud Logging
LOG_FORMAT=text   # default; human-readable, ANSI-coloured
```

Combine with `RUST_LOG` for level/target filtering. JSON output preserves spans and fields (`request_id`, `stream`, etc.) for downstream querying.

## Connection cap

```bash
EXSPEED_MAX_CONNS=1024   # default
```

Caps concurrent TCP connections to the broker port. When the cap is reached, new connections are accepted-then-immediately-closed; each rejection is logged and increments the `exspeed_connections_rejected_total` counter. Tune by watching that counter alongside `exspeed_connections_active`.

## Open file limit

The storage engine keeps every segment open for the life of the process:
two file descriptors per sealed segment (data + index) and about three per
stream for its active segment (reader, writer, index), plus one per client connection, connector and
replication link. Segments roll at 256 MiB by default, so a node holding
1 TiB has about 4,096 sealed segments, which is ~8,200 descriptors for
storage alone. Exspeed has no descriptor cache, so raise the limit
(`ulimit -n`, systemd `LimitNOFILE=`, Docker `--ulimit nofile=`; on
Kubernetes it comes from the container runtime) to at least
`2 × sealed segments + 3 × streams + max connections + headroom`. Running out
shows up as `Too many open files` errors on segment rolls and new
connections.

## Exclusive data-dir lock

`exspeed server` takes an exclusive `flock` on `{data_dir}/.exspeed.lock` at startup. A second process pointed at the same `data_dir` fails fast with a clear "already in use" error — no silent dual-writer corruption. The lock is released when the process exits (including crashes; the kernel frees the flock).

Do not delete the lockfile manually to "recover" — it's a TOCTOU footgun and not necessary. If the holding process is gone, the next start succeeds.

## Graceful shutdown

On `SIGTERM` or `SIGINT` the server shuts down in this order:

1. Stop accepting connections (TCP and HTTP) and end client sessions.
   In-flight requests get up to `server.drain_timeout_secs` (10 s); the
   HTTP server is waited for before storage closes.
2. Stop connectors. Sinks flush and commit, and sources finish their
   batch. Steps 2 to 5 share `server.stop_timeout_secs` (30 s), so a
   shutdown takes at most `drain_timeout_secs + stop_timeout_secs` (plus
   the final fsync).
3. Stop continuous queries. Each writes a final checkpoint.
4. Resign leadership. This stops consumers and retention, and waits for
   the consumers to persist their final state (ack floors, unacked records
   and delivery counts) while writes are still open. Then writes close,
   and in a cluster the lease is released so a follower takes over within
   one heartbeat interval.
5. Write the final dedup snapshot (single node).
6. Flush and fsync every partition, then release the data-dir lock.

In Kubernetes, set `terminationGracePeriodSeconds` above
`drain_timeout_secs + stop_timeout_secs` (40 s by default; 60 is a good
value) so the
kubelet doesn't `SIGKILL` the process partway through. The Helm chart does
this.

## Startup failures

`exspeed server` exits with an error, rather than running half-started, when
either listener (or, in a cluster, the cluster port) can't be bound (port in
use, bad address), a TLS file can't be loaded, the lease backend can't be
reached, or the connector or ExQL catalog can't be read. `/readyz` only
turns 200 after both listeners serve.

If a node wins the leader lease but can't start the leader's work (reloading
the ExQL or connector catalog, or starting consumers, fails), it steps down:
writes close, the lease is released so another node can lead, and the node
competes again after a hold-off that doubles per failed attempt (2 s up to
32 s). A single node simply retries after the hold-off. Each step-down is
logged at `error` and counted as
`exspeed_leader_transitions_total{direction="stepped_down"}`.

## Consumers vs. retention

If retention deletes records that a consumer has not reached yet, the consumer skips ahead to the earliest record still retained. It logs a warning and counts the skipped records in its stats (`stats.skipped` in consumer info). Size retention and consumer lag together so this doesn't happen.

Size your retention with your slowest expected consumer in mind. Metrics of interest are `exspeed_consumer_lag` and `exspeed_storage_bytes`.

## Consumer state durability

Consumer state is stored in the internal, compacted stream `__consumers`,
with one snapshot record per consumer. It therefore gets the same
durability as your data, and in a cluster it replicates with the log.

Snapshots are written at most every 100 ms while state changes, and once
more on graceful shutdown. A crash (not a clean shutdown) can lose up to
100 ms of acks. Those records are redelivered, which the at-least-once
contract already allows.

## `/healthz` vs `/readyz`

| Endpoint | Returns 200 when | Recommended use |
|---|---|---|
| `/healthz` | This pod is the cluster leader | LB traffic routing (only the leader serves traffic — see [high-availability.md](high-availability.md)) |
| `/readyz` | Startup complete (both listeners serving), the leader's dedup maps rebuilt, **and** `data_dir` is writable | k8s `readinessProbe` and startup gates |

Single-node deployments still benefit from `/readyz`. The HTTP API isn't
served until storage recovery and catalog loading finish (a probe waits or
times out), and `/readyz` then answers `503 {"status":
"dedup_rebuild_in_progress"}` until the dedup maps are rebuilt, so an LB or
systemd unit knows when the broker is actually serving. Other 503 bodies
are `{"status": "starting"}` and `{"status": "data_dir_unwritable"}`.

A stream whose partition is fenced (read-only after an IO error that couldn't
be rolled back) does **not** make the node unready, since that would take
every healthy stream out of service too. Instead `/readyz` answers
`200 {"status": "degraded", "failed_streams": [{"stream", "reason"}]}`,
`GET /api/v1/streams/{name}` shows `"status": "failed"` with the `failure`
reason, and `exspeed_partition_failed{stream}` is 1. Alert on that gauge;
writes to the stream fail until a restart runs recovery.

## Non-root container

The published Docker image runs as `uid 1000` (user `exspeed`, login shell `nologin`). For Kubernetes with a mounted PV:

```yaml
spec:
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000          # so the PV is writable by uid 1000
  containers:
    - name: exspeed
      image: nayth/exspeed:latest
      ...
```

Without `fsGroup`, the volume mount may be owned by root and the broker will fail at startup (the `flock` and segment writes both need write access).

## Backup and restore

### Online backup

`exspeed backup` downloads a backup from a running server. The server keeps
accepting writes while the backup streams:

```bash
exspeed backup --url http://exspeed:8080 --token "$ADMIN_TOKEN" \
  --output exspeed-$(date +%F).tar
```

It calls `GET /api/v1/backup`, which needs a **global admin** credential
when auth is on and is answered by the leader only. The response is an
uncompressed tar archive (pipe it through `gzip` or `zstd` if you want). The
CLI writes it to `<output>.partial`, reads the whole archive back to check
it is complete, and only then renames it to `<output>`. If the connection
drops, nothing is left at `<output>`.

The archive holds:

| Entry | Contents |
|-------|----------|
| `exspeed-backup.json` | Manifest, always first: `format`, `version` (1), `server_version`, `created_at`, and per stream `name`, `earliest_offset`, `next_offset`, `records`, `bytes` |
| `streams/<name>/stream.json` | Stream settings (retention, dedup, compaction) |
| `streams/<name>/partitions/0/*.seg`, `*.idx`, `*.meta` | Segments with their indexes and metadata |
| `connectors/`, `connectors.d/`, `connector-offsets/`, `connections/`, `connections.d/`, `exql/` | Configuration directories, when present |

It does not hold `credentials.toml`, `exspeed.toml`, the dedup snapshots
(the dedup map is rebuilt from the restored log at startup) or replication
state. Back up your credentials and config file separately. Connector and
connection configs can contain database URLs and passwords, so treat backup
files as secrets.

### Consistency guarantees

- **Per stream, point in time.** When the backup starts, the server reads
  each stream's high watermark `H` (the offset the next record gets) and
  includes exactly the records below it: every record in
  `[earliest_offset, next_offset)` of the manifest, byte for byte. Records
  appended after that are not in the backup. A record that was not yet
  visible to readers (in `sync` mode, one not yet fsynced) is never
  included.
- **Not across streams.** Streams are snapshotted one after another, all
  before the first byte is sent, so the snapshots are milliseconds apart.
  There is no atomic cut across streams: if your application writes to
  `orders` and then to `invoices`, the backup can hold the invoice without
  the order, or the order without the invoice.
- **Progress lags data, never leads it.** Internal streams (`__consumers`
  with consumer ack floors, `__connector_offsets`, `__exql_ckpt_*` query
  checkpoints) are snapshotted before the other streams. After a restore,
  consumers, sink connectors and continuous queries can redeliver or
  reprocess records written just before the backup, but never skip any.
  Query output and sources that use idempotency keys are deduplicated when
  they replay within the stream's dedup window. The file-based connector
  offset store (`connector-offsets/`, `[connectors] offset_store = "file"`)
  is copied after the streams and has no such guarantee.
- **Configuration directories** are copied file by file when the archive
  reaches them, after all streams.
- **Retention and compaction keep running.** Segments that retention deletes
  or compaction rewrites after the snapshot are still read through open file
  handles, so their disk space is freed only when the backup finishes.
- A backup that collides with a truncation (`truncate_from`, which only a
  replication follower repairing a divergent log does) fails instead of
  producing a mixed copy; retry it.

### Restore

Restore is offline. Stop the server (or use a new data directory), then:

```bash
exspeed restore --input exspeed-2026-10-03.tar --data-dir /var/lib/exspeed
exspeed server --data-dir /var/lib/exspeed
```

`restore` takes the data-directory lock, so it refuses to run while a server
uses the directory. It refuses a non-empty data directory unless you pass
`--force`. With `--force` it replaces `streams/` and the configuration
directories, and keeps other files such as `credentials.toml`,
`exspeed.toml`, `node_id` and `cluster/`. The archive is unpacked into a
staging directory and checked before anything is replaced:

- the manifest must come first, with a supported format and version;
- the archive may contain only `streams/` and the configuration directories,
  with no absolute paths, `..` components or links;
- every stream must open, and its offsets must match the manifest exactly.

Each restored stream continues at its manifest `next_offset`: the next
record appended gets that offset. Segment indexes come from the archive, so
startup does not rescan restored segments.

### Offline snapshot

`exspeed snapshot` archives a stopped server's data directory as-is (it
takes the data-directory lock, so the server must be stopped):

```bash
exspeed snapshot --data-dir /var/lib/exspeed --output exspeed-$(date +%F).tar.gz
```

To restore it, extract it into an empty data directory. Unlike `exspeed
backup`, it includes everything in the directory, credentials included.

### Clusters

Back up from the leader (followers answer `GET /api/v1/backup` with 503).
The archive holds no node identity, epoch histories or lease state. To
rebuild a cluster from it, restore into an empty data directory for one
node, start that node first so it takes the lease, then start the other
nodes with empty data directories. They replicate everything from it.

## Metrics

`GET /metrics` serves Prometheus text. It is open unless `[server]
metrics_token` is set; then it needs `Authorization: Bearer <token>` (see
[security.md](security.md#metrics-token)).

Every series is named `exspeed_*`, and counters end in `_total` exactly once.
Series that describe a stream, consumer, connector or continuous query are
removed when it is deleted.

| Area | Series |
|------|--------|
| Process | `exspeed_uptime_seconds`, `exspeed_connections_active`, `exspeed_connections_rejected_total` |
| Streams | `exspeed_records_published_total{stream}`, `exspeed_publish_latency_seconds{stream}` (histogram), `exspeed_storage_bytes{stream}`, `exspeed_storage_write_errors_total{stream,kind}`, `exspeed_partition_failed{stream}` |
| Consumers | `exspeed_consumer_lag{stream,consumer}` (leader only), `exspeed_consumer_dead_letters_total{consumer,outcome}` |
| Dedup | `exspeed_dedup_writes_total{stream,result}`, `exspeed_dedup_collisions_total{stream}`, `exspeed_dedup_map_full_total{stream}`, `exspeed_dedup_map_entries{stream}`, `exspeed_dedup_window_secs{stream}`, `exspeed_dedup_rebuild_duration_seconds{stream,source}`, `exspeed_dedup_snapshot_write_duration_seconds` |
| Leadership | `exspeed_is_leader`, `exspeed_leader_transitions_total{direction}` (`acquired`, `lost`, `stepped_down`, `resigned`), `exspeed_lease_held{name}`, `exspeed_lease_acquire_total{name,result}`, `exspeed_lease_lost_total{name}` |
| Replication | `exspeed_replication_*` (see [high-availability.md](high-availability.md#observability)) |
| Connectors | `exspeed_connector_*` (see [connectors.md](connectors.md)) |
| ExQL | `exspeed_exql_late_records_total{query}` |
| Auth | `exspeed_auth_denied_total{reason,transport,op}`: `op` is the TCP opcode or the HTTP route template (`/api/v1/streams/{name}`) |

`exspeed_consumer_lag` is the number of records at or after the consumer's
ack floor: the stream's end offset minus the lowest unacknowledged offset.
A consumer that has acknowledged everything has lag 0.
