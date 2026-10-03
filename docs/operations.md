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
`/readyz` answers 200, and you can also use it in other probes.

The repository's `docker-compose.yml` starts Exspeed together with
Postgres, RabbitMQ, MinIO, MySQL and SQL Server. It is meant for developing
connectors; see [development.md](development.md).

## Kubernetes (Helm)

The chart lives in [`deploy/helm/exspeed`](../deploy/helm/exspeed). By default it deploys
a single-node StatefulSet with these settings:

- a persistent volume
- `fsGroup: 1000`
- startup, readiness and liveness probes on `/readyz`
- a 30 s termination grace period

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

Caps concurrent TCP connections to the broker port. When the cap is reached, new connections are accepted-then-immediately-closed; each rejection is logged and increments the `connections_rejected_total` counter (scraped as `connections_rejected_total_total`). Tune by watching that counter alongside `connections_active`.

## Exclusive data-dir lock

`exspeed server` takes an exclusive `flock` on `{data_dir}/.exspeed.lock` at startup. A second process pointed at the same `data_dir` fails fast with a clear "already in use" error — no silent dual-writer corruption. The lock is released when the process exits (including crashes; the kernel frees the flock).

Do not delete the lockfile manually to "recover" — it's a TOCTOU footgun and not necessary. If the holding process is gone, the next start succeeds.

## Graceful shutdown

On `SIGTERM` or `SIGINT` the server shuts down in this order:

1. Stop accepting connections (TCP and HTTP) and end client sessions.
   In-flight requests get up to `server.drain_timeout_secs` (10 s); the
   HTTP server is waited for before storage closes.
2. Stop connectors. Sinks flush and commit, and sources finish their
   batch, within 30 s.
3. Resign leadership. This stops consumers, continuous queries and
   retention. In a cluster it releases the lease so a follower takes over
   within one heartbeat interval.
4. Wait for consumers to persist their final state: ack floors, unacked
   records and delivery counts.
5. Write the final dedup snapshot.
6. Flush and fsync every partition, then release the data-dir lock.

In Kubernetes, set `terminationGracePeriodSeconds` to at least 60 so the
kubelet doesn't `SIGKILL` the process partway through. The Helm chart does
this.

## Startup failures

`exspeed server` exits with an error, rather than running half-started, when
either listener can't be bound (port in use, bad address), a TLS file can't
be loaded, or the connector or ExQL catalog can't be read. `/readyz` only
turns 200 after both listeners serve.

If a node wins the leader lease but can't start the leader's work (reloading
the ExQL or connector catalog, or starting consumers, fails), it steps down:
writes close, the lease is released so another node can lead, and the node
competes again after a hold-off that doubles per failed attempt (1 s up to
32 s). A single node simply retries after the hold-off. Each step-down is
logged at `error` and counted as
`exspeed_leader_transitions_total{direction="stepped_down"}`.

## Consumers vs. retention

If retention deletes records that a consumer has not reached yet, the consumer skips ahead to the earliest record still retained. It logs a warning and counts the skipped records in its stats (`stats.skipped` in consumer info). Size retention and consumer lag together so this doesn't happen.

Size your retention with your slowest expected consumer in mind. Metrics of interest are `consumer_lag` and `storage_bytes`.

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
| `/readyz` | Startup complete **and** `data_dir` is writable | k8s `readinessProbe` and startup gates |

Single-node deployments still benefit from `/readyz` — it stays 503 during storage recovery and connector startup, so an LB or systemd unit knows when the broker is actually serving.

## Non-root container

The published Docker image runs as `uid 1000` (no shell, no home dir). For Kubernetes with a mounted PV:

```yaml
spec:
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000          # so the PV is writable by uid 1000
  containers:
    - name: exspeed
      image: exspeed:latest
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
  visible to readers (not yet fsynced in sync mode, or above the replication
  floor in multi-pod mode) is never included.
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
  they replay within the stream's dedup window. The legacy file-based
  connector offset store (`connector-offsets/`) is copied after the streams
  and has no such guarantee.
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
`--force`. With `--force` it replaces `streams/`, the configuration
directories and the replication state, and keeps other files such as
`credentials.toml` and `exspeed.toml`. The archive is unpacked into a
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
rebuild a cluster from it, restore into one node's data directory, start
that node first so it takes the lease, then start the other nodes with
empty data directories. They replicate everything from it.

## Metrics

`GET /metrics` serves Prometheus text with no authentication.

> ⚠️ **Metric names are inconsistent today.** Some series have the
> `exspeed_` prefix and some don't. The OTel exporter also appends `_total`
> to counters whose names already end in `_total`. The names below are what
> a scrape actually returns.

| Area | Series |
|------|--------|
| Connections | `connections_active`, `connections_rejected_total_total` |
| Throughput | `records_published_total`, `records_consumed_total` |
| Streams | `storage_bytes{stream}` |
| Consumers | `consumer_lag{stream,consumer}`, `subscription_queue_fill_ratio` |
| Dedup | `exspeed_dedup_collisions_total_total`, `exspeed_dedup_map_full_total_total`, `exspeed_dedup_rebuild_duration_seconds` |
| Leadership | `exspeed_is_leader`, `exspeed_leader_transitions_total_total`, `exspeed_lease_*` |
| Replication | `exspeed_replication_*` (see [high-availability.md](high-availability.md#metrics)) |
| Auth | `exspeed_auth_denied_total_total` |

Two of these series are unreliable:

- **Consumer lag** is off by one, because it counts the last acked record.
- **`auth_denied`** is labelled with the raw request path, so its
  cardinality is unbounded.
