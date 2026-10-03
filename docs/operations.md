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
- [Consumers vs. retention](#consumers-vs-retention)
- [Consumer state durability](#consumer-state-durability)
- [`/healthz` vs `/readyz`](#healthz-vs-readyz)
- [Non-root container](#non-root-container)
- [Backups](#backups)
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
| `replicas` | `>1` enables multi-pod mode (experimental — needs `cluster.*` secrets) |

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

1. Stop accepting connections and end client sessions. In-flight requests
   get up to `server.drain_timeout_secs` (10 s).
2. Stop connectors. Sinks flush and commit, and sources finish their
   batch, within 30 s.
3. Resign leadership. This stops consumers, continuous queries and
   retention, and in multi-pod mode it deletes the lease row so a standby
   takes over at once.
4. Wait for consumers to persist their final state: ack floors, unacked
   records and delivery counts.
5. Write the final dedup snapshot.
6. Flush and fsync every partition, then release the data-dir lock.

In Kubernetes, set `terminationGracePeriodSeconds` to at least 60 so the
kubelet doesn't `SIGKILL` the process partway through. The Helm chart does
this.

## Consumers vs. retention

If retention deletes records that a consumer has not reached yet, the consumer skips ahead to the earliest record still retained. It logs a warning and counts the skipped records in its stats (`stats.skipped` in consumer info). Size retention and consumer lag together so this doesn't happen.

Size your retention with your slowest expected consumer in mind. Metrics of interest are `consumer_lag` and `storage_bytes`.

## Consumer state durability

Consumer state is stored in the internal, compacted stream `__consumers`,
with one snapshot record per consumer. It therefore gets the same
durability as your data, and in multi-pod mode it replicates with the
log.

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

## Backups

```bash
# The server must be stopped: snapshot takes the same data-dir lock.
exspeed snapshot --data-dir /var/lib/exspeed --output exspeed-$(date +%F).tar.gz
```

To restore, extract the archive into an empty data directory and start the
server on it. There is no online backup yet.

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
