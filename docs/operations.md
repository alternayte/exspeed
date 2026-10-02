# Operations

These are the settings and behaviours that matter when you run a single node
in production. The defaults suit most deployments; usually you only change
them for Kubernetes integration or capacity tuning.

For multi-pod deployments, see [high-availability.md](high-availability.md).
For every flag and environment variable, see [configuration.md](configuration.md).

## Contents

- [Docker](#docker)
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

The image runs `exspeed server --data-dir /var/lib/exspeed`. It has no
`HEALTHCHECK`, so probe `GET /readyz` from your orchestrator.

The repository's `docker-compose.yml` starts Exspeed together with
Postgres, RabbitMQ, MinIO, MySQL and SQL Server. It is meant for developing
connectors; see [development.md](development.md).

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

On `SIGTERM` or `SIGINT` the server stops accepting new TCP connections, waits up to **10 seconds** for in-flight connections to drain, then exits. The HTTP listener and background tasks are cancelled in the same window.

> ⚠️ **Shutdown is not fully graceful yet.**
>
> - Connectors and continuous queries are aborted mid-batch. Their
>   at-least-once checkpoints make this safe, but they may reprocess
>   records after restart.
> - The final dedup snapshot may not finish writing.
> - In multi-pod mode the leader lease is not released, so failover waits
>   the full lease TTL even on a clean shutdown.
>
> See [REVIEW.md §3.7](REVIEW.md#37-server-http-api-auth).

For Kubernetes, set `terminationGracePeriodSeconds: 30` (or higher) on the pod so the kubelet doesn't `SIGKILL` the process before the drain completes.

## Consumers vs. retention

If a consumer's offset falls behind the retention window (age or size), the broker returns `StorageError::OffsetOutOfRange` on its next read and **terminates the subscription** — it does not silently jump forward to the first surviving record, which would look like successful consumption of data that was actually lost.

The delivery task logs a warning with `requested` and `earliest` offsets so operators can spot it in log aggregation. To recover, the application must explicitly re-seek — typically to the earliest available offset, or to a business-meaningful point. Naïve auto-reconnect from the lost offset will hit the same error; clients should treat this signal as "your position is gone, choose where to resume."

Operationally: size your retention with your slowest expected consumer in mind. Metrics of interest are `consumer_lag` and `storage_bytes`.

## Consumer state durability

Consumer offsets are persisted atomically (tempfile + rename + parent-dir fsync). If you see stray `*.json.tmp` files in `{data_dir}/consumers/` at startup, they're the remnants of a crashed save and are safely ignored by the loader.

> ⚠️ Ack-driven saves are debounced by about 100 ms, so a crash can replay
> recent acks. Known races can resurrect a deleted consumer or overwrite a
> `Seek`; see [REVIEW.md §3.3](REVIEW.md#33-broker-delivery-consumers-dedup).

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
