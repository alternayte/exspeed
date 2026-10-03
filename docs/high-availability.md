# High availability (multi-pod)

> ❌ **Not production-safe in v0.5.** A failover can lose writes and leave
> data inconsistent:
>
> - **Writes that never replicate.** Only single-record TCP publishes are
>   replicated live. Batch publishes, HTTP publishes, webhooks, connector
>   output, ExQL output and DLQ writes reach followers only when a follower
>   reconnects and catches up, and catch-up can wedge.
> - **Keys are lost.** Record keys are not replicated, so they are gone
>   after a failover.
> - **No write fencing.** A demoted leader keeps accepting writes on open
>   TCP connections. Followers accept TCP and webhook writes directly.
> - **Consumer state isn't refreshed on promotion.**
>
> Treat this mode as a warm standby you would fail over to by hand, and
> expect data loss. The analysis is in [REVIEW.md §3.4](REVIEW.md#34-ha-leadership--replication)
> and the redesign (epoch-fenced log replication) in [§5.4](REVIEW.md#54-ha-kafka-style-log-replication-with-epochs).

Exspeed supports **hot-standby multi-pod** via a single cluster-leader
lease. Running N broker pods means N identical pods; exactly one is the
**leader** at any moment and serves all traffic. Standbys are silent
until failover. If the leader crashes, a survivor takes over within the
lease TTL.

## A health-check-aware load balancer is REQUIRED

Standby pods return `503` on every `/api/v1/*` endpoint except
`/api/v1/leases` and `/api/v1/whoami`. The TCP port (5933) and `/webhooks/*`
are **not** gated on standbys: writes sent there land on the standby and
are later discarded. Without a probe-aware LB, consumers connecting to a
standby will see 503s on ~(N−1)/N of their requests. **This is a
deployment misconfiguration, not a bug.**

## Requirements

1. **A shared lease backend.** `EXSPEED_LEASE_BACKEND=postgres` or `=redis`
   (the old name `EXSPEED_CONSUMER_STORE` still works, with a warning).
   Without one the server runs single-node and says so on every boot.
   Consumer state needs no shared store: it lives in the internal
   `__consumers` stream and replicates with the log.

2. **One `data_dir` per pod.** The data-dir `flock` guarantees exclusive
   access. Multi-pod does NOT mean shared storage — each pod owns its own
   streams. Typical deployment: N identical pods, each with its own PV /
   local disk.

3. **A probe-aware LB in front of both HTTP (8080) and TCP (5933).** k8s
   `Service` + `readinessProbe` handles this natively — only pods passing
   the probe receive traffic on any port.

## k8s deployment (recommended)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: exspeed
spec:
  selector: { app: exspeed }
  ports:
    - name: api
      port: 8080
      targetPort: 8080
    - name: tcp
      port: 5933
      targetPort: 5933
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: exspeed
spec:
  replicas: 2
  selector:
    matchLabels: { app: exspeed }
  serviceName: exspeed
  template:
    metadata:
      labels: { app: exspeed }
    spec:
      containers:
        - name: exspeed
          image: exspeed:latest
          ports:
            - containerPort: 8080
            - containerPort: 5933
          env:
            - name: EXSPEED_LEASE_BACKEND
              value: postgres
            - name: EXSPEED_OFFSET_STORE_POSTGRES_URL
              valueFrom: { secretKeyRef: { name: pg, key: url } }
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            periodSeconds: 5
            failureThreshold: 2
            successThreshold: 1
          volumeMounts:
            - name: data
              mountPath: /var/lib/exspeed
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: { requests: { storage: 10Gi } }
```

## nginx (active health check; nginx Plus or a compatible module)

```nginx
upstream exspeed {
    server exspeed-0:8080;
    server exspeed-1:8080;
    health_check uri=/healthz interval=5s fails=2 passes=1;
}
```

## HAProxy

```
backend exspeed
    option httpchk GET /healthz
    http-check expect status 200
    default-server check inter 5s fall 2 rise 1
    server pod0 exspeed-0:8080
    server pod1 exspeed-1:8080
```

## Failover timing

| Scenario | Time to failover |
|---|---|
| Leader crashes (SIGKILL / OOM) | ≤ TTL + TTL/3 + probe_interval ≈ **30–40s** |
| Leader SIGTERM (graceful) | ≤ TTL/3 + probe_interval ≈ **5–15s** |
| Backend partition (heartbeat fails) | ~20s to detect, then failover per above |

## Tuning

```bash
EXSPEED_LEASE_TTL_SECS=30         # default 30
EXSPEED_LEASE_HEARTBEAT_SECS=10   # default 10 (~TTL/3)
```

Shorter TTL = faster failover + more chatty backend traffic. Longer TTL
= slower failover + less traffic.

## Operator visibility

- `GET /healthz` — 200 if this pod is the leader, 503 otherwise. Public.
- `GET /metrics` — Prometheus. Public. Includes `exspeed_is_leader`,
  `exspeed_leader_transitions_total{direction}`, and the existing
  `exspeed_lease_*` series (`name="cluster:leader"`).
- `GET /api/v1/leases` — bearer-authed; returns the single
  `cluster:leader` row. Available on any pod (leader and standby) so
  operators can discover who's in charge from anywhere.
- Postgres backend: `SELECT * FROM exspeed_leases WHERE name = 'cluster:leader';`

## Replication

In multi-pod mode Exspeed runs **asynchronous follower-pull replication**:
every non-leader pod mirrors the leader's `data_dir` over a persistent
TCP session on port 5934. When the leader dies, the surviving pod that
wins the lease already has an up-to-date copy of every stream, so
failover is data-preserving (subject to the RPO below). There is no
manual operator work between failover and serving traffic — the new
leader starts accepting writes as soon as `/healthz` returns 200.

### RPO (data loss on crash)

Writes are acknowledged when they hit the leader's local disk — the
leader does not wait for a follower to apply the record before
responding. The window between "leader acks" and "follower applies" is
reported as `exspeed_replication_lag_seconds` + `exspeed_replication_lag_records`
(the latter is best-effort; `lag_seconds` is the primary signal).
If the leader crashes with `lag > 0` at the moment of death, those
records can be lost on promotion. Mitigations:

- **Keep lag low.** Alert on `exspeed_replication_lag_seconds{stream=~".*"} > 10`.
- **Idempotent publishes do *not* help across failover yet.** Dedup state
  is not replicated or rebuilt on promotion, so a retry that lands on the
  new leader is written again.

### RTO (time-to-serve)

~30-40s for an unclean leader death (lease
TTL + retry slack + probe flip) and ~5-15s for a graceful SIGTERM. The
new leader's storage is already warm, so there's no data-reload step.

### Required replicator credential

The follower side of the handshake authenticates with a bearer token
carrying `Action::Replicate`. Declare it in your `credentials.toml`:

```toml
[[credentials]]
name = "replicator"
token_sha256 = "<sha256 of the bearer>"
permissions = [
  { streams = "*", actions = ["replicate"] },
]
```

Then point every pod at the bearer via `EXSPEED_REPLICATOR_CREDENTIAL`.
Startup hard-fails in multi-pod mode if this env var is unset — the
follower cannot authenticate without it.

### Tuning

| Env var | Default | Purpose |
|---|---|---|
| `EXSPEED_CLUSTER_BIND` | `0.0.0.0:5934` | Leader-side listener for follower sessions. |
| `EXSPEED_CLUSTER_ADVERTISE` | same as bind | What the leader writes into the `cluster:leader` lease row as its replication endpoint. Set when the listen address differs from what peers should dial (NAT / k8s pod-IP vs service-IP). |
| `EXSPEED_REPLICATION_BATCH_RECORDS` | 1000 | Max records per `RecordsAppended` frame. Smaller = lower latency, larger = better throughput. |
| `EXSPEED_REPLICATION_HEARTBEAT_SECS` | 5 | Leader-side keepalive cadence when no records are flowing. Paired with the 30s follower idle timeout (6× ratio) — a single dropped heartbeat does not tear a session down. |
| `EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS` | 30 | Follower tears the session down if it receives nothing for this long. |
| `EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS` | 100000 | Leader's per-follower mpsc queue capacity. Bigger = more memory per stuck follower, smaller = earlier drops under backpressure. |

### Seeding a large initial replica

The wire protocol streams every historical record from offset 0 on
first connect, which is fine for tens of millions of small records but
slow for TB-scale datasets. For those, rsync or snapshot the leader's
`data_dir` to the new pod offline, start the new pod pointed at the
seeded dir, and let the replication session pick up from the tail.
There's no manifest-fingerprint verification — the follower's cursor
is authoritative about where it left off.

### Metrics

- `exspeed_replication_role{role="leader|follower|standalone"}` — gauge set to 1 for the current role, 0 otherwise.
- `exspeed_replication_connected_followers` — gauge; count of active follower sessions on the leader.
- `exspeed_replication_lag_seconds{stream}` — gauge; seconds between leader's latest write and follower's last-applied record. **Primary indicator** — alert on `exspeed_replication_lag_seconds{stream=~".*"} > 10`.
- `exspeed_replication_lag_records{stream}` — gauge; same idea, in records. Best-effort — reflects offset-lag at the moment of the last applied batch, not the live tail; `lag_seconds` is the more reliable signal.
- `exspeed_replication_records_applied_total{stream}` — counter; records applied on the follower.
- `exspeed_replication_bytes_total{direction="in|out"}` — counter; bytes over the replication socket.
- `exspeed_replication_truncated_records_total{stream}` — counter; records truncated from the follower's local storage during divergent-history reconciliation.
- `exspeed_replication_reseed_total{stream}` — counter; streams wiped + rebuilt because the follower fell behind the leader's retention window.
- `exspeed_replication_follower_queue_drops_total` — counter; records dropped by the leader when a follower's queue was full.
- `exspeed_auth_denied_total{action="replicate"}` — counter; replication handshakes rejected for missing `Action::Replicate`.

> **Prometheus suffix quirk.** Scrapers observe counters here with a doubled `_total` suffix (e.g. `exspeed_replication_truncated_records_total_total`, `exspeed_replication_records_applied_total_total`). This is a known OTel-to-Prometheus exporter behaviour — it appends `_total` to counter names, including ones that already end in `_total`. Write PromQL and alert rules against the doubled name.

### Running the ignored replication integration tests

The five Postgres-backed replication tests are `#[ignore]`d by default because they require a live Postgres. To run them locally:

```bash
docker compose up -d postgres
EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://testuser:testpass@localhost:5432/testdb \
  cargo test -p exspeed -- --ignored --nocapture
```

### Operator endpoints

- `GET /api/v1/leases` — existing endpoint; now includes a
  `replication_endpoint` field on the `cluster:leader` row so operators
  (and followers) can see where to dial.
- `GET /api/v1/cluster/followers` — leader-only, admin-bearer-gated.
  Returns a list of currently-connected followers with `follower_id` +
  `registered_at`. Returns 503 on single-pod pods with an explicit
  `hint` string pointing at `EXSPEED_LEASE_BACKEND`.

### Known limitation: TCP publish leader-gate

Clients should connect only to the leader's port 5933 via the probe-aware
Service — the readiness probe only routes to the pod whose `/healthz`
returns 200. A TCP client that bypasses the Service and connects
directly to a follower's port 5933 to publish will succeed: the write
lands on the follower's local storage and is overwritten by the
divergent-history truncation on the next replication handshake cycle.
Tracked for a future release.

### k8s deployment with replication

Extend the Service + StatefulSet example above to expose 5934:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: exspeed
spec:
  selector: { app: exspeed }
  ports:
    - name: api
      port: 8080
      targetPort: 8080
    - name: tcp
      port: 5933
      targetPort: 5933
    - name: cluster
      port: 5934
      targetPort: 5934
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: exspeed
spec:
  replicas: 2
  selector:
    matchLabels: { app: exspeed }
  serviceName: exspeed
  template:
    metadata:
      labels: { app: exspeed }
    spec:
      containers:
        - name: exspeed
          image: exspeed:latest
          ports:
            - containerPort: 8080
            - containerPort: 5933
            - containerPort: 5934
          env:
            - name: EXSPEED_LEASE_BACKEND
              value: postgres
            - name: EXSPEED_OFFSET_STORE_POSTGRES_URL
              valueFrom: { secretKeyRef: { name: pg, key: url } }
            - name: EXSPEED_REPLICATOR_CREDENTIAL
              valueFrom: { secretKeyRef: { name: replicator, key: token } }
            - name: EXSPEED_CLUSTER_BIND
              value: "0.0.0.0:5934"
            - name: EXSPEED_CLUSTER_ADVERTISE
              value: "$(POD_NAME).exspeed.$(POD_NAMESPACE).svc.cluster.local:5934"
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            periodSeconds: 5
            failureThreshold: 2
            successThreshold: 1
```

## What's still not in v1

- **Synchronous replication.** Every ack is local-disk; the RPO window
  is non-zero on crash. There's no `wait_for_quorum` knob.
- **Per-stream replication factor.** Every stream replicates to every
  follower. You can't mark a stream as "leader-only" or "2/3 replicas".
- **Raft / consensus writes.** Lease coordination is single-key; there
  is no multi-stage write commit. A split-brain scenario with a
  partitioned lease backend is recoverable via divergent-history
  truncation, not prevented.
- **Geo / WAN replication.** The replication protocol assumes a
  low-RTT network between pods. Running followers across regions
  works mechanically but lag alerts will fire continuously.
- **Catastrophic S3-only restore.** There is no "restore from object
  storage" path independent of a live follower. Sink connectors to S3
  provide an archive, but restoring a stream from that archive into
  a new cluster is a manual operator task.
