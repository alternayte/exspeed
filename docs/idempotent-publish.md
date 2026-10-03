# Idempotent publish

Exspeed supports retry-safe (idempotent) publishes. You attach an ID to a
publish in one of these ways:

- the `msg_id` field on the TCP `PUBLISH` frame
- the `msg_id` field in the HTTP publish body
- the `--msg-id` CLI flag
- the `x-idempotency-key` header, kept for compatibility with existing connectors

> ⚠️ **Known gaps (v0.5).** These are tracked in [REVIEW.md §3.3](REVIEW.md#33-broker-delivery-consumers-dedup).
>
> - **During startup.** Dedup is not enforced until the startup rebuild of
>   the dedup map finishes. Only `/readyz` waits for the rebuild; TCP
>   publishes are accepted before it completes.
> - **Batch publish.** `PublishBatch` writes duplicates that occur within
>   the same batch, and it ignores `dedup_max_entries`.
> - **Deleted streams.** Deleting a stream doesn't clear its dedup map, so a
>   re-created stream can answer `duplicate: true` for a record that
>   doesn't exist.
> - **Failover.** Dedup state is not carried across failover, so retries
>   after a leader change are not deduplicated.
> - **Webhooks.** Webhook writes bypass dedup.

## Semantics

**First-body-wins.** If a client publishes `msg_id=X` with body `A`, a retry with the same `msg_id=X` and the same body `A` receives `PublishOk { duplicate: true, offset: <original> }` — the original offset is returned and no new record is written. If the retry arrives with a *different* body `B` (a likely bug in the caller), the server responds with a `KeyCollision` error that includes the offset of the first write. When the per-stream dedup map is at capacity and an eviction cannot free a slot, the server responds with `DedupMapFull { retry_after_secs }`, which is a retryable condition (try again after the window expires). Messages published without a `msg_id` bypass the dedup engine entirely — they are always written immediately and are unaffected by a full dedup map.

## CLI configuration

```bash
# Create a stream with a 10-minute dedup window and 2M-entry cap
exspeed create orders --dedup-window 10m --dedup-max-entries 2000000

# Update an existing stream's dedup window to 30 minutes
exspeed update-stream orders --dedup-window 30m
```

## Defaults

| Setting | Default | Minimum |
|---------|---------|---------|
| `dedup_window` | `5m` (300 s) | `1 s` (must be ≤ retention) |
| `dedup_max_entries` | `500_000` | `1` |

## Memory and on-disk cost

Each dedup entry stores a `msg_id` string (variable), an offset (8 bytes), a timestamp (8 bytes), and a body hash (8 bytes). At the default 500,000-entry cap with average 32-byte `msg_id` strings the in-memory footprint is approximately **150 MB per stream** worst case. On disk, each stream maintains a `dedup_snapshot.bin` file in its stream directory. The snapshot is written every 60 seconds and on graceful shutdown, and is included in `exspeed snapshot` offline backups. Online backups (`exspeed backup`) leave it out; after `exspeed restore` the map is rebuilt from the restored log (a full scan of the dedup window at startup).

Storage layout with dedup:

```
exspeed-data/
  streams/
    orders/
      stream.json           Stream config (retention, dedup_window, dedup_max_entries)
      dedup_snapshot.bin    Periodic dedup-map snapshot — restored on restart
      partitions/0/
        00000000000000000000.seg
        00000000000000000000.idx
```

## Prometheus alerts

```yaml
# Any body-collision is a producer bug — alert immediately.
- alert: ExspeedDedupCollision
  expr: rate(exspeed_dedup_collisions_total[5m]) > 0
  for: 1m
  annotations:
    summary: "Exspeed dedup key collision — same msg_id published with different body"

# Sustained cap hits mean the window is too long or throughput exceeds the cap.
- alert: ExspeedDedupMapFull
  expr: rate(exspeed_dedup_map_full_total[5m]) > 0.1
  for: 10m
  annotations:
    summary: "Exspeed dedup map full — increase dedup_max_entries or shorten dedup_window"

# Full-scan rebuilds at startup are expected only when the snapshot is missing.
# A p95 > 30s indicates abnormally large streams or slow storage.
- alert: ExspeedDedupSlowRebuild
  expr: histogram_quantile(0.95, exspeed_dedup_rebuild_duration_seconds_bucket{source="full_scan"}) > 30
  annotations:
    summary: "Exspeed dedup full-scan rebuild took > 30s"
```

## Connector compatibility

Connectors that already set an `x-idempotency-key` header (e.g. the Postgres outbox connector) continue to work unchanged — the same dedup engine processes both the header and the `msg_id` wire field. No connector reconfiguration is required to benefit from idempotent publish.
