# Idempotent publish

Exspeed supports retry-safe (idempotent) publishes. You attach an ID to a
publish in one of these ways:

- the `msg_id` field of a record in a TCP `Publish` or `PublishBatch`
  request (SDK: `msgId`)
- the `msg_id` field in the HTTP publish body, or the `x-idempotency-key`
  request header (the body field wins when both are set)
- the `--msg-id` CLI flag
- an `x-idempotency-key` record header, which is how connectors and ExQL
  output tag their records

The ID is stored on the record as its `x-idempotency-key` header.

Where dedup applies:

- **Every write path.** TCP, HTTP, webhooks, connectors and ExQL output all
  append through the broker's single write path, so a `msg_id` (or
  `x-idempotency-key` header) is deduplicated wherever the record comes
  from.
- **During startup.** Until the startup rebuild of the dedup maps finishes,
  publishes that carry a `msg_id` are refused with a retryable error
  (`503` over HTTP and TCP) instead of being written unchecked; publishes
  without one are accepted. Meanwhile `/readyz` answers `503` with
  `dedup_rebuild_in_progress`.
- **Batches.** `PublishBatch` deduplicates within the batch as well as
  against earlier records (a repeated `msg_id` in one batch is written once,
  or rejected as a collision when the bodies differ), and it honours
  `dedup_max_entries`.
- **Deleted streams.** Deleting a stream clears its dedup map, so a
  re-created stream starts empty.
- **Failover.** A promoted follower rebuilds every stream's dedup map from
  the replicated log (local snapshots are ignored) before it opens writes,
  so a retry after a leader change is still answered `duplicate: true`
  for records within the window.

## Semantics

**First-body-wins.** If a client publishes `msg_id=X` with body `A`, a retry with the same `msg_id=X` and the same body `A` succeeds with `duplicate: true` and the original offset; no new record is written. The body is the record value: subject, key and headers aren't compared. If the retry arrives with a *different* body `B` (a likely bug in the caller), the server rejects it with a conflict (`409`, with `stored_offset` naming the first write). When the per-stream dedup map is at capacity (`dedup_max_entries`) and evicting expired entries cannot free a slot, the server rejects the publish as retryable: `429` over TCP and `503` with a `Retry-After` header over HTTP, both with `retry_after_secs`, the time until the oldest entry expires. Messages published without a `msg_id` bypass the dedup engine entirely: they are always written immediately and are unaffected by a full dedup map.

An entry is remembered for the stream's dedup window, measured from the first write. A retry after the window has passed is written as a new record.

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
| `dedup_window` | `5m` (300 s), capped at the retention age | `1 s` (must be ≤ retention) |
| `dedup_max_entries` | `500_000` | `1` |

Both are per-stream settings: `dedup_window_secs` and `dedup_max_entries`
over HTTP (`POST`/`PATCH /api/v1/streams`) and in the SDK
(`dedupWindowSecs`, `dedupMaxEntries`). `exspeed info <stream>` shows them,
and `exspeed_dedup_window_secs{stream}` reports each stream's window.

## Memory and on-disk cost

Each dedup entry stores a `msg_id` string (variable), an offset, an insertion time and a 64-bit body hash. At the default 500,000-entry cap with average 32-byte `msg_id` strings the in-memory footprint is approximately **150 MB per stream** worst case. On disk, each stream maintains a `dedup_snapshot.bin` file in its stream directory. The snapshot is written every 60 seconds and on graceful shutdown, and is included in `exspeed snapshot` offline backups. Online backups (`exspeed backup`) leave it out; after `exspeed restore` the map is rebuilt from the restored log (a full scan of the dedup window at startup).

Storage layout with dedup:

```
exspeed-data/
  streams/
    orders/
      stream.json           Stream config (retention, dedup_window, dedup_max_entries)
      dedup_snapshot.bin    Periodic dedup-map snapshot — restored on restart
      partitions/0/
        00000000000000000000.seg    Records
        00000000000000000000.idx    Sparse offset index
        00000000000000000000.meta   Sealed-segment metadata (offset and time range, record count)
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

## Connectors and ExQL

Connectors that set an `x-idempotency-key` header (for example the Postgres outbox connector, or `http_poll` with `idempotent_items`) and ExQL continuous queries (deterministic keys on every output record) go through the same dedup engine as a `msg_id`. No extra configuration is needed: the stream's dedup window and cap apply.
