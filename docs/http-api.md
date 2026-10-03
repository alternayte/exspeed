# HTTP API reference

Base URL: `http://<host>:8080`. Request and response bodies are JSON.

## Authentication

When auth is enabled, every `/api/v1/*` request must carry
`Authorization: Bearer <token>`. Every `/api/v1/*` route except `whoami`
requires an **admin** permission:

- **Global admin** for queries, tables, connectors, connections, leases
  and cluster routes.
- **Admin on the stream** for stream routes.

| Path | Auth |
|------|------|
| `GET /healthz` | none |
| `GET /readyz` | none |
| `GET /metrics` | none |
| `POST /webhooks/*` | none, unless the webhook connector sets its own |

In multi-pod mode, standbys answer `503` on `/api/v1/*`. The exceptions are
`/api/v1/leases` and `/api/v1/whoami`. See [high-availability.md](high-availability.md).

## Endpoints

### Health and metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | `200` only on the cluster leader. Use it for load-balancer routing. |
| `GET` | `/readyz` | `200` once startup has finished and `data_dir` is writable. Use it for k8s readiness. |
| `GET` | `/metrics` | Prometheus text format |

### Streams

| Method | Path | Body / query | Description |
|--------|------|--------------|-------------|
| `GET` | `/api/v1/streams` | | List streams |
| `POST` | `/api/v1/streams` | `{"name", "max_age_secs"?, "max_bytes"?, "dedup_window_secs"?, "dedup_max_entries"?, "compaction"?}` | Create a stream. `compaction: true` keeps only the latest record per key. |
| `GET` | `/api/v1/streams/{name}` | | Offsets, size, retention and dedup settings |
| `PATCH` | `/api/v1/streams/{name}` | `{"max_age_secs"?, "max_bytes"?, "dedup_window_secs"?, "dedup_max_entries"?}` | Update settings |
| `DELETE` | `/api/v1/streams/{name}` | `?force=true` | Delete. Without `force`, fails if connectors, queries or consumers still reference the stream. |
| `POST` | `/api/v1/streams/{name}/publish` | `{"subject", "data", "key"?, "msg_id"?}` | Publish one record. The `x-idempotency-key` header can be used instead of `msg_id`. |
| `GET` | `/api/v1/streams/{name}/records?from=&limit=&filter=` | | Browse records without a consumer: `{records, next_offset, high_watermark}`. `limit` ≤ 1000; each record has `offset`, `timestamp_ms`, `subject`, `key`, `value` (JSON when it parses, else a UTF-8 string, else base64; see `encoding`), `headers`. |

```bash
curl -X POST localhost:8080/api/v1/streams -H 'Content-Type: application/json' \
  -d '{"name": "orders", "max_age_secs": 604800, "max_bytes": 10737418240}'

curl -X POST localhost:8080/api/v1/streams/orders/publish -H 'Content-Type: application/json' \
  -d '{"subject": "order.eu.created", "key": "ord-1", "msg_id": "ord-1-v1", "data": {"total": 99}}'
# {"offset": 0, "duplicate": false}
```

### Consumers

Delivery (push subscriptions, pulls, acks) is TCP-only; HTTP manages
consumers. See [concepts.md](concepts.md#consumers) for the model.

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `GET` | `/api/v1/consumers[?stream=]` | | List consumers (with state) |
| `POST` | `/api/v1/consumers` | consumer spec, e.g. `{"name": "billing", "stream": "orders", "filter_subjects": ["orders.placed"], "dlq_stream": "orders-dlq"}` | Create a durable consumer. Idempotent for an identical spec, `409` if it differs. Returns `201` with consumer info. |
| `GET` | `/api/v1/consumers/{name}` | | Spec, `next_offset`, `ack_floor`, `num_unacked`, `num_waiting`, `lag`, `subscribers`, `stats` |
| `POST` | `/api/v1/consumers/{name}/seek` | one of `"earliest"`, `"latest"`, `{"offset": n}`, `{"timestamp_ms": t}` | Reposition; drops unacked state |
| `DELETE` | `/api/v1/consumers/{name}` | | Delete; active subscriptions end with code 404 |

### Queries (ExQL)

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/api/v1/queries` | `{"sql"}` | Run any ExQL statement: a bounded `SELECT` (200), `CREATE STREAM/TABLE … AS SELECT` (201, returns the query), `DROP STREAM/TABLE/QUERY`, `PAUSE/RESUME QUERY` |
| `POST` | `/api/v1/queries/continuous` | `{"sql": "CREATE STREAM out AS SELECT …"}` | Create a continuous query (`CREATE` statements only) |
| `GET` | `/api/v1/queries` | | List continuous queries |
| `GET` | `/api/v1/queries/{id}` | | Definition, `status` (`running`/`paused`/`pending`/`failed`), `desired_state`, `error`, `stats` |
| `POST` | `/api/v1/queries/{id}/pause` | | Pause (state and position are kept) |
| `POST` | `/api/v1/queries/{id}/resume` | | Resume a paused or failed query from its checkpoint |
| `DELETE` | `/api/v1/queries/{id}` | | `DROP QUERY`: stop and remove the query; its output stream is kept |

A bounded query returns the rows below. `truncated` is true when more than
`EXSPEED_QUERY_MAX_ROWS` rows matched:

```json
{"columns": ["region", "n"], "rows": [["eu", 42]], "row_count": 1, "execution_time_ms": 12, "truncated": false}
```

A query, as returned by `CREATE` and `GET /api/v1/queries/{id}`:

```json
{"id": "big_orders_1a2b3c4d", "query_id": "big_orders_1a2b3c4d", "kind": "stream", "name": "big_orders",
 "target_stream": "big_orders", "status": "running", "desired_state": "running", "error": null,
 "sql": "CREATE STREAM big_orders AS …", "created_at": "…",
 "stats": {"records_in": 10, "records_out": 4, "late_records_dropped": 0, "checkpoints": 1,
           "watermark": "…", "last_checkpoint": "…"}}
```

Errors are returned in this form, and the HTTP status follows the code
(400 parse/plan/unsupported, 404 not found, 408 timeout, 409 conflict, 422
memory limit, 503 not leader):

```json
{"error": "parse error: …", "code": "PARSE_ERROR", "line": 1, "column": 25}
```

`CREATE INDEX` returns `UNSUPPORTED`; the `/api/v1/indexes` endpoints have
been removed.

### Materialized tables

`CREATE TABLE … AS SELECT … GROUP BY …` (alias `CREATE MATERIALIZED VIEW`):

| Method | Path | Body / query | Description |
|--------|------|--------------|-------------|
| `GET` | `/api/v1/views` | | List tables: `[{name, query_id, columns, row_count}]` |
| `POST` | `/api/v1/views` | `{"sql": "CREATE TABLE t AS SELECT …"}` | Create a table (201) |
| `GET` | `/api/v1/views/{name}` | `?key=<k>` | All rows (`{columns, rows, row_count}`), or one group's row (`{columns, row}`, 404 if absent). For several GROUP BY columns the key is a JSON array, e.g. `?key=["eu",3]` |

### Connectors

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/connectors` | List connectors with `status`, `last_error`, `restart_count`, `lag`, `last_success_ms`, `checkpoint` ([fields](connectors.md#status-restarts-and-metrics)) |
| `POST` | `/api/v1/connectors` | Create a connector (JSON form of the TOML config; see [connectors.md](connectors.md#http-api)). `400` invalid, `409` exists or collides |
| `GET` | `/api/v1/connectors/{name}` | Status of one connector, plus its `config` (`${VAR}` unresolved) |
| `PUT` | `/api/v1/connectors/{name}` | Replace an API-created connector's config and restart it; offsets are kept. `409` for file-defined connectors |
| `DELETE` | `/api/v1/connectors/{name}` | Stop the connector and delete it, including its offsets (and its replication slot with `drop_slot_on_delete`) |
| `POST` | `/api/v1/connectors/{name}/restart` | Restart a connector; also revives a `failed` one. `503` on a non-leader |

### External database connections

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `GET` | `/api/v1/connections` | | List connections |
| `POST` | `/api/v1/connections` | `{"name", "driver", "url"}` | Register a connection (driver `postgres`). Bounded queries read its tables as `<name>.<table>` or `<name>.<schema>.<table>` |
| `DELETE` | `/api/v1/connections/{name}` | | Remove an API-created connection. `409` for connections defined in `connections.d/` or the environment |

### Identity and cluster

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/whoami` | Identity and permissions of the caller's token |
| `GET` | `/api/v1/leases` | Current `cluster:leader` lease row, including `replication_endpoint` |
| `GET` | `/api/v1/cluster/followers` | Leader only. Connected followers. |

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks/{path}` | Ingest the request body through a matching `http_webhook` connector. Returns `{"offset": N}` once stored; `401`, `409` (idempotency key reused with another body), `503` (not leader). |

## TCP protocol

Applications publish and consume over the binary protocol on port 5933 (TLS
optional). The full specification is [protocol.md](protocol.md). Clients:
the Rust crate [`exspeed-client`](../crates/exspeed-client) and the
[TypeScript SDK](../sdks/typescript).
