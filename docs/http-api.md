# HTTP API reference

Base URL: `http://<host>:8080`. Request and response bodies are JSON.

The server describes this API as an OpenAPI 3.1 document at
`GET /api/v1/openapi.json` (no auth needed). Load it into any OpenAPI tool
(Swagger Editor, Redoc, an HTTP client generator); the server doesn't ship a
UI. Schemas and status codes are there; this page is the overview.

```bash
curl -s localhost:8080/api/v1/openapi.json | jq '.paths | keys'
```

## Authentication

When auth is enabled, every `/api/v1/*` request must carry
`Authorization: Bearer <token>`. Every `/api/v1/*` route except `whoami`,
the record browser (`GET /api/v1/streams/{name}/records`, which takes
`subscribe` or `admin` on the stream) and the key-value routes (which check
the bucket's stream, see [below](#key-value-buckets)) requires an **admin**
permission (`openapi.json` needs none):

- **Global admin** (`admin` on `streams = "*"`) for queries, tables,
  connectors, connections and backups.
- **Admin on the stream** for stream routes, including publish, and for
  consumer routes (admin on the consumer's stream; creating a consumer with
  a `dlq_stream` also needs admin on that stream).
- **Admin on any stream** for `/api/v1/leases` and `/api/v1/cluster`.

A missing or unknown token gets `401`; a token without the needed
permission gets `403`.

| Path | Auth |
|------|------|
| `GET /healthz` | none |
| `GET /readyz` | none |
| `GET /metrics` | none, or `Bearer <metrics_token>` when `[server] metrics_token` is set |
| `GET /api/v1/openapi.json` | none |
| `POST /webhooks/*` | none, unless the webhook connector sets its own |

In multi-pod mode, standbys answer `503` on `/api/v1/*`, with the leader's
client address in `leader` when it is known. The exceptions are
`/api/v1/leases`, `/api/v1/cluster`, `/api/v1/whoami`,
`/api/v1/openapi.json` and `GET /api/v1/streams/{name}/records` (followers
serve reads from their replica). See [high-availability.md](high-availability.md).

Internal streams (names starting with `__`: consumer state, catalogs,
connector offsets) are written only by the server. Creating, updating,
publishing to or deleting one answers `403`.

## Endpoints

### Health and metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | `200` `{leader: true, node_id}` on the cluster leader (always, on a single node); `503` `{leader: false, node_id, leader_hint}` on a follower. Use it for load-balancer routing. |
| `GET` | `/readyz` | `200` `{status: "ready"}` once startup has finished, the leader's dedup rebuild is done and `data_dir` is writable; otherwise `503` with `status` `starting`, `dedup_rebuild_in_progress` or `data_dir_unwritable`. A fenced (failed) stream doesn't make the node unready: the answer is then `200` `{status: "degraded", failed_streams: [{stream, reason}]}`. Use it for k8s readiness. |
| `GET` | `/metrics` | Prometheus text format |

### Streams

| Method | Path | Body / query | Description |
|--------|------|--------------|-------------|
| `GET` | `/api/v1/streams` | `?internal=true` | List the streams the caller has any permission on. Internal `__` streams are included only with `internal=true`, for global admins. |
| `POST` | `/api/v1/streams` | `{"name", "max_age_secs"?, "max_bytes"?, "dedup_window_secs"?, "dedup_max_entries"?, "compaction"?, "max_msgs"?, "discard"?, "max_msgs_per_subject"?, "allow_msg_ttl"?, "msg_ttl_ms"?, "allow_delayed"?, "retention"?, "capture_subjects"?}` | Create a stream. `compaction: true` keeps only the latest record per key. The limit, TTL, delay and `retention` (`limits`, `work_queue`, `interest`) settings are described in [queues.md](queues.md); `capture_subjects` (subject filters whose core and NATS messages are appended to the stream) in [nats.md](nats.md#stream-capture). |
| `GET` | `/api/v1/streams/{name}` | | `storage_bytes`, `earliest_offset`, `head_offset` (the next offset), every setting, and `status` (`healthy`, or `failed` with a `failure` reason when the partition is fenced read-only) |
| `PATCH` | `/api/v1/streams/{name}` | any setting except `name` and `compaction` | Update settings; absent fields keep their value |
| `DELETE` | `/api/v1/streams/{name}` | `?force=true` | Delete. Without `force`, answers `409` with the `blockers` (connectors, queries, consumers, subscriptions) that still reference the stream; with it, deletes them too. |
| `POST` | `/api/v1/streams/{name}/publish` | `{"data", "subject"?, "key"?, "msg_id"?}` | Publish one record. `data` is any JSON value, stored as its JSON encoding; `subject` defaults to the stream name. The `x-idempotency-key` header can be used instead of `msg_id`. `201` when stored, `200` for a duplicate, `409` when the `msg_id` was used with a different body, `429` when the stream is full and its discard policy is `new`, `503` while not leader, during the dedup rebuild or when the dedup map is full (with `Retry-After`). |
| `GET` | `/api/v1/streams/{name}/records?from=&limit=&filter=&wait_ms=` | | Browse records without a consumer: `{records, next_offset, high_watermark}`. Needs `subscribe` or `admin`; any node answers. `wait_ms` (≤ 30000) long-polls: with nothing new at `from`, the server waits for records before answering. `from` defaults to the earliest retained record, `limit` to 100 (≤ 1000); each record has `offset`, `timestamp_ms`, `subject`, `key`, `value` (JSON when it parses, else a UTF-8 string, else base64; see `encoding`), `headers`. |

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
| `GET` | `/api/v1/consumers/{name}` | | Spec, `next_offset`, `ack_floor`, `num_unacked`, `num_in_flight`, `num_delayed`, `num_waiting`, `lag`, `subscribers`, `pull_waiters`, `stats` |
| `POST` | `/api/v1/consumers/{name}/seek` | one of `"earliest"`, `"latest"`, `{"offset": n}`, `{"timestamp_ms": t}` | Reposition; drops unacked state |
| `DELETE` | `/api/v1/consumers/{name}` | | Delete; active subscriptions end with code 404 |

Ephemeral consumers belong to a TCP connection, so `POST /api/v1/consumers`
rejects `"ephemeral": true` with `400`. The delivery options
(`filter_headers`, `header_match`, `single_active`, `priority_window`,
`dead_letter_expired`) are described in [queues.md](queues.md#consumer-delivery-options).

### Key-value buckets

A bucket `B` is the stream `KV_B` (see [kv.md](kv.md)). These routes take
any authenticated caller and check the bucket's stream: reads need
`subscribe` (or `admin`), writes `publish`, creating a bucket `admin`. Like
every write route they answer `503` on a standby.

| Method | Path | Body / headers | Description |
|--------|------|----------------|-------------|
| `POST` | `/api/v1/kv` | `{"bucket", "history"?, "ttl_ms"?, "max_bytes"?}` | Create a bucket (`201`; idempotent for the same settings) |
| `GET` | `/api/v1/kv/{bucket}?filter=` | | Keys that have a value, sorted; `filter` is a subject filter over keys |
| `GET` | `/api/v1/kv/{bucket}/{key}?revision=` | | The value as raw bytes, with `X-Exspeed-Revision`, `ETag` and `X-Exspeed-Kv-Op`; `404` when absent or deleted |
| `PUT` | `/api/v1/kv/{bucket}/{key}?ttl_ms=` | raw body; `If-Match: <revision>` or `If-None-Match: *` | Set the key; `{"revision"}`. `If-Match` makes it a compare-and-set, `If-None-Match: *` creates it only if absent; `409` (with `current_revision`) otherwise |
| `DELETE` | `/api/v1/kv/{bucket}/{key}?purge=` | optional `If-Match` | Delete (or purge) the key; `{"revision"}` of the tombstone |
| `GET` | `/api/v1/kv/{bucket}/{key}/history` | | Kept revisions, oldest first: `[{key, revision, timestamp_ms, op, value_base64}]` |

```bash
curl -X POST localhost:8080/api/v1/kv -d '{"bucket": "config", "history": 5}' -H 'Content-Type: application/json'
curl -X PUT localhost:8080/api/v1/kv/config/app.mode -H 'If-None-Match: *' --data 'prod'
# {"revision": 1}
curl -i localhost:8080/api/v1/kv/config/app.mode
```

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
`exql.query_max_rows` (`EXSPEED_QUERY_MAX_ROWS`, default 10000) rows matched:

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

Secondary indexes are not supported: `CREATE INDEX` returns
`UNSUPPORTED`.

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
| `GET` | `/api/v1/leases` | Live lease records (`name`, `holder`, `epoch`, `expires_at`, `replication_endpoint`, `client_endpoint`, `isr`); empty on a single node. Any node. |
| `GET` | `/api/v1/cluster` | Any node. Role, epoch, leader endpoints; ISR and follower lag on the leader, replication session on a follower |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/backup` | Online backup: a tar archive (`application/x-tar`) streamed while writes continue. The first entry is the `exspeed-backup.json` manifest with each stream's `[earliest_offset, next_offset)`. Global admin. Use `exspeed backup` / `exspeed restore`; see [operations.md](operations.md#backup-and-restore). |
| `GET` | `/api/v1/openapi.json` | This API as an OpenAPI 3.1 document. No auth. |

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks/{path}` | Ingest the request body through a matching `http_webhook` connector. Returns `200` `{"offset": N}` once stored; `400` (rejected by the connector), `401` (the connector's own auth failed), `404` (no webhook connector for the path), `409` (idempotency key reused with another body), `503` (not leader, or retryable). |

## TCP protocol

Applications publish and consume over the binary protocol on port 5933 (TLS
optional). The full specification is [protocol.md](protocol.md). Clients:
the Rust crate [`exspeed-client`](../crates/exspeed-client) and the
[TypeScript SDK](../sdks/typescript).
