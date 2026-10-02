# HTTP API reference

Base URL: `http://<host>:8080`. Request and response bodies are JSON.

## Authentication

When auth is enabled, every `/api/v1/*` request must carry
`Authorization: Bearer <token>`. Every `/api/v1/*` route except `whoami`
requires an **admin** permission:

- **Global admin** for queries, connectors, connections, indexes, leases
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
| `POST` | `/api/v1/streams` | `{"name", "max_age_secs"?, "max_bytes"?, "dedup_window_secs"?, "dedup_max_entries"?}` | Create a stream |
| `GET` | `/api/v1/streams/{name}` | | Offsets, size, retention and dedup settings |
| `PATCH` | `/api/v1/streams/{name}` | `{"max_age_secs"?, "max_bytes"?, "dedup_window_secs"?, "dedup_max_entries"?}` | Update settings |
| `DELETE` | `/api/v1/streams/{name}` | `?force=true` | Delete. Without `force`, fails if connectors, queries or consumers still reference the stream. |
| `POST` | `/api/v1/streams/{name}/publish` | `{"subject", "data", "key"?, "msg_id"?}` | Publish one record. The `x-idempotency-key` header can be used instead of `msg_id`. |

```bash
curl -X POST localhost:8080/api/v1/streams -H 'Content-Type: application/json' \
  -d '{"name": "orders", "max_age_secs": 604800, "max_bytes": 10737418240}'

curl -X POST localhost:8080/api/v1/streams/orders/publish -H 'Content-Type: application/json' \
  -d '{"subject": "order.eu.created", "key": "ord-1", "msg_id": "ord-1-v1", "data": {"total": 99}}'
# {"offset": 0, "duplicate": false}
```

### Consumers

There is no HTTP endpoint for creating consumers. Clients create them over
TCP (see [concepts.md](concepts.md#consumers)).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/consumers` | List consumers |
| `GET` | `/api/v1/consumers/{name}` | Offset, lag, group and filter |
| `DELETE` | `/api/v1/consumers/{name}` | Delete a consumer |

### Queries (ExQL)

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/api/v1/queries` | `{"sql"}` | Run a bounded query. Also accepts `CREATE INDEX` / `DROP INDEX`. |
| `GET` | `/api/v1/queries` | | List continuous queries |
| `POST` | `/api/v1/queries/continuous` | `{"sql": "CREATE VIEW out AS SELECT …"}` | Start a continuous query |
| `GET` | `/api/v1/queries/{id}` | | Query details and status |
| `DELETE` | `/api/v1/queries/{id}` | | Stop and remove a query |

A bounded query returns:

```json
{"columns": ["region", "n"], "rows": [["eu", 42]], "row_count": 1, "execution_time_ms": 12}
```

Errors are returned in this form:

```json
{"error": "parse error: …", "code": "PARSE_ERROR", "line": 1, "column": 25}
```

### Materialized views

| Method | Path | Body / query | Description |
|--------|------|--------------|-------------|
| `GET` | `/api/v1/views` | | List views |
| `POST` | `/api/v1/views` | `{"sql": "CREATE MATERIALIZED VIEW v AS SELECT …"}` | Create a view |
| `GET` | `/api/v1/views/{name}` | `?key=<k>` | Return all rows, or one row by key |

### Indexes

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `GET` | `/api/v1/indexes` | | List secondary indexes |
| `POST` | `/api/v1/indexes` | `{"sql": "CREATE INDEX name ON stream(payload->>'field')"}` | Create an index |
| `DELETE` | `/api/v1/indexes/{name}` | | Drop an index (definition only; see [exql.md](exql.md#indexes)) |

### Connectors

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/connectors` | List connectors and their status |
| `POST` | `/api/v1/connectors` | Create a connector (JSON form of the TOML config; see [connectors.md](connectors.md#http-api)) |
| `GET` | `/api/v1/connectors/{name}` | Status of one connector |
| `DELETE` | `/api/v1/connectors/{name}` | Stop the connector and delete it, including its offsets |
| `POST` | `/api/v1/connectors/{name}/restart` | Restart a connector |

### External database connections

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `GET` | `/api/v1/connections` | | List connections |
| `POST` | `/api/v1/connections` | `{"name", "driver", "url"}` | Register a connection (`postgres`, `mysql`, `sqlite` or `mssql`) |
| `DELETE` | `/api/v1/connections/{name}` | | Remove a connection |

### Identity and cluster

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/whoami` | Identity and permissions of the caller's token |
| `GET` | `/api/v1/leases` | Current `cluster:leader` lease row, including `replication_endpoint` |
| `GET` | `/api/v1/cluster/followers` | Leader only. Connected followers. |

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks/{path}` | Ingest the request body through a matching `http_webhook` connector. Returns `{"offset": N}`. |

## TCP protocol

Applications publish and consume over the binary protocol on port 5933.
Each frame has a 10-byte header: `[version u8][opcode u8][correlation_id u32][payload_len u32]`.
The TypeScript SDK in [`sdks/typescript`](../sdks/typescript) is the
reference client. Protocol v2 (byte-bounded fetch, high-watermark, error
frames, version negotiation) is proposed in
[REVIEW.md §5](REVIEW.md#5-proposed-target-architecture).
