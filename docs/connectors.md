# Connectors

Connectors move data between Exspeed and external systems.

- **Sources** write into a stream.
- **Sinks** read from a stream and write somewhere else.

You can define a connector in two ways:

- **As a TOML file** in `<data-dir>/connectors.d/`. The file is the source of
  truth: it is hot-reloaded, `${VAR}` references are resolved when the
  connector starts, and nothing (in particular no resolved secret) is
  written to disk.
- **Through the HTTP API** at `POST /api/v1/connectors`. The config (with
  any `${VAR}` left as written) is stored in the compacted internal stream
  `__connectors`, keyed by connector name, so it replicates with the log and
  any node that becomes leader runs it. Creating, updating or deleting one
  needs the leader (`503` elsewhere). Older versions kept these configs as
  JSON under `<data-dir>/connectors/`; the first leader to start imports
  them into `__connectors` and renames the directory to
  `connectors.migrated/`. If a `connectors.d/` file defines a connector
  with the same name, the file wins and the API copy is deleted.

Connectors run on the leader. Each one has its own **supervisor** that
restarts it with backoff, reports its status, and implements the checkpoint
protocol described below.

## Contents

- [Delivery guarantees](#delivery-guarantees)
- [Config file format](#config-file-format)
- [Status, restarts and metrics](#status-restarts-and-metrics)
- [Errors, retries and the DLQ](#errors-retries-and-the-dlq)
- [Offsets](#offsets)
- [Sources](#sources)
- [Sinks](#sinks)
- [Transforms](#transforms)
- [Validating configs](#validating-configs)
- [HTTP API](#http-api)

## Delivery guarantees

**Sources.** The framework calls `poll()`, appends the records through the
broker's write path, persists the batch's checkpoint, and only then calls
the plugin's `ack()`. `ack()` is the only place a plugin may acknowledge
anything externally (confirm a WAL position, ack an AMQP delivery, delete
outbox rows). A crash at any point before `ack()` replays the batch.

**Sinks.** `write()` may buffer; `flush()` is a barrier that makes
everything written so far durable in the target. The framework commits the
stream offset only after `flush()` succeeds. It flushes on a timer
(`flush_interval_ms`), when the plugin's buffer is full, and on stop or
shutdown. After a crash, records after the last commit are written again.

"Effectively-once" below means a replay produces no visible duplicate:
either the target is idempotent (upserts, deterministic object keys), or
records carry an `x-idempotency-key` that the broker deduplicates within
the stream's dedup window (see [idempotent publish](idempotent-publish.md)).

| Plugin | Type | Guarantee | Tested against |
|--------|------|-----------|----------------|
| `postgres_cdc` | source | at-least-once; effectively-once for replays within the dedup window | real Postgres: envelope and keys, crash + restart without loss or duplicates, non-destructive dry run |
| `postgres_outbox` | source | effectively-once within the dedup window (`x-idempotency-key` = outbox id); at-least-once beyond it | real Postgres: poll mode with delete cleanup, CDC mode, two crashes before the delete leave each event exactly once |
| `postgres_poll` | source | at-least-once (a crash replays the last batch); rows committed late with an older tracking value are missed | real Postgres: timestamp/numeric/uuid/json decoding, tied tracking values, resume |
| `jdbc_poll` | source | at-least-once (a crash replays the last batch; no idempotency key); integer cursor, so late commits below the cursor are missed | SQLite; real MySQL and SQL Server: a crash between append and checkpoint replays exactly that batch, resume after a restart |
| `mssql_cdc` | source | at-least-once; effectively-once for replays within the dedup window | real SQL Server: two crashes + restart without loss or duplicates, update/delete/insert while stopped |
| `rabbitmq` | source | at-least-once (ack after the append); effectively-once with `dedup_on_message_id` | real RabbitMQ: a crash before the ack redelivers (duplicates, nothing lost, queue drained); exactly once with `dedup_on_message_id` across two crashes and a restart |
| `http_poll` | source | at-least-once per response; effectively-once with `idempotent_items` | in-process HTTP server: no truncation, pagination |
| `http_webhook` | source | `200` only after the record is stored: at-least-once from the sender's side; effectively-once with `Idempotency-Key` | HTTP API tests |
| `jdbc` | sink | effectively-once in `upsert` mode; at-least-once in `insert` mode (a duplicate-key error on replay counts as written) | Postgres, MySQL, SQLite, SQL Server; real MySQL and SQL Server: two crashes before the commit leave every row exactly once in `upsert` mode and in `insert` mode with a key, nothing in the DLQ |
| `http_sink` | sink | at-least-once; every request carries `Idempotency-Key` | in-process HTTP server: retries, 401, poison → DLQ |
| `rabbitmq` | sink | at-least-once (publisher confirms, persistent messages); `message_id` = idempotency key | real RabbitMQ: a crash before the commit republishes the batch with the same `message_id`s; a clean restart publishes only new records |
| `s3` | sink | effectively-once: one object per buffer, keyed by its first offset, so a retry overwrites the same object | S3-compatible store (moto in CI): two crashes before the commit overwrite the same objects (each record exactly once); a graceful stop flushes the partial buffer |

The framework itself (checkpoint ordering, crash between append and
checkpoint, crash before ack, sink flush failures, restarts, panics) is
tested with fake plugins in `crates/exspeed-connectors/tests/it/`.

### Testing against real services

The service-backed tests in `crates/exspeed-connectors/tests/it/` run the
real plugins under the supervisor and crash them mid-stream: a panic when
the checkpoint or sink offset is saved (after the append or the write is
durable), or just before a source's external ack. The supervisor restarts
the plugin, and the test checks what reached the stream or the target:
every upstream message at least once, and no duplicates where the plugin
promises effectively-once.

| Variable | Service | Tests |
|----------|---------|-------|
| `EXSPEED_POSTGRES_URL` | Postgres with `wal_level = logical` | `postgres_cdc`, `postgres_outbox`, `postgres_poll` |
| `EXSPEED_MYSQL_URL` | MySQL or MariaDB | `jdbc` sink, `jdbc_poll` |
| `EXSPEED_MSSQL_URL` | SQL Server with SQL Server Agent and a user database | `jdbc` sink, `jdbc_poll`, `mssql_cdc` |
| `EXSPEED_RABBITMQ_URL` | RabbitMQ | `rabbitmq` source and sink |
| `EXSPEED_S3_ENDPOINT` (with `EXSPEED_S3_ACCESS_KEY` and `EXSPEED_S3_SECRET_KEY`, default `minioadmin`) | An S3-compatible store (CI and docker-compose use moto; MinIO works too) | `s3` sink |

These tests are `#[ignore]`d. A test whose variable is unset skips, or
fails under `CI=true`. The `test-services` CI job runs all of these
services. To run them locally:

```bash
docker-compose up -d postgres mysql mssql rabbitmq s3
docker exec exspeed-mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa \
  -P 'Exspeed_Test!1' -Q "CREATE DATABASE exspeed"
export EXSPEED_POSTGRES_URL=postgres://testuser:testpass@127.0.0.1:5432/testdb
export EXSPEED_MYSQL_URL=mysql://exspeed:exspeed@127.0.0.1:3306/exspeed
export EXSPEED_MSSQL_URL='mssql://sa:Exspeed_Test!1@127.0.0.1:1433/exspeed?trust_server_certificate=true'
export EXSPEED_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/%2f
export EXSPEED_S3_ENDPOINT=http://127.0.0.1:9000
cargo test -p exspeed-connectors --test it -- --include-ignored
```

## Config file format

```toml
[connector]
name = "orders-cdc"          # [A-Za-z0-9_-], 1–100 chars, starts with a letter or digit
type = "source"              # "source" | "sink"
plugin = "postgres_cdc"      # see the plugin lists below
stream = "orders"            # stream to write to (source) or read from (sink)

# Optional, common to all connectors
subject_template = ""        # sources: subject of produced records (see each plugin)
subject_filter = ""          # sinks: only deliver matching subjects (NATS wildcards)
key_field = ""               # sources: JSON field used as the key when the plugin sets none
batch_size = 100             # max records per poll / per sink write
poll_interval_ms = 50        # sleep when there is nothing to do
flush_interval_ms = 1000     # sinks: flush + commit at most this often (default is per plugin)
dlq_stream = "orders-dlq"    # poison records go here; unset = drop and count
on_transient_exhausted = "loop_forever"   # "loop_forever" | "fail" | "dlq_batch"

[settings]                   # plugin-specific, typed
connection = "${DATABASE_URL}"
tables = ["public.orders"]

[retry]                      # in-place retries of a transient error
max_retries = 5
initial_backoff_ms = 100
max_backoff_ms = 30000
multiplier = 2.0
jitter = true

[restart]                    # supervisor restarts after a failed run
initial_backoff_ms = 1000
max_backoff_ms = 60000
multiplier = 2.0
jitter = true
max_restarts = 0             # consecutive failed runs before "failed"; 0 = never give up

[transform]                  # optional, sources only
sql = "SELECT payload->>'id' AS id, payload->>'total' AS total"
```

**Settings are typed.** Each plugin declares its settings and rejects
unknown keys, so a misspelled key is an error that names the key. Use native
TOML types: numbers (`interval_secs = 60`), booleans, arrays
(`tables = ["a", "b"]`) and inline tables
(`headers = { Authorization = "Bearer x" }`). Strings are accepted wherever
a number or boolean is expected, so `port = "${PG_PORT}"` works; lists also
accept a comma-separated string.

**`${VAR}` and `${VAR:-default}`** are replaced with values from the
server's environment, in strings at any depth of `[settings]`. This applies
only to files in `connectors.d/`. An unset variable without a default is a
config error. Resolved values are used to build the plugin and are never
persisted.

A file whose plugin or `[settings]` are invalid (unknown plugin, unknown
setting, bad value) is still listed, with status `failed` and the error in
`last_error`. A file that doesn't parse at all (bad TOML, unknown key in
`[connector]`) is skipped with a warning in the server log; if it already
defined a connector, the previous config keeps running and the parse error
is reported in that connector's `last_error`.

**Names** become file names, slot names and metric labels. Names that would
collide once lowercased with `-` mapped to `_` (for example `Orders-CDC` and
`orders_cdc`) are rejected.

## Status, restarts and metrics

Every connector is in one of these states:

```mermaid
stateDiagram-v2
  [*] --> Starting
  Starting --> Running
  Running --> Backoff: transient error (retries exhausted), lost connection, panic
  Backoff --> Starting
  Running --> Failed: fatal error (config, credentials) or max_restarts reached
  Running --> Stopped: stopped, deleted, demoted or shut down
  Failed --> [*]
  Stopped --> [*]
```

- A **restart** is an awaited `stop()` followed by `start()`, so a restarted
  instance never races the old one (for example for a replication slot).
- Restarts back off exponentially with jitter (`[restart]`), always bounded
  by `max_backoff_ms`. A run that commits at least one batch resets the
  backoff.
- Panics inside a plugin are caught and handled like a lost connection.
- `failed` is sticky: fix the cause, then `POST /api/v1/connectors/<name>/restart`
  or edit the config.

`GET /api/v1/connectors` and `GET /api/v1/connectors/<name>` return:

| Field | Meaning |
|-------|---------|
| `status` | `starting`, `running`, `backoff`, `failed` or `stopped` |
| `last_error` | the most recent error (kept while backing off; cleared when running again) |
| `restart_count` | supervisor restarts since the server started |
| `lag`, `lag_unit` | sinks: records behind the stream head (`records`); Postgres CDC/outbox: WAL bytes not yet confirmed (`bytes`) |
| `last_success_ms` | Unix ms of the last committed batch |
| `checkpoint` | last persisted position (source checkpoint or sink offset) |
| `records` | records moved since the server started |
| `status_secs` | seconds in the current status |
| `origin`, `file` | `api`, or `file` with the `connectors.d/` file name |
| `config` | (single connector only) the config as defined, with `${VAR}` unresolved |

Prometheus metrics (label `connector`):

| Metric | Description |
|--------|-------------|
| `exspeed_connector_state{state}` | 1 for the current state, 0 for the others |
| `exspeed_connector_restarts_total` | supervisor restarts |
| `exspeed_connector_lag{unit}` | as above |
| `exspeed_connector_last_success_timestamp_seconds` | last committed batch |
| `exspeed_connector_records_total{direction}` | `in` (appended by sources) or `out` (committed by sinks) |
| `exspeed_connector_retry_attempts_total{outcome}` | in-place retries (`retried`) and exhaustions (`exhausted`) |
| `exspeed_connector_transient_exhausted_total{action}` | `restart`, `fail` or `dlq_batch` |
| `exspeed_connector_dlq_total{reason}` | records written to the DLQ |
| `exspeed_connector_records_skipped_total{stream,reason}` | poison records dropped (no `dlq_stream`) |
| `exspeed_connector_dlq_failures_total` | DLQ appends that failed permanently |

## Errors, retries and the DLQ

Every plugin maps its errors into four classes:

| Class | Examples | What happens |
|-------|----------|--------------|
| **Transient** | timeout, HTTP 408/425/429/5xx, deadlock, serialization failure | retried in place with `[retry]` (honouring `Retry-After`); when exhausted, `on_transient_exhausted` applies |
| **Connection** | socket closed, server restart, RabbitMQ channel closed | supervisor restart with backoff (reconnect) |
| **Poison** | bad JSON, type mismatch, constraint violation, HTTP 4xx, unroutable AMQP message | the record goes to `dlq_stream`, or is dropped and counted; the connector continues |
| **Fatal** | bad config, HTTP 401/403, missing table, auth failure | connector → `failed` with a clear `last_error` |

`on_transient_exhausted`:

| Value | Behaviour |
|-------|-----------|
| `loop_forever` (default) | hand the error to the supervisor, which restarts the connector with bounded backoff (status `backoff`), forever |
| `fail` (alias `halt`) | move the connector to `failed` |
| `dlq_batch` | sinks only: send the remaining batch to `dlq_stream` and move on (`fail` without a `dlq_stream`) |

JDBC errors are classified per dialect: Postgres SQLSTATE classes, MySQL
vendor codes (1062 duplicate, 1213 deadlock, 1205 lock timeout, …), SQLite
extended result codes, and SQL Server error numbers (2601/2627 duplicate
key, 515 NULL, 1205 deadlock, …). Unknown errors are not retried forever:
data errors are poison, everything unrecognised is transient with the
bounded `[retry]` policy.

A DLQ record keeps the original key and body byte for byte and adds these
headers:

| Header | Value |
|--------|-------|
| `exspeed-dlq-origin` | name of the connector that rejected the record |
| `exspeed-dlq-reason` | stable label: `invalid_record`, `type_mismatch`, `http_client_error`, `sink_rejected`, `retries_exhausted`, … |
| `exspeed-dlq-detail` | human-readable error |
| `exspeed-dlq-original-offset` | offset on the source stream (sinks) |
| `exspeed-dlq-timestamp` | timestamp of the original record (sinks) |

DLQ writes carry their own idempotency key, so a replayed batch doesn't
duplicate DLQ entries.

## Offsets

Source checkpoints and sink positions are stored per connector.

- **Default (`EXSPEED_CONNECTOR_OFFSET_STORE=log`):** records keyed by
  connector name in the internal stream `__connector_offsets`, written
  through the broker's write path, so offsets live and replicate with the
  data. Loading reads backwards from the end of the stream, so it costs only
  the distance to the connector's last save.
- **`EXSPEED_CONNECTOR_OFFSET_STORE=file`:** one JSON file per connector in
  `<data-dir>/connector-offsets/`, written atomically (tmp file, fsync,
  rename, directory fsync). Local to one node.

If an offset can't be loaded (I/O error, corrupt record), the connector goes
to `failed` instead of silently starting over. Editing a connector keeps its
offsets; deleting it deletes them. The Postgres, Redis and S3 connector
offset stores were removed.

## Sources

| Plugin | What it does |
|--------|--------------|
| [`postgres_cdc`](#postgres_cdc) | Logical-replication CDC (pgoutput) with a Debezium-style envelope |
| [`postgres_poll`](#postgres_poll) | Polls tables by a tracking column with a composite cursor |
| [`postgres_outbox`](#postgres_outbox) | Transactional outbox, by polling or CDC |
| [`jdbc_poll`](#jdbc_poll) | Polls any SQL table by an integer cursor |
| [`mssql_cdc`](#mssql_cdc) | SQL Server Change Data Capture |
| [`rabbitmq`](#rabbitmq-source) | Consumes a queue |
| [`http_poll`](#http_poll) | Polls an HTTP endpoint, with pagination |
| [`http_webhook`](#http_webhook) | Accepts `POST /webhooks/<path>` |

### `postgres_cdc`

Needs `wal_level = logical` and a user with `REPLICATION`. The connector
creates the publication and the replication slot if they don't exist, and
alters the publication when `tables` changes.

```toml
[connector]
name = "users-cdc"
type = "source"
plugin = "postgres_cdc"
stream = "users_cdc"
subject_template = "{schema}.{table}.{op}"   # default; {op} = insert | update | delete

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
tables = ["public.users", "public.accounts"]
operations = ["insert", "update", "delete"]  # default
slot_name = "exspeed_users_cdc_slot"         # default: exspeed_<name>_slot
publication_name = "exspeed_users_cdc_pub"   # default: exspeed_<name>_pub
drop_slot_on_delete = false                  # drop the slot (and a derived publication) on delete
```

Each change becomes one record:

```json
{"op": "u",
 "before": {"id": 1},
 "after":  {"id": 1, "name": "b", "amount": 12.5, "active": true, "doc": {"x": 1}},
 "__unchanged": ["big_doc"],
 "source": {"connector": "users-cdc", "lsn": "0/16B3748", "txid": 731,
            "schema": "public", "table": "users", "ts_ms": 1700000000000}}
```

- `op` is `c`, `u` or `d`. `before` holds the replica-identity key columns
  (the full old row with `REPLICA IDENTITY FULL`); it is `null` for inserts
  and for updates that didn't change the key.
- Values are typed by column type: integers and floats are numbers,
  numerics are numbers when exact (strings otherwise), booleans are
  booleans, `json`/`jsonb` are parsed; everything else is the Postgres text
  form.
- Unchanged TOASTed columns are omitted from `after` and listed in
  `__unchanged`, never reported as `null`.
- **Key**: the replica-identity key columns in column order — the raw value
  for a single column, a JSON array for a composite key, none for
  `REPLICA IDENTITY NOTHING`.
- Each record carries `x-idempotency-key = pgcdc:<slot>:<commit LSN>:<n>`.
- The slot's position is confirmed only in `ack()`, after the records are
  durable. When nothing is in flight, keepalives advance it too, so an idle
  table doesn't pin WAL. Slot lag (`lag_unit = bytes`) is exported.
- `exspeed connector dry-run` peeks at an existing slot without consuming
  it, and creates nothing.

Without `drop_slot_on_delete`, deleting the connector leaves the slot in
place (it keeps retaining WAL); drop it with `pg_drop_replication_slot`.

### `postgres_poll`

```toml
[connector]
name = "orders-poll"
type = "source"
plugin = "postgres_poll"
stream = "orders"
poll_interval_ms = 5000
subject_template = "{schema}.{table}"   # default

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
tables = ["public.orders"]
tracking_column = "updated_at"          # default; must be NOT NULL
key_columns = ["id"]                    # tie-breaker; default: the primary key
```

Rows are emitted as `row_to_json`, so timestamps, numerics, uuids and json
decode correctly. Each table keeps its own cursor
`(tracking_column, key columns…)`, so rows that share a tracking value are
never skipped. The checkpoint is a JSON map of table → cursor.

Caveat: a row whose tracking value is lower than rows already polled (a
transaction that committed late, a clock going backwards) is missed. Use
`postgres_cdc` when that matters.

### `postgres_outbox`

```toml
[connector]
name = "pg-outbox"
type = "source"
plugin = "postgres_outbox"
stream = "domain_events"
subject_template = "{aggregate_type}.{event_type}"   # default

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
mode = "poll"                    # "poll" (default) | "cdc"
table = "outbox_events"          # default
id_column = "id"                 # int4, int8, uuid or text
key_column = "aggregate_id"      # becomes the record key
aggregate_type_column = "aggregate_type"
event_type_column = "event_type"
payload_column = "payload"       # text, json or jsonb; becomes the record value
order_column = "created_seq"     # poll + delete: delivery order (default: the id)
cleanup = "delete"               # "delete" (default) | "none"
# CDC mode only: slot_name, publication_name, drop_slot_on_delete
```

- Every record carries `x-idempotency-key` = the outbox id.
- With `cleanup = "delete"`, published rows are deleted in `ack()`, after
  the records are durable. Every remaining row is unpublished, so rows from
  transactions that commit late are picked up on the next poll.
- **Commit-order caveat:** poll mode with `cleanup = "none"` uses an
  `id > last_id` cursor, which needs an increasing integer id and can skip a
  row whose transaction commits after a row with a higher id was polled.
  Use `cleanup = "delete"` or `mode = "cdc"` (commit order) when that
  matters.
- CDC mode publishes inserts only.

### `jdbc_poll`

Works with Postgres, MySQL, SQLite (via sqlx) and SQL Server (via tiberius).
The `tracking_column` must be an **increasing integer**.

```toml
[connector]
name = "orders-poller"
type = "source"
plugin = "jdbc_poll"
stream = "orders_feed"
poll_interval_ms = 10000

[settings]
connection = "postgresql://user:pass@localhost:5432/app"   # or mysql://, sqlite:, mssql://
table = "orders"
tracking_column = "id"
schema = "id:bigint, customer:text, total:double, paid:boolean"   # required
```

The `schema` DSL is a list of `name:type` pairs. The supported types are
`text`, `bigint`, `double`, `boolean`, `timestamptz` and `jsonb`. Each
column is cast in SQL to its schema type, so native types such as
`DECIMAL`, `DATETIME`, `JSON`, `TINYINT(1)` or `BIT` decode; `timestamptz`
values are the database's text form (ISO 8601 on SQL Server). The
checkpoint is the last tracking value. For Postgres, prefer `postgres_poll`.

### `mssql_cdc`

```toml
[connector]
name = "cdc-orders"
type = "source"
plugin = "mssql_cdc"
stream = "orders-cdc"
subject_template = "mssql_cdc.{capture_instance}.{op}"   # default

[settings]
connection = "mssql://sa:Password1@localhost:1433/app?trust_server_certificate=true"
capture_instance = "dbo_orders"
schema = "id:bigint, total:double, status:text, placed_at:timestamptz"
key_columns = ["id"]             # optional record key
```

Records use the same envelope as `postgres_cdc` (`op`, `before`, `after`,
`source.lsn`, `source.seqval`); NULL is JSON `null`. The cursor is
`(__$start_lsn, __$seqval)`, so a transaction larger than `batch_size` is
paged through; once caught up, the cursor moves past the transaction with
`sys.fn_cdc_increment_lsn`. If the CDC cleanup job has removed changes past
the stored cursor, the connector fails instead of skipping them.

Before you start this connector, enable CDC on the database and the table,
and make sure SQL Server Agent is running.

### RabbitMQ source

```toml
[connector]
name = "rmq-ingest"
type = "source"
plugin = "rabbitmq"
stream = "incoming"
subject_template = "{routing_key}"   # default; also {exchange}, {queue}, {$.field}

[settings]
url = "amqp://guest:guest@localhost:5672/%2f"
queue = "my-queue"
prefetch_count = 100
declare_queue = true
queue_durable = true
queue_auto_delete = false
dedup_on_message_id = false   # AMQP message_id → x-idempotency-key
poll_wait_ms = 500
```

Deliveries are acknowledged only after the batch is durable; anything
unacked when the connector stops or crashes is redelivered by RabbitMQ.
AMQP headers and properties are copied into record headers
(`x-amqp-routing-key`, `x-amqp-exchange`, `x-message-id`, …).

### `http_poll`

```toml
[connector]
name = "weather-poller"
type = "source"
plugin = "http_poll"
stream = "weather"
subject_template = "weather.{$.kind}"

[settings]
url = "https://api.example.com/v1/items"
interval_secs = 60
method = "GET"
headers = { "X-API-Key" = "${WEATHER_API_KEY}" }
auth_type = "none"            # "none" | "bearer" | "basic" (with auth_token)
items_path = "data.items"     # one record per array element; unset = whole body
item_key = "id"               # record key
idempotent_items = true       # item_key → x-idempotency-key
next_page_path = "next"       # next page: a URL, or a token for page_param
page_param = "page"
timeout_secs = 30
```

Every item of every response is emitted (no truncation). With
`next_page_path`, all pages are fetched back to back, then the connector
waits for the next interval. Conditional requests (ETag, Last-Modified) are
used for the first page. HTTP 401/403 and other 4xx responses fail the
connector; 408/425/429/5xx are retried.

### `http_webhook`

```toml
[connector]
name = "stripe-webhook"
type = "source"
plugin = "http_webhook"
stream = "stripe_events"
subject_template = "stripe.{$.type}"   # {$.field} is read from the JSON body

[settings]
path = "stripe"                        # served at POST /webhooks/stripe
auth_type = "hmac_sha256"              # required: "none" | "bearer" | "hmac_sha256"
auth_secret = "${STRIPE_WEBHOOK_SECRET}"
signature_header = "X-Signature-256"   # hmac_sha256: hex HMAC of the raw body …
signature_prefix = "sha256="           # … optionally after this prefix
```

```bash
body='{"type": "payment_intent.succeeded"}'
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$STRIPE_WEBHOOK_SECRET" -hex | cut -d' ' -f2)
curl -X POST localhost:8080/webhooks/stripe -H "X-Signature-256: sha256=$sig" \
  -H 'Idempotency-Key: evt_123' -d "$body"
# {"offset": 0}
```

- `bearer` checks `Authorization: Bearer <secret>` in constant time.
- `Idempotency-Key` (or `x-idempotency-key`) makes retries by the sender
  return the original offset instead of a duplicate.
- `/webhooks/*` is never covered by the server's bearer-token auth, so
  `auth_type` must be chosen explicitly.
- Responses: `200 {"offset": N}` once stored; `401` bad credentials; `409`
  same idempotency key with a different body; `503` not the leader or still
  starting (retry).

## Sinks

| Plugin | What it does | Default flush interval |
|--------|--------------|------------------------|
| [`jdbc`](#jdbc) | Writes rows to Postgres, MySQL, SQLite or SQL Server | after every batch |
| [`http_sink`](#http_sink) | Sends each record to an HTTP endpoint | after every batch |
| [`rabbitmq`](#rabbitmq-sink) | Publishes to an exchange | after every batch |
| [`s3`](#s3) | Writes NDJSON objects to S3 or MinIO | 60 s |

### `jdbc`

```toml
[connector]
name = "analytics-sink"
type = "sink"
plugin = "jdbc"
stream = "analytics_events"
subject_filter = "event.>"

[settings]
connection = "postgresql://user:pass@localhost:5432/analytics"   # or mysql://, sqlite:, mssql://
table = "events"
mode = "upsert"                 # "upsert" (default) | "insert"
upsert_keys = ["id"]            # required for upsert with a schema
schema = "id:bigint, name:text, amount:double, at:timestamptz, raw:jsonb"
auto_create_table = false
max_rows_per_statement = 500    # multi-row statements, capped by the dialect's parameter limit
```

- **Typed mode** (`schema` set): each JSON field maps to its own column. A
  field with the wrong type, or a missing required field, is poison.
- **Blob mode** (no `schema`): `(offset, subject, key, value)` rows, upserted
  on `offset`.

Rows are written with multi-row statements. When one fails with a data
error, the chunk is retried row by row so only the bad row goes to the DLQ.

| Database | Upsert statement |
|----------|------------------|
| Postgres | `ON CONFLICT … DO UPDATE` (typed placeholders) |
| SQLite (3.24+) | `ON CONFLICT … DO UPDATE` |
| MySQL | `ON DUPLICATE KEY UPDATE` |
| SQL Server | `MERGE … WITH (HOLDLOCK)`, via tiberius |

### `http_sink`

```toml
[connector]
name = "notify"
type = "sink"
plugin = "http_sink"
stream = "notifications"

[settings]
url = "https://api.example.com/notify"
method = "POST"
content_type = "application/json"
headers = { Authorization = "Bearer ${API_TOKEN}" }
timeout_secs = 30
```

Each request carries `Idempotency-Key` (the record's `x-idempotency-key`,
or `<stream>:<offset>`), `X-Exspeed-Subject` and `X-Exspeed-Offset`.
Responses: 2xx success; 408/425/429/5xx and timeouts retried (honouring
`Retry-After`); 401/403 fail the connector; other 4xx are poison.

### RabbitMQ sink

```toml
[connector]
name = "rmq-publish"
type = "sink"
plugin = "rabbitmq"
stream = "outgoing"

[settings]
url = "amqp://guest:guest@localhost:5672/%2f"
exchange = "events"
exchange_type = "topic"         # default
declare_exchange = true
exchange_durable = true
routing_key = "{subject}"       # template: {subject}, {key}, {stream}, {$.field}, or a literal
persistent = true               # delivery_mode = 2
mandatory = true                # unroutable messages are returned → poison
propagate_headers = true
```

Publisher confirms are always on: a record counts as written only once the
broker has confirmed it. Messages carry the record headers, `message_id` =
idempotency key, `x-exspeed-offset` and `x-exspeed-subject`. With
`mandatory`, a record the exchange can't route goes to the DLQ; the records
after it in the same batch are not published again.

### `s3`

```toml
[connector]
name = "s3-archive"
type = "sink"
plugin = "s3"
stream = "orders"
flush_interval_ms = 60000       # default for s3

[settings]
bucket = "order-archive"
region = "us-east-1"
endpoint = "http://minio:9000"   # MinIO / S3-compatible stores
path_style = true                # usually needed for MinIO
access_key = "${AWS_ACCESS_KEY_ID}"      # both unset = the standard AWS credential chain
secret_key = "${AWS_SECRET_ACCESS_KEY}"
prefix = "archive/"
max_records = 10000              # flush early when the buffer is this big …
max_bytes = 16777216             # … or this many bytes
timeout_secs = 60
```

Objects are written to
`{prefix}{stream}/{YYYY}/{MM}/{DD}/{HH}/part-{first_offset:020}.ndjson`,
where the date is the first record's timestamp. Each line is
`{"offset", "timestamp", "subject", "key", "value", "headers"}` (key and
value as UTF-8, or base64 for binary).

## Transforms

A `[transform]` section on a **source** runs an ExQL projection or filter
over each record before it is appended. Sinks reject transforms.

```toml
[transform]
sql = "SELECT payload->>'id' AS id, payload->>'email' AS email WHERE payload->>'status' = 'active'"
```

## Validating configs

```bash
exspeed connector validate connectors.d/orders-cdc.toml
exspeed connector dry-run  connectors.d/orders-cdc.toml --max 3
```

- `validate` builds the plugin through the same registry as the server:
  syntax, name, stream, transform, `${VAR}` resolution and every plugin
  setting (a misspelled key fails).
- `dry-run` also connects and prints sample records **without side
  effects**: CDC sources peek at an existing slot and create nothing, queue
  sources don't ack, outbox sources don't delete, sinks only connect.

## HTTP API

Connectors created over HTTP use the same fields as the `[connector]`
table at the top level, plus `settings`, `retry`, `restart` and
`transform_sql`. Unknown fields are rejected. `${VAR}` substitution is
**not** applied to API-created connectors.

```bash
curl -X POST localhost:8080/api/v1/connectors -H 'Content-Type: application/json' -d '{
  "name": "my-webhook",
  "type": "source",
  "plugin": "http_webhook",
  "stream": "events",
  "settings": {"path": "my-webhook", "auth_type": "bearer", "auth_secret": "s3cret"}
}'

curl localhost:8080/api/v1/connectors                         # list with status
curl localhost:8080/api/v1/connectors/my-webhook              # status + config
curl -X PUT localhost:8080/api/v1/connectors/my-webhook -d @config.json   # replace, keeps offsets
curl -X POST localhost:8080/api/v1/connectors/my-webhook/restart          # also revives "failed"
curl -X DELETE localhost:8080/api/v1/connectors/my-webhook
```

| Status | When |
|--------|------|
| `400` | invalid config (the message names the field or setting) |
| `404` | no such connector |
| `409` | name already exists or collides; `PUT` on a file-defined connector |
| `503` | restart requested on a non-leader |
