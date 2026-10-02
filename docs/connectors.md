# Connectors

Connectors move data between Exspeed and external systems.

- **Sources** write into a stream.
- **Sinks** read from a stream and write somewhere else.

You can define a connector in two ways:

- **As a TOML file** in `<data-dir>/connectors.d/`. These files are hot-reloaded.
- **Through the HTTP API** at `POST /api/v1/connectors`.

> ⚠️ **Read this before relying on connectors.** The framework works, but
> several defaults lose data today:
>
> - **Dedup is on by default.** It is keyed on the record key, so connectors
>   whose key identifies an entity drop every event after the first one for
>   that entity in a 24-hour window. Set `dedup_enabled = false` unless your
>   key really is a message ID.
> - **API-created connectors get deleted.** The file watcher removes any
>   running connector that has no matching `<name>.toml` file. Pick one
>   method per deployment, and name each file `<connector name>.toml`.
> - **Editing a TOML file resets the connector's offsets.**
> - **Postgres CDC can lose data on a crash**, because it acknowledges WAL
>   before the records are appended.
> - **The S3 sink can lose data**, because it buffers records in memory and
>   reports them as committed.
>
> Each plugin's status note below lists its own known problems. The full
> analysis and the redesign are in [REVIEW.md §3.6](REVIEW.md#36-connectors-exspeed-connectors).

## Contents

- [Config file format](#config-file-format)
- [Sources](#sources)
- [Sinks](#sinks)
- [Retry and dead-letter queue](#retry-and-dead-letter-queue)
- [Transforms](#transforms)
- [Validating configs](#validating-configs)
- [HTTP API](#http-api)

## Config file format

```toml
[connector]
name = "orders-cdc"          # must match the file name: orders-cdc.toml
type = "source"              # "source" | "sink"
plugin = "postgres"          # see the plugin list below
stream = "orders"            # stream to write to (source) or read from (sink)

# Optional, common to all connectors
subject_template = ""        # sources: subject for produced records (placeholders are plugin-specific, see below)
subject_filter = ""          # sinks: only deliver matching subjects (NATS wildcards)
key_field = ""               # sources: JSON field to use as the record key
batch_size = 100
poll_interval_ms = 50
dedup_enabled = true         # ⚠️ default true — see the warning above
dedup_key = ""               # field to dedup on (default: the record key)
dedup_window_secs = 86400
on_transient_exhausted = "loop_forever"  # "loop_forever" | "halt" | "dlq_batch"

[settings]                   # plugin-specific; ALL values are strings
connection = "${DATABASE_URL}"

[retry]                      # optional
max_retries = 5
initial_backoff_ms = 100
max_backoff_ms = 30000
multiplier = 2.0
jitter = true

[transform]                  # optional, sources only
sql = "SELECT payload->>'id' AS id, payload->>'total' AS total"
```

**Every value under `[settings]` must be a TOML string.**

- Write `interval_secs = "60"`, not `interval_secs = 60`.
- Write `headers = "Authorization: Bearer x, X-Env: prod"`, not an inline table.

A connector whose settings contain non-string values fails to parse. It is
then **silently skipped** with a warning in the server log.

**`${VAR}` and `${VAR:-default}`** are replaced with environment variables
from the server's environment. This only applies to files in `connectors.d/`.

## Sources

| Plugin | What it does | Status |
|--------|--------------|--------|
| [`http_webhook`](#http_webhook) | Accepts `POST /webhooks/<path>` | Works. Auth is optional and **off by default**. |
| [`postgres` (`mode = "cdc"`)](#postgres) | Logical-replication CDC (pgoutput) | ⚠️ Can lose data on a crash. Picks an arbitrary column as the key. TOASTed columns come through as null. |
| [`postgres` (`mode = "poll"`)](#postgres) | Polls tables by a timestamp column | ❌ Stops after the first batch, because timestamps aren't decoded. |
| [`postgres_outbox`](#postgres_outbox) | Transactional outbox, by polling or CDC | ⚠️ Only `text`/`bigint` columns are supported. Turn dedup off. |
| [`jdbc_poll`](#jdbc_poll) | Polls any SQL table by an integer cursor | Works with an integer cursor. |
| [`mssql_cdc`](#mssql_cdc) | SQL Server Change Data Capture | ⚠️ Re-emits the last transaction on every poll. Stalls on transactions larger than `batch_size`. |
| [`rabbitmq`](#rabbitmq-source) | Consumes from a queue | ⚠️ Doesn't reconnect. Turn dedup off. |
| [`http_poll`](#http_poll) | Polls an HTTP endpoint | ⚠️ Silently truncates responses at `batch_size`. |

### `http_webhook`

```toml
[connector]
name = "stripe-webhook"
type = "source"
plugin = "http_webhook"
stream = "stripe_events"
subject_template = "stripe.{$.type}"   # optional; {$.field} is read from the JSON body

[settings]
path = "stripe"                        # served at POST /webhooks/stripe
auth_type = "bearer"                   # "none" (default!) | "bearer"
auth_secret = "${STRIPE_WEBHOOK_SECRET}"
```

```bash
curl -X POST localhost:8080/webhooks/stripe \
  -H "Authorization: Bearer $STRIPE_WEBHOOK_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{"type": "payment_intent.succeeded"}'
# {"offset": 0}
```

`/webhooks/*` is never covered by the server's bearer-token auth. Always set
`auth_type = "bearer"` on internet-facing webhooks. HMAC signatures (for
example Stripe's) are not verified.

### `postgres`

One plugin with two modes.

**CDC mode.** This needs `wal_level=logical`. The connector creates the
publication and the replication slot if they don't exist. It never drops
them, so when you delete the connector, run `pg_drop_replication_slot`
yourself.

```toml
[connector]
name = "users-cdc"
type = "source"
plugin = "postgres"
stream = "users_cdc"
dedup_enabled = false
subject_template = "{schema}.{table}"   # default: "<schema>.<table>.change"

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
mode = "cdc"
tables = "public.users,public.accounts"
operations = "INSERT,UPDATE,DELETE"     # default
slot_name = "exspeed_users"             # default derived from the connector name
publication_name = "exspeed_users_pub"  # default derived from the connector name
```

**Poll mode.** This is currently broken for timestamp columns (see the
status table above).

```toml
[settings]
connection = "postgresql://user:pass@localhost:5432/app"
mode = "poll"
tables = "public.users"
timestamp_column = "updated_at"
```

### `postgres_outbox`

```toml
[connector]
name = "pg-outbox"
type = "source"
plugin = "postgres_outbox"
stream = "domain_events"
subject_template = "{aggregate_type}.{event_type}"
dedup_enabled = false                 # outbox key = aggregate_id; dedup would drop events

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
mode = "poll"                         # "poll" (default) | "cdc"
outbox_table = "outbox_events"        # default
id_column = "id"                      # bigint
key_column = "aggregate_id"           # text
aggregate_type_column = "aggregate_type"
event_type_column = "event_type"
payload_column = "payload"            # text (jsonb is not decoded yet)
cleanup_mode = "delete"               # "delete" (default) — rows removed after publish
```

The connector sets an `x-idempotency-key` header on each record, so a retry
after a crash is deduplicated by the broker's [idempotent publish](idempotent-publish.md).

### `jdbc_poll`

This plugin works with Postgres, MySQL, SQLite (via sqlx) and SQL Server (via
tiberius). The `tracking_column` must be an **increasing integer**.

```toml
[connector]
name = "orders-poller"
type = "source"
plugin = "jdbc_poll"
stream = "orders_feed"
poll_interval_ms = 10000

[settings]
connection = "postgresql://user:pass@localhost:5432/app"
table = "orders"
tracking_column = "id"
schema = "id:bigint, customer:text, total:double, paid:boolean"   # required
```

The `schema` DSL is a list of `name:type` pairs. The supported types are
`text`, `bigint`, `double`, `boolean`, `timestamptz` and `jsonb`.

### `mssql_cdc`

```toml
[connector]
name = "cdc-orders"
type = "source"
plugin = "mssql_cdc"
stream = "orders-cdc"

[settings]
connection = "mssql://sa:Password1@localhost:1433/master?trust_server_certificate=true"
capture_instance = "dbo_orders"
schema = "id:bigint, total:bigint, status:text"
```

Each event is a JSON object with an `__op` field, set to one of `insert`,
`update_before`, `update_after` or `delete`.

Before you start this connector:

- CDC must be enabled on the table.
- SQL Server Agent must be running.

### RabbitMQ source

```toml
[connector]
name = "rmq-ingest"
type = "source"
plugin = "rabbitmq"
stream = "incoming"
dedup_enabled = false     # key = routing key; dedup would drop messages

[settings]
url = "amqp://guest:guest@localhost:5672"
queue = "my-queue"
prefetch_count = "100"
queue_durable = "true"
queue_auto_delete = "false"
```

### `http_poll`

```toml
[connector]
name = "weather-poller"
type = "source"
plugin = "http_poll"
stream = "weather"
subject_template = "weather.current"

[settings]
url = "https://api.example.com/v1/current"
interval_secs = "60"
method = "GET"
headers = "X-API-Key: ${WEATHER_API_KEY}"
items_path = "data.items"   # optional: emit one record per array element
item_key = "id"             # optional: record key from each item
auth_type = "none"          # or "bearer" with auth_token = "..."
```

The connector sends conditional requests using ETag and Last-Modified. It
has no pagination.

## Sinks

| Plugin | What it does | Status |
|--------|--------------|--------|
| [`jdbc`](#jdbc) | Writes rows to Postgres, MySQL, SQLite or SQL Server | Works. Unknown SQL errors are retried forever under the default policy. |
| [`http_sink`](#http_sink) | Sends each record to an HTTP endpoint | ⚠️ No request timeout. 401 and 429 responses are treated as poison. |
| [`rabbitmq`](#rabbitmq-sink) | Publishes to an exchange | ⚠️ Messages are not persistent. Doesn't reconnect. |
| [`s3`](#s3) | Writes batches of records to S3 or MinIO | ❌ Can lose buffered data on restart. Has no timed flush. |

### `jdbc`

```toml
[connector]
name = "analytics-sink"
type = "sink"
plugin = "jdbc"
stream = "analytics_events"
subject_filter = "event.>"

[settings]
connection = "postgresql://user:pass@localhost:5432/analytics"   # or mysql://, sqlite://, mssql://
table = "events"
mode = "upsert"                 # "upsert" (default) | "insert"
upsert_keys = "id"              # required for upsert with a schema
schema = "id:bigint, name:text, amount:double, at:timestamptz, raw:jsonb"
auto_create_table = "false"
```

The table can be laid out in two ways:

- **Typed mode** (`schema` is set): each JSON field maps to its own column.
- **Blob mode** (`schema` is empty): the whole record is stored in a single
  JSON column.

How each database does upserts:

| Database | Upsert statement |
|----------|------------------|
| Postgres | `ON CONFLICT … DO UPDATE` |
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
headers = "Authorization: Bearer ${API_TOKEN}"
```

### RabbitMQ sink

```toml
[connector]
name = "rmq-publish"
type = "sink"
plugin = "rabbitmq"
stream = "outgoing"

[settings]
url = "amqp://guest:guest@localhost:5672"
exchange = "events"
exchange_type = "topic"         # default
exchange_durable = "true"
routing_key_from = "subject"    # the record subject becomes the routing key
publisher_confirms = "true"
```

### `s3`

```toml
[connector]
name = "s3-archive"
type = "sink"
plugin = "s3"
stream = "orders"

[settings]
bucket = "order-archive"
region = "us-east-1"
endpoint = "http://minio:9000"   # for MinIO / S3-compatible stores
path_style = "true"              # usually needed for MinIO
access_key = "${AWS_ACCESS_KEY_ID}"
secret_key = "${AWS_SECRET_ACCESS_KEY}"
prefix = "orders/"
flush_interval_secs = "60"
```

## Retry and dead-letter queue

Errors fall into two classes.

**Transient errors** cause the whole batch to be retried using the `[retry]`
policy. When retries are exhausted, `on_transient_exhausted` decides what
happens:

| Value | Behaviour |
|-------|-----------|
| `loop_forever` | Keep retrying. This is the default. |
| `halt` | Stop the connector. Its status still reads "running"; this is a known issue. |
| `dlq_batch` | Send the batch to `dlq_stream` and move on. |

**Poison records**, such as bad JSON, a type mismatch or an HTTP 4xx, are
written to `settings.dlq_stream` if it is set. Otherwise they are dropped
and counted in a metric.

```toml
[settings]
dlq_stream = "events-dlq"
```

A DLQ record keeps the original body byte for byte and adds these headers:

| Header | Value |
|--------|-------|
| `exspeed-dlq-origin` | name of the connector that rejected the record |
| `exspeed-dlq-reason` | stable label, e.g. `type_mismatch` or `http_client_error` |
| `exspeed-dlq-detail` | human-readable error |
| `exspeed-dlq-original-offset` | offset on the source stream |
| `exspeed-dlq-timestamp` | timestamp of the original record |

## Transforms

A `[transform]` section on a **source** runs an ExQL projection over each
record before it is appended. Transforms on sinks are accepted but ignored.

```toml
[transform]
sql = "SELECT payload->>'id' AS id, payload->>'email' AS email WHERE payload->>'status' = 'active'"
```

## Validating configs

```bash
exspeed connector validate connectors.d/orders-cdc.toml   # syntax, plugin, stream name, transform
exspeed connector dry-run  connectors.d/orders-cdc.toml   # + connects and fetches one sample
```

Neither command checks plugin settings yet, so a misspelled setting name
passes validation. `dry-run` on a CDC source **creates the replication slot
and publication** and acknowledges one transaction. Don't run it against
production.

## HTTP API

Connectors created over HTTP use JSON with the same fields. The differences
are that `type` is spelled `connector_type`, `transform_sql` is a top-level
field, and **`${VAR}` substitution is not applied**.

```bash
curl -X POST localhost:8080/api/v1/connectors -H 'Content-Type: application/json' -d '{
  "name": "my-webhook",
  "connector_type": "source",
  "plugin": "http_webhook",
  "stream": "events",
  "dedup_enabled": false,
  "settings": {"path": "my-webhook", "auth_type": "bearer", "auth_secret": "s3cret"}
}'

curl localhost:8080/api/v1/connectors
curl localhost:8080/api/v1/connectors/my-webhook
curl -X POST localhost:8080/api/v1/connectors/my-webhook/restart
curl -X DELETE localhost:8080/api/v1/connectors/my-webhook
```

Connectors only run on the leader in multi-pod mode.
