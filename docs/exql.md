# ExQL — SQL over streams

ExQL lets you query streams with SQL. It runs on
[Apache DataFusion](https://datafusion.apache.org/) (v55) with JSON support
from `datafusion-functions-json`. There are three kinds of query:

- **Bounded:** a one-shot `SELECT` over the current contents of streams,
  materialized tables and registered external databases.
- **Continuous stream:** `CREATE STREAM out AS SELECT …` runs until
  dropped and appends each result row to the stream `out`.
- **Materialized table:** `CREATE TABLE t AS SELECT … GROUP BY …` keeps
  the current aggregate per key. You can query it with SQL and read it over
  HTTP, and it is backed by a changelog stream.

Every statement goes to `POST /api/v1/queries` (or `exspeed query "…"`).

## Contents

- [The stream table](#the-stream-table)
- [Running statements](#running-statements)
- [Bounded queries](#bounded-queries)
- [JSON](#json)
- [Continuous queries](#continuous-queries)
- [Event time, watermarks and late data](#event-time-watermarks-and-late-data)
- [Windows](#windows)
- [Joins in continuous queries](#joins-in-continuous-queries)
- [Materialized tables](#materialized-tables)
- [Output records](#output-records)
- [State, recovery and delivery guarantees](#state-recovery-and-delivery-guarantees)
- [Query lifecycle](#query-lifecycle)
- [External databases](#external-databases)
- [Functions](#functions)
- [Limits and configuration](#limits-and-configuration)
- [What is not supported](#what-is-not-supported)

## The stream table

Every stream is a table with these columns:

| Column | Type | Notes |
|--------|------|-------|
| `offset` | `UInt64` | |
| `timestamp` | `Timestamp(ms, UTC)` | Broker append time. Rendered as RFC 3339 (`2026-10-02T10:00:00.123Z`) |
| `subject` | text | |
| `key` | text, nullable | |
| `payload` | text (JSON) | Use `payload->>'field'` / `payload->'field'` ([JSON](#json)). Results render it as JSON |
| `headers` | text (JSON object) | `headers->>'content-type'` |

A materialized table with the same name shadows its changelog stream.
Unquoted names are lower-cased. Quote anything else:
`SELECT * FROM "Order-Events"`.

## Running statements

```bash
exspeed query "SELECT * FROM orders LIMIT 10"

curl -X POST localhost:8080/api/v1/queries -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM orders LIMIT 10"}'
# {"columns": [...], "rows": [[...]], "row_count": 10, "execution_time_ms": 3, "truncated": false}
```

| Statement | Result |
|-----------|--------|
| `SELECT …`, `WITH …`, `EXPLAIN …` | Rows (200) |
| `CREATE [OR REPLACE] STREAM\|VIEW [IF NOT EXISTS] <name> AS SELECT …` | The new query (201) |
| `CREATE [OR REPLACE] TABLE\|MATERIALIZED VIEW [IF NOT EXISTS] <name> AS SELECT …` | The new query (201) |
| `DROP STREAM\|VIEW [IF EXISTS] <name>` | Drops the query writing it, then deletes the stream |
| `DROP TABLE\|MATERIALIZED VIEW [IF EXISTS] <name>` | Drops the table's query, the table and its changelog |
| `DROP QUERY <id>` / `TERMINATE <id>` | Drops the query and keeps its output |
| `PAUSE QUERY <id>` / `RESUME QUERY <id>` | The query's new state |
| `CREATE INDEX` / `DROP INDEX` | Error `UNSUPPORTED`. Secondary indexes were removed |

Only one statement per request is accepted. Errors are structured, and the
HTTP status follows the code:

```json
{"error": "parse error: Expected: …, found: FORM at Line: 1, Column: 10", "code": "PARSE_ERROR", "line": 1, "column": 10}
{"error": "not supported: CREATE INDEX / DROP INDEX", "code": "UNSUPPORTED", "hint": "secondary indexes were removed; …"}
```

| Code | HTTP |
|------|------|
| `PARSE_ERROR`, `PLAN_ERROR`, `UNSUPPORTED`, `EXECUTION_ERROR` | 400 |
| `NOT_FOUND` | 404 |
| `TIMEOUT` | 408 |
| `CONFLICT` | 409 |
| `RESOURCES_EXHAUSTED` | 422 |
| `NOT_LEADER`, `TRANSIENT_ERROR` | 503 |

A misspelled stream, column or function is a `PLAN_ERROR`. It never gives
an empty result or NULL.

Bounded queries also run over TCP (`OpCode::Query`; SDK `client.query(sql)`),
and the result JSON is the same.

> 🔒 Every ExQL endpoint requires a global admin credential over both HTTP
> and TCP, and runs on the leader.

## Bounded queries

Everything DataFusion supports works. That includes `WHERE`, `GROUP BY`
(ordinals, aliases and expressions), `HAVING`, `DISTINCT`, every join type,
window functions (`OVER`), scalar, `IN` and `EXISTS` subqueries, CTEs,
`UNION`, `ORDER BY` on columns that aren't projected, `INTERVAL` arithmetic
and `now()`.

```sql
SELECT payload->>'region' AS region, COUNT(*) AS n, SUM(payload->>'total') AS revenue
FROM orders
WHERE timestamp > now() - INTERVAL '1 hour' AND payload->>'total' > 100
GROUP BY payload->>'region'
HAVING COUNT(*) > 10
ORDER BY revenue DESC;

SELECT * FROM orders ORDER BY offset DESC LIMIT 20;           -- reads only the tail

SELECT o.key, ROW_NUMBER() OVER (PARTITION BY o.subject ORDER BY o.offset) AS rn
FROM orders o;
```

**Pushdown.** The scan reads only what it needs:

| Predicate | Effect |
|-----------|--------|
| `offset >= a AND offset < b`, `offset = n`, `BETWEEN` | Reads only `[a, b)` |
| `timestamp >= / > / < / <= t` (constant, `now() - INTERVAL …` works) | Lower/upper bounds through the time index (`seek_by_time`) |
| `LIMIT n` (no ORDER BY) | Stops after `n` rows |
| `ORDER BY offset DESC LIMIT n` (with filters on top) | Reads the stream backwards from the end |

Predicates on other columns run in DataFusion after the scan. `EXPLAIN`
shows the scan, e.g. `StreamScanExec: stream=orders, offsets=[9000, 9100),
reverse=false, fetch=None`.

## JSON

`payload->'k'` returns a JSON value, `payload->>'k'` returns text, and paths
chain (`payload->'a'->>'b'`, `payload->'items'->0`). The
`json_get_str/int/float/bool/json`, `json_contains`, `json_length` and
`json_as_text` functions are also available.

`->>` yields text. ExQL gives it **numeric meaning where the context is
numeric**, so the common cases need no casts:

| Context | Behaviour |
|---------|-----------|
| `payload->>'amount' > 250`, `= 3`, `BETWEEN 1 AND 9`, `IN (1, 2)` | Compared as a number (`TRY_CAST … AS DOUBLE`) |
| `payload->>'a' + 1`, `* 2`, `% 3`, … | Arithmetic on numbers |
| `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VAR`, `MEDIAN`, … of JSON text | Numeric (also as window functions) |
| `ABS`, `ROUND`, `CEIL`, `FLOOR`, `SQRT`, `LN`, `POWER`, … | Numeric |
| `ORDER BY payload->>'amount'` (or an alias of it) | Numbers sort numerically. Non-numbers sort after them, as text |
| `a.payload->>'x' < b.payload->>'y'` (both JSON) | Numeric if both are numbers, text otherwise |
| `a.payload->>'id' = b.payload->>'id'` (both JSON) | **Text** equality, so it stays usable as a join key |
| `payload->>'flag' = true` | Boolean |

Non-numeric text in a numeric context becomes NULL, as `TRY_CAST` does.
JSON numbers stored as strings (`"amount": "1000"`) work the same as plain
numbers. `CAST(payload->>'x' AS VARCHAR)` opts out. These rules are covered
by a differential test suite that runs the same queries on SQLite.

## Continuous queries

```sql
CREATE STREAM big_orders AS
  SELECT key, payload->>'region' AS region, payload->>'total' AS total
  FROM orders
  WHERE payload->>'total' > 1000;

CREATE TABLE revenue_by_region AS
  SELECT payload->>'region' AS region, COUNT(*) AS n, SUM(payload->>'total') AS revenue
  FROM orders
  GROUP BY payload->>'region';
```

A continuous query is a micro-batch dataflow. It wakes on appends to its
source streams (falling back to polling every 200 ms), reads up to
1,000 records per source per micro-batch, runs them through the operators,
writes the output through the broker's write path (the same `Log` producers
use) and advances its positions. **Queries start from the beginning of their
sources**, so creating a query over history replays that history.

Supported shape (anything else is rejected with `UNSUPPORTED` before
anything is created):

```text
SELECT <expressions>                       -- any scalar expressions, CASE, functions, JSON
FROM <stream> [[AS] a] [TIMESTAMP BY <expr>]
  [ [LEFT] JOIN <stream|table> [[AS] b] [TIMESTAMP BY <expr>] [WITHIN <interval>] ON <cond> [WITHIN <interval>] ]
[WHERE <predicate>]
[WINDOW TUMBLING (SIZE <interval> [, GRACE PERIOD <interval>])
 | WINDOW HOPPING (SIZE <interval>, ADVANCE BY <interval> [, GRACE PERIOD <interval>])]
[GROUP BY <expressions>]
[HAVING <predicate>]
[GRACE PERIOD <interval>]
[EMIT CHANGES | EMIT FINAL]
```

- **Operators.** Filter and project use DataFusion physical expressions over
  Arrow batches. Aggregation keeps one DataFusion `Accumulator` per
  aggregate per group, so every DataFusion aggregate works (`COUNT`,
  `COUNT(DISTINCT …)`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `MEDIAN`,
  `LAST_VALUE`, `ARRAY_AGG`, `agg(x) FILTER (WHERE …)`, …), as do
  expressions over aggregates (`SUM(a)/COUNT(*)`, `ROUND(AVG(x), 1)`).
- **Intervals** can be written `INTERVAL '5 minutes'`, `'5 minutes'`,
  `5 MINUTES`, `INTERVAL '10' SECOND`, `'1h30m'` or `'500ms'`. Units go from
  ms to weeks.
- **EMIT CHANGES** (the default) emits one updated row per changed group
  per micro-batch. **EMIT FINAL** emits a window's row exactly once, when
  the window closes. It requires a `WINDOW`.
- Without GROUP BY, an aggregate is **global**: one row, which for tables
  exists (e.g. `COUNT(*) = 0`) before any input arrives.
- `HAVING` filters groups. For tables, a group that stops matching is
  deleted.

## Event time, watermarks and late data

- **Event time** is the record timestamp unless the relation has
  `TIMESTAMP BY <expr>`. The expression may give a timestamp, epoch
  milliseconds (number or numeric text), or RFC 3339 /
  `YYYY-MM-DD HH:MM:SS[.fff]` text (taken as UTC). If it is NULL or can't be
  parsed, the record timestamp is used.
- **Watermark** = min over sources of (max event time seen in that source)
  − `GRACE PERIOD`. The grace period defaults to **0**
  (`EXSPEED_EXQL_DEFAULT_GRACE_MS`). A source that has produced no records
  yet doesn't hold the watermark back. An idle source does hold it back,
  because there is no wall-clock timeout.
- **Windows close and join buffers are evicted on the watermark, never on
  the wall clock**, so replaying history gives the same results as live
  processing.
- **Late records** (a window assignment that has already closed, or a join
  input older than the watermark) are dropped. They are counted in the
  query's `stats.late_records_dropped` and the Prometheus counter
  `exspeed_exql_late_records_total{query}`.
- `now()` in a continuous query is the planning time. Don't use it for
  event-time logic.

## Windows

```sql
CREATE STREAM clicks_per_minute AS
  SELECT payload->>'user' AS usr, window_start, window_end, COUNT(*) AS n, MAX(payload->>'ms') AS slowest
  FROM clicks TIMESTAMP BY payload->>'ts'
  WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 10 SECONDS)
  GROUP BY payload->>'user'
  EMIT FINAL;

CREATE TABLE sliding_counts AS
  SELECT window_start, COUNT(*) AS n FROM clicks
  WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE);
```

- Windows are `[start, end)`, aligned to the epoch. A hopping window places
  each record in `SIZE / ADVANCE` windows (at most 1,000).
- `window_start` and `window_end` (`Timestamp(ms, UTC)`) can be used in the
  SELECT list and in HAVING. The window is implicitly part of the GROUP BY.
- A window closes when the watermark reaches its end. Its state is freed
  then, for both emit modes. A windowed **table** keeps one row per window
  and group.
- Session windows aren't supported.

## Joins in continuous queries

**Stream-stream join** (INNER or LEFT) with `WITHIN`:

```sql
CREATE STREAM paid_orders AS
  SELECT o.payload->>'id' AS order_id, p.payload->>'amount' AS amount
  FROM orders o TIMESTAMP BY o.payload->>'ts'
  LEFT JOIN payments p TIMESTAMP BY p.payload->>'ts' WITHIN 10 MINUTES
    ON o.payload->>'id' = p.payload->>'order_id' AND o.key = p.key AND p.payload->>'amount' > 0
  GRACE PERIOD 1 MINUTE;
```

- `ON` needs at least one equality between the two sides. All equalities
  form a composite key (either orientation, `a.x = b.y` or `b.y = a.x`).
  Everything else in `ON` is a residual predicate. NULL keys never match.
- A pair matches when the keys are equal, `|t_left - t_right| <= WITHIN`
  and the residual holds. Arrival order doesn't matter, within the grace
  period.
- LEFT JOIN emits a left row with NULLs once the watermark passes
  `t_left + WITHIN` with no match. A windowed aggregate after a join holds
  its windows open for an extra `WITHIN`, so these rows aren't late.
- Two inputs per query. RIGHT and FULL joins aren't supported; swap the
  sides.

**Stream-table join** (INNER, or `stream LEFT JOIN table`) looks each record
up in a materialized table. The table's current contents are used, and
updates are seen live:

```sql
CREATE TABLE customer_names AS
  SELECT payload->>'id' AS id, LAST_VALUE(payload->>'name') AS name FROM customers GROUP BY payload->>'id';

CREATE STREAM enriched_orders AS
  SELECT o.payload->>'order' AS order_id, c.name
  FROM orders o LEFT JOIN customer_names c ON o.payload->>'customer' = c.id;
```

A stream-table join is evaluated against the table **as of processing
time**. A replay after a restart sees the table's contents at that time.

## Materialized tables

`CREATE TABLE <name> AS SELECT … GROUP BY …` (alias `CREATE MATERIALIZED
VIEW`) requires an aggregation:

- The current rows live in memory, one per group, keyed by the group value.
- Every change is written to the **changelog stream `<name>`**. The record
  key is the group key and the payload is the row as JSON. A deleted group
  (dropped by HAVING) is written as an empty record with header
  `x-exql-op: delete`.
- You can query it with `SELECT … FROM <name>` (any SQL, including joins with
  streams) and read it over HTTP with `GET /api/v1/views/<name>`. Add
  `?key=<k>` to get one group. The key is the group value; for several
  GROUP BY columns it is a JSON array (`["eu",3]`), and empty for a global
  aggregate.
- Tables survive restarts. On boot a table is filled from its changelog,
  and once its query resumes it is rebuilt from the query's checkpointed
  state.

## Output records

Each output row becomes one record:

| Field | Value |
|-------|-------|
| payload | A JSON object of the output columns. JSON columns (`payload`, `headers`) are embedded as JSON, timestamps as RFC 3339 |
| key | Column `key` if selected. Otherwise the group key (aggregates), or the source record's key (filter/project over one stream) |
| subject | Column `subject` if selected and valid. Otherwise the source record's subject (filter/project over one stream), else empty |
| headers | `x-idempotency-key`, `x-exql-query` (query id), `x-exql-pos` (micro-batch boundary) |

Output columns need distinct names. ExQL names `a.payload->>'id'`
`a.payload ->> 'id'`, so two qualified JSON columns don't collide.

## State, recovery and delivery guarantees

What is implemented and tested:

- **Checkpoints.** Every 5 s (`EXSPEED_EXQL_CHECKPOINT_MS`) when there is
  progress, and when a query is paused, dropped or shut down, a query writes
  a checkpoint to the internal stream `__exql_ckpt_<query_id>` through the
  `Log`. The checkpoint holds its source positions, per-source max event
  time, output position, counters, and operator state (aggregate
  accumulator states, open windows, both join buffers, as Arrow IPC with a
  CRC32C). Large checkpoints are split into 4 MiB parts written in one
  batch. The newest complete checkpoint wins.
- **Recovery.** On restart or leader promotion the query loads its
  checkpoint, resumes from the checkpointed offsets, and scans the output
  written after the checkpoint. Records it already wrote are skipped, and
  the original micro-batch boundaries (the `x-exql-pos` headers) are
  replayed, so EMIT CHANGES output is reproduced exactly.
- **Effectively-once output.** Each output record has a deterministic
  idempotency key (`<query_id>:<source/offset or group>:<n>`). The skip
  above makes this hold independently of the broker's dedup window. The
  `Log` dedup (`x-idempotency-key`) is a second layer. **Tested:** a query
  killed mid-stream (aggregates, EMIT FINAL windows, a table, INNER and
  LEFT joins) and restarted produces output identical to an uninterrupted
  run, with no duplicate records.
- Stream-table joins are as-of processing time (see above). A replay can
  therefore differ if the table changed meanwhile, which is why such
  queries aren't covered by the identical-output guarantee.
- A query that was dropped and created again, or replaced with `CREATE OR
  REPLACE`, gets a new id and starts over from the beginning of its
  sources.

## Query lifecycle

- A query is validated (parsed, planned, names checked) before anything is
  persisted. Names follow stream-name rules (`[A-Za-z0-9_-]`); names that
  start with `__` are reserved. A query may not read its own output.
- Definitions and desired state are persisted atomically in
  `{data_dir}/exql/queries/<id>.json`. Desired state is `running`, `paused`
  or `stopped`.
- Status is `running`, `paused`, `pending` (wants to run but this node
  isn't the leader, or it is starting), or `failed` with the error. A
  failure (including a panic, which is caught) sets the desired state to
  `stopped`. **Paused and stopped queries don't restart on boot or leader
  promotion.** `RESUME QUERY` restarts either from the last checkpoint.
- Transient errors (not yet leader, dedup state loading, I/O) restart the
  query from its checkpoint with backoff. They don't mark it failed.
- Queries run only on the leader, and stop when leadership is lost.
- `GET /api/v1/queries/<id>` reports `stats`: `records_in`, `records_out`,
  `late_records_dropped`, `checkpoints`, `watermark`, `last_checkpoint`.

## External databases

Bounded queries can read Postgres tables through **registered connections
only**:

```bash
curl -X POST localhost:8080/api/v1/connections -H 'Content-Type: application/json' \
  -d '{"name": "warehouse", "driver": "postgres", "url": "postgresql://user:pass@host:5432/db"}'
```

```sql
SELECT o.key, c.name
FROM orders o JOIN warehouse.customers c ON o.payload->>'customer_id' = c.id;  -- schema public
SELECT * FROM warehouse.sales.targets;                                          -- schema sales
```

- The column list comes from `information_schema` through a parameterized
  query. The snapshot is fetched with quoted identifiers, so names are
  never interpolated raw.
- Snapshots are cached per table (TTL 30 s, LRU of 64 tables), and pools
  are reused per connection. A table larger than 1,000,000 rows is an
  error.
- Postgres column types map to Int64, Float64, Boolean, text, JSON text,
  `Timestamp(ms, UTC)` and Date32.
- **Bounded queries only**: continuous queries can't read external tables.
  The inline `postgres('url', 'table')` form has been removed.
- Connections can also be declared with `EXSPEED_CONNECTION_<NAME>_DRIVER`
  and `EXSPEED_CONNECTION_<NAME>_URL`.

## Functions

All DataFusion scalar, aggregate and window functions are available. That
includes strings (`upper`, `lower`, `substr`, `concat`, `regexp_like`, …),
math, dates and times (`date_trunc`, `date_bin`, `to_timestamp`,
`extract`, …), conditionals (`coalesce`, `nullif`, `CASE`), and
`CAST`/`TRY_CAST` to any Arrow type. ExQL adds:

| Function | Notes |
|----------|-------|
| `subject_part(subject, n)` | n-th dot-delimited token (1-based; negative counts from the end) |
| `subject_matches(subject, 'orders.>')` | NATS-style wildcard match (`*` one token, `>` the rest) |
| `json_get_*`, `->`, `->>`, `json_contains`, … | From `datafusion-functions-json` |
| `window_start`, `window_end` | In windowed continuous queries |

## Limits and configuration

| Setting | Default | Env var |
|---------|---------|---------|
| Bounded query timeout | 30 s | `EXSPEED_QUERY_TIMEOUT_SECS` |
| Max rows returned (more rows → `"truncated": true`) | 10,000 | `EXSPEED_QUERY_MAX_ROWS` |
| Memory pool for query execution (exceeding it → `RESOURCES_EXHAUSTED`; no spilling) | 512 MB | `EXSPEED_QUERY_MEMORY_MB` |
| DataFusion partitions | 1 | `EXSPEED_QUERY_PARTITIONS` |
| Continuous checkpoint interval | 5 s | `EXSPEED_EXQL_CHECKPOINT_MS` |
| Default grace period | 0 | `EXSPEED_EXQL_DEFAULT_GRACE_MS` |

When a client disconnects, its bounded query is cancelled: over HTTP the
request future is dropped, and over TCP the session cancels every waiting
request of a closed connection.

Continuous-query state (groups, open windows, join buffers) lives in memory
and is not counted against the memory pool. Choose `WITHIN` and window sizes
with your key cardinality in mind.

## What is not supported

| Feature | Status |
|---------|--------|
| Secondary indexes (`CREATE INDEX`) | Removed. Rejected with `UNSUPPORTED` |
| `INSERT`/`UPDATE`/DDL other than the statements above | Rejected |
| Continuous: ORDER BY, LIMIT, DISTINCT, window functions, UNION, subqueries, nested aggregation | Rejected (`UNSUPPORTED`) |
| Continuous: RIGHT/FULL/CROSS joins, joins of 3+ relations, table-table joins | Rejected |
| Continuous: external databases | Rejected (bounded-only) |
| Session windows | Rejected |
| Idle-source watermark advancement | Not implemented. An idle source holds the watermark |
| Query registry replicated to followers | Not yet. Definitions are local files, while checkpoints and outputs are in replicated streams |
