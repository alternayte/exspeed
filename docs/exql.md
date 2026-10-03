# ExQL — SQL over streams

ExQL lets you query streams with SQL. You can run a query in three ways:

- **Bounded:** one-shot over the current contents of a stream.
- **Continuous:** long-running, writing results to another stream.
- **Materialized view:** long-running, keeping a queryable in-memory table.

> ⚠️ **Correctness status (v0.5).** Simple filters and projections over
> stream columns work. Many other SQL features parse but give wrong results
> today. Check the [known limitations](#known-limitations) before you depend
> on ExQL output. A rewrite on Apache DataFusion is planned in
> [REVIEW.md §5.5](REVIEW.md#55-exql).

## Contents

- [The stream table](#the-stream-table)
- [Running queries](#running-queries)
- [Bounded queries](#bounded-queries)
- [Continuous queries](#continuous-queries)
- [Materialized views](#materialized-views)
- [Tumbling windows](#tumbling-windows)
- [Stream-stream joins](#stream-stream-joins)
- [External database joins](#external-database-joins)
- [Indexes](#indexes)
- [Functions](#functions)
- [Known limitations](#known-limitations)

## The stream table

Every stream can be queried as a table with these columns:

| Column | Type | Notes |
|--------|------|-------|
| `offset` | int | |
| `timestamp` | timestamp | ms in bounded queries, **ns in continuous queries** |
| `subject` | text | |
| `key` | text | |
| `payload` | json | `payload->'field'` returns JSON; `payload->>'field'` returns text |

Quote stream names that contain `-` or start with a digit:
`SELECT * FROM "order-events"`.

## Running queries

```bash
exspeed query "SELECT * FROM orders LIMIT 10"            # CLI (HTTP under the hood)

curl -X POST localhost:8080/api/v1/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM orders LIMIT 10"}'
# {"columns": [...], "rows": [[...]], "row_count": 10, "execution_time_ms": 3}
```

Errors are structured:

```json
{"error": "parse error: …", "code": "PARSE_ERROR", "line": 1, "column": 25}
```

The TypeScript SDK can also run bounded queries over TCP with `client.query(sql)`.

> 🔒 Queries require a global admin credential, over both HTTP and TCP.

## Bounded queries

```sql
-- Filter and project
SELECT offset, key, payload->>'region' AS region
FROM orders
WHERE subject = 'order.eu.created'
LIMIT 100;

-- Aggregate by a plain column
SELECT subject, COUNT(*) AS n, SUM(CAST(payload->>'total' AS DOUBLE)) AS revenue
FROM orders
GROUP BY subject
ORDER BY n DESC;

-- Latest N records
SELECT * FROM orders ORDER BY offset DESC LIMIT 20;
```

Rules that keep you on the working path:

- **Cast JSON before comparing it with a number.** For example,
  `CAST(payload->>'total' AS DOUBLE) > 100`. A bare `payload->>'total' > 100`
  compares text.
- **GROUP BY on plain columns** (`subject`, `key`) or a subquery alias. A
  grouping expression such as `payload->>'region'` comes back as NULL in
  the output.
- **ORDER BY only columns that appear in the SELECT list.**
- Results are capped at 10,000 rows, and the cap is applied silently.
- A misspelled stream name returns 0 rows, not an error.

## Continuous queries

A continuous query is created with **`CREATE VIEW <output> AS SELECT …`**.
It runs on the leader, reads new records from the source stream, and appends
each result row as a JSON record to a stream named `<output>`.

```sql
CREATE VIEW eu_orders AS
SELECT key, payload->>'total' AS total
FROM orders
WHERE payload->>'region' = 'eu'
EMIT CHANGES
```

```bash
exspeed query --continuous "CREATE VIEW eu_orders AS SELECT key, payload->>'total' AS total FROM orders WHERE payload->>'region' = 'eu'"

curl -X POST localhost:8080/api/v1/queries/continuous \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE VIEW eu_orders AS SELECT … FROM orders WHERE …"}'

curl localhost:8080/api/v1/queries              # list
curl localhost:8080/api/v1/queries/<id>         # details
curl -X DELETE localhost:8080/api/v1/queries/<id>
```

| Clause | Meaning |
|--------|---------|
| `EMIT CHANGES` (default) | Emit an updated row on every change |
| `EMIT FINAL` | Emit one row when a window closes (windowed queries only) |

Delivery is at-least-once. The source offset is checkpointed every 10 s,
and up to 10 s of output can repeat after a crash.

> ⚠️ **Known issues with continuous queries:**
>
> - Non-windowed `GROUP BY` aggregates come out as NULL in continuous
>   queries.
> - Stopped queries restart on every boot.

## Materialized views

```bash
curl -X POST localhost:8080/api/v1/views -H 'Content-Type: application/json' -d '{
  "sql": "CREATE MATERIALIZED VIEW eu_orders AS SELECT key, payload->>'"'"'total'"'"' AS total FROM orders WHERE payload->>'"'"'region'"'"' = '"'"'eu'"'"'"
}'

curl localhost:8080/api/v1/views                      # list
curl localhost:8080/api/v1/views/eu_orders            # all rows
curl "localhost:8080/api/v1/views/eu_orders?key=k1"   # one row by key
exspeed query "SELECT * FROM eu_orders"               # also queryable from SQL
```

> ⚠️ **Materialized views are in memory only.**
>
> - After a restart a view is re-created as a plain output stream, not a
>   table.
> - A view with no `GROUP BY` keeps only the last row.
> - Aggregates in views come out as NULL (same bug as continuous queries).

## Tumbling windows

Only the `tumbling(timestamp, '<n> <unit>')` form is supported. `<unit>` is
one of `second(s)`, `minute(s)`, `hour(s)` or `day(s)`.

```sql
CREATE VIEW hourly_counts AS
SELECT tumbling(timestamp, '1 hour') AS window_start, COUNT(*) AS cnt
FROM orders
GROUP BY tumbling(timestamp, '1 hour')
EMIT FINAL
```

> ⚠️ **Known issues with windows:**
>
> - Use **a single aggregate per windowed query.** Multiple aggregates
>   currently share one accumulator.
> - Windows close on the wall clock, not on event time.
> - Lateness is fixed at 5 minutes.
> - An unrecognised unit (`'1 week'`, `'1h'`) crashes the query task.
> - `TUMBLE(...)` and `INTERVAL '…'` are **not** supported.

## Stream-stream joins

Join two streams on an equality key within a time window by adding `WITHIN`:

```sql
CREATE VIEW paid_orders AS
SELECT o.key, o.payload AS order_payload, p.payload AS payment
FROM orders o
JOIN payments p ON o.key = p.key WITHIN '10 minutes'
EMIT CHANGES
```

> ⚠️ **Known issues with joins:**
>
> - The ON clause must be a **single equality, with the left stream's column
>   on the left** (`o.key = p.key`, not `p.key = o.key`).
> - `LEFT JOIN` behaves like an inner join.
> - Matching is driven by the wall clock, so replaying historical data
>   mostly doesn't join.
> - Bounded (non-`WITHIN`) joins currently return only the ON columns.

## External database joins

Register a connection, then refer to it in a bounded query:

```bash
curl -X POST localhost:8080/api/v1/connections -H 'Content-Type: application/json' \
  -d '{"name": "warehouse", "driver": "postgres", "url": "postgresql://user:pass@host:5432/db"}'
curl localhost:8080/api/v1/connections
curl -X DELETE localhost:8080/api/v1/connections/warehouse
```

Connections can also be declared through the `EXSPEED_CONNECTION_<NAME>_DRIVER`
and `EXSPEED_CONNECTION_<NAME>_URL` environment variables.

> ⚠️ **Known issues with external joins:**
>
> - Every query fetches the **whole** external table.
> - The table name is interpolated into SQL unescaped.
> - Only `text` and integer columns decode correctly.
> - External joins are not available in continuous queries.
> - Treat this feature as experimental.

## Indexes

You can define an index on a top-level JSON field:

```sql
CREATE INDEX orders_by_customer ON orders(payload->>'customer_id');
DROP INDEX orders_by_customer;
```

You can run these through `exspeed query` or `/api/v1/indexes`
(`GET`, `POST {"sql": "CREATE INDEX …"}`, `DELETE /{name}`).

> ⚠️ **Indexes do nothing yet.** The storage engine no longer builds
> secondary-index files, so a query planned as an index lookup runs as a
> filtered scan of the whole stream. Results are correct; there is no
> speed-up. The definitions are kept for the ExQL rewrite.

## Functions

**String functions:**

| Function | Notes |
|----------|-------|
| `UPPER(s)`, `LOWER(s)` | |
| `LENGTH(s)` | |
| `CONCAT(a, b, …)` | |
| `SUBSTRING(s, start, len)` | |
| `TRIM(s)` | |

**Numeric functions:**

| Function | Notes |
|----------|-------|
| `ABS(x)` | |
| `ROUND(x[, n])` | |
| `CEIL(x)`, `FLOOR(x)` | |

**Null handling:**

| Function | Notes |
|----------|-------|
| `COALESCE(a, b, …)` | |
| `NULLIF(a, b)` | |

**Subjects and headers:**

| Function | Notes |
|----------|-------|
| `SUBJECT_PART(subject, n)` | n-th dot-delimited token |
| `SUBJECT_MATCHES(subject, 'orders.>')` | NATS-style wildcard match |
| `HEADER('name')` | Currently always returns NULL |

**Time:**

| Function | Notes |
|----------|-------|
| `NOW()` | Returns nanoseconds |
| `tumbling(ts, '5 minutes')` | Start of the tumbling window that contains `ts` |

**Aggregates:**

| Function | Notes |
|----------|-------|
| `COUNT(*)`, `COUNT(x)` | |
| `SUM(x)`, `AVG(x)` | |
| `MIN(x)`, `MAX(x)` | |

**Operators:**

| Operator | Notes |
|----------|-------|
| `CASE WHEN … END` | |
| `CAST(x AS INT/BIGINT/DOUBLE/TEXT/BOOLEAN)` | `DECIMAL(p,s)`, `VARCHAR(n)` and `TIMESTAMP` give NULL |
| `IN`, `BETWEEN`, `LIKE` | `LIKE` is currently case-insensitive |
| `IS NULL` | |

Unknown function names return NULL rather than raising an error.

## Known limitations

The following parse without error but give **wrong results**. Each one is
tracked in [REVIEW.md §3.5](REVIEW.md#35-exql-exspeed-processing).

| Feature | What happens |
|---------|--------------|
| `HAVING` | Ignored |
| `DISTINCT` | Ignored |
| `COUNT(*) FILTER (WHERE …)` | Ignored |
| Window functions (`OVER (…)`) | Return NULL |
| Scalar subqueries | Return NULL |
| `GROUP BY 1` (ordinal) | Collapses all rows into one group |
| `GROUP BY <alias>` | Collapses all rows into one group |
| Expressions over aggregates (`SUM(x)/COUNT(*)`, `ROUND(AVG(x))`) | Return NULL |
| `INTERVAL '…'` literals | Evaluate to NULL, so `timestamp > now() - INTERVAL '1 hour'` returns nothing |
| `x NOT IN (…)` / `NOT BETWEEN` when `x` is NULL | Return true |
| Integer arithmetic | Goes through f64, so `7/2 = 3.5` |

These are rejected with an explicit error:

- `RIGHT JOIN`
- `FULL JOIN`
- `CROSS JOIN`
- `UNION`
