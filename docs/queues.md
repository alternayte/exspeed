# Queues, message lifetime and delivery options

The features a message queue needs on top of a log: how long a record
lives, whether acknowledging it removes it, when and in which order
consumers get it, and where it goes when it can't be processed. Every
setting here is off by default; a plain stream is a log.

| You want | Use |
|----------|-----|
| Messages that disappear after a while | `msg_ttl_ms` (whole stream) or `allow_msg_ttl` + the `exspeed-ttl` header (per record) |
| A message processed later, not now | `allow_delayed` + `exspeed-delay` / `exspeed-deliver-at` |
| A bounded queue that rejects when full | `max_msgs` + `discard = "new"` |
| A ring buffer of the last N records | `max_msgs` + `discard = "old"` |
| Only the latest value per subject | `max_msgs_per_subject = 1` |
| Acked messages removed (a queue) | `retention = "work_queue"` |
| Messages kept until every consumer acked them | `retention = "interest"` |
| Route on headers | consumer `filter_headers` |
| Strict ordering with failover | consumer `single_active` |
| Urgent messages first | consumer `priority_window` + the `exspeed-priority` header |
| Know why a message was dead-lettered | the `exspeed-dlq-cause` header |

Stream settings are set at creation or with an update: over HTTP
(`POST`/`PATCH /api/v1/streams`, see [http-api.md](http-api.md#streams)),
with the CLI (`exspeed create … --max-msgs 1000 --discard new`, see
[cli.md](cli.md#streams)), or in the SDKs' stream spec.

## Message lifetime

### TTL

A record with a TTL **expires** that long after it was appended. An expired
record is invisible: reads, SQL queries and consumers skip it, and a
consumer that already delivered it doesn't redeliver it. Compaction removes
expired records from disk; a stream-wide TTL that no record can override
also lets retention drop whole expired segments.

- `msg_ttl_ms`: the TTL of every record that doesn't set its own.
- `allow_msg_ttl = true`: records may carry their own TTL in the
  `exspeed-ttl` header. On a stream without it, a publish carrying the header
  is rejected with `400` (so a TTL is never silently ignored).

```bash
# Per-record TTL (TypeScript): publish(stream, value, { ttl: "30s" })
# Header values: 500ms, 30s, 5m, 2h, 1d, or a bare number of milliseconds.
```

A consumer can dead-letter expired records instead of dropping them: set
`dead_letter_expired = true` (with a `dlq_stream`). Each one is copied to
the DLQ with cause `expired`, whether it expired before or after it was
delivered.

### Delayed delivery

With `allow_delayed = true`, a record can name when consumers should first
see it:

- `exspeed-delay: 30s`: no earlier than 30 seconds after it was appended;
- `exspeed-deliver-at: 1767225600000`: no earlier than this time
  (milliseconds since the Unix epoch).

The record is stored right away (reads see it); consumers hold it until it
is due and deliver the records after it in the meantime. A consumer holds
at most 100,000 delayed records; while it does, it stops reading new ones.
Delayed records don't count against `max_ack_pending`, but they hold the
consumer's ack floor until delivered and acked. They survive restarts and
failover: the consumer's persisted state lists them, and their due time is
read back from the record.

Both times are measured from the record's append timestamp, so a retried
publish (same `msg_id`) gets the same delivery time.

### Count limits

- `max_msgs`: keep at most this many records. `discard` decides what happens
  at the limit:
  - `old` (default): the oldest records are dropped, record by record (a
    ring buffer).
  - `new`: a publish that doesn't fit is rejected whole with `429`
    (`StreamFull`); nothing is written. With `discard = "new"`, `max_bytes`
    rejects too instead of dropping old segments.
- `max_msgs_per_subject`: only the newest N records of each subject are
  visible; an older one disappears as soon as N newer ones exist. Reads,
  SQL and consumers skip superseded records, and compaction removes them
  from disk. A record that isn't replicated yet never hides an older one, so
  a subject never has fewer than N visible records while a write is in
  flight. This is what [key-value buckets](kv.md) are built on.

Records are counted as the span of offsets between the stream's first and
next record, so gaps left by compaction or superseded records still count
until compaction removes them.

## Retention by acknowledgement

`retention` decides whether acknowledging a record removes it:

- `limits` (default): records stay until a limit (age, size, count, TTL)
  removes them. Any number of consumers read the same records; this is a
  log.
- `work_queue`: a record is removed once the consumer that owns it acked
  it. The stream takes only consumers with `deliver: all` whose subject
  filters don't overlap (a second consumer that could see the same records
  is refused with `409`), so every record has at most one owner. Records
  wait for a consumer: a work queue without consumers keeps everything.
  Several app instances share one consumer to share the work.
- `interest`: a record is removed once every consumer of the stream acked
  it. With no consumers at all, nothing is kept.

The stream is trimmed to the lowest ack floor among its consumers (the
offset below which everything is acked), so a record acked out of order
stays until the ones before it are acked too; it is never redelivered.
Only acks the consumer has persisted (within about 100 ms) move the
trim point, and trims replicate to followers like any other write. Records
that match none of a work queue's consumers' filters are removed once every
consumer has passed them.

A retention policy other than `limits` can't be combined with compaction
(`400`): compaction keeps the latest record per key, these remove records
once acked.

```toml
# A bounded job queue: at most 100k pending jobs, publishers get 429 when full.
max_msgs = 100000
discard = "new"
retention = "work_queue"
```

## Consumer delivery options

These are consumer settings (see [concepts.md](concepts.md#consumers) for
the rest).

### Header filters

`filter_headers` delivers only records whose headers have the given values
(exact match), like a RabbitMQ headers exchange. `header_match` is `all`
(default) or `any`. Records that don't match are skipped, like records a
subject filter excludes.

```json
{"name": "eu-gold", "stream": "events",
 "filter_headers": {"region": "eu", "tier": "gold"}, "header_match": "all"}
```

### Single active consumer

With `single_active = true`, only one subscription receives at a time: the
oldest one still connected. The others are standbys. When the active one
disconnects, the next takes over, starting with the records the previous one
held unacked. Use it for strict per-consumer ordering with failover. Pull
requests are refused (`400`); subscribe instead.

### Priority

With `priority_window = N` (at most 10,000), the consumer reads up to N
records ahead and delivers the highest `exspeed-priority` (0–9, default 0)
first, oldest first within a priority. Priority only reorders records inside
the window: a high-priority record further back than N records waits until
the consumer reaches it. Buffered records are held like delayed ones, so
they survive restarts (after a restart they are delivered in offset order).
A prioritized consumer reads each record a second time to deliver it, so it
is slower than a plain one.

### Dead-letter causes

Every record a consumer dead-letters carries:

| Header | Value |
|--------|-------|
| `exspeed-dlq-cause` | `max_deliver` (attempts used up), `rejected` (the client called `term`), or `expired` (TTL, with `dead_letter_expired`) |
| `exspeed-dlq-reason` | Free text: the `term` reason, `max_deliver exceeded`, `expired` |
| `exspeed-dlq-deliveries` | How many times it was delivered |
| `exspeed-dlq-time` | When it was dead-lettered (ms since the epoch) |
| `exspeed-dlq-origin` / `exspeed-dlq-stream` / `exspeed-dlq-original-offset` | The consumer, stream and offset it came from |

Records dropped by a count, size or age limit are not dead-lettered.
