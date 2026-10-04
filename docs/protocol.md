# Client protocol (v2)

Exspeed clients speak a small binary protocol over TCP (default port 5933,
optionally TLS). This page is the reference for SDK authors. Applications
should use an SDK: the Rust crate [`exspeed-client`](../crates/exspeed-client)
or the [TypeScript SDK](../sdks/typescript/README.md).

The source of truth is `crates/exspeed-protocol/src/client.rs`. Its unit
tests round-trip every message.

## Framing

Every message is one frame: a 10-byte header followed by the payload.

| Bytes | Field | Notes |
|-------|-------|-------|
| 0 | version | `0x02`. Frames with another version are rejected and the connection closes. |
| 1 | opcode | See the tables below. |
| 2–5 | correlation id | `u32` little-endian. |
| 6–9 | payload length | `u32` little-endian, at most 16 MiB. |

All integers are little-endian. The payload encodings use these building
blocks:

| Name | Encoding |
|------|----------|
| `str` | `u16` length + UTF-8 bytes |
| `lstr` | `u32` length + UTF-8 bytes (SQL text) |
| `bytes` | `u32` length + raw bytes |
| `opt<T>` | `u8` flag (0 = absent, 1 = present) + `T` |
| `headers` | `u16` count + (`str` key, `str` value) pairs |
| `vec<T>` | `u32` count + items |

Decoders reject truncated payloads and trailing bytes.

## Correlation, pushes and fire-and-forget

- A request carries a non-zero correlation id, and its response echoes it.
  Responses can arrive **out of order**: a long pull or long-poll read does not
  block the requests behind it.
- `Publish` and `PublishBatch` requests on one connection are applied **in the
  order they were sent**, and the connection keeps reading while they are
  written. Publishes already queued for the same stream are appended
  together (one storage batch, one fsync; up to 4,096 records or 8 MiB per
  group), so a client that pipelines publishes without waiting for each
  reply shares fsyncs instead of paying one per record. Each request still
  gets its own reply, and a request whose records are invalid fails alone.
  Other requests don't wait for queued publishes, so a request that depends
  on a publish (a read of it, say) should wait for its reply.
- The server sends pushes (`Deliver`, `SubscriptionEnded`) with correlation id
  `0`.
- `Ack`, `Nack`, `Term`, `InProgress`, `Credit` and `Unsubscribe` sent with
  correlation id `0` are **fire-and-forget**: no reply on success. If one
  fails, the server sends an `Error` with correlation id `0`. Use this for
  `Ack` and `Credit` on hot paths. Other requests are always answered, with
  whatever correlation id they carried, so give them a non-zero one.

## Connection lifecycle

1. The first frame must be `Connect`, sent within 10 seconds
   (`server.handshake_timeout_secs`, which also bounds the TLS handshake). Any
   other first frame gets `Error 401`, and the connection closes.
2. On success the server replies `ConnectOk`. With auth enabled, an unknown or
   missing token gets `Error 401`, and the connection closes.
3. The server closes connections that send nothing for 120 seconds
   (`server.idle_timeout_secs`). Clients should `Ping` every 15–30 seconds.
4. A frame that can't be decoded (bad version, unknown opcode, oversize
   length) gets `Error 400` with correlation id 0, and the connection closes.
   This applies to the very first frame too: a client speaking another
   version gets a v2 `Error 400` "unsupported protocol version N; this server
   speaks 2" before the close.
   A request whose *payload* is malformed gets `Error 400` with its own
   correlation id, and the connection stays open.
5. When the connection closes, the server ends its subscriptions and deletes
   any ephemeral consumers it created.

## Shared structures

**PublishRecord**
: `str subject`, `opt<bytes> key`, `bytes value`, `headers`, `opt<str> msg_id`

**WireRecord**
: `u32 len`, `u32 crc`, `u16 delivery_count`, `u64 offset`,
  `u64 timestamp_ns`, `str subject`, `opt<bytes> key`, `bytes value`,
  `headers`

| Bytes | Field | Notes |
|-------|-------|-------|
| 0–3 | `len` | Bytes after this field (record size − 4). A record is at least 35 bytes, so `len` ≥ 31. |
| 4–7 | `crc` | CRC32C (Castagnoli) of bytes 10..end: everything after `delivery_count`. |
| 8–9 | `delivery_count` | 1 on first delivery, +1 on each redelivery, 0 for stateless reads. Not covered by the CRC. |
| 10–17 | `offset` | |
| 18–25 | `timestamp_ns` | Append time, **nanoseconds** since the Unix epoch. |
| 26– | `subject`, `key`, `value`, `headers` | Encoded as in `PublishRecord`. |

This is byte for byte how the server stores records in its segment files,
so it can answer `Read`, `Pull` and push deliveries by copying records out
of the file: the length prefix gives record boundaries without parsing, and
`delivery_count` sits at a fixed position outside the CRC so the server can
set it in place. Decoders must check that `len` matches the fields it
contains. Clients should verify the CRC (the Rust client does; the
TypeScript SDK exposes `verifyRecordCrc` but doesn't call it by default,
since a JavaScript CRC costs more than the rest of decoding). Records in a
`vec<WireRecord>` follow each other with no padding.

**StreamSpec**
: `str name`, `u64 max_age_secs`, `u64 max_bytes`, `u64 dedup_window_secs`,
  `u64 dedup_max_entries`, `u8 compaction`, then optionally `bytes limits`

0 means "server default" for every numeric field. `limits` is a JSON
object, sent only when one of its settings isn't the default (so a plain
spec is understood by every server version):

```json
{"max_msgs": 0, "discard": "old", "max_msgs_per_subject": 0,
 "allow_msg_ttl": false, "msg_ttl_ms": 0, "allow_delayed": false,
 "retention": "limits"}
```

`discard` is `old` or `new`; `retention` is `limits`, `work_queue` or
`interest`. Missing keys take these defaults. See [queues.md](queues.md).

**ConsumerSpec** (JSON, carried as `bytes`):

```json
{
  "name": "billing",
  "stream": "orders",
  "filter_subjects": ["orders.placed", "orders.eu.>"],
  "deliver": "all",
  "ack": "explicit",
  "ack_wait_ms": 30000,
  "max_deliver": 5,
  "backoff_ms": [1000, 5000, 30000],
  "max_ack_pending": 1000,
  "dlq_stream": "orders-dlq",
  "ephemeral": false,
  "dead_letter_expired": false,
  "filter_headers": {"region": "eu"},
  "header_match": "all",
  "single_active": false,
  "priority_window": 0
}
```

Only `name` and `stream` are required. The last five are described in
[queues.md](queues.md#consumer-delivery-options).

- `deliver` takes one of these forms:
  - `"all"`
  - `"new"`
  - `{"from_offset": 42}`
  - `{"from_time": <unix ms>}`
- `ack` is `"explicit"` or `"none"`.

## Requests (client → server)

| Opcode | Name | Payload | Success response |
|--------|------|---------|------------------|
| 0x01 | Connect | `str client_id`, `opt<str> token` | `ConnectOk` |
| 0x03 | Metadata | — | `Json {node_id, is_leader, leader, server_version}` |
| 0x10 | Publish | `str stream`, `PublishRecord` | `PublishOk` |
| 0x11 | PublishBatch | `str stream`, `vec<PublishRecord>` | `PublishBatchOk` |
| 0x18 | CreateStream | `StreamSpec` | `Ok` (idempotent for identical settings; 409 otherwise) |
| 0x19 | UpdateStream | `StreamSpec` | `Ok` |
| 0x1A | DeleteStream | `str name` | `Ok` (409 while consumers exist) |
| 0x1B | StreamInfo | `str name` | `Json` |
| 0x1C | ListStreams | — | `Json` array |
| 0x20 | Query | `lstr sql` | `Json {columns, rows, row_count, execution_time_ms, truncated}` |
| 0x40 | CreateConsumer | `bytes` (ConsumerSpec JSON) | `Json` consumer info (idempotent; 409 if the spec differs) |
| 0x41 | DeleteConsumer | `str name` | `Ok` |
| 0x42 | ConsumerInfo | `str name` | `Json` |
| 0x43 | ListConsumers | `opt<str> stream` | `Json` array |
| 0x44 | SeekConsumer | `str consumer`, `u8 kind` (0 earliest, 1 latest, 2 offset, 3 time ms), `u64 value` | `Ok` |
| 0x50 | Subscribe | `str consumer`, `u32 credits` | `SubscribeOk`, then `Deliver` pushes |
| 0x51 | Credit | `u32 sub_id`, `u32 credits` | `Ok` (or nothing with corr 0) |
| 0x52 | Unsubscribe | `u32 sub_id` | `Ok` |
| 0x53 | Pull | `str consumer`, `u32 max_messages`, `u32 max_bytes`, `u32 expires_ms` | `Messages` (empty on timeout) |
| 0x54 | Ack | `str consumer`, `vec<u64> offsets` | `Ok` |
| 0x55 | Nack | `str consumer`, `u64 offset`, `u32 delay_ms` (0 = consumer backoff) | `Ok` |
| 0x56 | Term | `str consumer`, `u64 offset`, `str reason` | `Ok` (dead-letters immediately) |
| 0x57 | InProgress | `str consumer`, `vec<u64> offsets` | `Ok` (resets the ack timers) |
| 0x60 | Read | `str stream`, `u64 from`, `u32 max_records`, `u32 max_bytes`, `u32 wait_ms`, `str filter` | `ReadResult` |
| 0x70 | CorePublish | `str subject`, `opt<str> reply_to`, `headers`, `bytes value` | `Ok` (nothing with corr 0); `404` "no responders" when `reply_to` is set and nobody received it |
| 0x71 | CoreSubscribe | `str subject_filter`, `opt<str> queue_group` | `SubscribeOk`, then `CoreMsg` pushes |
| 0x74 | KvPut | `str bucket`, `str key`, `bytes value`, `opt<u64> expected_revision`, `opt<u64> ttl_ms` | `PublishOk` (offset = the new revision); `409` on a revision mismatch |
| 0x75 | KvGet | `str bucket`, `str key`, `opt<u64> revision` | `Messages` with one record; `404` when absent |
| 0x76 | KvDelete | `str bucket`, `str key`, `u8 purge`, `opt<u64> expected_revision` | `PublishOk` |
| 0x77 | KvKeys | `str bucket`, `str filter` | `Json` array of keys |
| 0x78 | KvHistory | `str bucket`, `str key` | `Messages` |
| 0x79 | KvCreateBucket | `str bucket`, `u64 history`, `u64 ttl_ms`, `u64 max_bytes` | `Ok` |
| 0xF0 | Ping | — | `Pong` |

## Responses and pushes (server → client)

| Opcode | Name | Payload |
|--------|------|---------|
| 0x80 | Ok | — |
| 0x81 | Error | `u16 code`, `str message`, `opt<bytes> detail` (JSON) |
| 0x82 | Deliver | `u32 sub_id`, `vec<WireRecord>` (push, corr 0) |
| 0x83 | Messages | `vec<WireRecord>` |
| 0x84 | ReadResult | `u64 next_offset`, `u64 high_watermark`, `vec<WireRecord>` |
| 0x85 | Json | raw UTF-8 JSON |
| 0x86 | PublishOk | `u64 offset`, `u8 duplicate` |
| 0x87 | PublishBatchOk | `vec<(u64 offset, u8 duplicate)>` |
| 0x88 | ConnectOk | `str server_version`, `str node_id`, `opt<str> leader` |
| 0x89 | SubscribeOk | `u32 sub_id` |
| 0x8A | SubscriptionEnded | `u32 sub_id`, `u16 code`, `str message` (push, corr 0) |
| 0x8B | CoreMsg | `u32 sub_id`, `str subject`, `opt<str> reply_to`, `headers`, `bytes value` (push, corr 0) |
| 0xF1 | Pong | — |

### Size limits

Every frame payload is at most 16 MiB; a decoder that sees a larger length
rejects the frame and closes the connection, and the server never sends
one. To guarantee that, batches are budgeted by each record's full
`WireRecord` size (headers and framing included):

| What | Limit |
|------|-------|
| One published record | subject ≤ 1024 bytes, key ≤ 64 KiB, value ≤ 8 MiB, ≤ 256 headers with keys ≤ 1 KiB, values ≤ 32 KiB and **all header keys + values ≤ 64 KiB**, so one `WireRecord` is under ~8.2 MiB |
| `Read` `max_bytes` | 0 = 1 MiB, capped at 8 MiB |
| `Read` `max_records`, `wait_ms` | clamped to 1–10,000 records; waits at most 300 s |
| `Pull` `max_bytes` | 0 = 4 MiB, capped at 8 MiB |
| `Pull` `max_messages`, `expires_ms` | clamped to 1–10,000 records; waits at most 300 s |
| `Deliver` | sent once a subscriber's pending records reach 4 MiB; never grown past 8 MiB |

A response stops adding records before the next one would exceed its byte
budget, except that the first record is always included (so a large record
can't stall a reader). Records larger than the budget therefore arrive one
per frame, which always fits.

Opcodes 0x30, 0x31 and 0xA0–0xA6 are reserved. Replication between
servers uses its own protocol on the cluster port (see
`crates/exspeed-broker/src/cluster/wire.rs`).

## Error codes

| Code | Meaning | `detail` |
|------|---------|----------|
| 400 | Malformed request, invalid name or filter, invalid config | |
| 401 | Not authenticated | |
| 403 | The credential lacks the needed action on the stream | |
| 404 | Stream, consumer, bucket or key not found; a core request with no responders | |
| 409 | Exists with different settings; stream still has consumers; `msg_id` reused with a different body; a work-queue consumer overlapping another; a KV revision mismatch | `{"stored_offset": n}`, `{"consumers": [...]}`, `{"current_revision": n}` |
| 429 | Retry later: dedup map full, the stream is full (`discard = new`), or too many concurrent waiting requests on this connection (max 64) | `{"retry_after_secs": n}` |
| 500 | Internal error | |
| 503 | Not the leader, still starting, or (cluster with `acks = all`) not enough in-sync replicas / replication timed out | `{"leader": "host:port"}` when known; `{"in_sync": n, "required": m}` |
| 507 | The server's disk is full; nothing was written. Retry once space is freed. | |

## Consumers

A consumer is a named, durable cursor over one stream, created with
`CreateConsumer`. It tracks the following state:

- an **ack floor**: everything below it is acknowledged;
- the set of records **delivered but not yet acked**, each with an ack
  deadline (`ack_wait_ms`) and a delivery count;
- records **scheduled for redelivery**, after a nack or an expired deadline.

Records are delivered in two ways:

- **Push.** `Subscribe` with a credit window, then send `Credit` as the
  application consumes records. The server never sends more records than the
  remaining credit.
- **Pull.** `Pull` long-polls for up to `expires_ms` and returns whatever is
  available, up to `max_messages`.

Any number of subscriptions and pullers can share one consumer, on any
connection and from any application instance. Each record goes to exactly one
of them at a time, so this is a work queue. For fan-out, create one consumer
per independent reader.

An unacked record is redelivered when one of these happens:

- its deadline passes;
- the client sends `Nack` (after `delay_ms`, or after the consumer's backoff);
- the subscriber it was delivered to disconnects.

After `max_deliver` attempts, or immediately on `Term`, the record is
dead-lettered to `dlq_stream` if one is set; otherwise it is dropped and
counted in a metric. Dead letters carry these headers:

- `exspeed-dlq-origin` (consumer name)
- `exspeed-dlq-stream`
- `exspeed-dlq-original-offset`
- `exspeed-dlq-deliveries`
- `exspeed-dlq-cause` (`max_deliver`, `rejected` or `expired`)
- `exspeed-dlq-reason` (free text)
- `exspeed-dlq-time` (ms since the epoch)

They are written idempotently, so a crash mid-dead-letter produces no
duplicates.

`max_ack_pending` caps how many records can be unacked at once; delivery
pauses at the cap.

Consumer state lives in the internal stream `__consumers` (one snapshot per
consumer, keyed by name), so it survives restarts and replicates with the log.

A subscription ends with a `SubscriptionEnded` push in these cases:

| Code | Cause |
|------|-------|
| 404 | The consumer or its stream was deleted. |
| 503 | This node lost leadership. Reconnect, and follow `leader` if one is given. |

## Core messaging

`CoreSubscribe` returns a subscription id with the high bit set
(`0x80000000`); `CoreMsg` pushes carry it, and `Unsubscribe` with it ends
the subscription. Messages a connection can't keep up with (its queue of
65,536 messages is full) are dropped. Core messaging runs on the leader: a
standby answers `503` with `leader`, and leadership moving ends every core
subscription with `SubscriptionEnded` code 503. See
[messaging.md](messaging.md).

To send a request, subscribe once to an inbox (`_INBOX.<id>.*`), publish
with `reply_to` set to `_INBOX.<id>.<token>` and a non-zero correlation id,
and match the `CoreMsg` whose subject ends in `<token>`.

## Key-value

A bucket `B` is the stream `KV_B`. A key's revision is its record's offset
plus one; records returned by `KvGet` and `KvHistory` carry the plain stream
offset, so clients add one. A tombstone has the header `exspeed-kv-op`
(`DEL` or `PURGE`). See [kv.md](kv.md).

## Time and priority headers

These record headers have meaning to the server:

| Header | Meaning |
|--------|---------|
| `exspeed-ttl` | Expire this long after the append (`500ms`, `30s`, `5m`, `2h`, `1d`, or milliseconds). Needs `allow_msg_ttl` on the stream, else the publish fails with 400. |
| `exspeed-delay` | Consumers deliver it no earlier than this long after the append. Needs `allow_delayed`. |
| `exspeed-deliver-at` | Consumers deliver it no earlier than this time (ms since the epoch). Needs `allow_delayed`. |
| `exspeed-priority` | 0–9, for consumers with a `priority_window` |
| `x-idempotency-key` | The `msg_id` (see [idempotent-publish.md](idempotent-publish.md)) |

## Authorization

With auth enabled, every operation is checked against the credential's
per-stream actions:

| Operation | Needs |
|-----------|-------|
| Publish, PublishBatch | `publish` on the stream |
| Read | `subscribe` on the stream |
| Subscribe, Pull, Ack, Nack, Term, InProgress | `subscribe` on the consumer's stream |
| ConsumerInfo, SeekConsumer, CreateConsumer | `subscribe` or `admin` on the stream. Creating a consumer with `dlq_stream` also needs `publish` on that stream. |
| DeleteConsumer, CreateStream, UpdateStream, DeleteStream | `admin` on the stream |
| StreamInfo | `admin` or `subscribe` |
| ListStreams, ListConsumers | Return only the streams and consumers the credential can see (internal streams only to a global admin). |
| Query | Global admin (`streams = "*"`), since SQL can read any stream. |
| CorePublish, CoreSubscribe | `publish` / `subscribe` on the subject (a `subjects` permission, or `streams = "*"`); replies to `_INBOX.…` and subscribing to one's own inbox are always allowed |
| KvGet, KvKeys, KvHistory | `subscribe` on `KV_<bucket>` |
| KvPut, KvDelete | `publish` on `KV_<bucket>` |
| KvCreateBucket | `admin` on `KV_<bucket>` |

Streams whose names start with `__` are internal. Clients can't create,
update, publish to or delete them.
