# exspeed

Python client for [Exspeed](https://github.com/alternayte/exspeed). It speaks
the binary client protocol v2 ([`docs/protocol.md`](https://github.com/alternayte/exspeed/blob/main/docs/protocol.md))
over TCP or TLS, built on `asyncio`, fully type-annotated, with no runtime
dependencies. Requires Python 3.10 or later.

- One client is one connection. Requests are multiplexed, so a long pull
  never blocks other calls. Share one client across your application.
- Durable consumers with push (credit-windowed subscriptions) or pull
  delivery, acks, redelivery with backoff, and dead-lettering.
- A coalescing publisher for high-throughput, order-preserving writes.
- Stream limits, per-message TTLs, delayed delivery and priorities.
- Core (non-persistent) publish/subscribe, queue groups and request-reply.
- Key-value buckets with revisions, compare-and-set, history and watches.
- Automatic reconnection that re-establishes subscriptions.

## Install

```bash
pip install exspeed
```

## Quick start

```python
import asyncio

import exspeed
from exspeed import ConsumerSpec, StreamSpec


async def main() -> None:
    async with exspeed.connect("127.0.0.1", 5933) as client:
        await client.create_stream(StreamSpec("orders", max_age_secs=7 * 86400))  # idempotent

        result = await client.publish(
            "orders",
            "orders.placed",               # subject
            {"id": 42, "total": 99.5},     # dicts and lists are JSON-encoded
            key="customer-7",
        )
        print(result.offset)

        await client.create_consumer(ConsumerSpec("billing", "orders", filter_subjects=["orders.placed"]))

        async with client.subscribe("billing", window=256) as sub:
            async for msg in sub:
                order = msg.json()
                await charge(order)
                msg.ack()


asyncio.run(main())
```

`exspeed.connect(...)` (the same as `ExspeedClient.connect(...)`) can be
awaited, `client = await exspeed.connect(...)`, or used with `async with`,
which closes the client on exit. `client.subscribe(...)` and
`client.subscribe_core(...)` work the same way: `async with` unsubscribes on
exit.

**Units.** Client-side durations (`timeout`, `request_timeout`,
`keepalive`, `ttl`, `delay`, `wait`, `expires`) are seconds, as a `float` or
a `datetime.timedelta`. Settings whose name ends in `_ms` (`ack_wait_ms`,
`backoff_ms`, `msg_ttl_ms`) are integer milliseconds, as on the wire.

## Streams

```python
from exspeed import StreamSpec

await client.create_stream(StreamSpec(
    "orders",
    max_age_secs=86400,          # retention by age
    max_bytes=10 * 2**30,        # retention by size
    dedup_window_secs=300,       # how long msg_ids are remembered
    dedup_max_entries=1_000_000,
    compaction=False,
))
await client.create_stream("events")                 # a name alone: server defaults
await client.update_stream(StreamSpec("orders", max_age_secs=3 * 86400))
info = await client.stream_info("orders")            # name, earliest_offset, next_offset, records, config, internal
streams = await client.list_streams()
await client.delete_stream("orders")                 # 409 while consumers exist
```

Unset numeric settings (0) mean "server default". `create_stream` is
idempotent when the stream already exists with the same settings, and fails
with 409 when they differ. `update_stream` replaces all settings, so pass
every field you want to keep.

### Limits, lifetimes and retention

```python
await client.create_stream(StreamSpec(
    "jobs",
    max_msgs=100_000,          # most records the stream holds (0 = no limit)
    discard="old",             # at max_msgs: "old" drops the oldest, "new" rejects new records (429)
    max_msgs_per_subject=10,   # most records kept per subject
    allow_msg_ttl=True,        # accept the per-record `ttl` publish option
    msg_ttl_ms=86_400_000,     # default lifetime of every record (0 = none)
    allow_delayed=True,        # accept the `delay` / `deliver_at` publish options
    retention="work_queue",    # "limits" (default) | "work_queue" | "interest"
))
```

With `retention="work_queue"` the stream has at most one consumer, and a
record is removed once that consumer acked it. With `"interest"` a record is
removed once every consumer of the stream acked it. Expired records are
never read or delivered. These settings show up in
`(await client.stream_info(name)).config`.

## Publishing

```python
from exspeed import PublishRecord, new_msg_id

# One record. `value` may be bytes (sent as-is), a str (UTF-8) or anything
# JSON-serializable.
result = await client.publish(
    "orders",
    "orders.placed",
    {"id": 1},
    key="customer-7",
    headers={"trace-id": "abc"},
    msg_id=new_msg_id(),  # idempotency key
)
result.offset, result.duplicate

# Several records in one request; one result per record, in order.
results = await client.publish_batch("orders", [
    PublishRecord("orders.placed", {"id": 2}),
    PublishRecord("orders.placed", {"id": 3}, headers=[("tag", "a"), ("tag", "b")]),
])
```

Headers are a mapping, or `(key, value)` pairs when a key repeats.

### TTLs, delays and priorities

```python
from datetime import datetime, timedelta, timezone

await client.publish("jobs", "jobs.email", job, ttl=30)                     # expire after 30 s
await client.publish("jobs", "jobs.email", job, ttl="5m")                   # units ms, s, m, h, d
await client.publish("jobs", "jobs.email", job, delay=timedelta(seconds=10))  # deliver in 10 s
await client.publish("jobs", "jobs.email", job,
                     deliver_at=datetime(2026, 12, 24, 18, tzinfo=timezone.utc))
await client.publish("jobs", "jobs.email", job, priority=9)                 # 0 (default) to 9
```

These options are headers on the record: `exspeed-ttl`, `exspeed-delay`,
`exspeed-deliver-at` (ms since the epoch) and `exspeed-priority`. They work
the same in `PublishRecord` and the publisher. `deliver_at` takes a
`datetime` or a Unix timestamp in seconds (`time.time() + 60`). A `ttl`
needs a stream with `allow_msg_ttl`, and `delay` / `deliver_at` need
`allow_delayed`; otherwise the publish fails with 400. A delay holds a record
back from consumers only; stateless reads see it at once. Priorities take
effect for consumers with a `priority_window`.

**Idempotency.** When a record carries a `msg_id`, the server remembers it
for the stream's dedup window. A retry with the same `msg_id` and the same
body returns the original offset with `duplicate=True` and writes nothing.
Reusing a `msg_id` with a different body fails with 409
(`detail["stored_offset"]`). `new_msg_id()` generates a time-ordered UUIDv7.

### The coalescing publisher

For throughput, use a publisher. It gathers concurrent `publish` calls into
`PublishBatch` requests and keeps many batches in flight. Records reach the
stream in the order you called `publish`, and each call returns its own
record's result.

```python
async with client.publisher(
    batch_window=0.0,        # 0 = batch whatever was published in the same event-loop iteration
    max_batch_records=512,   # records per request
    max_in_flight=4096,      # accepted but unacknowledged records; publish() waits beyond this
) as publisher:
    await asyncio.gather(*(publisher.publish("events", "events.raw", e) for e in events))
    await publisher.flush()  # wait for everything accepted so far
# leaving the block flushes and closes the publisher
```

If a batch fails, every record in it raises the same error.

## Reading without a consumer

```python
page = await client.read(
    "orders",
    from_offset=0,          # first offset
    max_records=100,
    filter="orders.eu.>",   # NATS-style: `*` = one token, `>` = one or more
    wait=5,                 # long-poll for up to 5 s when caught up
)
for r in page.records:
    print(r.offset, r.subject, r.json())
# continue with from_offset=page.next_offset
```

Stateless reads keep no server-side state. Use them for replay, tools and
tailing. A record has `offset`, `subject`, `key`, `value` (bytes),
`headers`, `timestamp_ns`, `timestamp_ms`, `timestamp` (an aware UTC
`datetime`), and `text()`, `json()` and `header(name)`.

## Consumers

A consumer is a named, durable cursor over one stream, kept on the server.
It tracks what has been delivered, what is acknowledged and what is due for
redelivery.

```python
from exspeed import ConsumerSpec, DeliverFromOffset, DeliverFromTime

await client.create_consumer(ConsumerSpec(
    name="billing",
    stream="orders",
    filter_subjects=["orders.placed", "orders.eu.>"],  # default: all subjects
    deliver="all",               # "all" | "new" | DeliverFromOffset(42) | DeliverFromTime(datetime)
    ack="explicit",              # or "none" (at-most-once)
    ack_wait_ms=30_000,          # redeliver if not acked in time
    max_deliver=5,               # then dead-letter (0 = retry forever)
    backoff_ms=[1000, 5000, 30_000],  # redelivery delays by attempt (the last repeats)
    max_ack_pending=1000,        # pause delivery at this many unacked records
    dlq_stream="orders-dlq",     # where dead letters go (unset = dropped and counted)
    ephemeral=False,             # True = deleted when this connection closes
    dead_letter_expired=False,   # True = records whose TTL ends before the ack go to dlq_stream
    filter_headers={"region": "eu"},  # only records with these header values
    header_match="all",          # "all" filter_headers must match, or "any" of them
    single_active=False,         # True = one subscription at a time gets records; the next takes over
    priority_window=0,           # look this many records ahead and deliver higher `priority` first
))
```

Every field except `name` and `stream` is optional; `None` takes the
server's default. `create_consumer` is idempotent for an identical spec and
fails with 409 if the consumer exists with a different one. Also available:
`consumer_info(name)`, `list_consumers(stream=None)`, `delete_consumer(name)`
and `seek(name, target)`:

```python
from datetime import datetime, timezone
from exspeed import SeekTime

await client.seek("billing", "earliest")
await client.seek("billing", "latest")
await client.seek("billing", 1000)                                    # an offset
await client.seek("billing", datetime(2026, 10, 1, tzinfo=timezone.utc))  # a time
await client.seek("billing", SeekTime(1_790_000_000_000))             # ms since the epoch
```

`consumer_info` returns a `ConsumerInfo` with the consumer's spec (server
defaults filled in), position and counters: `next_offset`, `ack_floor`,
`num_unacked`, `num_in_flight`, `num_delayed` (records waiting for their
`delay` / `deliver_at`), `num_waiting`, `lag`, `subscribers`,
`pull_waiters` and `stats` (`delivered`, `redelivered`, `acked`,
`dead_lettered`, ...).

A `single_active` consumer refuses pulls, and its subscribers form a
failover group: the oldest live subscription gets every record.

### Push: subscriptions

```python
async with client.subscribe("billing", window=256) as sub:
    async for msg in sub:
        # msg.offset, msg.subject, msg.key, msg.value (bytes), msg.headers, msg.delivery_count,
        # msg.timestamp, msg.json(), msg.text(), msg.header(name)
        msg.ack()
print(sub.end_reason)  # EndReason(code, message)
```

The server pushes at most `window` records ahead of your code. The client
returns credit as you take messages (in batches of half the window), so a
slow handler slows delivery down instead of filling memory.

A subscription ends when:

- you call `await sub.unsubscribe()`, or leave its `async with` block
  (`end_reason.code == 0`);
- the consumer or its stream is deleted (404);
- the node loses leadership (503, see [Errors](#errors));
- the client closes (0), or the connection is lost and not re-established
  (503).

When a subscription ends, records delivered to it and not acked are
redelivered, including any it had buffered that your code never saw.
`await sub.next(timeout=...)` is an alternative to `async for`: it returns
the next message, or `None` on timeout or once the subscription has ended.
Without `async with`, call `unsubscribe()` when done; breaking out of an
`async for` loop alone leaves the subscription running.

### Pull

```python
msgs = await client.pull("billing", max_messages=100, expires=5)
for msg in msgs:
    await handle(msg)
    msg.ack()
```

`pull` long-polls for up to `expires` seconds and returns whatever is
available, or `[]` on timeout. It suits batch jobs and request-driven work.
Push suits steady streams.

### Work sharing

Any number of subscriptions and pullers, on any connection and in any
process, can share one consumer. Each record goes to exactly one of them at a
time, so to scale out, run more instances against the same consumer name:

```python
# in every instance of the billing service
await client.create_consumer(ConsumerSpec("billing", "orders"))  # idempotent
async with client.subscribe("billing") as sub:
    async for msg in sub:
        ...
```

For fan-out, where every reader sees every record, give each reader its own
consumer.

### Acks, redelivery and dead letters

| Call | Effect |
|------|--------|
| `msg.ack()` | Done. Fire-and-forget: no round trip; acks made in the same event-loop iteration share one frame. |
| `await msg.nack(delay=None)` | Redeliver after `delay` seconds, or after the consumer's `backoff_ms` when omitted. |
| `await msg.term(reason)` | Never redeliver: dead-letter now. |
| `await msg.in_progress()` | Still working: reset the ack deadline. |
| `await client.ack(consumer, offsets)` | Ack several offsets and wait for confirmation. |

An unacked record is redelivered when its `ack_wait_ms` deadline passes,
when it is nacked, or when the subscription it went to ends or disconnects.
`msg.delivery_count` is 1 on the first delivery and goes up on each
redelivery. After `max_deliver` deliveries, or on `term`, the record goes to
`dlq_stream` with the headers `exspeed-dlq-origin`, `exspeed-dlq-stream`,
`exspeed-dlq-original-offset`, `exspeed-dlq-deliveries`,
`exspeed-dlq-cause` (`max_deliver`, `rejected` or `expired`),
`exspeed-dlq-reason` and `exspeed-dlq-time`.

Delivery is **at-least-once**, so make handlers idempotent. Because
`msg.ack()` doesn't wait, an ack the server rejects surfaces as the client's
`"error"` event. An ack made while the connection is down is dropped, and the
record is redelivered.

## Core publish/subscribe

Core messages go to the subscriptions live at the moment they are
published. Nothing is stored, nothing is acked, and delivery is at most
once. Use them for notifications, cache invalidation and request-reply;
use streams when a message must not be lost.

```python
async with client.subscribe_core("orders.>") as sub:          # NATS-style filter
    await client.publish_core("orders.eu.created", {"id": 1}, headers={"trace-id": "t1"})
    async for m in sub:
        # m.subject, m.reply_to, m.headers, m.value (bytes), m.text(), m.json(), m.header(name)
        print(m.subject, m.json())

# A queue group: each message goes to one member of the group.
worker = await client.subscribe_core("jobs.resize", queue="resizers")
```

`publish_core` returns once the server has accepted the message. A core
subscription ends on `unsubscribe()` (or leaving its `async with` block),
when the client closes, or when the server ends it (`sub.end_reason`, code
503 when leadership moves to another node). After a reconnect the client
subscribes again with the same subject and queue group; messages published
while it was disconnected are missed. `await sub.next(timeout=...)` works as
it does for consumer subscriptions.

### Request-reply

```python
# The service: answer each request with m.respond(value).
async with client.subscribe_core("svc.upper", queue="svc") as requests:
    async for m in requests:
        await m.respond(m.text().upper())

# The caller: returns the first response, as a core message.
reply = await client.request("svc.upper", "hello", timeout=2)
reply.text()  # "HELLO"
```

`request` publishes with a reply subject and waits for the first response.
It fails at once with `ServerError` 404 when nobody is subscribed to the
subject ("no responders"), and with `TimeoutError` after `timeout` seconds
(default: `request_timeout`). All requests on a client share one inbox
subscription, `_INBOX.<random>.*`, which the first request sets up. When the
connection drops, requests waiting for a response fail with
`ConnectionError`, and the next request after the reconnect sets up a new
inbox. Request-reply needs no extra permissions: anyone may publish a reply
to an `_INBOX.…` subject, and a client may subscribe to its own inbox. The
caller needs publish permission on the request subject and the responder
subscribe permission on it.

## Key-value buckets

A bucket is a stream (`KV_<bucket>`) that keeps the latest values of each
key. Keys are subjects, so they are dot-separated tokens such as
`app.mode`.

```python
kv = client.kv("config")
await kv.create(history=5, ttl=None, max_bytes=0)   # idempotent; all optional

rev = await kv.put("app.mode", "prod")              # returns the new revision
entry = await kv.get("app.mode")                    # KvEntry, or None when absent or deleted
# entry.key, entry.value (bytes), entry.text(), entry.json(), entry.revision, entry.op, entry.timestamp

await kv.create_key("app.port", "8080")             # only if absent: 409 otherwise
await kv.update("app.mode", "dev", rev)             # compare-and-set: 409 unless still at `rev`
await kv.put("session.abc", token, ttl=60)          # this key expires after a minute
await kv.get_revision("app.mode", rev)              # an older value, while history keeps it
await kv.history("app.mode")                        # kept revisions, oldest first, deletes included
await kv.keys("app.*")                              # keys with a value, sorted ("" or omitted = all)
await kv.delete("app.port")                         # a tombstone; history stays
await kv.purge("app.port")                          # a tombstone that also hides older values
await kv.destroy()                                  # delete the bucket and everything in it
```

A revision is the position of the write in the bucket's stream, plus one,
so revisions only grow; 0 means "absent", which is what `create_key` checks.
`history` is how many values each key keeps (1 to 64, default 1); the
bucket's `ttl` expires every key that long after its last put. A failed
compare-and-set is a `ServerError` 409 with `detail["current_revision"]`.
`put`, `delete` and `purge` take `expected_revision=` too. `get` on a
missing bucket raises `ServerError` 404; a missing key returns `None`.
Tombstones carry the header `exspeed-kv-op` (`DEL` or `PURGE`), and an
entry's `op` is `"put"`, `"delete"` or `"purge"`.

### Watching

```python
async with kv.watch("app.*") as watch:    # "" or omitted = every key
    async for e in watch:
        if e.op == "put":
            apply(e.key, e.json())
        else:
            remove(e.key)                 # "delete" or "purge"
```

A watch first yields the current value of every matching key (deleted keys
left out), ordered by revision, then every change as it happens, deletes
included. It reads the bucket's stream with stateless long-poll reads, so it
holds no state on the server. `await watch.next(timeout=...)` returns `None`
when nothing changed in time, `watch.stop()` (or leaving the `async with`
block) ends it, and a failed read, such as `ConnectionError` when the
connection drops, is raised from the iterator.

## Queries and metadata

```python
r = await client.query('SELECT COUNT(*) AS n FROM "orders"')
r.columns, r.rows, r.row_count, r.execution_time_ms   # ["n"], [[123]], 1, 2

await client.metadata()   # Metadata(node_id, is_leader, leader, server_version)
await client.ping()       # round-trip time in seconds
client.server_info        # ServerInfo(server_version, node_id, leader) from the handshake
client.connected          # False while reconnecting or after close()
```

With auth enabled, `query` needs a global-admin credential, since SQL can
read any stream.

## Errors

All errors derive from `exspeed.ExspeedError`:

| Class | When |
|-------|------|
| `ServerError` | The server rejected the request. It has `code`, `message`, `detail` and `leader_hint`. |
| `ConnectionError` | Not connected: the connection could not be opened, was lost, is being re-established, or the client is closed. Also a built-in `ConnectionError`. |
| `TimeoutError` | No response within `request_timeout` (plus a pull's or read's own wait). Also a built-in `TimeoutError`. |
| `ProtocolError` | The server sent something this client can't decode. |

`ServerError.code` is HTTP-like; `exspeed.ErrorCode` names the codes:

| Code | Meaning | `detail` |
|------|---------|----------|
| 400 | Malformed request, invalid name, filter or config | |
| 401 | Not authenticated | |
| 403 | The credential lacks the needed permission | |
| 404 | Stream, consumer, bucket or key not found; a request with no responders | |
| 408 | A `query` timed out | |
| 409 | Exists with different settings; stream still has consumers; `msg_id` reused with a different body; a KV key not at the expected revision | `{"stored_offset"}`, `{"consumers"}`, `{"current_revision"}` |
| 422 | A `query` exceeded the server's query memory limit | |
| 429 | Retry later (dedup map full, stream full with `discard="new"`, too many concurrent waits on one connection) | `{"retry_after_secs"}` |
| 500 | Internal error | |
| 503 | Not the leader, still starting, or too few in-sync replicas | `{"leader"}`, `{"in_sync", "required"}` |
| 507 | The server's disk is full; nothing was written | |

`detail` is the decoded JSON the server sent, with snake_case keys. For 503
errors, `err.leader_hint` holds the leader's address when the server knows
it. A failed request isn't retried against the leader automatically; the
client follows leader hints only when it connects or reconnects (see
[Clusters](#clusters)).

```python
from exspeed import ErrorCode, ServerError

try:
    await client.publish("orders", "orders.placed", order)
except ServerError as err:
    if err.code == ErrorCode.UNAVAILABLE and err.leader_hint:
        ...  # reconnect to err.leader_hint
    raise
```

## Reconnection

Reconnection is on by default. When the connection drops:

1. Pending requests raise `ConnectionError`. They are not retried, because a
   publish may or may not have been applied. Retry publishes with a `msg_id`
   to make the retry safe. New requests also raise `ConnectionError` until
   the connection is back.
2. The client emits `"disconnect"` and reconnects with exponential backoff
   (0.1 s doubling to 5 s, with jitter, unlimited attempts by default).
3. Once reconnected, it re-creates the ephemeral consumers it created (the
   server deleted them with the old connection), then re-subscribes every
   live subscription with its original window. Your `async for` loop keeps
   running and doesn't notice the gap. Records that were buffered but not
   yet handed to your code are discarded, and the server redelivers them,
   along with anything delivered but not acked. A re-subscribe that fails
   (for example, the consumer was deleted meanwhile) ends that subscription
   with the server's error code. Core subscriptions are subscribed again
   too; core messages published during the gap are missed.
4. The client emits `"reconnect"`.

If the server rejects the credential (401/403) or `max_attempts` runs out,
the client closes: subscriptions end with code 503 and `"close"` is emitted.
The first `connect()` is never retried; it raises straight away.

```python
from exspeed import ReconnectOptions

client = await exspeed.connect(
    "exspeed.internal",
    reconnect=ReconnectOptions(max_attempts=30, initial_delay=0.2, max_delay=10),
    # reconnect=False  -> the client closes when the connection drops
)
client.on("disconnect", lambda err: log.warning("exspeed disconnected: %s", err))
client.on("reconnect", lambda info: log.info("exspeed reconnected: %s", info))
client.on("close", lambda err: log.error("exspeed closed: %s", err))
client.on("error", lambda err: log.warning("exspeed async error: %s", err))  # failed acks/credits
```

Listeners may be plain functions or coroutine functions (run as tasks);
`client.off(event, callback)` removes one. Durable consumers make
reconnection safe: the cursor and the unacked set live on the server, so
nothing is lost and nothing is skipped. An ephemeral consumer created with
`deliver="new"` does skip records published while it was gone, because the
re-created consumer starts at the end of the stream.

The client pings every `keepalive` seconds (20) so the server, which drops
connections idle for 120 s, keeps the connection open. A ping that times out
is treated as a dead connection.

## Clusters

Against a cluster, give the client some seed addresses. It connects to
whichever node is the leader, following the leader hints followers return.
After a failover it reconnects to the new leader and restores subscriptions
as described above.

```python
client = await exspeed.connect(
    servers=["exspeed-0.exspeed:5933", "exspeed-1.exspeed:5933", "exspeed-2.exspeed:5933"],
)
```

Without `servers`, a node that names another node as leader in its handshake
is still followed, so connecting through a Service that routes to any node
works too. A write that reaches a follower anyway fails with `ServerError`
503, and `err.leader_hint` names the leader.

## TLS and authentication

```python
import os
from exspeed import TlsOptions

client = await exspeed.connect(
    "exspeed.example.com",
    5933,
    token=os.environ["EXSPEED_TOKEN"],  # the server's --auth-token, or a credential token
    tls=True,                           # verify against the system CAs
    # tls=TlsOptions(ca_file="ca.pem")                         # private CA (or ca_data="-----BEGIN ...")
    # tls=TlsOptions(ca_file="ca.pem", server_hostname="exspeed.internal")  # name override
    # tls=my_ssl_context                                       # any ssl.SSLContext
)
```

Certificates are verified by default. A wrong or missing token fails
`connect()` with `ServerError` 401. A token without permission for an
operation gets 403. With scoped credentials, `list_streams` and
`list_consumers` return only what the credential can see. Python 3.13 and
later verify certificates strictly (`VERIFY_X509_STRICT`): a private CA
needs the `basicConstraints` and `keyUsage` extensions, or pass your own
`ssl.SSLContext`.

### Client certificates (mutual TLS)

When the server runs with `tls.client_ca`, it accepts only clients that
present a certificate signed by that CA:

```python
client = await exspeed.connect(
    "exspeed.example.com",
    tls=TlsOptions(
        ca_file="ca.pem",               # the server's CA
        cert_file="orders-client.pem",
        key_file="orders-client.key",
    ),
    # no token: the credential bound to the certificate's name (`cert_cn`) applies
)
```

Without a valid client certificate the connection is refused and
`connect()` raises `ConnectionError`. With auth on, a client with a
certificate and no token gets the permissions of the credential whose
`cert_cn` matches the certificate's common name (or its first DNS name), and
`connect()` fails with `ServerError` 401 when no credential names it. A
token, when given, takes precedence.

## Options

| Option | Default | |
|--------|---------|---|
| `host` | `"127.0.0.1"` | |
| `port` | `5933` | |
| `servers` | none | Cluster seeds (`"host:port"`); connects to the leader. Overrides `host`/`port`. |
| `token` | none | Bearer token |
| `tls` | off | `True`, a `TlsOptions`, or an `ssl.SSLContext` |
| `client_id` | `"exspeed-py"` | Client name, shown in server logs |
| `request_timeout` | `30.0` | Seconds per request, on top of a pull's or read's own wait. Also bounds connecting. |
| `keepalive` | `20.0` | Ping interval in seconds; `0` disables pings |
| `reconnect` | `True` | `False`, or `ReconnectOptions(max_attempts, initial_delay, max_delay)` |
| `verify_crc` | `False` | Check each received record's CRC32C (pure Python, so it costs CPU on large reads) |

The low-level protocol (frame codec, message types, opcodes) is available in
`exspeed.protocol` for tools and tests.

## Development

```bash
cd sdks/python
python -m venv .venv && . .venv/bin/activate
python -m pip install -e '.[dev]'
ruff check .
mypy src
pytest            # unit tests, plus e2e tests when a server binary is available
```

The end-to-end tests (`tests/e2e`) start real servers. They use the binary
named by `EXSPEED_BIN`, or else `target/debug/exspeed` at the repository root
(`cargo build -p exspeed --bin exspeed`). Without one, they are skipped with
a message.
