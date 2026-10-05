# Exspeed.Client

.NET client for [Exspeed](https://github.com/alternayte/exspeed). It speaks the
binary client protocol v2 ([`docs/protocol.md`](../../docs/protocol.md)) over
TCP or TLS. Targets .NET 8 and has no dependencies beyond the base class
library.

- One client is one connection. Requests are multiplexed, so a long pull
  never blocks other calls. Share one client across your application; it is
  thread-safe.
- Durable consumers with push (credit-windowed subscriptions as
  `IAsyncEnumerable<Message>`) or pull delivery, acks, redelivery with backoff,
  and dead-lettering.
- A coalescing publisher for high-throughput, order-preserving writes.
- Stream limits, per-message TTLs, delayed delivery, priorities and captured
  core subjects.
- Core (non-persistent) publish/subscribe, queue groups and request-reply.
- Key-value buckets with revisions, compare-and-set, history and watches.
- Automatic reconnection that re-establishes subscriptions.

Every asynchronous method takes a `CancellationToken`, and the client,
subscriptions, publishers and watches are `IAsyncDisposable`.

## Install

```bash
dotnet add package Exspeed.Client
```

## Quick start

```csharp
using Exspeed;

await using var client = await ExspeedClient.ConnectAsync(new ExspeedClientOptions { Host = "127.0.0.1", Port = 5933 });

await client.CreateStreamAsync(new StreamSpec("orders") { MaxAge = TimeSpan.FromDays(7) }); // idempotent

var result = await client.PublishAsync("orders", PublishRecord.Json("orders.placed", new { id = 42, total = 99.5 }) with
{
    Key = "customer-7"u8.ToArray(),
});
Console.WriteLine(result.Offset);

await client.CreateConsumerAsync(new ConsumerSpec("billing", "orders") { FilterSubjects = new[] { "orders.placed" } });

await foreach (var msg in await client.SubscribeAsync("billing", new SubscribeOptions { Window = 256 }))
{
    var order = msg.Json<Order>();
    await ChargeAsync(order);
    msg.Ack();
}
```

## Streams

```csharp
await client.CreateStreamAsync(new StreamSpec("orders")
{
    MaxAge = TimeSpan.FromDays(1),          // retention by age (whole seconds)
    MaxBytes = 10UL << 30,                  // retention by size
    DedupWindow = TimeSpan.FromMinutes(5),  // how long msg ids are remembered
    DedupMaxEntries = 1_000_000,
    Compaction = false,
});
await client.UpdateStreamAsync(new StreamSpec("orders") { MaxAge = TimeSpan.FromDays(3) });
StreamInfo info = await client.StreamInfoAsync("orders"); // Name, EarliestOffset, NextOffset, Records, Config, Internal, Raw
IReadOnlyList<StreamInfo> all = await client.ListStreamsAsync();
await client.DeleteStreamAsync("orders");                 // 409 while consumers exist
```

Unset numeric settings mean "server default". `CreateStreamAsync` is
idempotent when the stream already exists with the same settings, and fails
with 409 when they differ. `UpdateStreamAsync` replaces all settings, so pass
every field you want to keep.

### Limits, lifetimes, retention and captured subjects

```csharp
await client.CreateStreamAsync(new StreamSpec("jobs")
{
    MaxMsgs = 100_000,                      // most records the stream holds (0 = no limit)
    Discard = DiscardPolicy.Old,            // at MaxMsgs: Old drops the oldest, New rejects new records (429)
    MaxMsgsPerSubject = 10,                 // most records kept per subject
    AllowMsgTtl = true,                     // accept the per-record Ttl publish option
    MsgTtl = TimeSpan.FromDays(1),          // default lifetime of every record (zero = none)
    AllowDelayed = true,                    // accept the Delay / DeliverAt publish options
    Retention = RetentionPolicy.WorkQueue,  // Limits (default) | WorkQueue | Interest
});

// Core messages published to matching subjects are also appended to the stream.
await client.CreateStreamAsync(new StreamSpec("audit") { CaptureSubjects = new[] { "audit.>" } });
```

With `RetentionPolicy.WorkQueue` the stream has at most one consumer, and a
record is removed once that consumer acked it. With `Interest` a record is
removed once every consumer of the stream acked it. Expired records are never
read or delivered. `CaptureSubjects` turns core messages on matching subjects
into stream records; no two streams may capture overlapping subjects. These
settings show up in `StreamInfoAsync(name).Config`.

## Publishing

```csharp
// One record. The value is bytes; PublishRecord(subject, string) sends UTF-8 text and
// PublishRecord.Json(subject, value) serializes with System.Text.Json.
await client.PublishAsync("orders", new PublishRecord("orders.placed", "{\"id\":1}")
{
    Key = "customer-7"u8.ToArray(),
    Headers = new[] { new KeyValuePair<string, string>("trace-id", "abc") },
    MsgId = MsgId.New(), // idempotency key
});

// Several records in one request; one result per record.
IReadOnlyList<PublishResult> results = await client.PublishBatchAsync("orders", new[]
{
    PublishRecord.Json("orders.placed", new { id = 2 }),
    PublishRecord.Json("orders.placed", new { id = 3 }),
});
```

Each publish returns a `PublishResult(Offset, Duplicate)`.

### TTLs, delays and priorities

```csharp
await client.PublishAsync("jobs", job with { Ttl = TimeSpan.FromSeconds(30) });       // expire after 30 s
await client.PublishAsync("jobs", job with { Delay = TimeSpan.FromSeconds(10) });     // deliver in 10 s
await client.PublishAsync("jobs", job with { DeliverAt = DateTimeOffset.Parse("2026-12-24T18:00:00Z") });
await client.PublishAsync("jobs", job with { Priority = 9 });                          // 0 (default) to 9
```

These options are headers on the record: `exspeed-ttl`, `exspeed-delay`,
`exspeed-deliver-at` (ms since the epoch) and `exspeed-priority`
(`HeaderNames` has the constants). They work the same in `PublishBatchAsync`
and the publisher. A `Ttl` needs a stream with `AllowMsgTtl`, and `Delay` /
`DeliverAt` need `AllowDelayed`; otherwise the publish fails with 400. A delay
holds a record back from consumers only; stateless reads see it at once.
Priorities take effect for consumers with a `PriorityWindow`.

**Idempotency.** When a record carries a `MsgId`, the server remembers it for
the stream's dedup window. A retry with the same `MsgId` and the same body
returns the original offset with `Duplicate = true` and writes nothing.
Reusing a `MsgId` with a different body fails with 409
(`detail.stored_offset`). `MsgId.New()` generates a time-ordered UUIDv7.

### The coalescing publisher

For throughput, use a publisher. It gathers concurrent `PublishAsync` calls
into `PublishBatch` requests and keeps many batches in flight. Records reach
the stream in the order you called `PublishAsync`, and each call completes
with its own record's offset.

```csharp
await using var publisher = client.CreatePublisher(new PublisherOptions
{
    BatchWindow = TimeSpan.Zero, // zero = batch whatever arrives while a flush is being scheduled
    MaxBatchRecords = 512,       // records per request
    MaxInFlight = 4096,          // accepted but unacknowledged records; further records wait, in order
});

await Task.WhenAll(events.Select(e => publisher.PublishAsync("events", PublishRecord.Json("events.raw", e))));
await publisher.FlushAsync(); // wait for everything accepted so far
await publisher.CloseAsync(); // flush, then reject further publishes
```

If a batch fails, every record in it fails with the same exception.

## Reading without a consumer

```csharp
ReadResult page = await client.ReadAsync("orders", new ReadOptions
{
    From = 0,                   // first offset
    MaxRecords = 100,
    Filter = "orders.eu.>",     // NATS-style: `*` = one token, `>` = one or more
    Wait = TimeSpan.FromSeconds(5), // long-poll when caught up
});
foreach (var r in page.Records)
{
    Console.WriteLine($"{r.Offset} {r.Subject} {r.Text()}");
}
// continue with From = page.NextOffset
```

Stateless reads keep no server-side state. Use them for replay, tools and
tailing. A `StreamRecord` has `Offset`, `Timestamp` (and `TimestampNs`),
`Subject`, `Key`, `Value`, `Headers`, and the helpers `Text()`, `KeyText()`,
`Json<T>()` and `Header(name)`.

## Consumers

A consumer is a named, durable cursor over one stream, kept on the server. It
tracks what has been delivered, what is acknowledged and what is due for
redelivery.

```csharp
await client.CreateConsumerAsync(new ConsumerSpec("billing", "orders")
{
    FilterSubjects = new[] { "orders.placed", "orders.eu.>" }, // default: all subjects
    Deliver = DeliverPolicy.All,          // All | New | FromOffset(n) | FromTime(t)
    Ack = AckPolicy.Explicit,             // or None (at-most-once)
    AckWait = TimeSpan.FromSeconds(30),   // redeliver if not acked in time
    MaxDeliver = 5,                       // then dead-letter (0 = retry forever)
    Backoff = new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5) }, // redelivery delays (the last repeats)
    MaxAckPending = 1000,                 // pause delivery at this many unacked records
    DlqStream = "orders-dlq",             // where dead letters go (unset = dropped and counted)
    Ephemeral = false,                    // true = deleted when this connection closes
    DeadLetterExpired = false,            // true = records whose TTL ends before the ack go to DlqStream
    FilterHeaders = new Dictionary<string, string> { ["region"] = "eu" },
    HeaderMatch = HeaderMatch.All,        // All filter headers must match, or Any of them
    SingleActive = false,                 // true = one subscription at a time gets records; the next takes over
    PriorityWindow = 0,                   // look this many records ahead and deliver higher Priority first
});
```

Every property except the name and stream is optional; unset ones take the
server's defaults. `CreateConsumerAsync` is idempotent for an identical spec
and fails with 409 if the consumer exists with a different one. Also
available: `ConsumerInfoAsync(name)`, `ListConsumersAsync(stream?)`,
`DeleteConsumerAsync(name)` and `SeekAsync(name, target)`:

```csharp
await client.SeekAsync("billing", SeekTarget.Earliest);
await client.SeekAsync("billing", SeekTarget.Latest);
await client.SeekAsync("billing", SeekTarget.Offset(1000));
await client.SeekAsync("billing", SeekTarget.Time(DateTimeOffset.Parse("2026-10-01T00:00:00Z")));
```

`ConsumerInfo` reports the spec (with defaults filled in), the consumer's
position (`NextOffset`, `AckFloor`) and counters: `NumUnacked`,
`NumInFlight`, `NumDelayed` (records waiting for their delay or delivery
time), `NumWaiting`, `Lag`, `Subscribers`, `PullWaiters` and `Stats`.

A `SingleActive` consumer refuses pulls, and its subscribers form a failover
group: the oldest live subscription gets every record.

### Push: subscriptions

```csharp
Subscription sub = await client.SubscribeAsync("billing", new SubscribeOptions { Window = 256 });

await foreach (var msg in sub)
{
    // msg.Offset, msg.Timestamp, msg.DeliveryCount, msg.Subject, msg.Key, msg.Value,
    // msg.Headers, msg.Json<T>(), msg.Text(), msg.Header(name)
    msg.Ack();
}
Console.WriteLine(sub.EndReason); // EndReason(Code, Message)
```

The server pushes at most `Window` records ahead of your code. The client
returns credit as you take messages (in batches of half the window), so a
slow handler slows delivery down instead of filling memory.

A subscription ends when:

- you call `UnsubscribeAsync()` or leave the `await foreach` loop early with
  `break` or an exception (`EndReason.Code == 0`);
- the consumer or its stream is deleted (`404`);
- the node loses leadership (`503`, see [Errors](#errors));
- the client closes (`0`), or the connection is lost and not re-established
  (`503`).

When a subscription ends, records delivered to it and not acked are
redelivered, including any it had buffered that your code never saw.

`sub.NextAsync(timeout)` is an alternative to `await foreach`. It returns the
next message, or `null` on timeout or once the subscription has ended.

### Pull

```csharp
IReadOnlyList<Message> msgs = await client.PullAsync("billing", new PullOptions
{
    MaxMessages = 100,
    Expires = TimeSpan.FromSeconds(5),
});
foreach (var m in msgs)
{
    await HandleAsync(m);
    m.Ack();
}
```

`PullAsync` long-polls for up to `Expires` and returns whatever is available,
or an empty list on timeout. It suits batch jobs and request-driven work. Push
suits steady streams.

### Work sharing

Any number of subscriptions and pullers, on any connection and in any
process, can share one consumer. Each record goes to exactly one of them at a
time, so to scale out, run more instances against the same consumer name. For
fan-out, where every reader sees every record, give each reader its own
consumer.

### Acks, redelivery and dead letters

| Call | Effect |
|------|--------|
| `msg.Ack()` | Done. Fire-and-forget: no round trip; acks made close together share one frame. |
| `await msg.NackAsync(delay?)` | Redeliver after `delay`, or after the consumer's `Backoff` when omitted. |
| `await msg.TermAsync(reason)` | Never redeliver: dead-letter now. |
| `await msg.InProgressAsync()` | Still working: reset the ack deadline. |
| `await client.AckAsync(consumer, offsets)` | Ack several offsets and wait for confirmation. |

An unacked record is redelivered when its `AckWait` deadline passes, when it
is nacked, or when the subscription it went to ends or disconnects.
`msg.DeliveryCount` is 1 on the first delivery and goes up on each
redelivery. After `MaxDeliver` deliveries, or on `TermAsync`, the record goes
to `DlqStream` with these headers: `exspeed-dlq-origin`, `exspeed-dlq-stream`,
`exspeed-dlq-original-offset`, `exspeed-dlq-deliveries`, `exspeed-dlq-cause`,
`exspeed-dlq-reason` and `exspeed-dlq-time`.

Delivery is **at-least-once**, so make handlers idempotent. Because `Ack()`
doesn't wait, an ack the server rejects raises the client's `AsyncError`
event. An ack made while the connection is down is dropped, and the record is
redelivered.

## Core publish/subscribe

Core messages go to the subscriptions live at the moment they are published.
Nothing is stored (unless a stream captures the subject), nothing is acked,
and delivery is at most once. Use them for notifications, cache invalidation
and request-reply; use streams when a message must not be lost.

```csharp
CoreSubscription sub = await client.SubscribeCoreAsync("orders.>"); // NATS-style filter
await client.PublishCoreAsync("orders.eu.created", "{\"id\":1}", new CorePublishOptions
{
    Headers = new[] { new KeyValuePair<string, string>("trace-id", "t1") },
});

await foreach (var m in sub)
{
    // m.Subject, m.ReplyTo, m.Headers, m.Value, m.Text(), m.Json<T>(), m.Header(name)
    Console.WriteLine($"{m.Subject} {m.Text()}");
}

// A queue group: each message goes to one member of the group.
var worker = await client.SubscribeCoreAsync("jobs.resize", new CoreSubscribeOptions { Queue = "resizers" });
```

`PublishCoreAsync` completes once the server has accepted the message. A core
subscription ends on `UnsubscribeAsync()` (or leaving the loop early), when
the client closes, or when the server ends it (`EndReason`, code 503 when
leadership moves to another node). After a reconnect the client subscribes
again with the same subject and queue group; messages published while it was
disconnected are missed. `NextAsync(timeout)` works as it does for consumer
subscriptions.

### Request-reply

```csharp
// The service: answer each request with RespondAsync.
var requests = await client.SubscribeCoreAsync("svc.upper", new CoreSubscribeOptions { Queue = "svc" });
await foreach (var m in requests)
{
    await m.RespondAsync(m.Text().ToUpperInvariant());
}

// The caller: returns the first response, as a core message.
CoreMessage reply = await client.RequestAsync("svc.upper", "hello", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(2) });
reply.Text(); // "HELLO"
```

`RequestAsync` publishes with a reply subject and waits for the first
response. It fails at once with `ExspeedServerException` 404 when nobody is
subscribed to the subject ("no responders"), and with
`ExspeedTimeoutException` after the timeout (default: `RequestTimeout`). All
requests on a client share one inbox subscription, `_INBOX.<random>.*`, which
the first request sets up. When the connection drops, requests waiting for a
response fail with `ExspeedConnectionException`, and the next request after
the reconnect sets up a new inbox. Request-reply needs no extra permissions:
anyone may publish a reply to an `_INBOX.…` subject, and a client may
subscribe to its own inbox (see [Security](../../docs/security.md)).

## Key-value buckets

A bucket is a stream (`KV_<bucket>`) that keeps the latest values of each key.
Keys are subjects, so they are dot-separated tokens such as `app.mode`.

```csharp
KvBucket kv = client.Kv("config");
await kv.CreateAsync(new KvBucketOptions { History = 5 });    // idempotent; Ttl and MaxBytes optional

ulong rev = await kv.PutAsync("app.mode", "prod");             // returns the new revision
KvEntry? entry = await kv.GetAsync("app.mode");                // null when absent or deleted
// entry.Key, entry.Value, entry.Text(), entry.Json<T>(), entry.Revision, entry.Op, entry.Timestamp

await kv.CreateKeyAsync("app.port", "8080");                   // only if absent: 409 otherwise
await kv.UpdateAsync("app.mode", "dev", rev);                  // compare-and-set: 409 unless still at `rev`
await kv.PutAsync("session.abc", token, new KvPutOptions { Ttl = TimeSpan.FromMinutes(1) });
await kv.GetRevisionAsync("app.mode", rev);                    // an older value, while history keeps it
await kv.HistoryAsync("app.mode");                             // kept revisions, oldest first, deletes included
await kv.KeysAsync("app.*");                                   // keys with a value, sorted ("" = all)
await kv.DeleteAsync("app.port");                              // a tombstone; history stays
await kv.PurgeAsync("app.port");                               // a tombstone that also hides older values
await kv.DestroyAsync();                                       // delete the bucket and everything in it
```

A revision is the position of the write in the bucket's stream, plus one, so
revisions only grow; 0 means "absent", which is what `CreateKeyAsync` checks.
`History` is how many values each key keeps (1 to 64, default 1). A failed
compare-and-set is an `ExspeedServerException` 409 with
`detail.current_revision`. Deletes and purges take an `ExpectedRevision` too.
`GetAsync` on a missing bucket fails with 404. A tombstone carries the header
`exspeed-kv-op` (`DEL` or `PURGE`) and shows up as `KvOp.Delete` or
`KvOp.Purge`.

### Watching

```csharp
await using var watch = kv.Watch("app.*"); // "" = every key
await foreach (var e in watch)
{
    if (e.Op == KvOp.Put) Apply(e.Key, e.Text());
    else Remove(e.Key);
}
```

A watch first yields the current value of every matching key (deleted keys
left out), ordered by revision, then every change as it happens, deletes
included. It reads the bucket's stream with stateless long-poll reads, so it
holds no state on the server. `watch.NextAsync(timeout)` returns `null` when
nothing changed in time, `watch.Stop()` (or leaving the loop) ends it, and a
failed read, such as `ExspeedConnectionException` when the connection drops,
is thrown from the enumeration.

## Queries and metadata

```csharp
QueryResult r = await client.QueryAsync("SELECT COUNT(*) AS n FROM \"orders\"");
// r.Columns = ["n"], r.Rows[0][0].GetInt64(), r.RowCount, r.ExecutionTimeMs, r.Truncated

ServerMetadata md = await client.MetadataAsync(); // NodeId, IsLeader, Leader, ServerVersion
TimeSpan rtt = await client.PingAsync();
ServerInfo info = client.ServerInfo;              // ServerVersion, NodeId, Leader (from the handshake)
```

Rows are `System.Text.Json.JsonElement` values. With auth enabled,
`QueryAsync` needs a global-admin credential, since SQL can read any stream.

## Errors

All exceptions derive from `ExspeedException`:

| Type | When |
|------|------|
| `ExspeedServerException` | The server rejected the request. It has `Code`, `Message`, `Detail` (parsed JSON), `DetailJson` and `LeaderHint`. |
| `ExspeedConnectionException` | Not connected: the connection was lost, is being re-established, the client is closed, or connecting (including the TLS handshake) failed. |
| `ExspeedTimeoutException` | No response within `RequestTimeout` (plus a pull's or read's own wait). |
| `ExspeedProtocolException` | The server sent something this library can't decode. |

Cancelling a `CancellationToken` throws `OperationCanceledException` as
usual. `ExspeedServerException.Code` is HTTP-like; `ErrorCodes` names them:

| Code | Meaning | `Detail` |
|------|---------|----------|
| 400 | Malformed request, invalid name, filter or config | |
| 401 | Not authenticated | |
| 403 | The credential lacks the needed permission | |
| 404 | Stream, consumer, bucket or key not found; a request with no responders | |
| 408 | A query timed out | |
| 409 | Exists with different settings; stream still has consumers; `MsgId` reused with a different body; a KV key not at the expected revision | `stored_offset`, `consumers`, `current_revision` |
| 422 | A query exceeded the server's query memory limit | |
| 429 | Retry later (dedup map full, the stream is full with `Discard = New`, too many concurrent waits on one connection) | `retry_after_secs` |
| 500 | Internal error | |
| 503 | Not the leader, still starting, or too few in-sync replicas | `leader`, `in_sync`, `required` |
| 507 | The server's disk is full; nothing was written | |

`Detail` is passed through as the server sent it, with snake_case keys. For
503 errors, `LeaderHint` holds the leader's address when the server knows it.
A failed request isn't retried against the leader automatically; the client
follows leader hints only when it connects or reconnects (see
[Clusters](#clusters)).

```csharp
try
{
    await client.PublishAsync("orders", record);
}
catch (ExspeedServerException e) when (e.Code == ErrorCodes.Unavailable && e.LeaderHint is not null)
{
    // reconnect to e.LeaderHint
}
```

## Reconnection

Reconnection is on by default. When the connection drops:

1. Pending requests fail with `ExspeedConnectionException`. They are not
   retried, because a publish may or may not have been applied. Retry
   publishes with a `MsgId` to make the retry safe. New requests also fail
   with `ExspeedConnectionException` until the connection is back.
2. The client raises `Disconnected` and reconnects with exponential backoff
   (100 ms doubling to 5 s, with jitter, unlimited attempts by default).
3. Once reconnected, it re-creates the ephemeral consumers it created (the
   server deleted them with the old connection), then re-subscribes every live
   subscription with its original window. Your `await foreach` loop keeps
   running and doesn't notice the gap. Records that were buffered but not yet
   handed to your code are discarded, and the server redelivers them, along
   with anything delivered but not acked. A re-subscribe that fails (for
   example, the consumer was deleted meanwhile) ends that subscription with
   the server's error code. Core subscriptions are subscribed again too; core
   messages published during the gap are missed.
4. The client raises `Reconnected`.

If the server rejects the credential (401/403) or `MaxAttempts` runs out, the
client closes: subscriptions end with code 503 and `Closed` is raised. The
first `ConnectAsync` is never retried; it fails straight away.

```csharp
var client = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
{
    Host = "exspeed.internal",
    Reconnect = new ReconnectOptions
    {
        MaxAttempts = 30,
        InitialDelay = TimeSpan.FromMilliseconds(200),
        MaxDelay = TimeSpan.FromSeconds(10),
    },
    // Reconnect = null -> the client closes when the connection drops
});
client.Disconnected += (_, e) => log.LogWarning(e.Error, "exspeed disconnected");
client.Reconnected += (_, e) => log.LogInformation("exspeed reconnected to {Node}", e.ServerInfo.NodeId);
client.Closed += (_, e) => log.LogError(e.Error, "exspeed closed");
client.AsyncError += (_, e) => log.LogWarning(e.Error, "exspeed async error"); // failed acks/credits
```

Events are raised on the thread that noticed the change, so keep handlers
short. Durable consumers make reconnection safe: the cursor and the unacked
set live on the server, so nothing is lost and nothing is skipped. An
ephemeral consumer created with `DeliverPolicy.New` does skip records
published while it was gone, because the re-created consumer starts at the
end of the stream.

The client pings every `Keepalive` (20 s) so the server, which drops
connections idle for 120 s, keeps the connection open. A ping that times out
is treated as a dead connection.

## Clusters

Against a cluster ([High availability](../../docs/high-availability.md)),
give the client some seed addresses. It connects to whichever node is the
leader, following the leader hints followers return. After a failover it
reconnects to the new leader and restores subscriptions as described above.

```csharp
var client = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
{
    Servers = new[] { "exspeed-0.exspeed:5933", "exspeed-1.exspeed:5933", "exspeed-2.exspeed:5933" },
});
```

Without `Servers`, a node that names another node as leader in its handshake
is still followed, so connecting through a Service that routes to any node
works too. A write that reaches a follower anyway fails with
`ExspeedServerException` 503, and `LeaderHint` names the leader.

## TLS and authentication

```csharp
var client = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
{
    Host = "exspeed.example.com",
    Token = Environment.GetEnvironmentVariable("EXSPEED_TOKEN"), // --auth-token, or a credential token
    Tls = new ExspeedTlsOptions(),                               // verify against the system CAs
    // Tls = ExspeedTlsOptions.FromPem("ca.pem"),                 // private CA
    // Tls = new ExspeedTlsOptions { ServerName = "exspeed.internal" } // certificate name override
});
```

`ExspeedTlsOptions` takes `CaCertificates` (trust only these), a
`ClientCertificate`, a `ServerName`, the allowed `Protocols`, or a
`RemoteCertificateValidation` callback that replaces the default check.
Certificates are verified by default. A wrong or missing token fails
`ConnectAsync` with `ExspeedServerException` 401. A token without permission
for an operation gets 403. With scoped credentials, `ListStreamsAsync` and
`ListConsumersAsync` return only what the credential can see. See
[Security](../../docs/security.md).

### Client certificates (mutual TLS)

When the server runs with `tls.client_ca`, it accepts only clients that
present a certificate signed by that CA:

```csharp
var client = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
{
    Host = "exspeed.example.com",
    Tls = ExspeedTlsOptions.FromPem("ca.pem", "orders-client.pem", "orders-client.key"),
    // no token: the credential bound to the certificate's name (`cert_cn`) applies
});
```

Without a valid client certificate the connection is refused and
`ConnectAsync` fails with `ExspeedConnectionException`. With auth on, a client
with a certificate and no token gets the permissions of the credential whose
`cert_cn` matches the certificate's common name (or its first DNS name), and
`ConnectAsync` fails with `ExspeedServerException` 401 when no credential
names it. A token, when given, takes precedence.

## Options

| `ExspeedClientOptions` | Default | |
|--------|---------|---|
| `Host` | `"127.0.0.1"` | |
| `Port` | `5933` | |
| `Servers` | none | Cluster seeds (`"host:port"`); connects to the leader. Overrides `Host`/`Port`. |
| `Token` | none | Bearer token |
| `Tls` | none (plain TCP) | `ExspeedTlsOptions` |
| `ClientId` | `"exspeed-dotnet"` | Shown in server logs |
| `RequestTimeout` | 30 s | Per request, on top of a pull's or read's own wait. Also bounds connecting. |
| `Keepalive` | 20 s | `TimeSpan.Zero` disables pings |
| `Reconnect` | `new ReconnectOptions()` | `null` disables; or `{ MaxAttempts, InitialDelay, MaxDelay }` |

Offsets and revisions are `ulong`, as on the wire.

## Development

```bash
cd sdks/dotnet
dotnet build -c Release
dotnet test -c Release --no-build
dotnet format --verify-no-changes
```

The unit tests check the codec byte for byte against fixtures generated with
the Rust encoder, and the connection logic against a scriptable fake server.
The end-to-end tests (`tests/Exspeed.Client.Tests/E2E`) start a real server
from `EXSPEED_BIN`, or else `target/debug/exspeed` at the repository root
(`cargo build -p exspeed --bin exspeed`); they are skipped, with a message,
when neither exists. The TLS tests make their certificates with the base
class library, so they need no `openssl`.

## License

MIT
