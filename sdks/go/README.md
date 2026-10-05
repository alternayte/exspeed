# Exspeed Go client

Go client for [Exspeed](https://github.com/alternayte/exspeed). It speaks the
binary client protocol v2 ([`docs/protocol.md`](../../docs/protocol.md)) over
TCP or TLS, with no dependencies outside the standard library. Requires Go
1.22 or later.

- One client is one connection. Requests are multiplexed, so a long pull
  never blocks other calls. A `*Client` is safe for concurrent use; share one
  across your application.
- Every blocking call takes a `context.Context`.
- Durable consumers with push (credit-windowed subscriptions) or pull
  delivery, acks, redelivery with backoff, and dead-lettering.
- A coalescing publisher for high-throughput, order-preserving writes.
- Stream limits, per-message TTLs, delayed delivery and priorities.
- Core (non-persistent) publish/subscribe, queue groups and request-reply.
- Key-value buckets with revisions, compare-and-set, history and watches.
- Automatic reconnection that re-establishes subscriptions.

## Install

```bash
go get github.com/alternayte/exspeed/sdks/go
```

```go
import exspeed "github.com/alternayte/exspeed/sdks/go"
```

Releases of this module are tagged `sdks/go/vX.Y.Z` (the Go convention for a
module in a subdirectory), so `go get github.com/alternayte/exspeed/sdks/go@v0.7.0`
resolves the tag `sdks/go/v0.7.0`.

## Quick start

```go
ctx := context.Background()

client, err := exspeed.Connect(ctx, "127.0.0.1:5933")
if err != nil {
	log.Fatal(err)
}
defer client.Close()

// Idempotent: succeeds when the stream exists with the same settings.
err = client.CreateStream(ctx, exspeed.StreamSpec{Name: "orders", MaxAge: 7 * 24 * time.Hour})

res, err := client.Publish(ctx, "orders", exspeed.PublishRecord{
	Subject: "orders.placed",
	Value:   []byte(`{"id":42,"total":99.5}`),
	Key:     []byte("customer-7"),
})
fmt.Println("offset", res.Offset)

_, err = client.CreateConsumer(ctx, exspeed.ConsumerSpec{
	Name:           "billing",
	Stream:         "orders",
	FilterSubjects: []string{"orders.placed"},
})

sub, err := client.Subscribe(ctx, "billing", exspeed.SubscribeOptions{Window: 256})
for msg := range sub.Messages() {
	var order struct{ ID int }
	if err := msg.JSON(&order); err != nil {
		msg.Term(ctx, "bad JSON")
		continue
	}
	charge(order)
	msg.Ack()
}
```

## Connecting

```go
client, err := exspeed.Connect(ctx, "exspeed.internal:5933",
	exspeed.WithToken(os.Getenv("EXSPEED_TOKEN")),
	exspeed.WithClientID("billing-service"),
	exspeed.WithRequestTimeout(10*time.Second),
)
```

The address is `host:port`; the port defaults to 5933 and `""` means
`127.0.0.1:5933`. The context bounds connecting only. The first connection
attempt is not retried: `Connect` fails at once with a `*ConnectionError`, or
a `*ServerError` 401 when the token is refused.

| Option | Default | |
|--------|---------|---|
| `WithToken(token)` | none | Bearer token |
| `WithTLS(cfg)` | off | TLS; `nil` verifies against the system roots |
| `WithClientID(id)` | `"exspeed-go"` | Shown in server logs |
| `WithRequestTimeout(d)` | 30s | Per request, on top of a pull's or read's own wait. Also bounds connecting. `0` disables. |
| `WithKeepalive(d)` | 20s | Ping interval; `0` disables pings |
| `WithReconnect(policy)` / `WithoutReconnect()` | on | See [Reconnection](#reconnection) |
| `WithServers(addrs...)` | none | Cluster seeds; connects to the leader (see [Clusters](#clusters)) |
| `WithDisconnectHandler`, `WithReconnectHandler`, `WithCloseHandler`, `WithErrorHandler` | none | Event callbacks |

`client.ServerInfo()` returns the handshake info (`ServerVersion`, `NodeID`,
`Leader`), `client.Connected()` reports whether a connection is up, and
`client.Close()` closes the client: queued acks are sent first, pending
requests fail with a `*ConnectionError` wrapping `ErrClosed`, subscriptions
end, and the client's goroutines exit before `Close` returns.

## Streams

```go
err := client.CreateStream(ctx, exspeed.StreamSpec{
	Name:            "orders",
	MaxAge:          24 * time.Hour,  // retention by age (whole seconds)
	MaxBytes:        10 << 30,        // retention by size
	DedupWindow:     5 * time.Minute, // how long msg ids are remembered
	DedupMaxEntries: 1_000_000,
	Compaction:      false,           // keep only the latest record per key
})
err = client.UpdateStream(ctx, exspeed.StreamSpec{Name: "orders", MaxAge: 72 * time.Hour})
info, err := client.StreamInfo(ctx, "orders") // Name, EarliestOffset, NextOffset, Records, Config, Internal
list, err := client.ListStreams(ctx)
err = client.DeleteStream(ctx, "orders")      // 409 while consumers exist
```

Zero values mean "server default". `CreateStream` is idempotent when the
stream exists with the same settings, and fails with 409 when they differ.
`UpdateStream` replaces all settings, so pass every field you want to keep.

### Limits, lifetimes and retention

```go
err := client.CreateStream(ctx, exspeed.StreamSpec{
	Name:              "jobs",
	MaxMsgs:           100_000,                    // most records the stream holds (0 = no limit)
	Discard:           exspeed.DiscardOld,         // at MaxMsgs: DiscardOld drops the oldest, DiscardNew rejects new records (429)
	MaxMsgsPerSubject: 10,                         // most records kept per subject
	AllowMsgTTL:       true,                       // accept the per-record TTL option
	MsgTTL:            24 * time.Hour,             // default lifetime of every record (0 = none)
	AllowDelayed:      true,                       // accept the Delay / DeliverAt options
	Retention:         exspeed.RetentionWorkQueue, // RetentionLimits (default), RetentionWorkQueue, RetentionInterest
	CaptureSubjects:   []string{"jobs.>"},         // core messages published to these subjects are also stored here
})
```

With `RetentionWorkQueue` the stream has at most one consumer, and a record
is removed once that consumer acked it. With `RetentionInterest` a record is
removed once every consumer of the stream acked it. Expired records are never
read or delivered. With `CaptureSubjects`, core messages (see
[Core publish/subscribe](#core-publishsubscribe)) published to a matching
subject are also appended to the stream; no two streams may capture
overlapping subjects. These settings show up in `StreamInfo(...).Config`.

## Publishing

```go
res, err := client.Publish(ctx, "orders", exspeed.PublishRecord{
	Subject: "orders.placed",
	Value:   payload,                          // []byte, sent as-is
	Key:     []byte("customer-7"),             // nil = no key
	Headers: exspeed.Headers("trace-id", "abc"),
	MsgID:   exspeed.NewMsgID(),               // idempotency key
})
// res.Offset, res.Duplicate

results, err := client.PublishBatch(ctx, "orders", []exspeed.PublishRecord{
	{Subject: "orders.placed", Value: []byte(`{"id":2}`)},
	{Subject: "orders.placed", Value: []byte(`{"id":3}`)},
})
```

`PublishBatch` sends several records in one request and returns one result
per record, in order.

### TTLs, delays and priorities

```go
client.Publish(ctx, "jobs", exspeed.PublishRecord{Subject: "jobs.email", Value: job, TTL: 30 * time.Second})
client.Publish(ctx, "jobs", exspeed.PublishRecord{Subject: "jobs.email", Value: job, Delay: 10 * time.Second})
client.Publish(ctx, "jobs", exspeed.PublishRecord{Subject: "jobs.email", Value: job, DeliverAt: time.Date(2026, 12, 24, 18, 0, 0, 0, time.UTC)})
client.Publish(ctx, "jobs", exspeed.PublishRecord{Subject: "jobs.email", Value: job, Priority: 9}) // 0 (default) to 9
```

These options are headers on the record: `exspeed-ttl` and `exspeed-delay`
(whole milliseconds, as `"<n>ms"`), `exspeed-deliver-at` (ms since the epoch)
and `exspeed-priority`. They work the same in `PublishBatch` and the
publisher. A TTL needs a stream with `AllowMsgTTL`, and `Delay` / `DeliverAt`
need `AllowDelayed`; otherwise the publish fails with 400. A delay holds a
record back from consumers only; stateless reads see it at once. Priorities
take effect for consumers with a `PriorityWindow`.

**Idempotency.** When a record carries a `MsgID`, the server remembers it for
the stream's dedup window. A retry with the same `MsgID` and the same body
returns the original offset with `Duplicate` set and writes nothing. Reusing
a `MsgID` with a different body fails with 409 (`err.StoredOffset()`).
`NewMsgID()` generates a time-ordered UUIDv7.

### The coalescing publisher

For throughput, use a publisher. It gathers concurrent publishes into
`PublishBatch` requests and keeps many batches in flight. Records reach the
stream in the order you called `Publish` / `PublishAsync`, and each call gets
its own record's result.

```go
p := client.NewPublisher(exspeed.PublisherOptions{
	BatchWindow:     0,    // 0 = send whatever is queued as soon as the publisher runs
	MaxBatchRecords: 512,  // records per request
	MaxInFlight:     4096, // accepted but unacknowledged records; publishing waits beyond this
})

// Fire many, then collect the results.
acks := make([]*exspeed.PubAck, 0, len(events))
for _, e := range events {
	ack, err := p.PublishAsync(ctx, "events", exspeed.PublishRecord{Subject: "events.raw", Value: e})
	if err != nil {
		return err
	}
	acks = append(acks, ack)
}
for _, ack := range acks {
	if _, err := ack.Wait(ctx); err != nil {
		return err
	}
}

res, err := p.Publish(ctx, "events", rec) // one record, waiting for its result
err = p.Flush(ctx)                        // wait for everything accepted so far
err = p.Close(ctx)                        // flush, then reject further publishes
```

If a batch fails, every record in it fails with the same error. The
publisher's goroutine runs only while records are queued.

## Reading without a consumer

```go
page, err := client.Read(ctx, "orders", exspeed.ReadOptions{
	From:       0,             // first offset
	MaxRecords: 100,
	Filter:     "orders.eu.>", // NATS-style: * = one token, > = one or more
	Wait:       5 * time.Second, // long-poll when caught up
})
for _, r := range page.Records {
	fmt.Println(r.Offset, r.Subject, r.Text(), r.Time)
}
// continue with ReadOptions{From: page.NextOffset}
```

Stateless reads keep no server-side state. Use them for replay, tools and
tailing.

## Consumers

A consumer is a named, durable cursor over one stream, kept on the server.
It tracks what has been delivered, what is acknowledged and what is due for
redelivery.

```go
info, err := client.CreateConsumer(ctx, exspeed.ConsumerSpec{
	Name:              "billing",
	Stream:            "orders",
	FilterSubjects:    []string{"orders.placed", "orders.eu.>"}, // default: all subjects
	Deliver:           exspeed.DeliverAll(),     // DeliverNew(), DeliverFromOffset(n), DeliverFromTime(t)
	Ack:               exspeed.AckExplicit,      // or AckNone (at most once)
	AckWait:           30 * time.Second,         // redeliver if not acked in time
	MaxDeliver:        5,                        // then dead-letter (-1 = retry forever)
	Backoff:           []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}, // by attempt; the last repeats
	MaxAckPending:     1000,                     // pause delivery at this many unacked records (-1 = no limit)
	DLQStream:         "orders-dlq",             // where dead letters go ("" = dropped and counted)
	Ephemeral:         false,                    // true = deleted when this connection closes
	DeadLetterExpired: false,                    // true = records whose TTL ends before the ack go to DLQStream
	FilterHeaders:     map[string]string{"region": "eu"}, // only records with these header values
	HeaderMatch:       exspeed.HeaderMatchAll,   // or HeaderMatchAny
	SingleActive:      false,                    // true = one subscription at a time gets records
	PriorityWindow:    0,                        // look this many records ahead and deliver higher priorities first
})
```

Every field except `Name` and `Stream` is optional; zero values take the
server's defaults (for `MaxDeliver` and `MaxAckPending`, `-1` means
unlimited). `CreateConsumer` is idempotent for an identical spec and fails
with 409 if the consumer exists with a different one. `info.Spec` is the
server's resolved spec and can be passed back to `CreateConsumer` unchanged.
Also available: `ConsumerInfo(ctx, name)`, `ListConsumers(ctx, stream)`
(`""` = every stream), `DeleteConsumer(ctx, name)` and `Seek`:

```go
client.Seek(ctx, "billing", exspeed.SeekEarliest())
client.Seek(ctx, "billing", exspeed.SeekLatest())
client.Seek(ctx, "billing", exspeed.SeekOffset(1000))
client.Seek(ctx, "billing", exspeed.SeekTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)))
```

`ConsumerInfo` reports the consumer's position and counters: `NextOffset`,
`AckFloor`, `NumUnacked`, `NumInFlight`, `NumDelayed` (records waiting for
their delay or deliver-at time), `NumWaiting`, `Lag`, `Subscribers`,
`PullWaiters` and `Stats`.

A `SingleActive` consumer refuses pulls (400), and its subscribers form a
failover group: the oldest live subscription gets every record.

### Push: subscriptions

```go
sub, err := client.Subscribe(ctx, "billing", exspeed.SubscribeOptions{Window: 256})

// Either take messages one at a time...
for {
	msg, err := sub.Next(ctx)
	if errors.Is(err, exspeed.ErrSubscriptionEnded) {
		break
	} else if err != nil {
		return err // ctx done
	}
	// msg.Offset, msg.Time, msg.DeliveryCount, msg.Subject, msg.Key, msg.Value,
	// msg.Headers, msg.Text(), msg.JSON(&v), msg.Header(name)
	msg.Ack()
}

// ...or range over a channel, closed when the subscription ends.
for msg := range sub.Messages() {
	msg.Ack()
}
fmt.Println(sub.EndReason()) // *SubscriptionEndedError{Code, Message}
```

The server pushes at most `Window` records ahead of your code. The client
returns credit as you take messages (in batches of half the window), so a
slow handler slows delivery down instead of filling memory. Use either `Next`
or `Messages`, not both.

A subscription ends when:

- you call `sub.Unsubscribe(ctx)` (code 0);
- the consumer or its stream is deleted (404);
- the node loses leadership (503, see [Errors](#errors));
- the client closes (0), or the connection is lost and not re-established
  (503).

After a server-side end, `Next` and `Messages` still yield the records the
subscription had buffered, then report the end. When a subscription ends,
records delivered to it and not acked are redelivered, including any it had
buffered that your code never saw.

### Pull

```go
msgs, err := client.Pull(ctx, "billing", exspeed.PullOptions{MaxMessages: 100, Expires: 5 * time.Second})
for _, m := range msgs {
	handle(m)
	m.Ack()
}
```

`Pull` long-polls for up to `Expires` and returns whatever is available, or
an empty slice on timeout (`NoWait: true` returns at once). It suits batch
jobs and request-driven work. Push suits steady streams.

### Work sharing

Any number of subscriptions and pullers, on any connection and in any
process, can share one consumer. Each record goes to exactly one of them at a
time, so to scale out, run more instances against the same consumer name.
For fan-out, where every reader sees every record, give each reader its own
consumer.

### Acks, redelivery and dead letters

| Call | Effect |
|------|--------|
| `msg.Ack()` | Done. Fire-and-forget: acks made close together share one frame. |
| `msg.AckSync(ctx)` | Ack and wait for the server to confirm. |
| `msg.Nack(ctx, delay)` | Redeliver after `delay`, or after the consumer's `Backoff` when 0. |
| `msg.Term(ctx, reason)` | Never redeliver: dead-letter now. |
| `msg.InProgress(ctx)` | Still working: reset the ack deadline. |
| `client.Ack(ctx, consumer, offsets...)` | Ack several offsets and wait for confirmation. |

Queued acks go out before any later request on the client, whenever a
subscription runs out of buffered messages, and otherwise within a few
milliseconds. An unacked record is redelivered when its `AckWait` deadline
passes, when it is nacked, or when the subscription it went to ends or
disconnects. `DeliveryCount` is 1 on the first delivery and goes up on each
redelivery. After `MaxDeliver` deliveries, or on `Term`, the record goes to
`DLQStream` with the headers `exspeed-dlq-origin`, `exspeed-dlq-stream`,
`exspeed-dlq-original-offset`, `exspeed-dlq-deliveries`, `exspeed-dlq-cause`,
`exspeed-dlq-reason` and `exspeed-dlq-time`.

Delivery is **at least once**, so make handlers idempotent. Because
`msg.Ack()` doesn't wait, an ack the server rejects goes to the
`WithErrorHandler` callback. An ack made while the connection is down is
dropped, and the record is redelivered.

## Core publish/subscribe

Core messages go to the subscriptions live at the moment they are published.
Nothing is stored, nothing is acked, and delivery is at most once. Use them
for notifications, cache invalidation and request-reply; use streams when a
message must not be lost.

```go
sub, err := client.SubscribeCore(ctx, "orders.>") // NATS-style filter
err = client.PublishCore(ctx, "orders.eu.created", []byte(`{"id":1}`), exspeed.Headers("trace-id", "t1")...)

for m := range sub.Messages() {
	// m.Subject, m.ReplyTo, m.Headers, m.Value, m.Text(), m.JSON(&v), m.Header(name)
	fmt.Println(m.Subject, m.Text())
}

// A queue group: each message goes to one member of the group.
worker, err := client.QueueSubscribeCore(ctx, "jobs.resize", "resizers")
```

`PublishCore` returns once the server has accepted the message. A core
subscription ends on `Unsubscribe`, when the client closes, or when the
server ends it (`EndReason`, code 503 when leadership moves to another
node). After a reconnect the client subscribes again with the same subject
and queue group; messages published while it was disconnected are missed.

### Request-reply

```go
// The service: answer each request with m.Respond.
requests, err := client.QueueSubscribeCore(ctx, "svc.upper", "svc")
go func() {
	for m := range requests.Messages() {
		m.Respond(ctx, []byte(strings.ToUpper(m.Text())))
	}
}()

// The caller: the first response, as a core message.
rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
reply, err := client.Request(rctx, "svc.upper", []byte("hello"))
// reply.Text() == "HELLO"
```

`Request` publishes with a reply subject and waits for the first response.
It fails at once with 404 (`ErrNotFound`) when nobody is subscribed to the
subject ("no responders"), and with a `*TimeoutError` when no response
arrives before the context's deadline or the client's request timeout. All
requests on a client share one inbox subscription, `_INBOX.<random>.*`,
which the first request sets up. When the connection drops, requests waiting
for a response fail with a `*ConnectionError`, and the next request after the
reconnect sets up a new inbox. `PublishCoreWithReply` publishes with an
explicit reply subject. Request-reply needs no extra permissions: anyone may
publish a reply to an `_INBOX.…` subject, and a client may subscribe to its
own inbox (see [Security](../../docs/security.md)).

## Key-value buckets

A bucket is a stream (`KV_<bucket>`) that keeps the latest values of each
key. Keys are subjects, so they are dot-separated tokens such as `app.mode`.

```go
kv := client.KV("config")
err := kv.Create(ctx, exspeed.KVBucketOptions{History: 5}) // idempotent; TTL and MaxBytes too

rev, err := kv.Put(ctx, "app.mode", []byte("prod"))         // the new revision
entry, err := kv.Get(ctx, "app.mode")                       // *KVEntry, or nil when absent or deleted
// entry.Key, entry.Value, entry.Text(), entry.JSON(&v), entry.Revision, entry.Op, entry.Time

_, err = kv.CreateKey(ctx, "app.port", []byte("8080"))      // only if absent: 409 otherwise
_, err = kv.Update(ctx, "app.mode", []byte("dev"), rev)     // compare-and-set: 409 unless still at rev
_, err = kv.PutWith(ctx, "session.abc", token, exspeed.KVPutOptions{TTL: time.Minute})
old, err := kv.GetRevision(ctx, "app.mode", rev)            // an older value, while history keeps it
history, err := kv.History(ctx, "app.mode")                 // kept revisions, oldest first, deletes included
keys, err := kv.Keys(ctx, "app.*")                          // keys with a value, sorted ("" = all)
_, err = kv.Delete(ctx, "app.port")                         // a tombstone; history stays
_, err = kv.Purge(ctx, "app.port")                          // a tombstone that also hides older values
_, err = kv.DeleteWith(ctx, "app.port", exspeed.KVDeleteOptions{ExpectedRevision: &rev})
err = kv.Destroy(ctx)                                       // delete the bucket and everything in it
```

A revision is the position of the write in the bucket's stream, plus one, so
revisions only grow; 0 means "absent", which is what `CreateKey` checks.
`History` is how many values each key keeps (1 to 64, default 1). A failed
compare-and-set is a `*ServerError` 409 whose `CurrentRevision()` is the
key's revision. `Get` on a missing key returns `nil, nil`; on a missing
bucket it fails with 404.

### Watching

```go
w := kv.Watch("app.*") // "" = every key
defer w.Stop()
for {
	e, err := w.Next(ctx)
	if err != nil {
		return err
	}
	if e.Op == exspeed.KVPut {
		apply(e.Key, e.Value)
	} else {
		remove(e.Key) // KVDelete or KVPurge
	}
}
```

A watch first yields the current value of every matching key (deleted keys
left out), ordered by revision, then every change as it happens, deletes
included. It reads the bucket's stream with stateless long-poll reads, so it
holds no state on the server and resumes where it was after a reconnect.
`Next` returns the context's error when the context is done first (a read in
progress keeps running, and its entries are kept for the next call); a
failed read, such as a `*ConnectionError` while the connection is down, is
returned once. After `Stop`, `Next` returns `ErrWatchStopped`.

## Queries and metadata

```go
r, err := client.Query(ctx, `SELECT COUNT(*) AS n FROM "orders"`)
// r.Columns ["n"], r.Rows [[123]] (numbers as json.Number), r.RowCount, r.ExecutionTimeMs, r.Truncated

md, err := client.Metadata(ctx)  // NodeID, IsLeader, Leader, ServerVersion
rtt, err := client.Ping(ctx)     // round-trip time
```

With auth enabled, `Query` needs a global-admin credential, since SQL can
read any stream.

## Errors

| Type | When |
|------|------|
| `*ServerError` | The server rejected the request. It has `Code`, `Message` and `Detail` (raw JSON), plus `LeaderHint()`, `StoredOffset()`, `CurrentRevision()`, `RetryAfter()` and `DecodeDetail(&v)`. |
| `*ConnectionError` | Not connected: the connection was lost, is being re-established, or the client is closed (then it wraps `ErrClosed`). |
| `*TimeoutError` | No response within the request timeout (plus a pull's or read's own wait), or a core request got no answer. It matches `context.DeadlineExceeded`. |
| `*ProtocolError` | The server sent something this client can't decode. |
| `*SubscriptionEndedError` | Returned by `Next` once a subscription has ended; matches `ErrSubscriptionEnded`. |

When the caller's context is done first, calls return the context's error.
Arguments that can't be sent (a priority above 9, a subject over 65,535
bytes) give an error wrapping `ErrInvalidArgument`.

Compare server codes with `errors.Is` against the sentinels, or read
`Code` with `errors.As`:

```go
_, err := client.Publish(ctx, "orders", rec)
switch {
case errors.Is(err, exspeed.ErrConflict):
	// 409
case errors.Is(err, exspeed.ErrUnavailable):
	var se *exspeed.ServerError
	errors.As(err, &se)
	log.Printf("not the leader; the leader is %s", se.LeaderHint())
}
```

| Code | Sentinel | Meaning | Detail |
|------|----------|---------|--------|
| 400 | `ErrBadRequest` | Malformed request, invalid name, filter or config | |
| 401 | `ErrUnauthorized` | Not authenticated | |
| 403 | `ErrForbidden` | The credential lacks the needed permission | |
| 404 | `ErrNotFound` | Stream, consumer, bucket or key not found; a request with no responders | |
| 408 | | A query timed out | |
| 409 | `ErrConflict` | Exists with different settings; stream still has consumers; msg id reused with a different body; a KV key not at the expected revision | `stored_offset`, `consumers`, `current_revision` |
| 422 | | A query exceeded the server's query memory limit | |
| 429 | `ErrTooManyRequests` | Retry later (dedup map full, stream full with `DiscardNew`, too many concurrent waits on one connection) | `retry_after_secs` |
| 500 | `ErrInternal` | Internal error | |
| 503 | `ErrUnavailable` | Not the leader, still starting, or too few in-sync replicas | `leader`, `in_sync`, `required` |
| 507 | `ErrInsufficientStorage` | The server's disk is full; nothing was written | |

A failed request isn't retried against the leader automatically; the client
follows leader hints only when it connects or reconnects (see
[Clusters](#clusters)).

## Reconnection

Reconnection is on by default. When the connection drops:

1. Pending requests fail with a `*ConnectionError`. They are not retried,
   because a publish may or may not have been applied; retry publishes with
   a `MsgID` to make the retry safe. New requests also fail with a
   `*ConnectionError` until the connection is back.
2. The disconnect handler runs, and the client reconnects with exponential
   backoff (100ms doubling to 5s, with jitter, unlimited attempts by
   default).
3. Once reconnected, it re-creates the ephemeral consumers it created (the
   server deleted them with the old connection), then re-subscribes every
   live subscription with its original window. Your loop over `Next` or
   `Messages` keeps running. Records that were buffered but not yet taken are
   discarded, and the server redelivers them, along with anything delivered
   but not acked. A re-subscribe that fails (for example, the consumer was
   deleted meanwhile) ends that subscription with the server's error code.
   Core subscriptions are subscribed again too; core messages published
   during the gap are missed.
4. The reconnect handler runs.

If the server rejects the credential (401/403) or `MaxAttempts` runs out, the
client closes: subscriptions end with code 503 and the close handler runs.

```go
client, err := exspeed.Connect(ctx, "exspeed.internal:5933",
	exspeed.WithReconnect(exspeed.ReconnectPolicy{MaxAttempts: 30, InitialDelay: 200 * time.Millisecond, MaxDelay: 10 * time.Second}),
	// exspeed.WithoutReconnect(): the client closes when the connection drops
	exspeed.WithDisconnectHandler(func(err error) { log.Println("exspeed disconnected:", err) }),
	exspeed.WithReconnectHandler(func(info exspeed.ServerInfo) { log.Println("exspeed reconnected to", info.NodeID) }),
	exspeed.WithCloseHandler(func(err error) { log.Println("exspeed closed:", err) }),
	exspeed.WithErrorHandler(func(err error) { log.Println("exspeed async error:", err) }), // failed acks/credits
)
```

Handlers run in order on a goroutine of their own, so a slow handler never
stalls the connection. Durable consumers make reconnection safe: the cursor
and the unacked set live on the server, so nothing is lost and nothing is
skipped. An ephemeral consumer created with `DeliverNew()` does skip records
published while it was gone, because the re-created consumer starts at the
end of the stream.

The client pings every keepalive interval (20s) so the server, which drops
connections idle for 120s, keeps the connection open. A ping that times out
is treated as a dead connection.

## Clusters

Against a cluster ([High availability](../../docs/high-availability.md)),
give the client some seed addresses. It connects to whichever node is the
leader, following the leader hints followers return. After a failover it
reconnects to the new leader and restores subscriptions as described above.

```go
client, err := exspeed.Connect(ctx, "", exspeed.WithServers(
	"exspeed-0.exspeed:5933", "exspeed-1.exspeed:5933", "exspeed-2.exspeed:5933",
))
```

Without seeds, a node that names another node as leader in its handshake is
still followed, so connecting through a Service that routes to any node works
too. A write that reaches a follower anyway fails with 503, and
`LeaderHint()` names the leader.

## TLS and authentication

```go
client, err := exspeed.Connect(ctx, "exspeed.example.com:5933",
	exspeed.WithToken(os.Getenv("EXSPEED_TOKEN")), // the server's --auth-token, or a credential token
	exspeed.WithTLS(nil),                          // verify against the system roots
)

// A private CA:
cfg, err := exspeed.LoadTLSConfig("ca.pem", "", "")
client, err = exspeed.Connect(ctx, "exspeed.internal:5933", exspeed.WithTLS(cfg))
```

`WithTLS` takes any `*tls.Config`; `ServerName` defaults to the host being
dialed. Certificates are verified. A wrong or missing token fails `Connect`
with 401. A token without permission for an operation gets 403. With scoped
credentials, `ListStreams` and `ListConsumers` return only what the
credential can see. See [Security](../../docs/security.md).

### Client certificates (mutual TLS)

When the server runs with `tls.client_ca`, it accepts only clients that
present a certificate signed by that CA:

```go
cfg, err := exspeed.LoadTLSConfig("ca.pem", "orders-client.pem", "orders-client.key")
client, err := exspeed.Connect(ctx, "exspeed.example.com:5933", exspeed.WithTLS(cfg))
// no token: the credential bound to the certificate's name (cert_cn) applies
```

Without a valid client certificate the TLS handshake fails and `Connect`
returns a `*ConnectionError`. With auth on, a client with a certificate and
no token gets the permissions of the credential whose `cert_cn` matches the
certificate's common name (or its first DNS name), and `Connect` fails with
401 when no credential names it. A token, when given, takes precedence.

## Development

```bash
cd sdks/go
go vet ./...
test -z "$(gofmt -l .)"
go test -race -count=1 ./...
```

The unit tests check the codec byte for byte against fixtures generated with
the Rust encoder, and drive the client against a scriptable fake server.
The end-to-end tests (`e2e_*_test.go`) start real servers. They use the
binary named by `EXSPEED_BIN`, or else `target/debug/exspeed` at the
repository root (`cargo build -p exspeed --bin exspeed`). Without one, they
are skipped with a message.

## License

MIT, see [LICENSE](LICENSE).
