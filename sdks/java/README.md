# exspeed-client (Java)

Java client for [Exspeed](https://github.com/alternayte/exspeed). It speaks
the binary client protocol v2 ([`docs/protocol.md`](../../docs/protocol.md))
over TCP or TLS. Requires Java 17 or later and has no runtime dependencies.

- One client is one connection. Requests are multiplexed, so a long pull
  never blocks other calls. Share one client across your application; it is
  thread-safe.
- Every operation has a blocking form (`publish`) and an asynchronous form
  (`publishAsync`) that returns a `CompletableFuture`.
- Durable consumers with push (credit-windowed subscriptions) or pull
  delivery, acks, redelivery with backoff, and dead-lettering.
- A coalescing publisher for high-throughput, order-preserving writes.
- Stream limits, per-message TTLs, delayed delivery and priorities.
- Core (non-persistent) publish/subscribe, queue groups and request-reply.
- Key-value buckets with revisions, compare-and-set, history and watches.
- Automatic reconnection that re-establishes subscriptions.

## Install

Maven:

```xml
<dependency>
  <groupId>io.github.alternayte</groupId>
  <artifactId>exspeed-client</artifactId>
  <version>0.7.0</version>
</dependency>
```

Gradle:

```groovy
implementation 'io.github.alternayte:exspeed-client:0.7.0'
```

The package is `io.exspeed.client`; the jar's automatic module name is
`io.exspeed.client`.

## Quick start

```java
import io.exspeed.client.*;

try (ExspeedClient client = ExspeedClient.connect("127.0.0.1", 5933)) {
  client.createStream(StreamSpec.builder("orders").maxAgeSecs(7 * 86400).build()); // idempotent

  PublishResult r = client.publish("orders", PublishRecord.builder("orders.placed")
      .value("{\"id\":42,\"total\":99.5}")
      .key("customer-7")
      .build());

  client.createConsumer(ConsumerSpec.builder("billing", "orders").filterSubjects("orders.placed").build());

  try (Subscription sub = client.subscribe("billing", 256)) {
    for (Message msg : sub) {          // blocks; ends when the subscription ends
      charge(msg.text());
      msg.ack();
    }
  }
}
```

## Blocking and asynchronous calls

Each operation comes in two forms:

```java
StreamInfo info = client.streamInfo("orders");                    // blocks, throws ExspeedException
CompletableFuture<StreamInfo> f = client.streamInfoAsync("orders"); // completes later
```

The blocking form throws the client exception itself (`ServerException`,
`ConnectionException`, ...). The future of the asynchronous form completes
exceptionally with the same exception. Futures complete on the client's
callback executor (by default the `CompletableFuture` async pool), never on
the connection's I/O threads, so a continuation may call blocking methods.
Pass your own with `ClientOptions.Builder.callbackExecutor(...)`.

Each connection uses one reader thread and one writer thread; the client
adds one scheduler thread (timeouts, keepalive pings, batching), and a
short-lived thread while it reconnects. They are daemon threads and stop
when the client closes.

## Streams

```java
client.createStream(StreamSpec.builder("orders")
    .maxAgeSecs(86400)          // retention by age
    .maxBytes(10L << 30)        // retention by size
    .dedupWindowSecs(300)       // how long msg ids are remembered
    .dedupMaxEntries(1_000_000)
    .compaction(false)
    .build());
client.updateStream(StreamSpec.builder("orders").maxAgeSecs(3 * 86400).build());
StreamInfo info = client.streamInfo("orders"); // name, earliestOffset, nextOffset, records, config, internal
List<StreamInfo> all = client.listStreams();
client.deleteStream("orders");                 // 409 while consumers exist
```

Unset numeric settings mean "server default". `createStream` is idempotent
when the stream already exists with the same settings, and fails with 409
when they differ. `updateStream` replaces all settings, so set every field
you want to keep.

### Limits, lifetimes and retention

```java
client.createStream(StreamSpec.builder("jobs")
    .maxMsgs(100_000)                     // most records the stream holds (0 = no limit)
    .discard(DiscardPolicy.OLD)           // at maxMsgs: OLD drops the oldest, NEW rejects new records (429)
    .maxMsgsPerSubject(10)                // most records kept per subject
    .allowMsgTtl(true)                    // accept the per-record ttl option
    .msgTtlMs(86_400_000)                 // default lifetime of every record (0 = none)
    .allowDelayed(true)                   // accept the delay / deliverAt options
    .retention(RetentionPolicy.WORK_QUEUE) // LIMITS (default), WORK_QUEUE or INTEREST
    .build());
```

With `WORK_QUEUE` retention the stream has at most one consumer per set of
subjects, and a record is removed once it is acked. With `INTEREST` a record
is removed once every consumer of the stream acked it. Expired records are
never read or delivered. The settings show up in `streamInfo(name).config()`.

### Capturing core messages

```java
client.createStream(StreamSpec.builder("audit").captureSubjects("orders.>").build());
client.publishCore("orders.created", "{\"id\":1}"); // also appended to "audit"
```

Core messages published to subjects matching `captureSubjects` are also
appended to the stream. No two streams may capture overlapping subjects.

## Publishing

```java
// One record. The value is bytes, UTF-8 text, or JSON via jsonValue(...).
client.publish("orders", PublishRecord.builder("orders.placed")
    .jsonValue(Map.of("id", 1))
    .key("customer-7")
    .header("trace-id", "abc")
    .msgId(MsgId.newMsgId()) // idempotency key
    .build());

client.publish("orders", "orders.placed", "{\"id\":2}"); // shorthand

// Several records in one request; one result per record.
List<PublishResult> results = client.publishBatch("orders", List.of(
    PublishRecord.of("orders.placed", "{\"id\":3}"),
    PublishRecord.of("orders.placed", "{\"id\":4}")));
```

Each publish returns a `PublishResult(offset, duplicate)`.

### TTLs, delays and priorities

```java
PublishRecord.builder("jobs.email").value(job).ttl(Duration.ofSeconds(30)).build();   // expire after 30 s
PublishRecord.builder("jobs.email").value(job).ttl("5m").build();                     // units ms, s, m, h, d
PublishRecord.builder("jobs.email").value(job).delay(Duration.ofSeconds(10)).build(); // deliver in 10 s
PublishRecord.builder("jobs.email").value(job).deliverAt(Instant.parse("2026-12-24T18:00:00Z")).build();
PublishRecord.builder("jobs.email").value(job).priority(9).build();                    // 0 (default) to 9
```

These options are headers on the record: `exspeed-ttl`, `exspeed-delay`,
`exspeed-deliver-at` (ms since the epoch) and `exspeed-priority`, appended
after your own headers. A TTL needs a stream with `allowMsgTtl`, and a delay
or delivery time needs `allowDelayed`; otherwise the publish fails with 400.
A delay holds a record back from consumers only; stateless reads see it at
once. Priorities take effect for consumers with a `priorityWindow`.

**Idempotency.** When a record carries a msg id, the server remembers it for
the stream's dedup window. A retry with the same msg id and the same body
returns the original offset with `duplicate() == true` and writes nothing.
Reusing a msg id with a different body fails with 409
(`detail("stored_offset")`). `MsgId.newMsgId()` generates a time-ordered
UUIDv7.

### The coalescing publisher

For throughput, use a publisher. It gathers concurrent `publishAsync` calls
into `PublishBatch` requests and keeps many batches in flight. Records reach
the stream in the order you called `publishAsync`, and each future completes
with its own record's offset.

```java
PublisherOptions opts = PublisherOptions.DEFAULT
    .withBatchWindow(Duration.ZERO) // ZERO = send whatever has queued up right away
    .withMaxBatchRecords(512)       // records per request
    .withMaxInFlight(4096);         // unacknowledged records; publishAsync blocks beyond this
try (Publisher publisher = client.publisher(opts)) {
  List<CompletableFuture<PublishResult>> results = new ArrayList<>();
  for (String e : events) {
    results.add(publisher.publishAsync("events", PublishRecord.of("events.raw", e)));
  }
  publisher.flush(); // wait for everything accepted so far
}                    // close() flushes, then rejects further publishes
```

If a batch fails, every record in it fails with the same exception.

## Reading without a consumer

```java
ReadResult page = client.read("orders", ReadOptions.builder()
    .from(0)                       // first offset
    .maxRecords(100)
    .filter("orders.eu.>")         // NATS-style: * = one token, > = one or more
    .waitTime(Duration.ofSeconds(5)) // long-poll when caught up
    .build());
for (StreamRecord r : page.records()) {
  System.out.println(r.offset() + " " + r.subject() + " " + r.text());
}
// continue with ReadOptions.from(page.nextOffset())
```

Stateless reads keep no server-side state. Use them for replay, tools and
tailing. A `StreamRecord` has `offset()`, `timestamp()` (an `Instant`),
`timestampNs()`, `subject()`, `key()`, `value()`, `text()`, `json()`,
`headers()` and `header(name)`.

## Consumers

A consumer is a named, durable cursor over one stream, kept on the server.
It tracks what has been delivered, what is acknowledged and what is due for
redelivery.

```java
client.createConsumer(ConsumerSpec.builder("billing", "orders")
    .filterSubjects("orders.placed", "orders.eu.>")   // default: all subjects
    .deliver(DeliverPolicy.ALL)                        // ALL, NEW, fromOffset(n), fromTime(instant)
    .ack(AckPolicy.EXPLICIT)                           // or NONE (at-most-once)
    .ackWait(Duration.ofSeconds(30))                   // redeliver if not acked in time
    .maxDeliver(5)                                     // then dead-letter (0 = retry forever)
    .backoff(Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(30)) // by attempt; the last repeats
    .maxAckPending(1000)                               // pause delivery at this many unacked records
    .dlqStream("orders-dlq")                           // where dead letters go (unset = dropped and counted)
    .ephemeral(false)                                  // true = deleted when this connection closes
    .deadLetterExpired(false)                          // true = records whose TTL ends before the ack go to the DLQ
    .filterHeader("region", "eu")                      // only records with these header values
    .headerMatch(HeaderMatch.ALL)                      // ALL filter headers must match, or ANY
    .singleActive(false)                               // true = one subscription at a time gets records
    .priorityWindow(0)                                 // look this many records ahead for higher priorities
    .build());
```

Every setting except the name and stream is optional and defaults on the
server. `createConsumer` is idempotent for an identical spec and fails with
409 if the consumer exists with a different one. Also available:
`consumerInfo(name)`, `listConsumers()`, `listConsumers(stream)`,
`deleteConsumer(name)` and `seek(name, target)`:

```java
client.seek("billing", SeekTarget.EARLIEST);
client.seek("billing", SeekTarget.LATEST);
client.seek("billing", SeekTarget.offset(1000));
client.seek("billing", SeekTarget.time(Instant.parse("2026-10-01T00:00:00Z")));
```

`consumerInfo` returns the spec as the server holds it, the position and the
counters: `nextOffset`, `ackFloor`, `numUnacked`, `numInFlight`,
`numDelayed` (records waiting for their delay or delivery time),
`numWaiting`, `lag`, `subscribers`, `pullWaiters` and `stats()`.

A `singleActive` consumer refuses pulls (400), and its subscribers form a
failover group: the oldest live subscription gets every record.

### Push: subscriptions

```java
try (Subscription sub = client.subscribe("billing", 256)) {
  Message msg = sub.next(Duration.ofSeconds(5)); // null on timeout or once ended
  for (Message m : sub) {                         // or iterate (blocking)
    // m.offset(), m.deliveryCount(), m.subject(), m.text(), m.json(), m.header(name), ...
    m.ack();
  }
  System.out.println(sub.endReason());            // EndReason(code, message)
}
```

Or hand the subscription a callback, which runs on a dedicated thread:

```java
Subscription sub = client.subscribe("billing");
CompletableFuture<EndReason> done = sub.listen(m -> {
  handle(m);
  m.ack();
});
```

If the callback throws, the subscription is closed (its unacked records are
redelivered) and `done` completes with the exception.

The server pushes at most `window` records ahead of your code. The client
returns credit as you take messages (in batches of half the window), so a
slow handler slows delivery down instead of filling memory.

A subscription ends when:

- you call `unsubscribe()` or `close()` (`endReason().code() == 0`);
- the consumer or its stream is deleted (`404`);
- the node loses leadership (`503`, see [Errors](#errors));
- the client closes (`0`), or the connection is lost and not re-established
  (`503`).

When a subscription ends, records delivered to it and not acked are
redelivered, including any it had buffered that your code never saw. After
a server-side end, `next()` still returns the buffered messages before it
returns `null`.

### Pull

```java
List<Message> msgs = client.pull("billing", PullOptions.of(100, Duration.ofSeconds(5)));
for (Message m : msgs) {
  handle(m);
  m.ack();
}
```

`pull` long-polls for up to the expiry and returns whatever is available, or
an empty list on timeout. It suits batch jobs and request-driven work. Push
suits steady streams.

### Work sharing

Any number of subscriptions and pullers, on any connection and in any
process, can share one consumer. Each record goes to exactly one of them at
a time, so to scale out, run more instances against the same consumer name.
For fan-out, where every reader sees every record, give each reader its own
consumer.

### Acks, redelivery and dead letters

| Call | Effect |
|------|--------|
| `msg.ack()` | Done. Fire-and-forget: no round trip; acks made back to back share one frame. |
| `msg.nack()` / `msg.nack(delay)` | Redeliver after `delay`, or after the consumer's backoff. |
| `msg.term(reason)` | Never redeliver: dead-letter now. |
| `msg.inProgress()` | Still working: reset the ack deadline. |
| `client.ack(consumer, offsets...)` | Ack several offsets and wait for confirmation. |

`nack`, `term` and `inProgress` wait for the server; each has an `...Async`
form. An unacked record is redelivered when its ack wait passes, when it is
nacked, or when the subscription it went to ends or disconnects.
`deliveryCount()` is 1 on the first delivery and goes up on each
redelivery. After `maxDeliver` deliveries, or on `term`, the record goes to
the DLQ stream with the headers `exspeed-dlq-origin`, `exspeed-dlq-stream`,
`exspeed-dlq-original-offset`, `exspeed-dlq-deliveries`,
`exspeed-dlq-cause` (`max_deliver`, `rejected` or `expired`),
`exspeed-dlq-reason` and `exspeed-dlq-time`.

Delivery is **at-least-once**, so make handlers idempotent. Because
`msg.ack()` doesn't wait, an ack the server rejects reaches
`ClientListener.onError`. An ack made while the connection is down is
dropped, and the record is redelivered.

## Core publish/subscribe

Core messages go to the subscriptions live at the moment they are
published. Nothing is stored, nothing is acked, and delivery is at most
once. Use them for notifications, cache invalidation and request-reply; use
streams when a message must not be lost.

```java
CoreSubscription sub = client.subscribeCore("orders.>");   // NATS-style filter
client.publishCore("orders.eu.created", "{\"id\":1}".getBytes(UTF_8),
    CorePublishOptions.headers(Map.of("trace-id", "t1")));

for (CoreMessage m : sub) {
  // m.subject(), m.replyTo(), m.headers(), m.value(), m.text(), m.json(), m.header(name)
}

// A queue group: each message goes to one member of the group.
CoreSubscription worker = client.subscribeCore("jobs.resize", "resizers");
```

`publishCore` returns once the server has accepted the message. A core
subscription ends on `unsubscribe()` / `close()`, when the client closes, or
when the server ends it (`endReason()`, code 503 when leadership moves to
another node). After a reconnect the client subscribes again with the same
subject and queue group; messages published while it was disconnected are
missed. `next(Duration)`, iteration and `listen(callback)` work as they do
for consumer subscriptions.

### Request-reply

```java
// The service: answer each request with m.respond(value).
CoreSubscription requests = client.subscribeCore("svc.upper", "svc");
requests.listen(m -> m.respond(m.text().toUpperCase()));

// The caller: waits for the first response.
CoreMessage reply = client.request("svc.upper", "hello".getBytes(UTF_8),
    RequestOptions.timeout(Duration.ofSeconds(2)));
reply.text(); // "HELLO"
```

`request` publishes with a reply subject and waits for the first response.
It fails at once with `ServerException` 404 when nobody is subscribed to the
subject ("no responders"), and with `RequestTimeoutException` after the
timeout (default: the client's request timeout). All requests on a client
share one inbox subscription, `_INBOX.<random>.*`, which the first request
sets up. When the connection drops, requests waiting for a response fail
with `ConnectionException`, and the next request after the reconnect sets up
a new inbox. Request-reply needs no extra permissions: anyone may publish a
reply to an `_INBOX.…` subject, and a client may subscribe to its own inbox
(see [Security](../../docs/security.md)).

## Key-value buckets

A bucket is a stream (`KV_<bucket>`) that keeps the latest values of each
key. Keys are subjects, so they are dot-separated tokens such as `app.mode`.

```java
KvBucket kv = client.kv("config");
kv.create(KvBucketOptions.history(5));      // idempotent; also withTtl(...), withMaxBytes(...)

long rev = kv.put("app.mode", "prod");      // returns the new revision
KvEntry entry = kv.get("app.mode");         // null when absent, deleted or expired
// entry.key(), entry.value(), entry.text(), entry.json(), entry.revision(), entry.op(), entry.timestamp()

kv.createKey("app.port", "8080");           // only if absent: 409 otherwise
kv.update("app.mode", "dev", rev);          // compare-and-set: 409 unless still at rev
kv.put("session.abc", token, KvPutOptions.ttl(Duration.ofMinutes(1))); // this value expires
kv.getRevision("app.mode", rev);            // an older value, while history keeps it
kv.history("app.mode");                     // kept revisions, oldest first, deletes included
kv.keys("app.*");                           // keys with a value, sorted (keys() = all)
kv.delete("app.port");                      // a tombstone; history stays
kv.purge("app.port");                       // a tombstone that also hides older values
kv.destroy();                               // delete the bucket and everything in it
```

A revision is the position of the write in the bucket's stream, plus one,
so revisions only grow; 0 means "absent", which is what `createKey` checks.
`history` is how many values each key keeps (1 to 64, default 1). A failed
compare-and-set is a `ServerException` 409 with
`detail("current_revision")`. `delete` and `purge` take an expected revision
too. `get` on a missing bucket fails with 404.

### Watching

```java
try (KvWatch watch = kv.watch("app.*")) {  // watch() = every key
  for (KvEntry e : watch) {
    if (e.op() == KvOp.PUT) apply(e.key(), e.text());
    else remove(e.key());                    // DELETE or PURGE
  }
}
```

A watch first yields the current value of every matching key (deleted keys
left out), ordered by revision, then every change as it happens, deletes
included. It reads the bucket's stream with stateless long-poll reads, so it
holds no state on the server. `watch.next(timeout)` returns `null` when
nothing changed in time, `close()` ends it, and a failed read, such as
`ConnectionException` when the connection drops, is thrown from `next()`.

## Queries and metadata

```java
QueryResult r = client.query("SELECT COUNT(*) AS n FROM \"orders\"");
// r.columns() = [n], r.rows() = [[123]], r.rowCount(), r.executionTimeMs(), r.truncated()

client.metadata();   // Metadata(nodeId, isLeader, leader, serverVersion)
client.ping();       // round-trip time as a Duration
client.serverInfo(); // ServerInfo(serverVersion, nodeId, leader) from the handshake
```

With auth enabled, `query` needs a global-admin credential, since SQL can
read any stream. Query result values are what `Json.parse` produces: `Long`,
`Double`, `String`, `Boolean`, `List`, `Map` or `null`.

## Errors

All exceptions are unchecked and extend `ExspeedException`:

| Class | When |
|-------|------|
| `ServerException` | The server rejected the request. It has `code()`, `getMessage()`, `detail()`, `detail(key)`, `detailJson()` and `leaderHint()`. |
| `ConnectionException` | Not connected: the connection could not be opened, was lost, is being re-established, or the client is closed. |
| `RequestTimeoutException` | No response within the request timeout (plus a pull's or read's own wait), or no reply to a core request in time. |
| `ProtocolException` | The server sent something this client can't decode, or a value doesn't fit its wire encoding. |

Invalid arguments to the builders (a priority of 12, an empty stream name)
throw `IllegalArgumentException`.

`ServerException.code()` is HTTP-like; `ErrorCode` names every code:

| Code | Meaning | `detail` |
|------|---------|----------|
| 400 | Malformed request, invalid name, filter or config | |
| 401 | Not authenticated | |
| 403 | The credential lacks the needed permission | |
| 404 | Stream, consumer, bucket or key not found; a request with no responders | |
| 408 | A `query` timed out | |
| 409 | Exists with different settings; stream still has consumers; msg id reused with a different body; a KV key not at the expected revision | `stored_offset`, `consumers`, `current_revision` |
| 422 | A `query` exceeded the server's query memory limit | |
| 429 | Retry later (dedup map full, stream full with `discard = new`, too many concurrent waits on one connection) | `retry_after_secs` |
| 500 | Internal error | |
| 503 | Not the leader, still starting, or too few in-sync replicas | `leader`, `in_sync`, `required` |
| 507 | The server's disk is full; nothing was written | |

`detail()` is the parsed JSON the server sent, with snake_case keys. For 503
errors, `leaderHint()` holds the leader's address when the server knows it.
A failed request isn't retried against the leader automatically; the client
follows leader hints only when it connects or reconnects (see
[Clusters](#clusters)).

```java
try {
  client.publish("orders", record);
} catch (ServerException e) {
  if (e.code() == ErrorCode.UNAVAILABLE && e.leaderHint() != null) {
    // reconnect to e.leaderHint()
  }
  throw e;
}
```

## Reconnection

Reconnection is on by default. When the connection drops:

1. Pending requests fail with `ConnectionException`. They are not retried,
   because a publish may or may not have been applied. Publish with a msg id
   to make a retry safe. New requests also fail with `ConnectionException`
   until the connection is back.
2. Listeners get `onDisconnect`, and the client reconnects with exponential
   backoff (100 ms doubling to 5 s, with jitter, unlimited attempts by
   default).
3. Once reconnected, it re-creates the ephemeral consumers it created (the
   server deleted them with the old connection), then re-subscribes every
   live subscription with its original window. Your loop keeps running and
   doesn't notice the gap. Records that were buffered but not yet handed to
   your code are discarded, and the server redelivers them, along with
   anything delivered but not acked. A re-subscribe that fails (for example,
   the consumer was deleted meanwhile) ends that subscription with the
   server's error code. Core subscriptions are subscribed again too; core
   messages published during the gap are missed.
4. Listeners get `onReconnect`.

If the server rejects the credential (401/403) or the attempts run out, the
client closes: subscriptions end with code 503 and listeners get `onClose`.
The first `connect()` is never retried; it throws straight away.

```java
ExspeedClient client = ExspeedClient.connect(ClientOptions.builder()
    .host("exspeed.internal")
    .reconnect(new ReconnectOptions(30, Duration.ofMillis(200), Duration.ofSeconds(10)))
    // .reconnect(false)  -> the client closes when the connection drops
    .listener(new ClientListener() {
      @Override public void onDisconnect(Throwable cause) { log.warn("exspeed disconnected", cause); }
      @Override public void onReconnect(ServerInfo info) { log.info("exspeed reconnected {}", info); }
      @Override public void onClose(Throwable cause) { log.error("exspeed closed", cause); }
      @Override public void onError(ExspeedException error) { log.warn("failed ack or credit", error); }
    })
    .build());
```

Durable consumers make this safe: the cursor and the unacked set live on the
server, so nothing is lost and nothing is skipped. An ephemeral consumer
created with `DeliverPolicy.NEW` does skip records published while it was
gone, because the re-created consumer starts at the end of the stream.

The client pings every `keepalive` (20 s) so the server, which drops
connections idle for 120 s, keeps the connection open. A ping that times out
is treated as a dead connection.

## Clusters

Against a cluster ([High availability](../../docs/high-availability.md)),
give the client some seed addresses. It connects to whichever node is the
leader, following the leader hints followers return. After a failover it
reconnects to the new leader and restores subscriptions as described above.

```java
ExspeedClient client = ExspeedClient.connect(ClientOptions.builder()
    .servers("exspeed-0.exspeed:5933", "exspeed-1.exspeed:5933", "exspeed-2.exspeed:5933")
    .build());
```

Without seeds, a node that names another node as leader in its handshake is
still followed, so connecting through a Service that routes to any node
works too. A write that reaches a follower anyway fails with
`ServerException` 503, and `leaderHint()` names the leader.

## TLS and authentication

```java
ExspeedClient client = ExspeedClient.connect(ClientOptions.builder()
    .host("exspeed.example.com")
    .port(5933)
    .token(System.getenv("EXSPEED_TOKEN"))  // the server's --auth-token, or a credential token
    .tls(TlsOptions.systemDefault())        // verify against the JVM's trust store
    // .tls(TlsOptions.builder().caPem(Path.of("ca.pem")).build())             // private CA
    // .tls(TlsOptions.builder().serverName("exspeed.internal").build())       // SNI / certificate name override
    .build());
```

Certificates and host names are verified by default.
`TlsOptions.Builder` also takes a `KeyStore` (`trustStore`) or a ready-made
`SSLContext` (`sslContext`). A wrong or missing token fails `connect()` with
`ServerException` 401. A token without permission for an operation gets 403.
With scoped credentials, `listStreams` and `listConsumers` return only what
the credential can see. See [Security](../../docs/security.md).

### Client certificates (mutual TLS)

When the server runs with `tls.client_ca`, it accepts only clients that
present a certificate signed by that CA:

```java
TlsOptions tls = TlsOptions.builder()
    .caPem(Path.of("ca.pem"))                                                  // the server's CA
    .clientCertificate(Path.of("orders-client.pem"), Path.of("orders-client.key"))
    .build();
ExspeedClient client = ExspeedClient.connect(ClientOptions.builder().host("exspeed.example.com").tls(tls).build());
// no token: the credential bound to the certificate's name (cert_cn) applies
```

The key is read from unencrypted PEM: PKCS#8 (`BEGIN PRIVATE KEY`), PKCS#1
(`BEGIN RSA PRIVATE KEY`) or SEC1 (`BEGIN EC PRIVATE KEY`). Without a valid
client certificate the handshake fails and `connect()` throws
`ConnectionException`. With auth on, a client with a certificate and no
token gets the permissions of the credential whose `cert_cn` matches the
certificate's common name (or its first DNS name), and `connect()` fails
with `ServerException` 401 when no credential names it. A token, when given,
takes precedence.

## Options

| `ClientOptions.Builder` | Default | |
|--------|---------|---|
| `host` | `"127.0.0.1"` | |
| `port` | `5933` | |
| `servers` | none | Cluster seeds (`"host:port"`); connects to the leader. Overrides `host`/`port`. |
| `token` | none | Bearer token |
| `tls` | off | `TlsOptions` |
| `clientId` | `"exspeed-java"` | Shown in server logs |
| `requestTimeout` | 30 s | Per request, on top of a pull's or read's own wait. Also bounds connecting. |
| `keepalive` | 20 s | `Duration.ZERO` disables pings |
| `reconnect` | on | `false`, or `ReconnectOptions(maxAttempts, initialDelay, maxDelay)` |
| `callbackExecutor` | `CompletableFuture` async pool | Where futures complete and listeners run |
| `verifyCrc` | `true` | Verify each received record's CRC32C |
| `listener` | none | A `ClientListener` registered from the start |

Offsets, revisions and timestamps are unsigned 64-bit numbers on the wire,
held in Java `long`s.

## Development

```bash
cd sdks/java
mvn -B verify     # compile (with doclint), unit tests, e2e tests when a server binary is available
```

The end-to-end tests (`src/test/java/io/exspeed/client/e2e`) start real
servers. They use the binary named by `EXSPEED_BIN`, or else
`target/debug/exspeed` at the repository root
(`cargo build -p exspeed --bin exspeed`). Without one, they are skipped with
a message. The TLS tests also need `openssl` on the `PATH`.
