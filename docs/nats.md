# NATS protocol

Exspeed speaks the core NATS client protocol on a port of its own, so any
NATS client library (Go, Python, JavaScript, Java, .NET, Rust, C and the
rest) and the `nats` CLI connect to it unchanged. NATS clients publish,
subscribe, use queue groups and request-reply on the same
[core message bus](messaging.md) as Exspeed's own clients: a message
published over one protocol reaches subscribers on the other. A stream can
also **capture** subjects, so a NATS publish becomes a durable record, and
JetStream's publish calls get their acknowledgement.

## Enable it

The listener is off by default. Give it an address:

```toml
[nats]
bind = "0.0.0.0:4222"     # EXSPEED_NATS_BIND, --nats-bind
```

```bash
exspeed server --nats-bind 0.0.0.0:4222
nats --server nats://localhost:4222 pub orders.eu.created '{"id":1}'
```

```go
nc, _ := nats.Connect("nats://localhost:4222")
nc.Subscribe("orders.>", func(m *nats.Msg) { fmt.Println(m.Subject, string(m.Data)) })
reply, _ := nc.Request("svc.users.get", []byte(`{"id":7}`), 2*time.Second)
```

## What works

| Feature | Notes |
|---------|-------|
| `PUB`, `HPUB` (headers), `SUB`, `UNSUB` | Subjects and wildcards as in NATS (`*`, `>`); headers map to Exspeed headers one to one |
| Queue groups | `SUB <subject> <queue> <sid>`: each message goes to one member |
| Request-reply | Old-style (`UNSUB <sid> 1`) and new-style (one `_INBOX.<id>.*` subscription) inboxes |
| No responders | A request nobody is subscribed to gets the `503` status at once (clients raise `ErrNoResponders`) |
| `echo: false` | A connection doesn't get its own messages |
| Auto-unsubscribe | `UNSUB <sid> <max>` |
| `verbose` | `+OK` after each command |
| `PING` / `PONG` | The server also pings every 60 s and closes connections that miss two pings |
| Authentication | A token (`auth_token`, or `pass` with any `user`) or a client certificate, checked against the same credentials as the Exspeed port |
| TLS | With `[tls]` set, the listener requires TLS (`tls_required` in `INFO`); with `tls.client_ca` it requires client certificates too |
| Max payload | 8 MiB (the record value limit), announced in `INFO` |
| JetStream publish | Through stream capture (below): `js.Publish`, `js.PublishAsync`, `Nats-Msg-Id` deduplication |

Not supported: the rest of the JetStream API (`$JS.API.>`: stream and
consumer management, pull and push consumers, KV and object store; use
Exspeed's own [consumers](concepts.md#consumers) and [KV](kv.md)),
accounts, NKEYs and JWTs, leaf nodes, gateways and server clustering,
WebSocket and MQTT, and the TLS-first handshake (`handshake_first`).

## Stream capture

`capture_subjects` on a stream appends every core message published to a
matching subject, over NATS or Exspeed's `CorePublish`, to that stream.
Core subscribers still receive the message.

```bash
exspeed create orders --capture 'orders.>'
# or: POST /api/v1/streams {"name": "orders", "capture_subjects": ["orders.>"]}
```

A captured message published with a reply subject is acknowledged there
in JetStream's `PubAck` format, which is what JetStream publish calls
wait for:

```json
{"stream": "orders", "seq": 42}
{"stream": "orders", "seq": 42, "duplicate": true}
{"error": {"code": 429, "description": "stream orders is full (max_msgs = 1000); nothing was written (discard policy: new)"}}
```

`seq` is the record's offset plus one (JetStream sequences start at 1). A
`Nats-Msg-Id` header is the record's `msg_id`: a retry within the stream's
dedup window returns the original sequence with `"duplicate": true`. Error
codes follow the HTTP API: `400` invalid record, `429` stream full
(`discard = "new"`), `503` not the leader or not enough replicas.

```go
js, _ := jetstream.New(nc)
ack, err := js.Publish(ctx, "orders.eu.created", data, jetstream.WithMsgID("o-1"))
// ack.Stream == "orders", ack.Sequence == offset + 1
```

Captured messages from one connection are appended in publish order, in
batches. A message published without a reply subject that can't be stored
is dropped and logged. No two streams may capture overlapping subjects
(`400` on create or update), and capturing needs `publish` permission on
the stream as well as on the subject.

Read the records with any Exspeed client, a [consumer](concepts.md#consumers)
or [ExQL](exql.md); the record's subject and headers are the message's.

## Clusters

Core messaging runs on the leader. A standby answers a NATS connection
with `INFO` and closes it, and when leadership moves the old leader closes
its NATS connections, so clients reconnect to the next server in their
list. Give NATS clients the address of every node:

```go
nats.Connect("nats://exspeed-0:4222,nats://exspeed-1:4222,nats://exspeed-2:4222")
```

On shutdown the server sends `INFO {"ldm": true}` (lame-duck mode) before
closing, and clients reconnect elsewhere at once.

## Permissions

NATS connections use the same credentials as the Exspeed port (see
[security.md](security.md#subject-permissions)): publishing and
subscribing need a `subjects` permission (or `streams = "*"`), replies to
`_INBOX.…` and a client's own inbox are always allowed, and a violation is
reported as NATS does (`-ERR 'Permissions Violation for Publish to
"subject"'`) without closing the connection. A wrong or missing token
closes it with `-ERR 'Authorization Violation'`.

## Monitoring

NATS connections count toward `max_connections`,
`exspeed_connections_active` and `exspeed_connections_rejected_total`, and their messages toward
`exspeed_core_messages_delivered_total` and
`exspeed_core_messages_dropped_total` (a subscriber that can't keep up
loses messages, as in NATS).
