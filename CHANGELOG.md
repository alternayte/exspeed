# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **NATS protocol.** `[nats] bind` (`EXSPEED_NATS_BIND`, `--nats-bind`,
  off by default) serves the core NATS client protocol, so NATS client
  libraries and the `nats` CLI connect unchanged: `PUB`/`HPUB`, `SUB`
  with queue groups, `UNSUB` with a max, request-reply with the
  `no_responders` 503 status, `echo: false`, verbose mode, server pings,
  token or client-certificate auth with subject permissions, TLS, and
  lame-duck `INFO` on shutdown. NATS and Exspeed clients share one core
  message bus. Standbys close NATS connections after `INFO`; a failover
  closes them on the old leader. See `docs/nats.md`.
- **Stream capture.** A stream's `capture_subjects` (HTTP, protocol stream
  spec, `exspeed create --capture`) stores the core messages published to
  matching subjects, over NATS or `CorePublish`. A captured publish with a
  reply subject is acknowledged with a JetStream-style `PubAck`
  (`{"stream","seq"}`, `seq` = offset + 1), so `js.Publish` /
  `js.PublishAsync` in NATS clients work; `Nats-Msg-Id` deduplicates.
  Overlapping capture subjects across streams are refused.

### Changed

- Embedders building `exspeed_broker::pubsub::CoreMessage` set its new
  `origin` field (the publishing connection; `0` when unknown), and
  `StreamLimits` is no longer `Copy` (it gained `capture_subjects`).

## [0.7.0] — 2026-10-04

**TL;DR:** Exspeed 0.7 adds the features people stay on NATS or RabbitMQ
for. Streams can now behave like queues: messages can expire, wait before
delivery, be removed once acknowledged, and be capped with "reject when
full". Consumers can filter on headers, deliver urgent messages first, or
keep one active subscriber with automatic failover. Beyond streams there is
non-persistent publish/subscribe with request-reply, a key-value store with
compare-and-set, and client-certificate (mTLS) authentication. Everything is
off by default; existing streams, clients and data directories keep working
unchanged.

### What's new

- **Messages that expire.** Give a stream a TTL, or let each message carry
  its own (`exspeed-ttl` header). Expired messages disappear from reads,
  SQL and consumers; a consumer can send them to its dead-letter stream
  instead.
- **Delayed delivery.** A message can ask to be delivered later
  (`exspeed-delay: 30s`) or at a time (`exspeed-deliver-at`). It survives
  restarts and failover while it waits.
- **Real work queues.** `retention = "work_queue"` removes a message once it
  is acknowledged; `interest` removes it once every consumer has. Combine
  with `max_msgs` and `discard = "new"` for a bounded queue that tells
  publishers "full" (429) instead of growing without limit.
- **Ring buffers and last-value streams.** `max_msgs` with `discard =
  "old"` keeps the newest N messages; `max_msgs_per_subject` keeps only the
  newest N per subject.
- **Smarter consumers.** Filter on header values (like a RabbitMQ headers
  exchange), deliver higher-priority messages first, or run a single
  active consumer that fails over to a standby. Dead-lettered messages say
  why (`max_deliver`, `rejected` or `expired`).
- **Publish/subscribe and request-reply.** Core messages go straight to
  whoever is subscribed, with queue groups to share the load and
  request-reply that fails fast when no service is listening. Nothing is
  stored, like core NATS.
- **Key-value buckets.** Get, put, delete, history, compare-and-set,
  per-key TTLs and live watches, over TCP and HTTP. Buckets are streams, so
  they persist, replicate and fail over like everything else.
- **Client certificates.** The TCP port can require certificates from your
  CA, and a credential can be bound to a certificate instead of a token.
  Credentials can also grant publish/subscribe on subjects for core
  messaging.
- **SDKs.** The TypeScript SDK and the Rust client support all of the above.

### Upgrading from 0.6

Upgrade in place: 0.7 opens 0.6 data directories, speaks to 0.6 clients,
and reads 0.6 config and credentials files. In a cluster, upgrade the
followers first and the leader last, so a node that becomes leader always
understands every stream setting. Don't downgrade a data directory after
using the new features: 0.6 ignores them (expired, superseded and acked
work-queue records would reappear).

TypeScript SDK: the low-level `client.request(req)` that sends a raw
protocol request is now `client.rawRequest(req)`; `client.request` is the
new request-reply call.

### Get it

- Docker (amd64 and arm64): `docker pull ghcr.io/alternayte/exspeed:0.7.0`
- Binaries and installers for macOS, Linux and Windows: below.
- TypeScript SDK: `@exspeed/sdk` 0.7.0, in [`sdks/typescript`](https://github.com/alternayte/exspeed/tree/v0.7.0/sdks/typescript).
- Docs: [queues](https://github.com/alternayte/exspeed/blob/v0.7.0/docs/queues.md),
  [messaging](https://github.com/alternayte/exspeed/blob/v0.7.0/docs/messaging.md),
  [key-value](https://github.com/alternayte/exspeed/blob/v0.7.0/docs/kv.md),
  [everything else](https://github.com/alternayte/exspeed/tree/v0.7.0/docs).

<details>
<summary>Full list of changes</summary>

#### Streams

- New stream settings (`StreamConfig`, the protocol's stream spec, the HTTP
  API and `exspeed create` / `update-stream`): `max_msgs`, `discard`
  (`old`/`new`), `max_msgs_per_subject`, `allow_msg_ttl`, `msg_ttl_ms`,
  `allow_delayed`, `retention` (`limits`/`work_queue`/`interest`). On the
  wire they travel as an optional JSON trailer on the stream spec, sent only
  when set.
- Storage has a record-exact log start offset (`partitions/0/log_start`):
  `trim_up_to` no longer works on whole segments only. Backups carry it and
  followers mirror it.
- Reads, SQL and consumers hide expired records and records superseded under
  `max_msgs_per_subject` (an in-memory per-subject index, rebuilt on start
  and when the limit changes). Compaction removes them from disk; a
  stream-wide TTL that records can't override also expires whole segments.
- `discard = new` rejects a publish that doesn't fit with `429`
  (`StorageError::StreamFull`), whole batches at a time.
- Publishing an `exspeed-ttl`, `exspeed-delay` or `exspeed-deliver-at`
  header to a stream that doesn't allow it, or with an invalid value, fails
  with `400`.
- `GET /api/v1/streams/{name}` and stream info include `earliest_offset`
  and every new setting.

#### Retention by acknowledgement

- Consumers report their ack floor after each persisted state; the leader
  trims `work_queue` and `interest` streams to the lowest floor (an
  `interest` stream with no consumers to its head).
- Work-queue streams accept only `deliver: all` consumers whose subject
  filters don't overlap (`409` otherwise). Retention policies can't be
  combined with compaction.

#### Consumers

- Delayed records are held until due (at most 100,000 per consumer), don't
  count against `max_ack_pending`, hold the ack floor, and are persisted;
  consumer info reports `num_delayed`.
- New consumer settings: `dead_letter_expired`, `filter_headers` +
  `header_match`, `single_active` (pulls are refused), `priority_window`
  (records carry `exspeed-priority` 0–9).
- Dead letters gain `exspeed-dlq-cause` and `exspeed-dlq-time`.
- A consumer no longer stalls when a whole read batch is hidden (expired or
  superseded records).

#### Core messaging

- Protocol: `CorePublish` (0x70), `CoreSubscribe` (0x71), `CoreMsg` push
  (0x8B); `Unsubscribe` ends core subscriptions (ids with the high bit set).
- Queue groups, request-reply with `404` "no responders", a 65,536-message
  queue per connection with drops counted in
  `exspeed_core_messages_dropped_total` (and
  `exspeed_core_messages_delivered_total`).
- Leader only; subscriptions end with `503` when leadership moves.

#### Key-value

- Protocol: `KvPut`, `KvGet`, `KvDelete`, `KvKeys`, `KvHistory`,
  `KvCreateBucket` (0x74–0x79). HTTP: `POST /api/v1/kv`,
  `GET /api/v1/kv/{bucket}`, `GET/PUT/DELETE /api/v1/kv/{bucket}/{key}`
  (`If-Match` / `If-None-Match: *`), `.../history`.
- A bucket is the stream `KV_<bucket>`; revisions are offset + 1.
  Compare-and-set runs under a per-bucket lock against the committed log.

#### Security

- `tls.client_ca` (`EXSPEED_TLS_CLIENT_CA`, `--tls-client-ca`) requires
  client certificates on the TCP port.
- Credentials: `cert_cn` binds a credential to a certificate name instead of
  `token_sha256`; `subjects = "…"` permissions grant publish/subscribe on
  core-message subjects. Replies to `_INBOX.…` are always allowed.

#### Clients

- Rust client: stream limits in `StreamSpec`, `PublishRecord::ttl` /
  `delay` / `deliver_at` / `priority`, `publish_core`, `subscribe_core`,
  `respond`, `request_core`, and `Client::kv` (get, put, create_key,
  update, delete, purge, keys, history, watch).
- TypeScript SDK: stream limits, `ttl` / `delay` / `deliverAt` / `priority`
  publish options, the new consumer settings and `numDelayed`,
  `publishCore`, `subscribeCore`, `request` (request-reply), `client.kv`
  (`KvBucket` with `createKey`, `update`, `getRevision`, `watch`, …), and
  mutual TLS documented. The raw protocol call `request(req)` is renamed
  `rawRequest(req)`.

</details>

## [0.6.0] — 2026-10-03

**TL;DR:** Exspeed 0.6 is a ground-up rebuild of the broker for correctness.
Writes you get an acknowledgement for are never lost: not on a crash, not
on a full disk, not on a failover. Consumers now work like NATS JetStream
(acks, redelivery, dead letters, work shared across app instances). ExQL
runs real SQL on Apache DataFusion, connectors are checkpointed and tested
against the real systems they talk to, and multi-node clusters replicate
every stream. It is also faster: on the same machine with every broker
syncing to disk, Exspeed published about 4.5× as many messages per second as
Kafka and drained a backlog about 3.5× faster. Nothing is compatible with
0.5: read **Upgrading** below before you install it.

### What's new

- **Data you can trust.** A new storage engine with group commit, crash-safe
  recovery and lock-free reads. A full disk returns a clean `507` instead of
  corrupting data. Tested by killing the server mid-write in a loop and by
  filling a real disk.
- **Consumers that behave.** Push and pull delivery, per-message ack, nack,
  term and in-progress, ack timeouts, redelivery with backoff, a maximum
  delivery count and dead-letter streams. One consumer can be shared by many
  app instances to split the work.
- **Duplicate-free retries.** Give a message a `msg_id` and a retry within
  the dedup window (5 minutes by default) never creates a duplicate, on
  every write path and across failover.
- **SQL that works.** ExQL bounded queries run on Apache DataFusion (joins,
  aggregates, window functions, subqueries, JSON fields as numbers).
  Continuous queries add event-time windows, stream joins, materialized
  tables and checkpointed state; a restart never duplicates their output.
- **Connectors you can rely on.** Postgres CDC, outbox and poll; JDBC sink
  and poll for Postgres, MySQL and SQL Server; SQL Server CDC; RabbitMQ;
  S3; HTTP poll, sink and webhooks. Each one checkpoints only after data is
  safe and has a crash-and-resume test against the real service.
- **High availability.** Run several nodes behind a Postgres or Redis lease:
  every stream is replicated, a deposed leader can't accept writes, `acks =
  all` or `quorum` guarantees acknowledged writes survive a failover, and
  readers never see a record a failover could still drop. Traffic between
  nodes can use TLS.
- **Easier to run.** One `exspeed.toml` config file, a Helm chart, graceful
  shutdown, online backup and restore, an OpenAPI spec, Prometheus metrics
  with consistent `exspeed_` names, and a Docker image for amd64 and arm64
  on `ghcr.io/alternayte/exspeed`.
- **Clients.** The TypeScript SDK (`@exspeed/sdk`) and a Rust client speak
  the new protocol, with automatic reconnect, leader discovery and
  pipelined publishing.

### Upgrading from 0.5

0.6 changes the wire protocol, the on-disk format, connector configs, the
SDK API and the metric names. There is no in-place upgrade:

- Start 0.6 on an **empty data directory** (0.6 refuses a 0.5 one) and
  re-create streams, consumers and connectors.
- Update clients to `@exspeed/sdk` 0.6 or the matching `exspeed-client`.
- Connector configs: `plugin = "postgres"` is now `postgres_cdc` or
  `postgres_poll`, settings are typed, and unknown keys are rejected. Run
  `exspeed connector validate <file>` on each config.
- Dashboards and alerts: every metric is now named `exspeed_*` (see
  `docs/operations.md`).
- Every node of a cluster must run the same version.

### Get it

- Docker (amd64 and arm64): `docker pull ghcr.io/alternayte/exspeed:0.6.0`
- Prebuilt binaries and installers: below.
- Documentation: [`docs/`](https://github.com/alternayte/exspeed/tree/v0.6.0/docs).

The detailed change list follows.

<details>
<summary>Full list of changes</summary>

#### Storage

- New file engine: a writer thread per partition with group commit, readers
  that take no locks, a high watermark (readers only see durable data),
  sparse offset/time indexes, and fencing on write/fsync errors. Sidecars and
  truncation are crash-safe.
- Compaction (`compaction = true` per stream): keeps the latest record per
  key; tombstones delete keys.
- Removed: bloom filters, secondary indexes (`.sidx`), S3 tiering and its
  `EXSPEED_STORAGE_S3_*` variables.
- Records are stored in the client protocol's `WireRecord` encoding
  (segment format version 3; older data dirs are refused). `Read`, consumer
  push (`Deliver`) and pull (`Messages`) build their replies from the raw
  segment bytes: one `pread`, the delivery count patched in place, subjects
  parsed in place for filtering, no decoding or per-record allocation. New
  `StorageEngine::read_raw` returning a `RawBatch`. Each record keeps its own
  length and CRC32C, so torn-write detection, recovery, compaction, backup
  and truncation work as before. The connection writer flushes once per
  burst of queued frames instead of once per frame.

#### Consumers and client protocol v2

- New binary protocol ([docs/protocol.md](docs/protocol.md)), version byte 2.
  A `WireRecord` is the stored record: `u32 len`, `u32 crc` (CRC32C, which
  the Rust client verifies), `u16 delivery_count`, `u64 offset`,
  `u64 timestamp_ns` (nanoseconds; was milliseconds), then the fields.
  `WireRecord::timestamp_ms` became `timestamp_ns` (with a `timestamp_ms()`
  helper) in the Rust client; the TypeScript SDK adds
  `StreamRecord.timestampNs` (a `bigint`) next to `timestamp` (ms) and
  exports `verifyRecordCrc`.
  It adds out-of-order replies, fire-and-forget acks and credits,
  `SubscriptionEnded` pushes, JSON admin replies, structured errors with
  details, and handshake and idle timeouts.
- JetStream-style consumers:
  - push (credit-based) and pull delivery
  - `ack`, `nack(delay)`, `term`, `in_progress`
  - `ack_wait`, `max_deliver`, `backoff`, `max_ack_pending`, DLQ with
    provenance headers
  - work sharing between any number of subscribers on one consumer, across
    connections and app instances
  - immediate redelivery when a subscriber goes away
  - ephemeral consumers
- Consumer state lives in the compacted internal stream `__consumers`.
  Streams named `__*` are internal.
- Removed: consumer groups, the file/Postgres/Redis/S3 consumer stores, the
  work coordinators, `Fetch`/`Seek`/`Record` opcodes, `--delivery-buffer`.
- HTTP: `POST /api/v1/consumers`, `POST /api/v1/consumers/{name}/seek`,
  `GET /api/v1/streams/{name}/records`, and `compaction` on stream creation.
- `EXSPEED_LEASE_BACKEND=postgres|redis` selects multi-pod mode.
  `EXSPEED_CONSUMER_STORE` is a deprecated alias.
- New Rust client crate `exspeed-client`, with a coalescing publisher.
  `exspeed tail` no longer goes through SQL.

#### Operations

- Online backup: `GET /api/v1/backup` (global admin, leader) streams a tar
  archive while writes continue. Each stream is a point-in-time copy of
  every record below its high watermark at snapshot start: sealed segments
  whole, the active segment cut after its last complete record, indexes and
  metadata regenerated to match. A manifest (`exspeed-backup.json`) lists
  each stream's `[earliest_offset, next_offset)`. Connector, connection and
  ExQL directories are included; credentials are not. Consistency is per
  stream, not across streams; internal progress streams are snapshotted
  first so a restore replays rather than skips.
- `exspeed backup --url … [--token …] --output backup.tar` downloads and
  verifies a backup; `exspeed restore --input backup.tar --data-dir DIR
  [--force]` restores it offline (takes the data-dir lock, refuses a
  non-empty dir, validates the manifest, paths and every stream's offsets
  before moving anything into place). See
  [docs/operations.md](docs/operations.md#backup-and-restore).
- OpenAPI 3.1 document for the HTTP API at `GET /api/v1/openapi.json`
  (no auth), generated with utoipa from annotations on the handlers. Tests
  keep it in sync with the router in both directions.
- HTTP responses for streams, publish and record browsing are now typed
  structs (same JSON).
- `exspeed-bench catchup`: backlog-drain throughput with stateless reads and
  with a push consumer; part of `exspeed-bench all`. Single-scenario
  commands take `--duration-secs`, `--payload-sizes`, `--rate` and
  `--catchup-records` overrides.
- BENCHMARKS.md and the README numbers re-measured on Linux (4-vCPU cloud
  VM, virtio disk; ~62k msg/s durable 1 KiB publish), with the exact
  commands and raw results in `bench/results/2026-10-03-linux-*.json`. `bench/compare/` has a docker-compose file (Kafka in
  KRaft mode, NATS JetStream) and `run.sh` to run equivalent workloads with
  each system's own perf tools; no comparison numbers are published until
  they are run on the same hardware.

#### Connectors

- A supervisor per connector (starting/running/backoff/failed/stopped):
  restarts with backoff, catches panics, and reports status, last error,
  restart count, lag and last success over HTTP and in metrics.
- Uniform checkpoint protocol: source checkpoints are saved only after the
  append is durable; sink offsets are committed only after `flush()`.
- Typed per-plugin settings that reject unknown keys and accept native TOML
  types. Errors are classified as transient, poison or fatal.
- Connector offsets are stored in the internal `__connector_offsets` stream
  by default (`EXSPEED_CONNECTOR_OFFSET_STORE=log|file`). The Postgres, Redis
  and S3 offset stores are removed.
- Config changes:
  - `postgres` is split into `postgres_cdc` and `postgres_poll`.
  - `dlq_stream` moves to `[connector]`.
  - `http_webhook` needs an explicit `auth_type` (adds `hmac_sha256`).
  - See [docs/connectors.md](docs/connectors.md).
- Plugin fixes:
  - CDC keys and envelope, TOAST columns, LSN feedback on idle
  - poll ties, outbox id types, `mssql_cdc` cursor
  - batched JDBC inserts, HTTP status taxonomy
  - S3 sink idempotent object keys, RabbitMQ confirms
- Crash/resume tests against real services for every remaining plugin: the
  RabbitMQ source and sink, the S3 sink (against moto, an S3-compatible server), the JDBC sink and
  `jdbc_poll` on MySQL, SQL Server and Postgres, `mssql_cdc`, and the
  Postgres outbox (crash before the delete). Each one crashes the connector
  mid-stream and checks at-least-once delivery, or no duplicates where the
  plugin promises effectively-once. The CI service
  job now also runs RabbitMQ, an S3-compatible store (moto) and SQL Server (with Agent, for CDC).
  See [docs/connectors.md](docs/connectors.md#testing-against-real-services).
- Fixes found by these tests:
  - `jdbc_poll` failed every poll on columns that sqlx's `Any` driver can't
    map: MySQL `TINYINT(1)`, `DECIMAL`, `DATETIME` and `JSON`, and Postgres
    `numeric` and `timestamptz`. The error was classified as transient, so
    the connector restarted forever. On SQL Server, `datetime2` columns came
    out as `null`. Columns are now cast in SQL to their `schema` type. A
    tracking value that can't be read now fails the connector instead of
    re-reading the same rows forever.
  - `rabbitmq` sink: when a message was returned as unroutable
    (`mandatory`), every record after it in the batch was published again,
    once for each returned message in the batch. The rest of the batch is
    now settled from the publisher confirms that were already received.

#### High availability

- Clusters are now safe to run: Kafka-style pull replication with leader
  epochs replaces the old push replication. Every stream replicates,
  internal ones included, with original offsets, timestamps, keys and
  headers. Stream metadata (create, config, delete) and retention trims
  replicate too. See [docs/high-availability.md](docs/high-availability.md).
- Lease records carry an epoch (fencing token), the holder's replication
  and client endpoints, and the in-sync replica set. Releasing a lease
  expires it rather than deleting it. Node ids are stable
  (`{data_dir}/node_id`). The Postgres table is now `exspeed_cluster_leases`.
  The heartbeat bounds every backend call and steps down before the lease
  can expire elsewhere.
- `cluster.acks = "all"` (default) acknowledges writes once every in-sync
  replica has them. `cluster.min_insync_replicas` rejects writes when too
  few replicas are in sync. Only ISR members can be elected unless
  `cluster.unclean_leader_election = true`.
- A deposed leader truncates writes it never replicated when it rejoins
  (per-stream epoch history, KIP-101).
- Promotion rebuilds the dedup state from the replicated log, so retried
  `msg_id` publishes stay idempotent across failover.
- Leader hints: followers name the leader in `ConnectOk`, `Metadata`, 503
  errors (TCP and HTTP) and `/healthz`. New `cluster.client_advertise`.
  The Rust client gained `Client::connect_cluster`. The TypeScript SDK takes
  `servers: [...]` and finds the leader again after a failover.
- `GET /api/v1/cluster` (any node) shows the role, epoch, ISR and follower
  progress. It replaces `/api/v1/cluster/followers`.
- New settings: `cluster.client_advertise`, `node_id`, `acks`,
  `min_insync_replicas`, `replica_lag_max_ms`, `ack_timeout_ms`,
  `unclean_leader_election`. Removed: `follower_queue_records`. Lease
  defaults are now a 15 s TTL and a 3 s heartbeat. A replicator credential is
  only required when auth is on.
- Tests: in-process multi-node clusters over an in-memory lease backend.
  They cover replication of every write kind, failover without losing
  acknowledged writes, divergent-leader truncation, follower restart and
  `min_insync_replicas`. A shared conformance suite runs against the
  memory, Postgres and Redis lease backends.
#### Cluster metadata in the log

- ExQL query definitions, ExQL connections and API-created connector configs
  move from node-local files into compacted internal streams written through
  the single write path: `__exql_queries`, `__exql_connections` and
  `__connectors` (key = id, value = JSON definition, delete = tombstone).
  They replicate with the log, and writes are leader-only.
- The catalogs are reloaded at the start of every leader tenure, so a
  promoted follower runs the queries and connectors the old leader had.
- Migration: on first start, the leader imports `exql/queries/*.json`,
  `connections/*.json` and `connectors/*.json` into the streams and renames
  those directories to `<dir>.migrated`.
- Connections defined in `connections.d/` or the environment can no longer
  be deleted through the API (`409`); unparseable `connections.d/` files are
  logged instead of silently skipped.

#### Crash safety and HA hardening

- Kill -9 loop tests against the real server binary, under concurrent
  single and batch publishes and a pulling consumer.
- Real-disk ENOSPC tests on a small tmpfs (CI mounts one): writes fail
  cleanly, acknowledged data stays readable, writing resumes once space
  frees. A full disk now answers `507` on TCP and HTTP instead of `500`.
- Seeded, model-based storage property tests (appends, restarts,
  torn-tail crashes, retention).
- TLS on the cluster port (`cluster.tls`, `cluster.tls_ca`). The Helm chart
  turns it on with `tls.secretName`.
- `cluster.acks = "quorum"` with `cluster.size`: `all`, plus never fewer
  than a majority of nodes in sync.
- Jepsen-style randomized partition and restart test, and a kill -9 test
  of a real three-process cluster on a Postgres lease.

#### Fixes from the final review

Every finding in `docs/history/2026-10-review.md` was re-checked; §7 there lists each one
with the test that proves it.

**Breaking**

- Metrics: every series is now `exspeed_`-prefixed and counters end in
  `_total` exactly once (for example `consumer_lag` →
  `exspeed_consumer_lag`, `exspeed_auth_denied_total_total` →
  `exspeed_auth_denied_total`). Dead series are removed. See
  `docs/operations.md`.
- A record may carry at most 64 KiB of header keys and values in total, so
  every reply fits in a 16 MiB frame. Published subjects may not contain
  `*`, `>` or control characters.
- ExQL `subject_matches` with an invalid pattern is a query error.
- `postgres_outbox` idempotency keys are `pgoutbox:<schema.table>:<id>`;
  CDC-only settings are rejected in poll mode.
- `GET /api/v1/streams` lists only streams the caller has a permission on,
  and internal `__` streams only with `?internal=true` for global admins.
  HTTP writes to internal streams answer `403`.

**Fixed**

- Graceful shutdown keeps writes open until consumers have saved their
  final state; the HTTP task is joined.
- Startup fails on a port already in use, an unreadable catalog or bad TLS
  files, and `/readyz` only reports ready after both listeners are bound. A
  tenure whose catalog reload fails steps down for real.
- A corrupt sealed segment fences only its partition instead of stopping
  startup; fenced partitions show in `/readyz` (`degraded`), the
  `exspeed_partition_failed` gauge and stream info.
- An undecodable first frame is answered with Error 400.
- Followers behind the leader's earliest offset re-replicate instead of
  keeping records the leader dropped.
- DLQ writes no longer collide after the source stream is recreated.
- ExQL: `ORDER BY offset LIMIT n` honours the limit; continuous `FILTER`,
  `COALESCE` and `NVL` work; out-of-range `TIMESTAMP BY` values fall back
  to the record timestamp; `->` works in numeric contexts; big JSON
  integers compare exactly.
- Connectors: pgoutput errors restart from the saved LSN; webhooks create
  their stream; a JDBC record stuck on an unclassified error is
  dead-lettered; http_poll keeps validators only after a readable body.
- The `examples/order-processing` configs load, and CI validates every
  example config.
- In a cluster, readers on the leader could see a record before the
  in-sync followers had it, so a record that a failover then dropped could
  already have been delivered. User streams now have a read floor (Kafka's
  high watermark): readers see a record once every in-sync replica has it;
  followers hold their readers to the leader's floor. With `acks = leader`
  a write becomes readable once the followers have it. The replication
  protocol between nodes changed (fetch positions and responses carry high
  watermarks): upgrade every node together.
- `storage.dedup_window_secs` now sets the dedup window of new streams;
  it only fed an internal fallback before.
- `exspeed restore --force` removes the target's `cluster/` state (stale
  epoch histories) instead of a `replication/` directory nothing creates.
- A webhook connector with a `[transform]` is rejected; the transform was
  silently ignored.
- The old environment names `EXSPEED_CONSUMER_STORE` and
  `EXSPEED_OFFSET_STORE_{POSTGRES_URL,POSTGRES_SCHEMA,REDIS_URL}` are still
  read as aliases of `EXSPEED_LEASE_BACKEND` and the lease connection
  settings, but are no longer documented; use the `EXSPEED_LEASE_*` names.
- Publishes on one TCP connection were applied one at a time, each waiting
  for its own fsync before the next request was read, so a pipelining
  client got about one record per fsync (under 100/s on a busy host). The
  connection now feeds an ordered publish pipeline that appends queued
  publishes together and keeps reading.

**Added**

- `[exql]` config section (all ExQL settings, plus
  `max_event_time_skew_ms`), `[server] handshake_timeout_secs`,
  `idle_timeout_secs`, `stop_timeout_secs` and `metrics_token`.
- External Postgres tables push projection and filters into the remote
  query, count snapshots against the memory pool and map `numeric(p, s)` to
  decimals.
- `GET /api/v1/streams/{name}/records?wait_ms=` long-polls; `exspeed tail`
  uses it, works on followers and needs only subscribe permission.
- `exspeed healthcheck` derives its URL from the server config
  (`EXSPEED_HEALTHCHECK_URL` overrides).
- In-process servers accept pre-bound listeners
  (`ServerArgs::tcp_listener` / `api_listener`).

</details>

## [0.5.0] — 2026-04-24

Indexing release. Queries on timestamp, key, and payload fields are now
index-aware — orders of magnitude faster on large streams.

### Indexing

- **Timestamp seek.** `WHERE timestamp > X` uses the existing TimeIndex
  (`.tix`) on sealed segments to jump to the approximate position via
  `seek_by_time()` instead of scanning from offset 0.
- **Bloom filters on key.** Per-segment `.bloom` files are built at seal
  time. `WHERE key = 'x'` skips sealed segments whose bloom filter
  proves the key is absent.
- **Secondary indexes on payload fields.**
  `CREATE INDEX idx ON stream(payload->>'field')` builds sorted
  `(hash, offset)` index files (`.sidx`) per segment. Existing sealed
  segments are backfilled on creation. The planner automatically
  detects indexed payload predicates and uses `IndexScan` for targeted
  offset lookups instead of sequential scans.
- **Index management API.** `POST /api/v1/indexes`,
  `GET /api/v1/indexes`, `DELETE /api/v1/indexes/{name}`.
- **CLI support.** `exspeed query "CREATE INDEX ..."` and
  `exspeed query "DROP INDEX ..."` route to the indexes API.
- **Startup reload.** Saved index definitions are loaded from
  `{data_dir}/indexes/` at startup and registered on partitions.

### Connectors

- **`key_field` config.** New global connector option in `[connector]`
  section. Extracts record keys from JSON payloads for any source
  connector. Plugin-specific key logic takes precedence.

### Bug fixes

- **Timestamp cross-type comparison.** `WHERE timestamp > 1000` now
  compares numerically instead of falling through to string comparison
  (`Value::Timestamp` vs `Value::Int`).

## [0.4.1] — 2026-04-24

ExQL query engine hardening — correctness fixes, safety nets, and
ORDER BY optimizations.

### Bug fixes

- **COUNT(column) now skips NULLs.** The accumulator had a tautological
  condition that counted all rows regardless. COUNT(*) still counts
  all rows; COUNT(column) skips NULLs per SQL semantics.
- **DISTINCT aggregates now work.** The `distinct` flag on
  `SUM(DISTINCT x)`, `COUNT(DISTINCT x)`, etc. was silently discarded.
  Values are now deduplicated per group before accumulation.
- **NULL join keys no longer match.** Hash join converted NULL to the
  string "NULL", causing all NULL-keyed rows to match. NULLs are now
  excluded from the lookup (SQL semantics: NULL ≠ NULL).
- **ReDoS in LIKE patterns.** Replaced recursive backtracking
  (`like_match_recursive`) with an iterative O(n×m) two-pointer
  algorithm. Pathological patterns like `%a%b%c%d%...%` no longer
  cause exponential CPU usage.
- **`last_offset()` no longer allocates entire record vec.** Scans
  sequentially with O(1) memory instead of `read_all()`.

### Safety nets

- **Default result row limit (10,000).** Bounded queries without an
  explicit LIMIT are capped at 10,000 rows to prevent OOM. Users
  override with an explicit `LIMIT` clause.
- **GROUP BY cardinality limit (100,000).** Queries exceeding 100,000
  distinct groups return an error instead of consuming unbounded memory.
- **Hash join right-side limit (100,000).** Joins against streams
  exceeding 100,000 rows on the right side return an error.

### Performance

- **ORDER BY offset ASC sort elimination.** Since offsets are
  monotonically increasing in storage, `ORDER BY offset ASC` removes
  the Sort operator entirely.
- **ORDER BY offset DESC LIMIT N reverse scan.** Reads the last N
  records directly from the tail of the stream — no full scan or sort.
- **TopN operator.** For all other `ORDER BY ... LIMIT N` queries
  (timestamp, key, subject), replaces the full Sort with a bounded
  binary heap using O(N) memory.

## [0.4.0] — 2026-04-24

ExQL query engine improvements — structured errors, predicate pushdown,
and SQL queries over the TCP wire protocol.

### Bug fixes

- **Active segment full-scan on read.** `read_from()` on the active
  (unsealed) segment decoded the entire file then filtered, making
  every bounded query O(total_records) regardless of LIMIT. Now uses
  sequential read that stops after the requested batch size.

### ExQL improvements

- **Structured error messages.** Parse errors now include line/column
  position extracted from the SQL parser. Unsupported-feature errors
  include a hint listing what ExQL does support. All error responses
  (HTTP and TCP) return a JSON object with `error`, `code`, and
  context-specific fields (`line`, `column`, `hint`).
- **Predicate pushdown.** When a `WHERE` clause sits directly above a
  stream scan, the filter is absorbed into the scan operator. Rows are
  evaluated during storage batch reads — non-matching rows are never
  materialized, significantly improving performance on large streams
  with selective predicates.
- **TCP query protocol.** Bounded SQL queries can now be executed over
  the binary TCP protocol via `OpCode::Query` (0x20) /
  `OpCode::QueryResult` (0x85). JSON-encoded results match the HTTP
  API format.

### TypeScript SDK

- **`client.query(sql)`** — execute bounded SQL queries over TCP.
  Returns `QueryResult` with `columns`, `rows`, `rowCount`,
  `executionTimeMs`. Errors throw `QueryError` with structured `code`,
  `line`, `column`, `hint` fields.

### Documentation

- Fixed all connector TOML examples in the README to use the correct
  `[connector]` section format with `type` (not bare `connector_type`).
- Added documentation for JDBC poll source, SQLite JDBC sink, and
  SQL Server JDBC sink connectors.
- Fixed JDBC sink `connection_url` → `connection` in README example.

## [0.3.0] — 2026-04-23

Connector-framework release. Substantially extends the connector surface
with new dialects, new connectors, and cross-cutting resilience primitives.
All changes are backwards-compatible — existing connector configs continue
to parse and run unchanged.

### New connectors + dialects

- **JDBC sink: SQL Server dialect.** New `mssql://` / `sqlserver://` scheme
  support. MERGE + WITH (HOLDLOCK) upsert grammar, ISJSON check-constraint
  on JSON columns, DATETIMEOFFSET for timestamptz. Backed by `tiberius` +
  `bb8-tiberius` (sqlx does not support MSSQL). E2E coverage against
  SQL Server 2022 Developer edition.
- **JDBC sink: SQLite dialect.** New `sqlite:` scheme support via sqlx's
  native SQLite driver. Standard `ON CONFLICT ... DO UPDATE` upsert
  (SQLite 3.24+).
- **`jdbc_poll` source (new connector).** Periodically polls a SQL table
  for rows whose `tracking_column` exceeds the last-seen value, emitting
  each row as JSON. Supports Postgres, MySQL, SQLite (via sqlx), and SQL
  Server (via tiberius). Uses dialect-specific SQL (`LIMIT` vs `TOP(N)`).
- **`mssql_cdc` source (new connector).** Streams insert/update/delete
  events from SQL Server tables with Change Data Capture enabled. Reads
  from `[cdc].[fn_cdc_get_all_changes_<capture_instance>]`; persists the
  last-processed Log Sequence Number as a hex string.

### Connector resilience framework

- **`RetryPolicy` primitive.** Full-jitter exponential backoff; per-
  connector `[retry]` TOML section configures `max_retries`,
  `initial_backoff_ms`, `max_backoff_ms`, `multiplier`, `jitter`. Applied
  by the manager on whole-batch transient failures and on source-poll
  errors.
- **Dead Letter Queue (`DlqWriter`).** Routes poison records to a
  configurable exspeed stream (`dlq_stream` setting) with
  `exspeed-dlq-*` metadata headers (origin, reason, detail, original
  offset, timestamp). Original payload preserved byte-identically so the
  DLQ stream is replay-ready.
- **`on_transient_exhausted` dispatch.** New config option selects post-
  exhaustion behavior: `halt` (stop the connector), `dlq_batch` (route
  the remaining batch to DLQ), or `loop_forever` (default; preserves
  pre-0.3 behavior).
- **Enriched `WriteResult`.** Sinks now return `Poison { poison_offset,
  reason, record, … }` or `TransientFailure { error, … }` instead of
  the opaque `AllFailed`. Each built-in sink (JDBC, HTTP, RabbitMQ, S3)
  classifies its errors explicitly.
- **JDBC sink SQLSTATE classifier.** PK violations (Postgres 23505,
  MySQL 1062, MSSQL 2627) are treated as duplicate-ignored; NOT NULL
  (23502), numeric overflow (22003), invalid text (22P02) are routed as
  `Poison`; anything else is `Transient`.
- **HTTP sink: retry delegated to manager.** The inline
  `1 << attempt` backoff loop is removed; 4xx → `Poison`, 5xx/network
  → `TransientFailure`. Manager applies `RetryPolicy`. Existing
  `retry_count` setting is deprecated.

### Observability

New OpenTelemetry counters on every connector:
- `exspeed_connector_dlq_total{connector, reason}`
- `exspeed_connector_dlq_failures_total{connector}`
- `exspeed_connector_retry_attempts_total{connector, outcome}`
- `exspeed_connector_transient_exhausted_total{connector, action}`

### Tests

- 4 always-runnable DLQ + retry E2E tests (axum mock HTTP sink, TCP fetch
  protocol for DLQ stream inspection).
- 2 SQLite sink E2E tests.
- 1 SQLite `jdbc_poll` E2E test.
- 6 MSSQL sink E2E tests (DB-gated on `EXSPEED_MSSQL_URL`).
- 1 MSSQL `jdbc_poll` E2E test (DB-gated).
- 9 SQLSTATE classifier unit tests; 8 RetryPolicy unit tests; 7 DlqWriter
  and `PoisonReason` unit tests; 8 `jdbc_poll` unit tests; 7 `mssql_cdc`
  unit tests.

### Breaking (internal only)

- `WriteResult::AllFailed` removed from `exspeed-connectors` public
  types. Crate is not published to crates.io; the public surface of the
  `exspeed` binary (TCP wire protocol, HTTP API, CLI) is unchanged.

## [0.2.0] — 2026-04-21

First public release since 0.1.1. Rolls up multiple internal milestones
(perf-overhaul-v0.2, perf-round-2, profile-driven fixes, storage
unification) into a single tagged version.

### Performance

On a macbook-laptop (APFS+NVMe), relative to 0.1.1:

| Metric                       | 0.1.1          | 0.2.0           | Δ                |
|------------------------------|----------------|-----------------|------------------|
| Publish 1 KB sync msg/s      | ~225           | 7,010           | ~31×             |
| Publish 1 KB async msg/s     | n/a            | 69,728          | new mode         |
| E2E p50 @ 10k/s sync         | 250+ ms        | 15.8 ms         | −94%             |
| E2E p99 @ 10k/s sync         | 250+ ms        | 60.8 ms         | −76%             |
| E2E p99 @ 10k/s async        | n/a            | 23.3 ms         | new mode         |

Sync throughput is fsync-bound on macOS APFS (`F_FULLFSYNC` ~5 ms). The
v0.2 group-commit writer already amortized fsyncs across records, so the
storage-unification work in 0.2.0 did not change sync throughput — its
wins are elsewhere: sync p50 was halved, async throughput climbed 61%
over the previous internal milestone, and async p99 dropped 73%.

### Storage — breaking on-disk change

- The separate `wal.log` file is gone. Segments are now the sole journal.
- On startup, if a legacy `wal.log` is found in any partition directory,
  the server fails fast with remediation instructions. Wipe your data
  dir or downgrade to 0.1.1.
- Write path does one encoding, one CRC pass, one write, one fsync per
  batch (sync mode) or per timer tick (async mode).
- Crash recovery is a CRC-validating tail scan of the active segment,
  truncating the first torn or corrupt frame.

### Wire protocol

- New opcodes: `PublishBatch = 0x0A` (client → server) and
  `PublishBatchOk = 0x8A` (server → client). Per-record results.
  Legacy `Publish` / `PublishOk` still work.
- `RecordsBatch = 0x83` now used for batched push delivery.

### Client SDKs

- Rust SDK: new `Publisher` with transparent coalescing (default 100µs
  window, 256 records max) plus explicit `publish_batch(Vec<req>)`.
- TypeScript SDK: same Publisher pattern (`batchWindowMs` option,
  `publishBatch(stream, reqs)`).

### Operational tuning (new flags)

- `--storage-sync sync|async` (env `EXSPEED_STORAGE_SYNC`)
- `--storage-flush-window-us` / `--storage-flush-threshold-records` /
  `--storage-flush-threshold-bytes`
- `--storage-sync-interval-ms` (async only)
- `--delivery-buffer`

See README's "Operational tuning" section for defaults and trade-offs.

### Broker

- Per-partition group-commit writer coalesces concurrent `storage.append`
  callers into shared fsyncs.
- Consumer-store saves are debounced (100ms per-consumer, latest wins)
  and moved off the Ack hot path.
- `DashMap` replaces `RwLock<HashMap>` on partition / appender / syncer
  maps for lock-free publish-path reads.

### Testing

- Reproducible comparison kit under `bench/` (Kafka via docker-compose;
  NATS instructions in BENCHMARKS.md).
- `exspeed-bench` harness: publish, latency, fanout, continuous-query
  scenarios.

## [0.1.1] — 2026-04-20

### Fixed

- **Consumer state is now persisted atomically.** Offsets under
  `{data_dir}/consumers/` are written via tempfile + rename + parent-dir fsync
  instead of open-with-truncate + write. A crash during a save previously
  left a truncated / empty JSON file that failed to parse on next startup,
  silently losing the consumer's offset. Stray `*.json.tmp` files from a
  crashed save are safely ignored by the loader.
- **`StorageEngine::read` no longer silently skips trimmed-away history.**
  When a consumer asks for an offset that retention has already deleted,
  both `FileStorage` and `MemoryStorage` now return
  `StorageError::OffsetOutOfRange { requested, earliest }` instead of
  quietly jumping forward to the first surviving record. The broker's
  delivery task logs a warning with both offsets and terminates the
  subscription so the client can re-seek deliberately. Reading past `next`
  (normal tailing) is unchanged — only reading **below earliest** errors.

### Added

- `StorageError::OffsetOutOfRange { requested, earliest }` variant on the
  public `exspeed-streams` API. SDK callers that care about this case can
  match on it; everyone else will see it as a generic error surfaced via
  the existing error path.

### Notes for operators

Upgrading from 0.1.0 is a drop-in replacement — no data migration, no
configuration changes. The only behavior change that might surface is
that a consumer lagging past retention will now see its subscription end
with an `offset X is below earliest retained offset Y` log line, instead
of silently starting to consume from the new earliest.

## [0.1.0] — Initial release

- File-backed stream broker with custom binary wire protocol (TCP :5933)
  and HTTP management API (:8080).
- ExQL: SQL parser, logical plan, physical operators, bounded +
  continuous queries, tumbling windows, stream-stream joins with WITHIN.
- NATS-style subject filtering (`*`, `>`).
- Single-partition-per-stream model with segment rolling, WAL, offset +
  time indexes, CRC32C framing.
- Async replication (Plan G): leader/follower pull-based fan-out with
  cursor persistence and divergent-history recovery.
- Built-in source/sink connectors for Postgres (outbox + CDC), RabbitMQ,
  S3, HTTP (webhook, poller, sink), JDBC.
- TypeScript SDK (`@exspeed/sdk`) implementing the wire protocol.
