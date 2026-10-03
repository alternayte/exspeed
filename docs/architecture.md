# Architecture

This page describes the system **as it is today**. The proposed target
architecture is in [REVIEW.md §5](REVIEW.md#5-proposed-target-architecture).

## Crates

Each crate depends only on crates listed above it.

```
exspeed-common       shared types (StreamName, Offset), subject filters, auth store, metrics
exspeed-streams      StorageEngine trait (async), Record / StoredRecord, StreamConfig
exspeed-protocol     wire protocol: Frame codec, opcodes, client protocol v2 (client.rs), replication messages
exspeed-storage      FileStorage (segments, sparse indexes, retention, compaction), MemoryStorage
exspeed-broker       Log (the single write path), BrokerAppend (dedup), consumers, leases, replication
exspeed-connectors   connector manager, per-connector supervisor, retry/DLQ, offset stores, built-in plugins
exspeed-processing   ExQL: parser → plan → bounded / continuous runtime
exspeed-api          Axum HTTP API, auth and leader-gate middleware, webhooks
exspeed              binary: CLI, server bootstrap (cli/server.rs), TCP sessions (session.rs)
exspeed-client       async Rust client for protocol v2 (also used by the benchmarks and tests)
exspeed-bench        benchmark harness (not shipped in the image)
exspeed-testkit      test helpers
```

## Data flow

```
 Producers ──TCP──► session.rs ─────────┐
           ──HTTP─► exspeed-api ────────┤
 Webhooks  ──HTTP─► exspeed-api ────────┼──► Log ──► (leader gate, validation, dedup) ──► FileStorage
 Sources   ───────► connector manager ──┤            └──► acks=all wait for the ISR, metrics
 ExQL out  ───────► continuous runtime ─┘
 Consumers ───────► __consumers stream ─┘

 FileStorage ──► consumer actors (one per consumer, leader only) ──► subscriptions / pulls ──TCP──► apps
            ──► stateless reads (TCP Read, HTTP /records)
            ──► sink connectors ──► external systems
            ──► continuous queries / materialized tables
            ──► fetch server (leader) ◄──TCP 5934── followers pull (cluster::follower → append_at)
```

Every write, whatever its origin, goes through `exspeed_broker::log::Log`.
It enforces the leader gate, validates records, applies `msg_id` dedup,
appends to storage, waits for the in-sync replicas in a cluster with
`acks = all`, and records metrics. The one other writer is the replication
follower, which applies the leader's records with `StorageEngine::append_at`
while the node is not the leader (see [high-availability.md](high-availability.md)).

## Consumers

`exspeed_broker::consumer::ConsumerManager` runs one actor task per
consumer, only on the leader, under the leadership token. An actor owns a
pure state machine (`consumer/core.rs`), which holds:

- the next offset to read;
- the ack floor;
- the in-flight records, with deadlines and delivery counts;
- the records scheduled for redelivery.

The actor feeds push subscriptions (credit-based) and pull waiters from the
log, reacting to `watch_appends` notifications. It redelivers on timeout,
nack, or subscriber loss; dead-letters through `Log` with idempotency keys;
and persists a snapshot to the compacted `__consumers` stream at most every
100 ms. On promotion, a new leader restores every consumer from
`__consumers`. Delivery is at-least-once.

## Wire protocol

Clients use protocol v2 ([protocol.md](protocol.md)). Every frame has a
10-byte header:

```
[version u8 = 2][opcode u8][correlation_id u32 LE][payload_len u32 LE][payload …]
```

Each TCP connection is served by `crates/exspeed/src/session.rs`:

- a reader loop decodes and dispatches requests;
- one writer task owns the socket;
- requests that wait (pull, long-poll read, query) run concurrently, so
  replies can arrive out of order;
- each subscription has a forwarder task that turns consumer events into
  `Deliver` pushes (correlation id 0).

Frames are capped at 16 MB.

## Storage layout

```
<data-dir>/
  .exspeed.lock                      exclusive flock held by the running server
  credentials.toml                   optional, auto-detected
  .trash/                            streams being deleted (cleared on startup)
  streams/<stream>/
    stream.json                      retention, dedup and compaction config
    dedup_snapshot.bin               periodic dedup-map snapshot
    partitions/0/
      00000000000000000000.seg       append-only segment (CRC32C-framed records), rolls at 256 MB
      00000000000000000000.idx       sparse offset + time index, one entry about every 4 KiB
      00000000000000000000.meta      sealed-segment metadata (offsets, timestamps, length)
      truncate.json                  only while a truncation is in progress
  streams/__consumers/               consumer state (internal compacted stream)
  streams/__connector_offsets/       connector offsets (internal compacted stream)
  connectors.d/*.toml                connector configs (hot-reloaded)
  connectors/                        API-created connector configs (JSON)
  connector-offsets/                 connector offsets (EXSPEED_CONNECTOR_OFFSET_STORE=file only;
                                     the default stores them in the __connector_offsets stream)
  exql/queries/<id>.json             continuous query definitions + desired state
                                     (checkpoints live in the stream __exql_ckpt_<id>)
```

Segment files are named after their base offset and start with a 16-byte
header (magic `EXSG`, format version 2). Older segment files are refused;
there is no migration because nobody runs Exspeed in production yet.

**Writes.** Each partition has one dedicated writer thread. Every change to
the partition (appends, segment rolls, retention, truncation, installing a
compacted segment) runs on that thread, so file IO never blocks the tokio
runtime. Appends are group-committed: the writer collects requests for up
to `--storage-flush-window-us` (or until a flush threshold is reached),
writes them in one `write` and, in `sync` mode, issues one `fdatasync`.
Index entries are appended to the `.idx` file as the segment grows, so a
roll never re-reads a segment. The time column holds the running maximum
timestamp, so it is monotonic even when producer timestamps are not.

**High watermark.** Readers see records only below the high watermark. In
`sync` mode it advances after the fsync; in `async` mode after the write.
`watch_appends` wakes subscribers when it moves. A replication floor hook
(`FileStorage::set_replication_floor`) can hold it back further; nothing
sets it yet.

**Reads.** Readers never take the writer's lock and never fsync. They load
the segment list (swapped atomically by the writer), binary-search it, look
up the sparse index, then `pread` and decode forward. `seek_by_time`
returns the first record with a timestamp at or after the target, across
all segments, or the high watermark if there is none.

**Failures.** If a write or fsync fails, the writer truncates the file back
to the last committed length and returns an error. The failed batch was
never visible or acknowledged, so its offsets are handed out again; no
committed offset is ever reused. If that truncation also fails, the
partition is marked failed: it stays readable, rejects every write, and
reports the reason through `FileStorage::partition_status` and
`failed_streams`. A restart runs recovery and clears it.

**Crash safety.** Sidecar files (`stream.json`, `.meta`, `truncate.json`)
are written to a temporary file, renamed, and the directory is fsynced.
Creating or deleting a segment also fsyncs the directory. `truncate_from`
writes `truncate.json` first and is completed by recovery if the process
dies part-way. At startup only the active segment is scanned: offsets must
be strictly increasing and every frame must pass its CRC. A torn tail is
truncated. Corruption with valid data after it fails startup in `sync`
mode and is truncated (with a warning) in `async` mode. Sealed segments are
opened from their `.meta` file without reading the data.

**Retention** runs every 60 s on the writer thread. It deletes whole sealed
segments from the front of the log, by age (the newest record is older than
`max_age_secs`) or by size (total size above `max_bytes`). The active
segment is never deleted.

**Compaction.** A stream with `compaction = true` in its config is visited
by a background compactor every 60 s. It rewrites sealed segments to keep
only the newest record for each key (looking across the whole log,
including the active segment). Records without a key are always kept. A
record with a key and an empty value is a tombstone: it deletes the key and
is itself removed once it is older than `tombstone_retention_secs`
(default 24 h). Offsets are preserved, so a compacted stream has offset
gaps, and every read path skips them. A rewrite goes to `*.compacting`
files, is fsynced, renamed over the original, and the segment list is then
swapped atomically; a crash at any point leaves either the old or the new
segment. Compaction can only be turned on through `StreamConfig` (for the
internal metadata streams planned in REVIEW.md §6); the HTTP API and CLI do
not expose it yet.

**Replication.** `StorageEngine::append_at` appends records that already
carry their offsets, timestamps and keys. Offsets must be strictly
increasing and at or above the next offset; gaps are allowed. It is
implemented by `FileStorage` and `MemoryStorage` and is how followers apply
replicated records (`exspeed_broker::cluster::follower`).

## Server startup sequence

`crates/exspeed/src/cli/server.rs` starts the server in this order:

1. Take the data-dir `flock`.
2. Load credentials and TLS.
3. Open `FileStorage`. This recovers each stream's active segment with a
   tail scan and starts one writer thread per stream and the compactor.
4. Build `BrokerAppend`, then rebuild the dedup maps in the background.
   These come from the snapshot when one exists, otherwise from a scan.
5. Build the lease backend, bind the cluster port (cluster mode), and build
   the `Broker`, which contains the `Log` and the `ConsumerManager`.
6. In cluster mode build `cluster::Cluster` (epoch store, write-path hooks,
   follower, fetch server). Start `ClusterLeadership` with it as the role
   hooks, and gate writes on leadership. A node starts as a follower;
   on acquiring the lease it stops following, stamps the new epoch on every
   stream and rebuilds dedup state before writes open.
7. Build the `ConnectorManager` and `ExqlEngine`, and load their configs and
   queries.
8. Spawn the background tasks: the dedup snapshot task and the leader
   supervisor. When this pod becomes leader, the supervisor starts the
   consumers (restored from `__consumers`), connectors, continuous queries
   and retention.
9. Spawn the HTTP API.
10. Enter the TCP accept loop, which spawns one task per connection.

## Concurrency model

- **One tokio runtime.**
- **Storage:** one writer OS thread per partition does group commit and all
  file IO, off the tokio runtime. Readers take no lock.
- **Consumers:** one actor task per consumer (leader only), woken by
  `watch_appends`, timers and client commands.
- **Connections:** a reader, a writer and one forwarder per subscription.
- **Connectors:** one supervisor task each, under the leader's cancellation
  token. The supervisor runs the plugin, catches panics and restarts it with
  backoff (see [connectors.md](connectors.md#status-restarts-and-metrics)).
- **Continuous queries:** one task each, under the leader's cancellation
  token.
