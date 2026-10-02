# Architecture

This page describes the system **as it is today**. The proposed target
architecture is in [REVIEW.md §5](REVIEW.md#5-proposed-target-architecture).

## Crates

The workspace has ten crates. Each one depends only on crates listed above
it.

```
exspeed-common       shared types (StreamName, Offset), subject matching, auth store, metrics
exspeed-streams      StorageEngine trait (async), Record / StoredRecord
exspeed-protocol     wire protocol: Frame codec, OpCodes, Client/Server messages, replication messages
exspeed-storage      FileStorage (segments, indexes, retention), MemoryStorage, S3 tiering (experimental)
exspeed-broker       BrokerAppend (dedup), consumers, delivery, ack/nack, DLQ, leases, replication
exspeed-connectors   connector manager, retry/DLQ, offset stores, built-in plugins
exspeed-processing   ExQL: parser → logical plan → physical operators → bounded / continuous runtime
exspeed-api          Axum HTTP API, auth and leader-gate middleware, webhooks
exspeed              binary: CLI + server bootstrap (cli/server.rs)
exspeed-bench        benchmark harness (not shipped in the image)
```

## Data flow

```
 Producers ──TCP──► server.rs handle_connection ──► broker handlers ──► BrokerAppend ──► FileStorage
           ──HTTP─► exspeed-api ───────────────────────────────────────► BrokerAppend ──► FileStorage
 Webhooks  ──HTTP─► exspeed-api/webhooks ───────────────────────────────────────────────► FileStorage
 Sources   ───────► connector manager ─────────────────────────────────► BrokerAppend ──► FileStorage
 ExQL out  ───────► continuous runtime ───────────────────────────────────────────────► FileStorage

 FileStorage ──► delivery task (one per subscription) ──mpsc──► connection ──TCP──► consumers
            ──► sink connectors ──► external systems
            ──► continuous queries / materialized views
            ──► replication server (leader) ──TCP 5934──► followers
```

Writes reach storage along several different paths. Each path applies a
different subset of dedup, metrics, replication and leader checks; see
[REVIEW.md §1](REVIEW.md#1-verdict). This is the main structural problem
that the rebuild addresses.

## Wire protocol

Every frame starts with a 10-byte header:

```
[version u8][opcode u8][correlation_id u32][payload_len u32][payload …]
```

- Each response carries the correlation ID of the request it answers.
- Push-delivered records use correlation ID `0`, with opcode `Record`
  (0x82) or `RecordsBatch` (0x83).
- Frames are capped at 16 MB.

## Storage layout

```
<data-dir>/
  .exspeed.lock                      exclusive flock held by the running server
  credentials.toml                   optional, auto-detected
  streams/<stream>/
    stream.json                      retention + dedup config
    dedup_snapshot.bin               periodic dedup-map snapshot
    partitions/0/
      00000000000000000000.seg       append-only segment (CRC32C-framed records), rolls at 256 MB
      00000000000000000000.idx       offset index      (written when the segment is sealed)
      00000000000000000000.tix       time index        (written when the segment is sealed)
      00000000000000000000.bloom     key bloom filter  (written when the segment is sealed)
      00000000000000000000.sidx.<name>  secondary index (if any)
  consumers/<name>.json              consumer state (file backend)
  connectors.d/*.toml                connector configs (hot-reloaded)
  connectors/                        persisted connector configs + file offsets
  queries/                           continuous query registry + checkpoints
  indexes/<name>.json                secondary index definitions
```

The active (last) segment is recovered at startup by a CRC-validating tail
scan. It is truncated at the first torn frame it finds.

## Server startup sequence

`crates/exspeed/src/cli/server.rs` starts the server in this order:

1. Take the data-dir `flock`.
2. Load credentials and TLS.
3. Open `FileStorage`, with tail-scan recovery. Wrap it in S3 tiering if
   that is enabled.
4. Build `BrokerAppend`, then rebuild the dedup maps in the background.
   These come from the snapshot when one exists, otherwise from a scan.
5. Build the consumer store, lease backend, work coordinator, and
   `ClusterLeadership`.
6. Build the `Broker` and load the persisted consumers.
7. Build the `ConnectorManager` and `ExqlEngine`, and load their configs,
   queries and indexes.
8. Spawn the background tasks: retention, dedup snapshot, queue depth, and
   the leader supervisor. When this pod becomes leader, the supervisor
   starts the connectors and continuous queries, then starts the
   replication server or client.
9. Spawn the HTTP API.
10. Enter the TCP accept loop, which spawns one task per connection.

## Concurrency model

- **One tokio runtime.**
- **Appends:** each partition has an appender task that does group commit.
  Fsync runs on the runtime threads.
- **Subscriptions:** each one gets a delivery task that polls storage in
  batches of 100 every 50 ms. It filters by subject and sends batches over
  an mpsc channel to the connection task.
- **Connectors:** one task each, under the leader's cancellation token.
- **Continuous queries:** one task each, under the leader's cancellation
  token.
