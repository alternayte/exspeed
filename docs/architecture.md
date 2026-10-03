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
exspeed-storage      FileStorage (segments, sparse indexes, retention, compaction), MemoryStorage
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
  .trash/                            streams being deleted (cleared on startup)
  streams/<stream>/
    stream.json                      retention, dedup and compaction config
    dedup_snapshot.bin               periodic dedup-map snapshot
    partitions/0/
      00000000000000000000.seg       append-only segment (CRC32C-framed records), rolls at 256 MB
      00000000000000000000.idx       sparse offset + time index, one entry about every 4 KiB
      00000000000000000000.meta      sealed-segment metadata (offsets, timestamps, length)
      truncate.json                  only while a truncation is in progress
  consumers/<name>.json              consumer state (file backend)
  connectors.d/*.toml                connector configs (hot-reloaded)
  connectors/                        persisted connector configs + file offsets
  queries/                           continuous query registry + checkpoints
  indexes/<name>.json                ExQL index definitions (storage builds no index files)
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
implemented by `FileStorage` and `MemoryStorage` and is not used by the
replication code yet.

## Server startup sequence

`crates/exspeed/src/cli/server.rs` starts the server in this order:

1. Take the data-dir `flock`.
2. Load credentials and TLS.
3. Open `FileStorage`. This recovers each stream's active segment with a
   tail scan and starts one writer thread per stream and the compactor.
4. Build `BrokerAppend`, then rebuild the dedup maps in the background.
   These come from the snapshot when one exists, otherwise from a scan.
5. Build the consumer store, lease backend, work coordinator, and
   `ClusterLeadership`.
6. Build the `Broker` and load the persisted consumers.
7. Build the `ConnectorManager` and `ExqlEngine`, and load their configs,
   queries and index definitions.
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
