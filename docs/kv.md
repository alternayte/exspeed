# Key-value buckets

A **bucket** is a named map of keys to values with history, compare-and-set,
TTLs and change watching. It is built on a stream, so it persists, replicates
and fails over like one, and you can read or query its stream like any other.

## The model

A bucket `config` is the stream `KV_config` with `max_msgs_per_subject` set
to the bucket's history (see [queues.md](queues.md#count-limits)):

- each **key** is a subject (`app.mode`, `users.42.email`);
- each **put** appends a record; the value is the payload;
- a key's **revision** is its record's offset in the stream plus one, so `0`
  always means "absent";
- the stream keeps the newest `history` values (1 to 64) of every key;
  older ones disappear;
- a **delete** appends a tombstone (header `exspeed-kv-op: DEL`); a
  **purge** (`PURGE`) also hides the key's older values from its history.

Gets are O(1): the storage keeps an in-memory index of each subject's newest
offsets (rebuilt from the log at startup).

Keys are dot-separated tokens of letters, digits and `_ - / =` (at most 1024
bytes). Bucket names are `[A-Za-z0-9_-]`.

## Operations

| Operation | What it does |
|-----------|--------------|
| `create(options)` | `history` (default 1), `ttl_ms` (every key expires this long after its last put; 0 = never), `max_bytes`. Idempotent for the same settings. |
| `get(key)` | The current value and revision; nothing when absent, deleted or expired |
| `get_revision(key, rev)` (TS `getRevision`) | A specific revision, while the bucket still keeps it |
| `put(key, value)` | Set the key; returns the new revision. Optionally with a TTL for this value |
| `create_key(key, value)` (TS `createKey`) | Set the key only if it doesn't exist (or was deleted) |
| `update(key, value, rev)` | Set the key only if it is at revision `rev` (compare-and-set) |
| `delete(key)` / `purge(key)` | Tombstone the key (purge also hides its history) |
| `keys(filter)` | Keys that currently have a value, sorted; `filter` is a subject filter (`users.*`) |
| `history(key)` | Kept revisions, oldest first, tombstones included, from the last purge on |
| `watch(filter)` | The current value of every matching key, then every change as it happens |

```ts
// TypeScript
const kv = client.kv("config");
await kv.create({ history: 5 });
const rev = await kv.put("app.mode", "prod");
const entry = await kv.get("app.mode");          // { key, value, revision, op, ... }
await kv.update("app.mode", "maintenance", rev); // throws 409 if someone wrote in between
for await (const change of kv.watch("app.>")) console.log(change.key, change.op);
```

```rust
// Rust
let kv = client.kv("config");
kv.create(BucketOptions { history: 5, ..Default::default() }).await?;
let rev = kv.put("app.mode", "prod").await?;
kv.update("app.mode", "maintenance", rev).await?;
let mut w = kv.watch("app.>");
let change = w.next().await?;
```

## Compare-and-set

`create` and `update` (and a delete with an expected revision) compare the
key's current revision with the expected one and write only if they match;
otherwise they fail with `409` and the current revision in the error detail
(`current_revision`). Writes to a bucket run one at a time on the leader and
compare against the committed log, values still replicating included, so of
two concurrent writers expecting the same revision exactly one wins. Use it
for counters, leases and configuration that must not lose an update.

Write through the KV API: a record published to the bucket's stream directly
bypasses the check.

## TTLs

A bucket's `ttl_ms` expires every key that long after its last put; a single
put can carry its own TTL. An expired key reads as absent and disappears from
`keys`; compaction later removes it from disk.

## Watching

`watch` first delivers the current value of each live key matching the
filter (deleted keys left out), oldest revision first, then follows the
bucket and delivers every put, delete and purge as it happens. Clients build
it on stateless long-poll reads of the bucket's stream, so a watch costs no
server-side state and resumes from where it was after a reconnect.

## HTTP

`POST /api/v1/kv` creates a bucket; `GET/PUT/DELETE
/api/v1/kv/{bucket}/{key}` read and write a key (`If-Match: <revision>` for
compare-and-set, `If-None-Match: *` for create-only); `GET
/api/v1/kv/{bucket}` lists keys; `.../history` lists revisions. See
[http-api.md](http-api.md#key-value-buckets).

## Permissions

A bucket's operations check its stream `KV_<bucket>`: reads need
`subscribe`, puts and deletes `publish`, creating a bucket `admin`.
