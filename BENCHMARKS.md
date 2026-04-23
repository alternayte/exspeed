# Exspeed Benchmarks

_Last refreshed: **2026-04-21** (git `9782c1d`, exspeed 0.2.0)_

## How these numbers were produced

- **Host:** macbook-laptop — 8 vCPU, 16 GB RAM, apfs-nvme
- **OS / kernel:** macOS 26.2 / 25.2.0
- **Broker + workload driver on the same box.**
- **Profile:** `Local`.
- **git_sha:** `9782c1d`

## How to reproduce

```bash
git checkout 9782c1d
cargo build --release -p exspeed-bench
# Sync mode (default, durable)
./target/release/exspeed-bench all --profile reference \
  --output bench/results/refresh-sync.json
# Async mode (opt-in — start broker with EXSPEED_STORAGE_SYNC=async)
./target/release/exspeed-bench all --profile reference \
  --output bench/results/refresh-async.json
./target/release/exspeed-bench render bench/results/refresh-sync.json --out BENCHMARKS.md
```

## Publish (sync mode — default, durable)

| Payload | msg/s | MB/s | Duration |
|---------|-------|------|----------|
| 1024 B | 7010 | 7.2 | 5.1s |

## Latency (sync mode)

At a sustained **10k** msg/s (payload 1024 B) over 10s:

| p50 | p90 | p99 | p99.9 | p99.99 | max |
|-----|-----|-----|-------|--------|-----|
| 15847µs | 49183µs | 60831µs | 73151µs | 76159µs | 76159µs |

## Fan-out (sync mode)

| Consumers | Producer rate | Aggregate consumer rate | Max lag (msgs) |
|-----------|---------------|-------------------------|----------------|
| 1 | 5000 | 164 | 0 |
| 4 | 5000 | 424 | 0 |

## Async-sync mode

With `--storage-sync=async`, Exspeed batches fsync on a timer (default
10 ms) rather than per-flush. On a crash you may lose up to one tick of
acked data. Numbers from `bench/results/2026-04-21-laptop-v020-async.json`:

### Publish (async)

| Payload | msg/s | MB/s | Duration |
|---------|-------|------|----------|
| 1024 B | 69728 | 71.4 | 5.0s |

### Latency (async) at sustained 10k msg/s

| p50 | p90 | p99 | p99.9 | p99.99 | max |
|-----|-----|-----|-------|--------|-----|
| 11055µs | 16447µs | 23343µs | 28623µs | 29167µs | 29167µs |

### Fan-out (async)

| Consumers | Producer rate | Aggregate consumer rate | Max lag (msgs) |
|-----------|---------------|-------------------------|----------------|
| 1 | 5000 | 165 | 0 |
| 4 | 5000 | 334 | 0 |

### Summary

| Workload                | Sync (default, durable)        | Async (opt-in)                |
|-------------------------|--------------------------------|-------------------------------|
| Publish, 1 KB payload   | 7,010 msg/s                    | 69,728 msg/s                  |
| E2E latency @ 10k/s     | p50 15.8 ms / p99 60.8 ms      | p50 11.1 ms / p99 23.3 ms     |
| Fan-out @ 4 consumers   | 424 msg/s aggregate            | 334 msg/s aggregate           |

Keep the default (sync) for production unless throughput is critical
and you understand the data-loss tradeoff. This trade matches NATS
JetStream's default (async); our default is stricter (sync) because
durability is our differentiator.

> **ExQL note:** The binary-search found no candidate that sustained
> ≥95% of any target rate above the 5,000 msg/s floor on macOS. This
> reflects APFS write overhead under the dual publish+query load, not
> a bug in the query engine. The ExQL row is omitted from tables to
> avoid publishing an "N/A" number.

## Comparison to prior versions

Same laptop, same workload, three successive pre-release milestones.
The v0.3 line is the internal milestone immediately before storage
unification; v0.2.0 is what ships now.

### Sync mode (default, durable)

| Version  | Publish 1 KB msg/s | p50 latency @ 10k/s | p99 latency @ 10k/s |
|----------|-------------------:|--------------------:|--------------------:|
| 0.1.1    | ~225               | 250+ ms             | 250+ ms             |
| v0.3     | 7,406              | 33.3 ms             | 65.9 ms             |
| 0.2.0    | 7,010              | 15.8 ms             | 60.8 ms             |

### Async mode (opt-in)

| Version  | Publish 1 KB msg/s | p50 latency @ 10k/s | p99 latency @ 10k/s |
|----------|-------------------:|--------------------:|--------------------:|
| 0.1.1    | n/a (not offered)  | n/a                 | n/a                 |
| v0.3     | 43,382             | 41.4 ms             | 86.9 ms             |
| 0.2.0    | 69,728             | 11.1 ms             | 23.3 ms             |

### Honest commentary

- **Sync throughput is essentially flat from v0.3 → 0.2.0** (7,406 → 7,010
  msg/s, a wash). Single-writer sync publish on APFS is bounded by
  `F_FULLFSYNC` (~5 ms per fsync on this laptop), and the v0.2
  group-commit writer had already amortized fsyncs across records. Storage
  unification did not unlock more sync throughput — it wasn't the
  bottleneck.
- **The real sync-mode win is latency.** p50 dropped from 33.3 ms to
  15.8 ms (roughly halved) because there is now one encode/CRC/write/fsync
  per batch instead of two (WAL + segment). p99 also improved (65.9 → 60.8
  ms), but the tail is still APFS-dominated.
- **Async publish throughput rose 61%** (43,382 → 69,728 msg/s). In async
  mode the fsync isn't on the publish hot path, so removing the duplicate
  write/encode path actually shows up in throughput.
- **Async p99 latency dropped 73%** (86.9 → 23.3 ms). This is the most
  dramatic single improvement — one write path per record produces a much
  tighter tail under load.
- Compared to the 0.1.1 baseline, sync publish is ~31× faster and sync p50
  latency is ~94% lower; but that delta is the cumulative work of every
  milestone between 0.1.1 and 0.2.0 (perf-overhaul-v0.2, perf-round-2,
  profile-driven fixes, and now storage unification), not storage
  unification alone.

## Comparison to NATS JetStream

To compare Exspeed v0.2.0 to NATS JetStream on your hardware, see
[`bench/README.md`](bench/README.md) for methodology. We don't publish
comparison numbers in this file — run your own measurement.

Note on mode semantics: Exspeed **sync** mode is a genuine durable
group-commit + fsync-per-batch, whereas NATS JetStream's "sync" mode is
closer to wait-for-in-memory-ack. Exspeed **async** mode (fsync on a timer)
is the apples-to-apples comparison to NATS JetStream's default async fsync.

## ExQL bounded scan (pre-streaming)

Baseline before the streaming-scan / lazy-payload rewrite (plan
`docs/superpowers/plans/2026-04-23-exql-streaming-scan-lazy-payload.md`).

| Case | Stream size | Baseline |
|------|-------------|----------|
| `SELECT * FROM s LIMIT 5` | 500 000 rows / 82 MB (SQL Server CDC payloads) | ~36 000 ms (user-reported) |

Root cause (`crates/exspeed-processing/src/runtime/bounded.rs:108-125`): the
bounded executor loads the entire stream and eager-parses every payload to
`serde_json::Value` before `LimitOperator` ever runs. 82 MB ÷ 36 s ≈ 2.3 MB/s,
well below disk throughput — CPU-bound JSON parse + allocation.
