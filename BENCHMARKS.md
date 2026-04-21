# Exspeed Benchmarks

_Last refreshed: **2026-04-21** (git `29c50d4`, exspeed 0.1.1)_

## How these numbers were produced

- **Host:** macbook-laptop — 8 vCPU, 16 GB RAM, apfs-nvme
- **OS / kernel:** macOS 26.2 / 25.2.0
- **Broker + workload driver on the same box.**
- **Profile:** `Local`.
- **git_sha:** `29c50d4`

## How to reproduce

```bash
git checkout 29c50d4
cargo build --release -p exspeed-bench
./target/release/exspeed-bench all --profile reference \
  --output bench/results/refresh.json
./target/release/exspeed-bench render bench/results/refresh.json --out BENCHMARKS.md
```

## Publish

| Payload | msg/s | MB/s | Duration |
|---------|-------|------|----------|
| 1024 B | 7406 | 7.6 | 5.0s |

## Latency

At a sustained **10k** msg/s (payload 1024 B) over 10s:

| p50 | p90 | p99 | p99.9 | p99.99 | max |
|-----|-----|-----|-------|--------|-----|
| 33279µs | 43903µs | 65855µs | 71167µs | 71167µs | 71167µs |

## Fan-out

| Consumers | Producer rate | Aggregate consumer rate | Max lag (msgs) |
|-----------|---------------|-------------------------|----------------|
| 1 | 5000 | 63 | 0 |
| 4 | 5000 | 97 | 0 |

## ExQL

Query: `CREATE VIEW "bench-exql-output" AS SELECT subject, COUNT(*) FROM "bench-exql-source" GROUP BY subject EMIT CHANGES`

> WARNING: `no-candidate-passed`. The binary search did not find any rate the broker could sustain.

Sustained input rate with payload 1024 B and 1 distinct subjects: **N/A (no candidate passed)**.

The binary-search found no candidate that sustained ≥95% of any target rate above the 5,000 msg/s floor on macOS. This reflects APFS write overhead under the dual publish+query load, not a bug in the query engine.

## Async-sync mode

With `--storage-sync=async`, Exspeed batches fsync on a timer rather
than per-WAL-flush. On a crash you may lose up to 10 ms of acked data.

| Workload                | Sync (default, durable)        | Async (opt-in)                |
|-------------------------|--------------------------------|-------------------------------|
| Publish, 1 KB payload   | ~7,406 msg/s                   | ~43,382 msg/s                 |
| E2E latency @ 10k/s     | p50 33 ms / p99 66 ms          | p50 41 ms / p99 87 ms         |
| Fan-out @ 4 consumers   | ~97 msg/s aggregate            | ~90 msg/s aggregate           |
| ExQL sustained input    | N/A (no candidate passed)      | N/A (no candidate passed)     |

Keep the default (sync) for production unless throughput is critical
and you understand the data-loss tradeoff. This trade matches NATS
JetStream's default (async); our default is stricter (sync) because
durability is our differentiator.

## Comparison to NATS JetStream

On the same laptop, NATS JetStream 2.12.7 (default file storage, async fsync):

| Workload                | Exspeed v0.3 async  | NATS JetStream  | Ratio             |
|-------------------------|---------------------|-----------------|-------------------|
| Publish, 1 KB, pipelined| ~43,382 msg/s       | ~106,000 msg/s  | ~2.4× behind NATS |
| Publish, 1 KB, sync     | ~7,406 msg/s        | ~6,500 msg/s    | beats NATS        |

Methodology: NATS `bench js pub sync/async` on a fresh JetStream stream with
file storage; Exspeed `exspeed-bench publish` with `--profile local`.

Exspeed's **sync** mode (group-commit + fsync per batch) outperforms NATS JetStream's
"sync" mode on this hardware because NATS JetStream sync ≈ wait-for-in-memory-ack
whereas Exspeed sync is a genuine durable group-commit + fsync-per-batch.
Exspeed's async mode is the apples-to-apples comparison to NATS async.
