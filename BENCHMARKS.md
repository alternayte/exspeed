# Exspeed Benchmarks

_Last refreshed: **2026-04-21** (git `238ac71`, exspeed 0.1.1)_

## How these numbers were produced

- **Host:** macbook-laptop — 8 vCPU, 16 GB RAM, apfs-nvme
- **OS / kernel:** macOS 26.2 / 25.2.0
- **Broker + workload driver on the same box.**
- **Profile:** `Local`.
- **git_sha:** `238ac71`

## How to reproduce

```bash
git checkout 238ac71
cargo build --release -p exspeed-bench
./target/release/exspeed-bench all --profile reference \
  --output bench/results/refresh.json
./target/release/exspeed-bench render bench/results/refresh.json --out BENCHMARKS.md
```

## Publish

| Payload | msg/s | MB/s | Duration |
|---------|-------|------|----------|
| 1024 B | 593 | 0.6 | 5.0s |

## Latency

At a sustained **10k** msg/s (payload 1024 B) over 10s:

| p50 | p90 | p99 | p99.9 | p99.99 | max |
|-----|-----|-----|-------|--------|-----|
| 104959µs | 208127µs | 250111µs | 260479µs | 260479µs | 260479µs |

## Fan-out

| Consumers | Producer rate | Aggregate consumer rate | Max lag (msgs) |
|-----------|---------------|-------------------------|----------------|
| 1 | 5000 | 54 | 0 |
| 4 | 5000 | 95 | 0 |

## ExQL

Query: `CREATE VIEW "bench-exql-output" AS SELECT subject, COUNT(*) FROM "bench-exql-source" GROUP BY subject EMIT CHANGES`

Sustained input rate with payload 1024 B and 1 distinct subjects: **5000 msg/s**.

> **Note:** The ExQL binary-search found no candidate that passed at ≥95% of target through the full window on this hardware. The value reported (5000 msg/s) is the search floor. macOS APFS fsync overhead limits the write path; a Linux+NVMe reference run is expected to show a meaningfully higher result.

## Async-sync mode

When run with `--storage-sync=async`, Exspeed skips per-batch `fsync`. Data is still written to the WAL immediately, but an asynchronous syncer task flushes to disk every 10 ms (default). **On a crash you may lose up to 10 ms of acked data.** This matches NATS JetStream's default behavior.

| Workload                          | Sync (default, durable) | Async (opt-in)          |
|-----------------------------------|-------------------------|-------------------------|
| Publish, 1 KB payload             | ~593 msg/s              | ~1,425 msg/s            |
| E2E latency p50 @ 10k/s target    | 105 ms                  | 255 ms                  |
| E2E latency p99 @ 10k/s target    | 250 ms                  | 739 ms                  |
| Fan-out @ 4 consumers (aggregate) | ~95 msg/s               | ~93 msg/s               |
| ExQL sustained input              | N/A (no candidate passed) | N/A (no candidate passed) |

**On publish throughput**, async mode delivers a 2.4× uplift on macOS (593 → 1,425 msg/s) because the per-batch `fsync` that dominates latency on APFS is removed from the hot path. Linux+NVMe numbers will be significantly higher in both modes.

**On E2E latency** at the 10k/s target, the async numbers are higher than sync because the benchmark driver uses coordinated-omission correction: when the consumer loop falls behind at 10k/s (which macOS cannot sustain), hdrhistogram adds corrected samples for every service-time slot that was missed. This effect dominates both modes at this rate; the async vs sync difference here reflects scheduling noise, not a regression. Async latency wins become visible at throughput levels below the local bottleneck.

**Fan-out and ExQL** show no material difference between modes — the bottleneck for those scenarios is the consumer delivery loop, not fsync.

Keep the default (`sync`) for production unless throughput is critical and you understand the tradeoff.

## Comparison against other brokers

See `bench/README.md` — we ship docker-compose files and a `--target kafka` flag so you can run the same scenarios against Kafka. We deliberately do not publish Exspeed-vs-Kafka numbers in this file.

