# Exspeed benchmark kit

This directory holds the comparison setup and the result JSON files produced
by `exspeed-bench`. The published numbers and how they were measured are in
[BENCHMARKS.md](../BENCHMARKS.md).

## Run Exspeed's own benchmarks

```bash
cargo build --release -p exspeed -p exspeed-bench

# Terminal 1: the broker (sync is the default; add --storage-sync async to compare)
./target/release/exspeed server --data-dir /tmp/exspeed-bench

# Terminal 2: one scenario at a time, or `all`
./target/release/exspeed-bench publish --profile reference --output bench/results/publish.json
./target/release/exspeed-bench catchup --profile reference --output bench/results/catchup.json
./target/release/exspeed-bench latency --profile local     --output bench/results/latency.json
./target/release/exspeed-bench fanout  --profile local     --output bench/results/fanout.json
./target/release/exspeed-bench all --profile reference --output bench/results/all.json

# Markdown tables from a result file
./target/release/exspeed-bench render bench/results/all.json
```

Scenarios:

| Scenario | What it measures |
|----------|------------------|
| `publish` | 4 producer tasks on one coalescing publisher, 64 publishes in flight each, as fast as acks come back |
| `latency` | End-to-end publish → push-consumer delivery at a fixed rate (`latency_target_rate`) |
| `fanout` | N consumers on one stream at a fixed producer rate |
| `catchup` | Draining a backlog already on disk: stateless `Read` requests (1000 records each) and a durable push consumer with batched acks |
| `exql` | Highest input rate a continuous `GROUP BY` query keeps up with |

`--profile local` runs short (5–10 s) scenarios; `reference` runs 60 s ones
with more payload sizes and a 2M-record catch-up backlog.

## Reproduce a comparison with Kafka and NATS JetStream

`bench/compare/` has a docker-compose file with single-node Kafka (KRaft) and
NATS JetStream, and `run.sh`, which runs equivalent workloads against
Exspeed and both of them on the same machine, each with its own standard
perf tool:

```bash
cargo build --release -p exspeed -p exspeed-bench
BENCH_MODE=sync  bench/compare/run.sh    # every broker fsyncs before acking
BENCH_MODE=async bench/compare/run.sh    # page cache / timer fsync (Kafka and NATS defaults)
```

| Workload | Exspeed | Kafka | NATS JetStream |
|----------|---------|-------|----------------|
| Publish, 1 KiB, 4 producers | `exspeed-bench publish` | `kafka-producer-perf-test.sh` ×4, `acks=all` | `nats bench js pub sync --clients 4` |
| Consume the backlog | `exspeed-bench catchup` | `kafka-consumer-perf-test.sh` | `nats bench js consume` |
| End-to-end latency | `exspeed-bench latency` | `kafka-e2e-latency.sh` (one message at a time) | `nats rtt` (core NATS round trip, reference only) |

Durability parity is set by `BENCH_MODE`: `sync` runs Exspeed with
`--storage-sync sync` (its default), Kafka with
`log.flush.interval.messages=1` and JetStream with `sync_interval: always`.
`async` uses Exspeed `--storage-sync async` and the Kafka and JetStream
defaults. Results land in `bench/results/compare-<date>-<mode>/`, one log
per tool, plus `host.txt`.

The tools don't measure the same things the same way (Kafka's e2e latency
tool sends one message at a time, `exspeed-bench latency` runs at a fixed
rate, `nats bench` reports its own aggregates), so compare the logs with
that in mind. Each competitor runs with stock settings apart from the fsync
policy; tuning them fairly is out of scope.

**We don't publish comparison numbers.** They are only meaningful when all
three run on the same hardware, and the kit has not been run on the
benchmark machine yet (it has no Docker daemon). Run it yourself.

The `comparison` feature of `exspeed-bench` also has an rdkafka-based Kafka
driver (`--target kafka`) for the publish, latency and fan-out scenarios:

```bash
docker compose -f bench/compare/docker-compose.yml up -d kafka
cargo run --release -p exspeed-bench --features comparison -- publish \
  --server localhost:9092 --target kafka --output bench/results/kafka.json
```
