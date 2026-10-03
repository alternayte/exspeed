# Exspeed benchmark kit

This directory holds the comparison kit (`compare/`) and the results
(`results/`): JSON files produced by `exspeed-bench` and the logs of the
comparison runs. The published numbers and how they were measured are in
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

`--profile local` runs short (5–10 s) scenarios with 1 KiB payloads and a
200k-record catch-up backlog; `reference` runs 60 s ones with 100 B, 1 KiB
and 10 KiB payloads and a 2M-record backlog. `--duration-secs`,
`--payload-sizes`, `--rate` (latency target) and `--catchup-records`
override the profile for one run.

## Reproduce a comparison with Kafka and NATS JetStream

`bench/compare/` runs equivalent workloads against Exspeed, single-node
Kafka (KRaft) and NATS JetStream on the same machine, one system at a time,
each with its own standard perf tool. There are two runners:

- `run-native.sh` uses release binaries installed on the host (Kafka 3.8.1
  with a JRE 17+, nats-server 2.10.22 and the `nats` CLI 0.1.5). It
  produced the published comparison.
- `run.sh` does the same with the Docker images pinned in
  `docker-compose.yml` (`apache/kafka:3.8.1`, `nats:2.10.22`,
  `natsio/nats-box`).

```bash
cargo build --release -p exspeed -p exspeed-bench

# No Docker
KAFKA_HOME=/opt/kafka_2.13-3.8.1 NATS_SERVER=/opt/nats-server NATS=/opt/nats \
  BENCH_MODE=sync bench/compare/run-native.sh     # then BENCH_MODE=async

# With Docker
BENCH_MODE=sync  bench/compare/run.sh    # every broker fsyncs before acking
BENCH_MODE=async bench/compare/run.sh    # page cache / timer fsync (Kafka and NATS defaults)
```

`SYSTEMS="exspeed kafka"` limits a run to some of the systems; `RECORDS`
(default 1,000,000) sets the backlog size and `LATENCY_RATE` (default
1,000 msg/s) the Exspeed latency rate. `run-native.sh` runs the Exspeed
publish with 1 KiB records for `DURATION` seconds (default 30); `run.sh`
uses the `reference` profile's payload sizes and durations.

| Workload | Exspeed | Kafka | NATS JetStream |
|----------|---------|-------|----------------|
| Publish, 1 KiB, 4 producers | `exspeed-bench publish` | `kafka-producer-perf-test.sh` ×4, `acks=all`, `linger.ms=1` | 4 clients publishing synchronously (`nats bench --js --pub 4 --syncpub` natively, `nats bench js pub sync --clients 4` in Docker) |
| Consume the backlog | `exspeed-bench catchup` | `kafka-consumer-perf-test.sh` | a durable pull consumer (`nats bench --js --sub 1 --pull` natively, `nats bench js consume` in Docker) |
| End-to-end latency | `exspeed-bench latency` at `LATENCY_RATE` | `kafka-e2e-latency.sh` (one message at a time) | `nats rtt` (core NATS round trip, reference only) |

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
policy (and, natively, a 2 GiB Kafka heap); tuning them fairly is out of
scope.

The published comparison, measured with `run-native.sh` on the same VM as
Exspeed's own numbers, is in
[BENCHMARKS.md](../BENCHMARKS.md#comparison-with-kafka-and-nats-jetstream),
with its raw logs in `bench/results/compare-2026-10-03-{sync,async}/`. The
numbers only hold for that hardware; run the kit on yours to compare there.

The `comparison` feature of `exspeed-bench` also has an rdkafka-based Kafka
driver (`--target kafka`) for the publish, latency and fan-out scenarios:

```bash
docker compose -f bench/compare/docker-compose.yml up -d kafka
cargo run --release -p exspeed-bench --features comparison -- publish \
  --server localhost:9092 --target kafka --output bench/results/kafka.json
```
