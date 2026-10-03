# Exspeed Benchmarks

_Single-node results measured **2026-10-03** at git `d8bb597` / `89202d9`
(same server binary; the second commit only added the bench's override
flags). The [comparison with Kafka and NATS
JetStream](#comparison-with-kafka-and-nats-jetstream) was measured the same
day at git `3973a40`._

These are single-node numbers from one run on a small cloud VM, with the
broker and the benchmark driver on the same machine. Treat them as an order
of magnitude for this hardware, not as a ceiling: on a dedicated host with
local NVMe they will be higher, and on a busier VM lower.

## Machine

| | |
|---|---|
| Type | Cloud VM (KVM), shared host |
| CPU | 4 vCPU, Intel Xeon @ 2.80 GHz (`nproc` = 4) |
| RAM | 15 GiB |
| Disk | virtio block device, ext4. `dd` probe: 4 KiB `O_DSYNC` writes ≈ 0.65 ms each (≈ 1,500/s); 1 GiB sequential with `fdatasync` ≈ 117 MB/s |
| OS | Ubuntu 24.04, Linux 6.18 |
| Build | `cargo build --release -p exspeed -p exspeed-bench` (Rust 1.99.0, release profile) |
| Load | Nothing else running during the measurements (load average < 0.1 before each mode) |

## Results

Records are published over the TCP protocol with `exspeed-client`'s
coalescing publisher. **Sync** is the default durability: a batch is
fsynced before it is acknowledged or visible to readers. **Async**
(`--storage-sync async`) acknowledges once written and fsyncs every 10 ms or
4 MiB, so a crash can lose up to that much acknowledged data.

### Publish

4 producer tasks sharing one connection, 64 publishes in flight each, as fast
as acknowledgements come back.

| Payload | Sync msg/s | Sync MB/s | Async msg/s | Async MB/s | Duration |
|---------|-----------:|----------:|------------:|-----------:|---------:|
| 100 B   | 74,578 | 7.5 | 78,529 | 7.9 | 30 s |
| 1 KiB   | 61,983 | 63.5 | 67,473 | 69.1 | 30 s |
| 10 KiB  | 13,405 | 137.3 | 15,403 | 157.7 | 10 s |

Group commit amortizes the fsync: sync mode reaches ~92% of async throughput
at 1 KiB on a disk that manages ~1,500 single-record fsyncs per second. At
small payloads the limit is the per-record cost on 4 vCPUs shared by the
broker and the driver, not the disk. At 10 KiB it is disk bandwidth.

### Catch-up (draining a backlog)

1,000,000 records of 1 KiB already on disk (mostly in the page cache).

| Reader | Sync-mode server msg/s | MB/s | Async-mode server msg/s | MB/s |
|--------|-----------------------:|-----:|------------------------:|-----:|
| Stateless reads, 1000 records per request, one request at a time | 353,790 | 362.3 | 300,750 | 308.0 |
| Durable push consumer from offset 0, acks batched every 1024 records | 284,860 | 291.7 | 316,884 | 324.5 |

The storage mode doesn't affect reads; the difference between the two
columns is run-to-run noise.

### End-to-end latency

Publish → push-consumer delivery at a steady **5,000 msg/s** of 1 KiB for
30 s, measured from the publish call to receipt by the subscriber (both on
the same host, so no clock skew).

| Mode | p50 | p90 | p99 | p99.9 | p99.99 | max |
|------|----:|----:|----:|------:|-------:|----:|
| Sync  | 5.3 ms | 8.0 ms | 11.2 ms | 26.0 ms | 33.4 ms | 34.7 ms |
| Async | 5.3 ms | 7.9 ms | 76.3 ms | 224.6 ms | 226.0 ms | 228.2 ms |

Both modes have the same ~5 ms median, so it isn't fsync; where it goes
(client batching, the delivery path, the rate-driven producer on a shared
4-vCPU box) hasn't been profiled yet. The async run had one stall of about
220 ms that sets its tail; one run is not enough to say whether that is the
VM or the broker, so don't read async as having the worse tail in general.

### Fan-out

Producer at 5,000 msg/s of 1 KiB for 10 s; N consumers on the same stream,
each receiving every record.

| Consumers | Producer rate | Aggregate delivery | Max lag at the end |
|----------:|--------------:|-------------------:|-------------------:|
| 1 (sync)  | 5,000 | 4,979 msg/s | 1 |
| 4 (sync)  | 5,000 | 19,911 msg/s | 1 |
| 1 (async) | 5,000 | 4,977 msg/s | 1 |
| 4 (async) | 5,000 | 19,911 msg/s | 1 |

Every consumer kept up: aggregate delivery is N × the producer rate.

## How to reproduce

```bash
cargo build --release -p exspeed -p exspeed-bench

# Terminal 1 (repeat with --storage-sync async for the async columns)
./target/release/exspeed server --data-dir /tmp/exspeed-bench --storage-sync sync

# Terminal 2
B=./target/release/exspeed-bench
$B publish --profile reference --payload-sizes 100,1024 --duration-secs 30 --output publish.json
$B publish --profile reference --payload-sizes 10240 --duration-secs 10 --output publish-10k.json
$B catchup --profile reference --catchup-records 1000000 --output catchup.json
$B latency --profile local --duration-secs 30 --rate 5000 --output latency.json
$B fanout  --profile local --duration-secs 10 --output fanout.json
```

Start each mode on an empty data directory. The raw results are in
[`bench/results/2026-10-03-linux-*.json`](bench/results/). The smaller
payload set and shorter durations than `--profile reference` keep the run
inside this VM's free disk space. See [bench/README.md](bench/README.md) for
every scenario.

## Comparison with Kafka and NATS JetStream

_Measured **2026-10-03** on the same VM (see [Machine](#machine)), git
`3973a40`, with [`bench/compare/run-native.sh`](bench/compare/run-native.sh):
Kafka 3.8.1 (KRaft, OpenJDK 21, 2 GiB heap), nats-server 2.10.22 with the
`nats` CLI 0.1.5, and Exspeed. One stream/topic, one partition, no
replication, 1 KiB records, every system on the same disk, one system
running at a time. Raw logs: `bench/results/compare-2026-10-03-{sync,async}/`._

**Sync** = every broker fsyncs before acknowledging (Exspeed `--storage-sync
sync` with group commit, Kafka `log.flush.interval.messages=1`, JetStream
`sync_interval: always`). **Async** = Exspeed `--storage-sync async`, Kafka
and JetStream defaults (page cache / timer).

| Workload | Exspeed sync | Kafka sync | JetStream sync | Exspeed async | Kafka async | JetStream async |
|---|---:|---:|---:|---:|---:|---:|
| Publish, 4 publishers (msg/s) | **51,711** | 11,407 | 2,324 | **64,939** | 40,777 | 12,865 |
| Consume a 1M backlog (msg/s) | **410,741** | 116,795 | 75,482 | **414,953** | 111,062 | 71,896 |
| End-to-end latency p50 / p99 | 6.0 / 10.1 ms | 1 / 7 ms | — | 5.2 / 9.3 ms | 1 / 7 ms | — |

Read the rows with the tools' differences in mind:

- **Publish.** Exspeed: `exspeed-bench publish`, 4 tasks on one connection
  with the coalescing publisher (requests pipelined) for 30 s. Kafka:
  `kafka-producer-perf-test.sh`, 4 processes × 250,000 records, `acks=all`,
  `linger.ms=1` (also pipelined and batched). JetStream: `nats bench --js
  --pub 4 --syncpub`, which waits for each acknowledgement before sending
  the next message, so it is strictly one message per round trip (and, in
  sync mode, per fsync) per client. Its numbers are for that pattern, not
  for JetStream's async publishing.
- **Consume.** Exspeed: a durable push consumer from offset 0 with acks
  batched (`exspeed-bench catchup`; stateless reads gave 396,244 msg/s).
  Kafka: `kafka-consumer-perf-test.sh` (`nMsg.sec`, which includes the
  ~4 s group rebalance; its fetch-only rate was 214,730 msg/s). JetStream:
  `nats bench --sub 1 --pull`, a durable pull consumer with explicit acks.
- **Latency.** Exspeed: publish → push delivery at a steady 1,000 msg/s for
  30 s. Kafka: `kafka-e2e-latency.sh`, one message at a time (produce, then
  consume it, then the next), 10,000 messages. `nats bench` has no
  JetStream end-to-end latency workload; the core-NATS round trip was
  0.22 ms. Kafka's median is lower than Exspeed's here. Exspeed's ~5 ms
  median is the same in both modes, so it isn't fsync; it hasn't been
  profiled yet (see [End-to-end latency](#end-to-end-latency)).

To reproduce (no Docker needed; [`bench/compare/run.sh`](bench/compare/run.sh)
does the same with Docker images):

```bash
cargo build --release -p exspeed -p exspeed-bench
KAFKA_HOME=/opt/kafka_2.13-3.8.1 NATS_SERVER=/opt/nats-server NATS=/opt/nats \
  BENCH_MODE=sync bench/compare/run-native.sh     # then BENCH_MODE=async
```
