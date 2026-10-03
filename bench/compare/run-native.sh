#!/usr/bin/env bash
# The same comparison as run.sh, without Docker: Kafka (KRaft) and NATS
# JetStream run from their release binaries on this machine.
#
#   Kafka   https://archive.apache.org/dist/kafka/3.8.1/kafka_2.13-3.8.1.tgz
#           (needs a JRE 17+, e.g. apt install openjdk-21-jre-headless)
#   NATS    nats-server v2.10.22 and the nats CLI v0.1.5 (GitHub releases)
#
# Usage:
#   cargo build --release -p exspeed -p exspeed-bench
#   KAFKA_HOME=/opt/kafka_2.13-3.8.1 NATS_SERVER=/opt/nats-server NATS=/opt/nats \
#     BENCH_MODE=sync bench/compare/run-native.sh
#   SYSTEMS="exspeed nats" ... bench/compare/run-native.sh
#
# Workloads (1 KiB records, one stream/topic, one partition, no
# replication, every system on the same disk):
#   1. publish  - 4 concurrent publishers waiting for acks
#   2. consume  - drain the backlog written by step 1
#   3. latency  - end-to-end publish -> consume
#
# Durability parity (BENCH_MODE):
#   sync   fsync before acknowledging: Exspeed --storage-sync sync (group
#          commit), Kafka log.flush.interval.messages=1, NATS
#          sync_interval: always
#   async  page cache / timer: Exspeed --storage-sync async, Kafka and NATS
#          defaults
#
# Logs go to bench/results/compare-<date>-<mode>/.
set -euo pipefail

cd "$(dirname "$0")/../.."
MODE="${BENCH_MODE:-sync}"
SYSTEMS="${SYSTEMS:-exspeed kafka nats}"
RECORDS="${RECORDS:-1000000}"
SIZE=1024
LATENCY_MSGS="${LATENCY_MSGS:-10000}"
LATENCY_RATE="${LATENCY_RATE:-1000}"
DURATION="${DURATION:-30}"
OUT="bench/results/compare-$(date +%F)-${MODE}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
EXSPEED="${TARGET_DIR}/release/exspeed"
BENCH="${TARGET_DIR}/release/exspeed-bench"
WORK="$(mktemp -d)"
# Stop any server still running (a failed step under `set -e`) and clean up.
trap 'kill $(jobs -p) 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$WORK"' EXIT
mkdir -p "$OUT"

case "$MODE" in
  sync | async) ;;
  *) echo "BENCH_MODE must be sync or async" >&2; exit 2 ;;
esac

{
  echo "date: $(date -u +%FT%TZ)"
  echo "mode: $MODE  records: $RECORDS  size: $SIZE  publish duration: ${DURATION}s"
  echo "git: $(git rev-parse --short HEAD)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)  nproc: $(nproc)"
  free -g | head -2
  uname -srm
  df -hT "$WORK" | tail -1
  echo "kafka: ${KAFKA_HOME:-unset}  nats-server: $(${NATS_SERVER:-false} --version 2>/dev/null || echo unset)"
  java -version 2>&1 | grep -v JAVA_TOOL_OPTIONS | head -1 || true
} | tee "$OUT/host.txt"

wait_port() {
  for _ in $(seq 300); do
    (echo >"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.2
  done
  echo "port $1 never opened" >&2
  return 1
}

run_exspeed() {
  local data="$WORK/exspeed"
  "$EXSPEED" server --data-dir "$data" --storage-sync "$MODE" \
    >"$OUT/exspeed-server.log" 2>&1 &
  local pid=$!
  for _ in $(seq 100); do
    curl -fs localhost:8080/readyz >/dev/null 2>&1 && break
    sleep 0.1
  done
  "$BENCH" publish --profile reference --payload-sizes $SIZE --duration-secs "$DURATION" \
    --output "$OUT/exspeed-publish.json" | tee "$OUT/exspeed-publish.log"
  "$BENCH" catchup --profile reference --catchup-records "$RECORDS" \
    --output "$OUT/exspeed-catchup.json" | tee "$OUT/exspeed-catchup.log"
  # Latency at a light load (LATENCY_RATE msg/s), like Kafka's
  # one-at-a-time e2e tool; a rate above the publish maximum would measure
  # queueing instead.
  "$BENCH" latency --profile reference --duration-secs "$DURATION" --rate "$LATENCY_RATE" \
    --output "$OUT/exspeed-latency.json" | tee "$OUT/exspeed-latency.log"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
}

run_kafka() {
  local cfg="$WORK/kafka.properties" flush=9223372036854775807
  [ "$MODE" = sync ] && flush=1
  cat >"$cfg" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:19093
listeners=PLAINTEXT://localhost:19092,CONTROLLER://localhost:19093
advertised.listeners=PLAINTEXT://localhost:19092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
log.flush.interval.messages=$flush
EOF
  local k="$KAFKA_HOME/bin"
  "$k/kafka-storage.sh" format -t "$("$k/kafka-storage.sh" random-uuid)" -c "$cfg" >/dev/null
  KAFKA_HEAP_OPTS="-Xms2g -Xmx2g" "$k/kafka-server-start.sh" "$cfg" >"$OUT/kafka-server.log" 2>&1 &
  local pid=$!
  wait_port 19092
  for t in bench bench-lat; do
    "$k/kafka-topics.sh" --bootstrap-server localhost:19092 --create --topic "$t" \
      --partitions 1 --replication-factor 1 >/dev/null
  done
  # 1. publish: 4 producer processes, acks=all.
  for i in 1 2 3 4; do
    "$k/kafka-producer-perf-test.sh" --topic bench --num-records $((RECORDS / 4)) \
      --record-size $SIZE --throughput -1 \
      --producer-props bootstrap.servers=localhost:19092 acks=all linger.ms=1 \
      >"$OUT/kafka-publish-$i.log" 2>&1 &
  done
  wait $(jobs -p | grep -v "^$pid$") 2>/dev/null || true
  tail -n 1 "$OUT"/kafka-publish-*.log
  # 2. consume the backlog.
  "$k/kafka-consumer-perf-test.sh" --bootstrap-server localhost:19092 --topic bench \
    --messages "$RECORDS" --timeout 120000 | tee "$OUT/kafka-consume.log"
  # 3. latency: one message at a time, produce -> consume round trip.
  "$k/kafka-e2e-latency.sh" localhost:19092 bench-lat "$LATENCY_MSGS" all $SIZE \
    | tee "$OUT/kafka-latency.log"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
}

run_nats() {
  local conf="$WORK/nats.conf" sync=""
  [ "$MODE" = sync ] && sync="sync_interval: always"
  cat >"$conf" <<EOF
port: 14222
jetstream {
  store_dir: "$WORK/nats-data"
  max_file_store: 4GB
  $sync
}
EOF
  "$NATS_SERVER" -c "$conf" >"$OUT/nats-server.log" 2>&1 &
  local pid=$!
  wait_port 14222
  local nb=("$NATS" -s nats://127.0.0.1:14222)
  # 1. publish: 4 clients, synchronous JetStream publishes (wait for ack).
  "${nb[@]}" bench bench.s --js --pub 4 --msgs "$RECORDS" --size $SIZE --syncpub \
    --storage file --replicas 1 --maxbytes 2GB --purge --no-progress \
    | tee "$OUT/nats-publish.log"
  # 2. consume the backlog with a durable, explicitly acked pull consumer.
  "${nb[@]}" bench bench.s --js --sub 1 --pull --msgs "$RECORDS" --size $SIZE \
    --storage file --replicas 1 --maxbytes 2GB --no-progress \
    | tee "$OUT/nats-consume.log"
  # 3. latency: core-NATS round trip only (nats bench has no JetStream
  #    end-to-end latency workload); for reference.
  "${nb[@]}" rtt | tee "$OUT/nats-rtt.log"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
}

for s in $SYSTEMS; do
  echo "=== $s ($MODE) ==="
  "run_$s"
done
echo "results in $OUT"
