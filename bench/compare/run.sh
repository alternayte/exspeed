#!/usr/bin/env bash
# Run equivalent single-node workloads against Exspeed, Kafka (KRaft) and
# NATS JetStream on THIS machine, each with its own standard perf tool:
#
#   Exspeed  exspeed-bench (publish, latency, catchup)
#   Kafka    kafka-producer-perf-test.sh, kafka-consumer-perf-test.sh,
#            kafka-e2e-latency.sh (shipped in the apache/kafka image)
#   NATS     nats bench (nats-box image)
#
# Workloads (1 KiB records, one stream/topic, one partition, no replication):
#   1. publish   - 4 producers, as fast as acks allow
#   2. consume   - drain the backlog written by step 1
#   3. latency   - end-to-end publish -> consume latency
#
# Usage:
#   cargo build --release -p exspeed -p exspeed-bench
#   BENCH_MODE=sync  bench/compare/run.sh      # fsync before ack everywhere
#   BENCH_MODE=async bench/compare/run.sh      # page cache / timer fsync
#   SYSTEMS="exspeed kafka" bench/compare/run.sh
#
# Output goes to bench/results/compare-<date>-<mode>/ (one log per tool).
# Each tool reports in its own format and with its own definition of
# latency (see BENCHMARKS.md, "Reproduce a comparison"); read the logs
# rather than diffing single numbers. Tool flags drift between versions:
# the images are pinned in docker-compose.yml, check `--help` if you bump
# them.
set -euo pipefail

cd "$(dirname "$0")/../.."
MODE="${BENCH_MODE:-sync}"
SYSTEMS="${SYSTEMS:-exspeed kafka nats}"
RECORDS="${RECORDS:-1000000}"
SIZE=1024
LATENCY_MSGS="${LATENCY_MSGS:-10000}"
OUT="bench/results/compare-$(date +%F)-${MODE}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
EXSPEED="${TARGET_DIR}/release/exspeed"
BENCH="${TARGET_DIR}/release/exspeed-bench"
COMPOSE=(docker compose -f bench/compare/docker-compose.yml)
mkdir -p "$OUT"

case "$MODE" in
  sync) export KAFKA_FLUSH_MESSAGES=1 ;;
  async) export KAFKA_FLUSH_MESSAGES=9223372036854775807 ;;
  *) echo "BENCH_MODE must be sync or async" >&2; exit 2 ;;
esac
export BENCH_MODE="$MODE"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "mode: $MODE  records: $RECORDS  size: $SIZE"
  echo "git: $(git rev-parse --short HEAD)"
  echo "nproc: $(nproc)"
  free -g | head -2
  uname -srm
  df -hT . | tail -1
} | tee "$OUT/host.txt"

run_exspeed() {
  local data
  data="$(mktemp -d)"
  "$EXSPEED" server --data-dir "$data" --storage-sync "$MODE" \
    >"$OUT/exspeed-server.log" 2>&1 &
  local pid=$!
  trap 'kill $pid 2>/dev/null || true; rm -rf "$data"' RETURN
  for _ in $(seq 100); do
    curl -fs localhost:8080/readyz >/dev/null 2>&1 && break
    sleep 0.1
  done
  "$BENCH" publish --profile reference --output "$OUT/exspeed-publish.json" | tee "$OUT/exspeed-publish.log"
  "$BENCH" catchup --profile reference --output "$OUT/exspeed-catchup.json" | tee "$OUT/exspeed-catchup.log"
  "$BENCH" latency --profile local --output "$OUT/exspeed-latency.json" | tee "$OUT/exspeed-latency.log"
}

run_kafka() {
  "${COMPOSE[@]}" up -d kafka
  trap '"${COMPOSE[@]}" down -v' RETURN
  for _ in $(seq 60); do
    docker exec exspeed-bench-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
    sleep 1
  done
  for t in bench bench-lat; do
    docker exec exspeed-bench-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --topic "$t" --partitions 1 --replication-factor 1
  done
  # 1. publish: 4 producer processes, acks=all.
  for i in 1 2 3 4; do
    docker exec exspeed-bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
      --topic bench --num-records $((RECORDS / 4)) --record-size $SIZE --throughput -1 \
      --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=1 \
      >"$OUT/kafka-publish-$i.log" 2>&1 &
  done
  wait
  tail -n 1 "$OUT"/kafka-publish-*.log
  # 2. consume the backlog.
  docker exec exspeed-bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
    --bootstrap-server localhost:9092 --topic bench --messages "$RECORDS" --timeout 60000 \
    | tee "$OUT/kafka-consume.log"
  # 3. latency: one message at a time, produce -> consume round trip.
  docker exec exspeed-bench-kafka /opt/kafka/bin/kafka-e2e-latency.sh \
    localhost:9092 bench-lat "$LATENCY_MSGS" all $SIZE | tee "$OUT/kafka-latency.log"
}

run_nats() {
  "${COMPOSE[@]}" up -d nats
  trap '"${COMPOSE[@]}" down -v' RETURN
  sleep 2
  local nb=("${COMPOSE[@]}" run --rm nats-box nats -s nats://nats:4222)
  # 1. publish: 4 clients, synchronous JetStream publishes (wait for ack).
  "${nb[@]}" bench js pub sync bench --create --clients 4 --msgs "$RECORDS" --size $SIZE \
    --storage file --replicas 1 | tee "$OUT/nats-publish.log"
  # 2. consume the backlog with a durable consumer.
  "${nb[@]}" bench js consume bench --msgs "$RECORDS" | tee "$OUT/nats-consume.log"
  # 3. latency: request/reply over JetStream is not a standard nats bench
  #    workload; report core-NATS RTT for reference only.
  "${nb[@]}" rtt | tee "$OUT/nats-rtt.log"
}

for s in $SYSTEMS; do
  echo "=== $s ($MODE) ==="
  "run_$s"
done
echo "results in $OUT"
