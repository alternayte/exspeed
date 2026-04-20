# Exspeed benchmark kit

This directory holds the comparison setup and results JSON files produced by `exspeed-bench`.

## Run Exspeed's own benchmarks

```bash
# Start an exspeed server in another terminal
cargo run --release -p exspeed -- server --data-dir /tmp/exspeed-bench

# Run the full suite locally (laptop-friendly profile)
cargo run --release -p exspeed-bench -- all \
  --server localhost:5933 --api http://localhost:8080 \
  --profile local \
  --output bench/results/local.json

# Render Markdown
cargo run --release -p exspeed-bench -- render bench/results/local.json --out BENCHMARKS.md
```

## Run the comparison kit against Kafka

Requires Docker and a working `librdkafka` toolchain (`brew install librdkafka` on macOS,
`apt install librdkafka-dev` on Debian/Ubuntu).

```bash
docker compose -f bench/compare/kafka.yml up -d
cargo run --release -p exspeed-bench --features comparison -- all \
  --server localhost:9092 \
  --target kafka \
  --profile local \
  --output bench/results/kafka-local.json
cargo run --release -p exspeed-bench -- render bench/results/kafka-local.json --out /tmp/kafka.md
docker compose -f bench/compare/kafka.yml down
```

Compare `bench/results/local.json` against `bench/results/kafka-local.json` side by side.

## What Exspeed does *not* do

- We do not publish head-to-head Exspeed vs Kafka numbers in `README.md` or `BENCHMARKS.md`.
  If a fair comparison matters to you, run the kit yourself.
- The comparison kit uses stock distribution defaults for each competitor. Tuning each
  broker fairly is out of scope; this is a "run your own measurement" tool, not a "trust
  our numbers" tool.
