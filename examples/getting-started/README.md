# Getting Started: Crypto Price Tracker

A minimal example showing Exspeed in action. It publishes to, reads and subscribes to a stream of cryptocurrency prices.

## Prerequisites

- **Exspeed server** running on `localhost:5933`, for example with `docker compose up -d` in this directory (it builds the server image from the repository root)
- **Bun** installed (https://bun.sh), or Node.js 18 or later
- The SDK built, since the example uses it from this repository: `npm ci && npm run build` in `sdks/typescript`

## Quick Start

```bash
bun install
bun run start
```

With Node.js: `npm install && npx tsx index.ts`. Set `EXSPEED_HOST` / `EXSPEED_PORT` to use a server elsewhere.

## What It Does

`index.ts`:

1. Connects to the Exspeed server on `localhost:5933`
2. Creates the `crypto-prices` stream
3. Publishes a sample price record
4. Reads up to 10 records from the start of the stream and prints them
5. Creates the durable consumer `price-watcher` on the stream
6. Subscribes and prints each record as it arrives, acknowledging it

## Live Data with the HTTP Poller Connector

`exspeed/connectors.d/crypto-prices.toml` is an `http_poll` source that fetches prices for BTC, ETH and SOL from the CoinGecko API every 60 seconds and publishes each response to the `crypto-prices` stream. The subscriber in this example prints each update as it arrives.

The included `docker-compose.yml` mounts `exspeed/connectors.d/` into the server's data directory, so the connector starts with the server. For a server you run yourself, copy the file into `{data_dir}/connectors.d/` (`./exspeed-data/connectors.d/` by default, `/var/lib/exspeed/connectors.d/` in the Docker image); the server picks up new files without a restart:

```bash
cp exspeed/connectors.d/crypto-prices.toml ./exspeed-data/connectors.d/
```

The same directory holds `crypto-to-postgres.toml`, a `jdbc` sink that upserts the `crypto_flat_btc` stream into a Postgres table. It expects a Postgres server at `host.docker.internal:5432` and a `crypto_flat_btc` stream; without them `exspeed connectors` shows it as `backoff` or `failed`, which doesn't affect the rest of the example.
