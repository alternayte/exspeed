# Getting Started: Crypto Price Tracker

A minimal example showing Exspeed in action. Publishes and subscribes to a stream of cryptocurrency prices.

## Prerequisites

- **Exspeed server** running on `localhost:5933`
- **Bun** installed (https://bun.sh)

## Quick Start

```bash
bun install
bun run start
```

## What It Does

1. Connects to a local Exspeed server
2. Creates a `crypto-prices` stream
3. Publishes a sample price record (BTC, ETH, SOL)
4. Fetches records from the stream
5. Subscribes and prints records as they arrive in real time

## Live Data with the HTTP Poller Connector

To get real crypto prices streamed automatically, copy the included connector config to your Exspeed server:

```bash
cp exspeed/connectors.d/crypto-prices.toml /var/lib/exspeed/connectors.d/
```

This configures Exspeed to poll the CoinGecko API every 60 seconds and publish the results to the `crypto-prices` stream. The subscriber in this example will print each update as it arrives.
