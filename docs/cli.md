# CLI reference

The `exspeed` binary includes both the server and a client CLI. The client
commands talk to the HTTP API.

## Global options

| Option | Env | Default | Description |
|--------|-----|---------|-------------|
| `--server <url>` | `EXSPEED_URL` | `http://localhost:8080` | HTTP API base URL |
| `--json` | — | off | Print JSON instead of tables |
| — | `EXSPEED_AUTH_TOKEN` | — | Bearer token sent with every request |
| — | `EXSPEED_INSECURE_SKIP_VERIFY=1` | — | Skip TLS certificate verification (dev only) |

Most client commands call `/api/v1/*` endpoints that require an **admin**
permission. A publish-only credential cannot use `exspeed pub`.

## Server

```bash
exspeed server [--bind 0.0.0.0:5933] [--api-bind 0.0.0.0:8080] [--data-dir ./exspeed-data]
```

The full list of flags and environment variables is in
[configuration.md](configuration.md).

## Streams

```bash
exspeed create <name> [--retention 7d] [--max-size 10gb] \
                      [--dedup-window 5m] [--dedup-max-entries 500k]
exspeed update-stream <name> [--retention …] [--max-size …] [--dedup-window …] [--dedup-max-entries …]
exspeed streams                     # list
exspeed info <name>                 # offsets, size, retention, dedup config
exspeed delete <name> [--force]     # --force also removes connectors/queries/consumers that use it
```

Values:

- **Durations:** `30s`, `10m`, `24h`, `7d`.
- **Sizes:** `256mb`, `10gb`.
- **Entry counts:** `100000`, `500k`, `2M`.

## Publish

```bash
exspeed pub <stream> '<data>' [--subject order.eu.created] [--key ord-1] [--msg-id <id>]
```

`--msg-id` turns on [idempotent publish](idempotent-publish.md).

## Tail

```bash
exspeed tail <stream>                       # follow new records
exspeed tail <stream> --last 10 --no-follow
exspeed tail <stream> --from-beginning
exspeed tail <stream> --subject 'order.eu.*'
```

`tail` polls the SQL endpoint every 200 ms. Each poll scans the whole
stream, so it is slow on large streams.

## Consumers

```bash
exspeed consumers                  # list
exspeed consumer-info <name>       # offset, lag, group, subject filter
```

Consumers are created by clients over TCP, for example with the SDK's
`createConsumer`. The CLI cannot create them.

## Queries

```bash
exspeed query "SELECT * FROM orders LIMIT 10"
exspeed query "CREATE STREAM eu_orders AS SELECT … FROM orders WHERE payload->>'region' = 'eu'"
exspeed query "CREATE TABLE revenue AS SELECT payload->>'region' AS region, SUM(payload->>'total') AS total FROM orders GROUP BY payload->>'region'"
exspeed query "PAUSE QUERY eu_orders_1a2b3c4d"
exspeed query "RESUME QUERY eu_orders_1a2b3c4d"
exspeed query "DROP STREAM eu_orders"
exspeed query --continuous "CREATE STREAM …"   # only accepts CREATE statements
```

Every statement goes to `POST /api/v1/queries`. Secondary indexes
(`CREATE INDEX`) were removed, and the server rejects them with `UNSUPPORTED`.

See [exql.md](exql.md).

## Views

```bash
exspeed views                 # list materialized tables
exspeed view <name>           # rows of a table
```

## Connectors

```bash
exspeed connectors                                   # list running connectors
exspeed connector validate <file.toml>               # syntax, plugin, stream, transform
exspeed connector dry-run  <file.toml>               # + connect and fetch a sample
```

See [connectors.md](connectors.md).

## Auth helpers

```bash
exspeed auth gen-token                 # token on stdout, sha256 on stderr
echo -n "$TOKEN" | exspeed auth hash   # sha256 of a token
exspeed auth lint credentials.toml     # validate a credentials file offline
exspeed auth whoami                    # identity + permissions of $EXSPEED_AUTH_TOKEN
```

See [security.md](security.md).

## Snapshot (offline backup)

```bash
exspeed snapshot --data-dir /var/lib/exspeed --output backup.tar.gz
```

This takes the data-directory lock, so the server must be stopped first.
To restore, extract the archive into an empty data directory. There is no
online backup or `restore` command yet.
