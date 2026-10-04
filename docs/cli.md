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
exspeed server [--config exspeed.toml] [--bind 0.0.0.0:5933] [--api-bind 0.0.0.0:8080] [--data-dir ./exspeed-data]

exspeed config print-default          # commented exspeed.toml with every setting
exspeed config validate -c FILE       # resolve file + env + flags, check, exit non-zero on error
exspeed config show -c FILE           # resolved settings, secrets redacted
exspeed healthcheck [--url URL] [--timeout 3]   # exit 0 when /readyz answers 200 (Docker HEALTHCHECK)
```

Settings come from defaults < config file < environment < flags; the full
list is in [configuration.md](configuration.md), and `exspeed server --help`
lists every flag. `healthcheck` probes `/readyz` on the `api_bind` port
(`https` when TLS is configured), resolved from `EXSPEED_CONFIG` and the
environment the same way `exspeed server` resolves them; `--url` or
`EXSPEED_HEALTHCHECK_URL` overrides it.

## Streams

```bash
exspeed create <name> [--retention 7d] [--max-size 10gb] \
                      [--dedup-window 5m] [--dedup-max-entries 500k] \
                      [--max-msgs 1M] [--discard old|new] [--max-msgs-per-subject N] \
                      [--allow-msg-ttl] [--msg-ttl 1h] [--allow-delayed] \
                      [--retention-policy limits|work_queue|interest]
exspeed update-stream <name> [any of the flags above]   # only the flags given change
exspeed streams                     # list
exspeed info <name>                 # offsets, size, retention, dedup config
exspeed delete <name> [--force]     # --force also removes connectors/queries/consumers that use it
```

Values:

- **Durations:** `30s`, `10m`, `24h`, `7d`; a bare number is seconds.
- **Sizes:** `256mb`, `10gb` (binary units: 1 GB = 1024³ bytes); a bare number is bytes.
- **Entry counts:** `100000`, `500k`, `2M`.

`create` without `--dedup-window` / `--dedup-max-entries` uses the stream
defaults (5 minutes, 500,000 entries). Log compaction has no CLI flag:
create a compacted stream over HTTP (`"compaction": true`) or with the SDK.

The limit flags are described in [queues.md](queues.md): `--max-msgs` with
`--discard`, `--max-msgs-per-subject`, `--allow-msg-ttl` / `--msg-ttl`
(TTLs; `--msg-ttl` takes `500ms`, `30s`, `5m`, `2h`, `1d`), `--allow-delayed`,
and `--retention-policy`. On `update-stream`, the boolean flags take an
explicit value: `--allow-delayed false`.

## Publish

```bash
exspeed pub <stream> '<data>' [--subject order.eu.created] [--key ord-1] [--msg-id <id>]
```

`pub` posts to `POST /api/v1/streams/{name}/publish`. `<data>` is sent as
JSON when it parses as JSON, and as a JSON string otherwise. Without
`--subject` the subject is the stream name. `--msg-id` turns on
[idempotent publish](idempotent-publish.md); a duplicate prints
`duplicate=true` and the original offset.

## Tail

```bash
exspeed tail <stream>                       # follow new records
exspeed tail <stream> --last 10 --no-follow
exspeed tail <stream> --from-beginning
exspeed tail <stream> --subject 'order.eu.*'
```

`tail` reads through `GET /api/v1/streams/{name}/records`, page by page.
Once caught up it long-polls (`wait_ms`): the server holds the request
until new records arrive. It doesn't create a consumer, needs only the
`subscribe` permission on the stream, and works against any node of a
cluster (followers serve reads from their replica). Output
lines are `#offset [unix.ms] subject key=… value`; `--json` prints each
record as JSON.

## Consumers

```bash
exspeed consumers                  # list: ack floor, unacked, lag, subscribers
exspeed consumer-info <name>       # spec and live state
```

Consumers are created by applications (SDK `createConsumer`) or with
`POST /api/v1/consumers`.

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

Every statement goes to `POST /api/v1/queries` (`--continuous` posts to
`/api/v1/queries/continuous`). Secondary indexes are not supported:
`CREATE INDEX` is rejected with `UNSUPPORTED`.

See [exql.md](exql.md).

## Views

```bash
exspeed views                 # list materialized tables
exspeed view <name>           # rows of a table
```

## Connectors

```bash
exspeed connectors                                   # list connectors and their status
exspeed connector validate <file.toml>               # syntax, names, transform and every plugin setting
exspeed connector dry-run  <file.toml> [--max 3]     # + connect and print samples, without side effects
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

## Backup and restore

```bash
# Online: download a backup from a running server (global admin).
exspeed backup [--url http://host:8080] [--token T] --output backup.tar

# Offline: restore into a data directory no server is using.
exspeed restore --input backup.tar --data-dir /var/lib/exspeed [--force]

# Offline snapshot of a stopped server's data directory, as-is.
exspeed snapshot --data-dir /var/lib/exspeed --output backup.tar.gz
```

`backup` uses `--url`, or the global `--server`, and `--token`, or
`EXSPEED_AUTH_TOKEN`. It prints the manifest (streams with their offset
ranges) once the archive is complete and verified. `restore` refuses a
non-empty data directory unless you pass `--force`. Consistency guarantees
and what the archive contains are in
[operations.md](operations.md#backup-and-restore).

