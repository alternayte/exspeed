# Security

Auth and TLS are both off by default. You turn each one on separately with
environment variables, the `[auth]` and `[tls]` sections of `exspeed.toml`,
or the equivalent flags (see [configuration.md](configuration.md)). Auth is
also on whenever `{data_dir}/credentials.toml` exists.

Authorization at a glance:

- Listing streams and consumers (TCP and HTTP) returns only those the caller
  has a permission on. Internal `__` streams are listed for global admins
  only (over HTTP only with `?internal=true`).
- Internal `__` streams (consumer state, catalogs, connector offsets) are
  written only by the server: client creates, updates, publishes and deletes
  on them are refused with 403 over both TCP and HTTP.
- SQL queries need a **global admin** credential, over both TCP and HTTP.
- Ack, nack, term and in-progress need `subscribe` on the consumer's stream.
  Any connection with that permission can settle that stream's consumers'
  records, which is what lets several app instances share one consumer.
- Key-value operations check the bucket's stream `KV_<bucket>`: reads need
  `subscribe`, puts and deletes `publish`, creating a bucket `admin`.
- Core messages check subjects, not streams (see
  [subject permissions](#subject-permissions)).
- HTTP management routes need an `admin` permission; see
  [http-api.md](http-api.md#authentication) for the per-route rules. The
  HTTP record browser (`GET /api/v1/streams/{name}/records`) needs
  `subscribe` or `admin` on the stream.
- `/metrics` is open unless you set a metrics token (see below). It exposes
  stream, consumer and connector names.

## Token authentication

```bash
export EXSPEED_AUTH_TOKEN=$(openssl rand -hex 32)
```

When the token is set (`EXSPEED_AUTH_TOKEN`, `auth.token` in `exspeed.toml`,
or `--auth-token`), every TCP client must send this token in the `token` field of the `Connect` handshake,
and every HTTP request to `/api/v1/*` must include
`Authorization: Bearer <token>`. A missing or wrong token fails the TCP
handshake and gets `401` over HTTP. The token is a full admin on every
stream. The following paths always bypass auth; they're meant to be
reachable by probes, scrapers, API tooling and webhook senders:

| Path | Who uses it |
|---|---|
| `GET /healthz` | Liveness probes |
| `GET /readyz` | Readiness probes |
| `GET /metrics` | Prometheus scrape (see the metrics token below) |
| `GET /api/v1/openapi.json` | API tooling (it describes the API, not its data) |
| `POST /webhooks/*` | External webhook senders (each `http_webhook` connector can carry its own auth) |

### Metrics token

Broker credentials are a poor fit for scrapers, so `/metrics` has its own
optional token:

```toml
[server]
metrics_token = "..."   # or EXSPEED_METRICS_TOKEN
```

When it is set, `GET /metrics` answers 401 unless the request carries
`Authorization: Bearer <metrics_token>`. In Prometheus, set
`authorization: { credentials: <token> }` on the scrape job; the Helm
chart sets the variable and the ServiceMonitor's credentials from the Secret
named by `metricsTokenSecret`. Without
the token `/metrics` is open, which is fine only when the API port is not
reachable by untrusted clients.

## Managing credentials (multiple named identities)

`EXSPEED_AUTH_TOKEN` gives one shared admin token. For more than one app
sharing the broker, point `EXSPEED_CREDENTIALS_FILE` at a TOML file with
one entry per app (or put it at `{data_dir}/credentials.toml`). Each entry
stores `sha256(token)`, never the token itself, and a list of permissions.
Each permission pairs a stream glob with actions:

- `streams`: a stream-name pattern with `*` as the only wildcard (zero or
  more characters), for example `orders-*` or `*`. Patterns may contain
  letters, digits, `_`, `-` and `*`. A permission on `*` with `admin` is a
  **global admin**.
- `actions`: any of `publish`, `subscribe`, `admin`, and `replicate` (the
  cluster port; followers need it, see
  [high-availability.md](high-availability.md)).

Names and token hashes must be unique; an unknown action or an invalid
pattern stops the server from starting.

### Subject permissions

[Core messages](messaging.md) aren't stored in a stream, so their
permissions name subjects. A permission entry has `subjects` (a NATS-style
filter) instead of `streams`, with `publish` and/or `subscribe`:

```toml
[[credentials]]
name = "pricing-service"
token_sha256 = "..."
permissions = [
  { subjects = "pricing.>", actions = ["subscribe"] },   # serve requests
  { subjects = "events.prices.*", actions = ["publish"] },
]
```

- Publishing needs a permission whose filter matches the subject.
- Subscribing needs a permission whose filter covers every subject the
  subscription's filter can match: `pricing.>` allows subscribing to
  `pricing.quotes.*` but not to `>`.
- A wildcard-all stream permission (`streams = "*"`) grants the same verbs
  on every subject.
- Request-reply works without extra entries: anyone may publish a reply to an
  `_INBOX.…` subject, and a client may subscribe to its own inbox
  (`_INBOX.<id>.>` or `_INBOX.<id>.*`). Subscribing to every inbox
  (`_INBOX.>`, `_INBOX.*.…`) is refused unless a permission covers it.
- The same rules apply to [NATS](nats.md) connections, which authenticate
  with the same tokens (`auth_token`, or `pass`) and client certificates.
  Publishing to a subject a stream [captures](nats.md#stream-capture) also
  needs `publish` on that stream.

**Generate a credential:**

```bash
$ exspeed auth gen-token
a3f7...                    # stdout: raw token — give this to the app, ONCE
b29c...                    # stderr: sha256(token) — paste into the TOML
```

**Example `credentials.toml`:**

```toml
[[credentials]]
name = "orders-service"
token_sha256 = "<the stderr output from gen-token>"
permissions = [
  { streams = "orders-*", actions = ["publish", "subscribe"] },
]

[[credentials]]
name = "ops-admin"
token_sha256 = "<another sha256>"
permissions = [
  { streams = "*", actions = ["publish", "subscribe", "admin"] },
]
```

**Run the server:**

```bash
EXSPEED_CREDENTIALS_FILE=/etc/exspeed/credentials.toml \
EXSPEED_TLS_CERT=/etc/exspeed/cert.pem \
EXSPEED_TLS_KEY=/etc/exspeed/key.pem \
  exspeed server
```

**Shared token and credentials file together:** you can set both. The
server registers every entry in the file plus a `legacy-admin` identity
(full global admin) for `EXSPEED_AUTH_TOKEN`. This lets you move apps to
scoped credentials one at a time and unset the shared token afterwards.
The name `legacy-admin` is reserved while the token is set: a file entry
with that name stops the server from starting.

**Rotation:** add a new credential, point the client at its new token,
remove the old entry, restart the server. Credentials are loaded once at
startup, so changes take effect on restart.

**Multi-pod:** each pod reads its own `EXSPEED_CREDENTIALS_FILE`.
Distribute the same file to every pod via a k8s Secret mount, Coolify
file mount, Ansible, etc. Divergent files produce inconsistent authz
across the cluster.

**Verify what your token grants:** `exspeed auth whoami` calls
`GET /api/v1/whoami` and prints the identity + permissions JSON.

**Validate the file offline (CI):** `exspeed auth lint /path/to/credentials.toml`
exits non-zero with a specific error on any parse or validation failure.

## TLS

```bash
export EXSPEED_TLS_CERT=/etc/exspeed/tls/fullchain.pem
export EXSPEED_TLS_KEY=/etc/exspeed/tls/privkey.pem
```

Both variables must be set together, or neither. When set, **both** the TCP
(5933) and HTTP (8080) listeners serve TLS using the same cert/key pair: one
cert, two ports. Make sure the cert's SAN list covers every hostname clients
will use. In a cluster, `cluster.tls = true` serves the same certificate on
the replication port and makes followers verify it; see
[high-availability.md](high-availability.md#tls-on-the-cluster-port).

TLS uses pure-Rust `rustls`, with TLS 1.2 and 1.3.

### Client certificates (mutual TLS)

```toml
[tls]
cert = "/etc/exspeed/tls.crt"
key = "/etc/exspeed/tls.key"
client_ca = "/etc/exspeed/clients-ca.crt"   # EXSPEED_TLS_CLIENT_CA, --tls-client-ca
```

With `client_ca` set, the TCP port accepts only clients that present a
certificate signed by that CA; the TLS handshake fails for everyone else.
The HTTP port keeps using bearer tokens.

A credential can be bound to a certificate instead of a token: `cert_cn`
names the certificate's subject common name (or, when it has none, its first
DNS subject-alternative name).

```toml
[[credentials]]
name = "orders-service"
cert_cn = "orders.internal"
permissions = [{ streams = "orders-*", actions = ["publish", "subscribe"] }]
```

A client that connects with a valid certificate and no token gets the
permissions of the credential bound to the certificate's name. A token, when
sent, takes precedence. A valid certificate that no credential names fails
the handshake with `401` (when auth is on; with auth off, any client holding
a valid certificate gets full access).

The TypeScript SDK passes `tls: { cert, key, ca }` through to Node's TLS; the
Rust client takes a rustls `ClientConfig` built with
`with_client_auth_cert`.

### Dev certs

For local development, generate a self-signed cert:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout dev-key.pem -out dev-cert.pem
```

Point the server at it:

```bash
EXSPEED_TLS_CERT=dev-cert.pem EXSPEED_TLS_KEY=dev-key.pem \
  exspeed server
```

Clients that don't trust your dev CA need to opt in:

- CLI: `EXSPEED_INSECURE_SKIP_VERIFY=1 exspeed streams`
- TypeScript SDK: `ExspeedClient.connect({ tls: { rejectUnauthorized: false } })`,
  or better, `tls: { ca: readFileSync("dev-cert.pem") }` to trust the dev
  cert explicitly.

`client.publisher()` and every other SDK API run on the client's own
connection, so they use its `token` and `tls` settings.

## Rotation

Certificates, the shared token and `credentials.toml` are read at startup,
so rotating any of them takes a server restart. On a single node the
restart is a blip of a few seconds. In multi-pod mode a gracefully stopped
leader releases its lease, so a follower takes over within about one
heartbeat interval (a crashed leader is replaced once its lease TTL, 15 s by
default, runs out). Restart the nodes one at a time.

## Not supported

- Client certificates on the HTTP port (it uses bearer tokens)
- SASL / JWT / OAuth2
- Reloading certificates or credentials without a restart
- Rate limiting on failed auth attempts: use an ingress WAF or fail2ban

The target deployment model is "trust the network boundary" (VPC, service
mesh, Hetzner private network, k8s namespace) with per-app scoped
credentials on top.
