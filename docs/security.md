# Security

Auth and TLS are both off by default. You turn each one on separately with
environment variables (or the equivalent flags; see [configuration.md](configuration.md)).

> ⚠️ **Known authorization gaps (v0.5).** Until these are fixed, don't give
> credentials to tenants who must not see each other's data. Details are in
> [REVIEW.md §3.7](REVIEW.md#37-server-http-api-auth).
>
> - **TCP `Query` has no authorization check.** Any authenticated client can
>   read any stream, and any registered external database, with SQL.
> - **TCP `Ack`/`Nack` can move another consumer's offset.**
> - **Scoped admins can list every tenant's streams and consumers.**
> - **`/metrics` is unauthenticated** and exposes every stream and consumer
>   name.
> - **Webhook connectors default to `auth_type = "none"`.**

## Token authentication

```bash
export EXSPEED_AUTH_TOKEN=$(openssl rand -hex 32)
```

When set, every TCP client must include this token in the `Connect` handshake
(`AuthType::Token`) and every HTTP request to `/api/v1/*` must include
`Authorization: Bearer <token>`. The following paths always bypass auth —
they're designed to be reachable by probes, scrapers, and webhook senders:

| Path | Who uses it |
|---|---|
| `GET /healthz` | Liveness probes |
| `GET /readyz` | Readiness probes |
| `GET /metrics` | Prometheus scrape |
| `POST /webhooks/*` | External webhook senders (they carry their own per-webhook auth) |

If you need to authenticate `/metrics` as well, run a reverse-proxy sidecar
that adds Basic auth. Broker-wide bearer tokens aren't a good fit for scrapers.

## Managing credentials (multiple named identities)

`EXSPEED_AUTH_TOKEN` gives one shared admin token. For more than one app
sharing the broker, point `EXSPEED_CREDENTIALS_FILE` at a TOML file with
one entry per app. Each entry stores `sha256(token)` — never the token
itself — and a list of per-stream permissions (`publish`, `subscribe`,
`admin`).

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

**Migration from single `EXSPEED_AUTH_TOKEN`:** the env var keeps working
as a synthetic `legacy-admin` identity (full global admin). Add scoped
credentials to the file; migrate one app at a time; unset the env var
when done. You can set both at once — the server registers `legacy-admin`
from the env var plus every entry in the TOML. (Reserved: an entry named
`legacy-admin` while the env var is set refuses to start; rename the
entry or unset the env var.)

**Rotation:** add a new credential, point the client at its new token,
remove the old entry, restart the server. Restart is required —
credentials are loaded once at boot (v1).

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
(5933) and HTTP (8080) listeners serve TLS using the same cert/key pair — one
cert, two ports. Make sure the cert's SAN list covers every hostname clients
will use.

TLS uses pure-Rust `rustls`. Default protocol versions: TLS 1.2 and 1.3.

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
- TypeScript SDK: `new ExspeedClient({ tls: { rejectUnauthorized: false } })`

> The SDK's `Publisher` class doesn't pass `auth` or `tls` through yet, so
> it can't connect to a secured server. Use `client.publish()` instead.

## Rotation

Cert and token rotation require a server restart. On a single node the
restart is a blip of about 10 seconds. In multi-pod mode the leader doesn't
release its lease on shutdown, so a failover waits the full lease TTL
(30 s by default). Live SIGHUP reload is on the roadmap but not in v1.

## What's not in v1

- mTLS (no client-cert verification)
- SASL / JWT / OAuth2
- Live SIGHUP reload of `credentials.toml` — changes need a restart
- Rate limiting on failed auth attempts — use an ingress WAF or fail2ban

The target deployment model is "trust the network boundary" (VPC, service
mesh, Hetzner private network, k8s namespace) with per-app scoped
credentials on top.
