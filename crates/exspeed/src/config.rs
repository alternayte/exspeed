//! Server configuration: `exspeed.toml`, environment variables and flags.
//!
//! Every setting resolves in this order, later winning:
//! **built-in default < config file < environment variable < command-line
//! flag**. The result is a [`ServerArgs`], validated before anything starts.
//!
//! `exspeed config print-default` prints a commented file with every key;
//! `exspeed config validate` resolves and checks a configuration without
//! starting the server; `exspeed config show` prints the resolved values
//! (secrets redacted).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Deserialize;

use crate::cli::server::{ServerArgs, StorageSyncArg};

/// Environment variable naming the config file when `--config` is absent.
pub const CONFIG_ENV: &str = "EXSPEED_CONFIG";

// ---------------------------------------------------------------------------
// File format
// ---------------------------------------------------------------------------

/// `exspeed.toml`. Every key is optional; unknown keys are errors.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub auth: AuthSection,
    #[serde(default)]
    pub tls: TlsSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub cluster: ClusterSection,
    #[serde(default)]
    pub connectors: ConnectorsSection,
    #[serde(default)]
    pub log: LogSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    pub bind: Option<String>,
    pub api_bind: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub max_connections: Option<usize>,
    pub drain_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSection {
    pub token: Option<String>,
    pub credentials_file: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSection {
    pub sync: Option<String>,
    pub flush_window_us: Option<u64>,
    pub flush_threshold_records: Option<usize>,
    pub flush_threshold_bytes: Option<usize>,
    pub sync_interval_ms: Option<u64>,
    pub sync_bytes: Option<usize>,
    pub dedup_window_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSection {
    pub lease: Option<String>,
    pub postgres_url: Option<String>,
    pub postgres_schema: Option<String>,
    pub redis_url: Option<String>,
    pub redis_key_prefix: Option<String>,
    pub lease_ttl_secs: Option<u64>,
    pub lease_heartbeat_secs: Option<u64>,
    pub bind: Option<String>,
    pub advertise: Option<String>,
    pub client_advertise: Option<String>,
    pub node_id: Option<String>,
    pub replicator_credential: Option<String>,
    pub acks: Option<String>,
    pub min_insync_replicas: Option<usize>,
    pub replica_lag_max_ms: Option<u64>,
    pub ack_timeout_ms: Option<u64>,
    pub unclean_leader_election: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorsSection {
    pub offset_store: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    /// `text` or `json`.
    pub format: Option<String>,
    /// A tracing filter, e.g. `info` or `exspeed=debug,warn`.
    pub level: Option<String>,
}

impl FileConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parsing config file {}", path.display()))
    }
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

/// `exspeed server` flags. Each overrides the config file and the
/// environment; unset flags fall through to them.
#[derive(Args, Debug, Default, Clone)]
pub struct ServeArgs {
    /// Config file (TOML). Defaults to $EXSPEED_CONFIG when set.
    #[arg(long, short = 'c')]
    pub config: Option<PathBuf>,
    /// Client protocol listener [default: 0.0.0.0:5933]
    #[arg(long)]
    pub bind: Option<String>,
    /// HTTP API listener [default: 0.0.0.0:8080]
    #[arg(long)]
    pub api_bind: Option<String>,
    /// Data directory [default: ./exspeed-data]
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Shared admin bearer token (prefer a credentials file).
    #[arg(long, hide_env_values = true)]
    pub auth_token: Option<String>,
    /// Credentials file (TOML) [default: {data_dir}/credentials.toml if it exists]
    #[arg(long)]
    pub credentials_file: Option<PathBuf>,
    /// PEM certificate chain; requires --tls-key
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key; requires --tls-cert
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
    /// `sync` (fsync per group commit) or `async` (fsync on a timer) [default: sync]
    #[arg(long, value_enum)]
    pub storage_sync: Option<StorageSyncArg>,
    /// Group-commit window in microseconds [default: 500]
    #[arg(long)]
    pub storage_flush_window_us: Option<u64>,
    /// Commit early at this many queued records [default: 256]
    #[arg(long)]
    pub storage_flush_threshold_records: Option<usize>,
    /// Commit early at this many queued bytes [default: 1048576]
    #[arg(long)]
    pub storage_flush_threshold_bytes: Option<usize>,
    /// Async mode: fsync interval in ms [default: 10]
    #[arg(long)]
    pub storage_sync_interval_ms: Option<u64>,
    /// Async mode: fsync early after this many unsynced bytes, 0 = timer only [default: 4194304]
    #[arg(long)]
    pub storage_sync_bytes: Option<usize>,
    /// Maximum concurrent client connections [default: 1024]
    #[arg(long)]
    pub max_connections: Option<usize>,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print a commented config file with every setting and its default
    PrintDefault,
    /// Resolve the configuration (file + env + flags) and check it
    Validate(ServeArgs),
    /// Print the resolved configuration (secrets redacted)
    Show(ServeArgs),
}

pub fn run_config_command(cmd: ConfigCommand) -> Result<()> {
    match cmd {
        ConfigCommand::PrintDefault => {
            print!("{DEFAULT_CONFIG}");
            Ok(())
        }
        ConfigCommand::Validate(a) => {
            let args = resolve(&a)?;
            validate(&args)?;
            println!("configuration OK");
            Ok(())
        }
        ConfigCommand::Show(a) => {
            let args = resolve(&a)?;
            print!("{}", show(&args));
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// One layer of settings; `None` = not set at this layer.
#[derive(Default)]
struct Layer {
    bind: Option<String>,
    api_bind: Option<String>,
    data_dir: Option<PathBuf>,
    max_connections: Option<usize>,
    drain_timeout_secs: Option<u64>,
    auth_token: Option<String>,
    credentials_file: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    storage_sync: Option<StorageSyncArg>,
    flush_window_us: Option<u64>,
    flush_threshold_records: Option<usize>,
    flush_threshold_bytes: Option<usize>,
    sync_interval_ms: Option<u64>,
    sync_bytes: Option<usize>,
    dedup_window_secs: Option<u64>,
    lease: Option<String>,
    postgres_url: Option<String>,
    postgres_schema: Option<String>,
    redis_url: Option<String>,
    redis_key_prefix: Option<String>,
    lease_ttl_secs: Option<u64>,
    lease_heartbeat_secs: Option<u64>,
    cluster_bind: Option<String>,
    cluster_advertise: Option<String>,
    client_advertise: Option<String>,
    node_id: Option<String>,
    replicator_credential: Option<String>,
    acks: Option<String>,
    min_insync_replicas: Option<usize>,
    replica_lag_max_ms: Option<u64>,
    ack_timeout_ms: Option<u64>,
    unclean_leader_election: Option<bool>,
    connector_offset_store: Option<String>,
    log_format: Option<String>,
    log_level: Option<String>,
}

fn parse_sync(s: &str) -> Result<StorageSyncArg> {
    match s {
        "sync" => Ok(StorageSyncArg::Sync),
        "async" => Ok(StorageSyncArg::Async),
        other => bail!("storage sync must be `sync` or `async`, got `{other}`"),
    }
}

impl Layer {
    fn from_file(f: FileConfig) -> Result<Self> {
        Ok(Self {
            bind: f.server.bind,
            api_bind: f.server.api_bind,
            data_dir: f.server.data_dir,
            max_connections: f.server.max_connections,
            drain_timeout_secs: f.server.drain_timeout_secs,
            auth_token: f.auth.token,
            credentials_file: f.auth.credentials_file,
            tls_cert: f.tls.cert,
            tls_key: f.tls.key,
            storage_sync: f.storage.sync.as_deref().map(parse_sync).transpose()?,
            flush_window_us: f.storage.flush_window_us,
            flush_threshold_records: f.storage.flush_threshold_records,
            flush_threshold_bytes: f.storage.flush_threshold_bytes,
            sync_interval_ms: f.storage.sync_interval_ms,
            sync_bytes: f.storage.sync_bytes,
            dedup_window_secs: f.storage.dedup_window_secs,
            lease: f.cluster.lease,
            postgres_url: f.cluster.postgres_url,
            postgres_schema: f.cluster.postgres_schema,
            redis_url: f.cluster.redis_url,
            redis_key_prefix: f.cluster.redis_key_prefix,
            lease_ttl_secs: f.cluster.lease_ttl_secs,
            lease_heartbeat_secs: f.cluster.lease_heartbeat_secs,
            cluster_bind: f.cluster.bind,
            cluster_advertise: f.cluster.advertise,
            client_advertise: f.cluster.client_advertise,
            node_id: f.cluster.node_id,
            replicator_credential: f.cluster.replicator_credential,
            acks: f.cluster.acks,
            min_insync_replicas: f.cluster.min_insync_replicas,
            replica_lag_max_ms: f.cluster.replica_lag_max_ms,
            ack_timeout_ms: f.cluster.ack_timeout_ms,
            unclean_leader_election: f.cluster.unclean_leader_election,
            connector_offset_store: f.connectors.offset_store,
            log_format: f.log.format,
            log_level: f.log.level,
        })
    }

    /// Environment layer. `get` abstracts `std::env::var` for tests.
    fn from_env(get: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        // Empty values count as unset (a common shell/k8s artifact).
        let s = |k: &str| get(k).filter(|v| !v.is_empty());
        fn num<T: std::str::FromStr>(k: &str, v: Option<String>) -> Result<Option<T>> {
            v.map(|v| {
                v.parse::<T>()
                    .map_err(|_| anyhow::anyhow!("{k}: not a number: {v}"))
            })
            .transpose()
        }
        let first = |keys: &[&str]| keys.iter().find_map(|k| s(k));
        Ok(Self {
            bind: s("EXSPEED_BIND"),
            api_bind: s("EXSPEED_API_BIND"),
            data_dir: s("EXSPEED_DATA_DIR").map(PathBuf::from),
            max_connections: num("EXSPEED_MAX_CONNS", s("EXSPEED_MAX_CONNS"))?,
            drain_timeout_secs: num(
                "EXSPEED_DRAIN_TIMEOUT_SECS",
                s("EXSPEED_DRAIN_TIMEOUT_SECS"),
            )?,
            auth_token: s("EXSPEED_AUTH_TOKEN"),
            credentials_file: s("EXSPEED_CREDENTIALS_FILE").map(PathBuf::from),
            tls_cert: s("EXSPEED_TLS_CERT").map(PathBuf::from),
            tls_key: s("EXSPEED_TLS_KEY").map(PathBuf::from),
            storage_sync: s("EXSPEED_STORAGE_SYNC")
                .as_deref()
                .map(parse_sync)
                .transpose()?,
            flush_window_us: num("EXSPEED_FLUSH_WINDOW_US", s("EXSPEED_FLUSH_WINDOW_US"))?,
            flush_threshold_records: num(
                "EXSPEED_FLUSH_THRESHOLD_RECORDS",
                s("EXSPEED_FLUSH_THRESHOLD_RECORDS"),
            )?,
            flush_threshold_bytes: num(
                "EXSPEED_FLUSH_THRESHOLD_BYTES",
                s("EXSPEED_FLUSH_THRESHOLD_BYTES"),
            )?,
            sync_interval_ms: num("EXSPEED_SYNC_INTERVAL_MS", s("EXSPEED_SYNC_INTERVAL_MS"))?,
            sync_bytes: num("EXSPEED_SYNC_BYTES", s("EXSPEED_SYNC_BYTES"))?,
            dedup_window_secs: num("EXSPEED_DEDUP_WINDOW_SECS", s("EXSPEED_DEDUP_WINDOW_SECS"))?,
            lease: first(&["EXSPEED_LEASE_BACKEND", "EXSPEED_CONSUMER_STORE"]),
            postgres_url: first(&[
                "EXSPEED_LEASE_POSTGRES_URL",
                "EXSPEED_OFFSET_STORE_POSTGRES_URL",
            ]),
            postgres_schema: first(&[
                "EXSPEED_LEASE_POSTGRES_SCHEMA",
                "EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA",
            ]),
            redis_url: first(&["EXSPEED_LEASE_REDIS_URL", "EXSPEED_OFFSET_STORE_REDIS_URL"]),
            redis_key_prefix: s("EXSPEED_LEASE_REDIS_KEY_PREFIX"),
            lease_ttl_secs: num("EXSPEED_LEASE_TTL_SECS", s("EXSPEED_LEASE_TTL_SECS"))?,
            lease_heartbeat_secs: num(
                "EXSPEED_LEASE_HEARTBEAT_SECS",
                s("EXSPEED_LEASE_HEARTBEAT_SECS"),
            )?,
            cluster_bind: s("EXSPEED_CLUSTER_BIND"),
            cluster_advertise: s("EXSPEED_CLUSTER_ADVERTISE"),
            client_advertise: s("EXSPEED_CLIENT_ADVERTISE"),
            node_id: s("EXSPEED_NODE_ID"),
            replicator_credential: s("EXSPEED_REPLICATOR_CREDENTIAL"),
            acks: s("EXSPEED_ACKS"),
            min_insync_replicas: num(
                "EXSPEED_MIN_INSYNC_REPLICAS",
                s("EXSPEED_MIN_INSYNC_REPLICAS"),
            )?,
            replica_lag_max_ms: num(
                "EXSPEED_REPLICA_LAG_MAX_MS",
                s("EXSPEED_REPLICA_LAG_MAX_MS"),
            )?,
            ack_timeout_ms: num("EXSPEED_ACK_TIMEOUT_MS", s("EXSPEED_ACK_TIMEOUT_MS"))?,
            unclean_leader_election: num(
                "EXSPEED_UNCLEAN_LEADER_ELECTION",
                s("EXSPEED_UNCLEAN_LEADER_ELECTION"),
            )?,
            connector_offset_store: s("EXSPEED_CONNECTOR_OFFSET_STORE"),
            log_format: s("LOG_FORMAT"),
            log_level: s("RUST_LOG"),
        })
    }

    fn from_flags(a: &ServeArgs) -> Self {
        Self {
            bind: a.bind.clone(),
            api_bind: a.api_bind.clone(),
            data_dir: a.data_dir.clone(),
            max_connections: a.max_connections,
            auth_token: a.auth_token.clone(),
            credentials_file: a.credentials_file.clone(),
            tls_cert: a.tls_cert.clone(),
            tls_key: a.tls_key.clone(),
            storage_sync: a.storage_sync,
            flush_window_us: a.storage_flush_window_us,
            flush_threshold_records: a.storage_flush_threshold_records,
            flush_threshold_bytes: a.storage_flush_threshold_bytes,
            sync_interval_ms: a.storage_sync_interval_ms,
            sync_bytes: a.storage_sync_bytes,
            ..Self::default()
        }
    }

    fn apply(self, t: &mut ServerArgs) {
        macro_rules! set {
            ($src:ident => $($dst:tt)+) => {
                if let Some(v) = self.$src {
                    t.$($dst)+ = v;
                }
            };
        }
        set!(bind => bind);
        set!(api_bind => api_bind);
        set!(data_dir => data_dir);
        set!(max_connections => max_connections);
        set!(drain_timeout_secs => drain_timeout_secs);
        if self.auth_token.is_some() {
            t.auth_token = self.auth_token;
        }
        if self.credentials_file.is_some() {
            t.credentials_file = self.credentials_file;
        }
        if self.tls_cert.is_some() {
            t.tls_cert = self.tls_cert;
        }
        if self.tls_key.is_some() {
            t.tls_key = self.tls_key;
        }
        set!(storage_sync => storage_sync);
        set!(flush_window_us => storage_flush_window_us);
        set!(flush_threshold_records => storage_flush_threshold_records);
        set!(flush_threshold_bytes => storage_flush_threshold_bytes);
        set!(sync_interval_ms => storage_sync_interval_ms);
        set!(sync_bytes => storage_sync_bytes);
        set!(dedup_window_secs => dedup_window_secs);
        set!(lease => cluster.lease);
        if self.postgres_url.is_some() {
            t.cluster.postgres_url = self.postgres_url;
        }
        set!(postgres_schema => cluster.postgres_schema);
        if self.redis_url.is_some() {
            t.cluster.redis_url = self.redis_url;
        }
        set!(redis_key_prefix => cluster.redis_key_prefix);
        set!(lease_ttl_secs => cluster.lease_ttl_secs);
        set!(lease_heartbeat_secs => cluster.lease_heartbeat_secs);
        set!(cluster_bind => cluster.bind);
        if self.cluster_advertise.is_some() {
            t.cluster.advertise = self.cluster_advertise;
        }
        if self.client_advertise.is_some() {
            t.cluster.client_advertise = self.client_advertise;
        }
        if self.node_id.is_some() {
            t.cluster.node_id = self.node_id;
        }
        if self.replicator_credential.is_some() {
            t.cluster.replicator_credential = self.replicator_credential;
        }
        set!(acks => cluster.acks);
        set!(min_insync_replicas => cluster.min_insync_replicas);
        set!(replica_lag_max_ms => cluster.replica_lag_max_ms);
        set!(ack_timeout_ms => cluster.ack_timeout_ms);
        set!(unclean_leader_election => cluster.unclean_leader_election);
        set!(connector_offset_store => connector_offset_store);
        if self.log_format.is_some() {
            t.log_format = self.log_format;
        }
        if self.log_level.is_some() {
            t.log_level = self.log_level;
        }
    }
}

/// Resolve defaults < file < env < flags into server settings.
pub fn resolve(flags: &ServeArgs) -> Result<ServerArgs> {
    resolve_with(flags, &|k| std::env::var(k).ok())
}

fn resolve_with(flags: &ServeArgs, env: &dyn Fn(&str) -> Option<String>) -> Result<ServerArgs> {
    let mut args = ServerArgs::default();
    let path = flags
        .config
        .clone()
        .or_else(|| env(CONFIG_ENV).filter(|v| !v.is_empty()).map(PathBuf::from));
    if let Some(p) = &path {
        Layer::from_file(FileConfig::load(p)?)?.apply(&mut args);
        args.config_file = Some(p.clone());
    }
    Layer::from_env(env)?.apply(&mut args);
    Layer::from_flags(flags).apply(&mut args);
    // Normalize: empty token = unset.
    args.auth_token = args.auth_token.filter(|t| !t.is_empty());
    Ok(args)
}

/// Static checks that don't need the network.
pub fn validate(a: &ServerArgs) -> Result<()> {
    use std::net::SocketAddr;
    a.bind
        .parse::<SocketAddr>()
        .with_context(|| format!("bind `{}` is not host:port", a.bind))?;
    a.api_bind
        .parse::<SocketAddr>()
        .with_context(|| format!("api_bind `{}` is not host:port", a.api_bind))?;
    if a.tls_cert.is_some() != a.tls_key.is_some() {
        bail!("TLS needs both a certificate and a key");
    }
    for p in [&a.tls_cert, &a.tls_key, &a.credentials_file]
        .into_iter()
        .flatten()
    {
        if !p.exists() {
            bail!("file not found: {}", p.display());
        }
    }
    if a.max_connections == 0 {
        bail!("max_connections must be at least 1");
    }
    match a.cluster.lease.as_str() {
        "none" => {}
        "postgres" if a.cluster.postgres_url.is_none() => {
            bail!("cluster.lease = postgres needs cluster.postgres_url")
        }
        "redis" if a.cluster.redis_url.is_none() => {
            bail!("cluster.lease = redis needs cluster.redis_url")
        }
        "postgres" | "redis" => {
            a.cluster
                .bind
                .parse::<SocketAddr>()
                .with_context(|| format!("cluster.bind `{}` is not host:port", a.cluster.bind))?;
            let auth_on = a.auth_token.is_some()
                || a.credentials_file.is_some()
                || a.data_dir.join("credentials.toml").exists();
            if auth_on && a.cluster.replicator_credential.is_none() {
                bail!(
                    "with auth enabled, a cluster needs cluster.replicator_credential \
                     (a token with the `replicate` action)"
                );
            }
            if a.cluster.lease_heartbeat_secs == 0
                || a.cluster.lease_heartbeat_secs * 3 > a.cluster.lease_ttl_secs
            {
                bail!("cluster.lease_heartbeat_secs must be between 1 and lease_ttl_secs / 3");
            }
            if a.cluster.acks != "all" && a.cluster.acks != "leader" {
                bail!(
                    "cluster.acks must be `all` or `leader`, got `{}`",
                    a.cluster.acks
                );
            }
            if a.cluster.min_insync_replicas == 0 {
                bail!("cluster.min_insync_replicas must be at least 1");
            }
            if let Some(c) = &a.cluster.client_advertise {
                if !c.contains(':') {
                    bail!("cluster.client_advertise `{c}` must be host:port");
                }
            }
        }
        other => bail!("cluster.lease must be none, postgres or redis, got `{other}`"),
    }
    match a.connector_offset_store.as_str() {
        "log" | "file" => {}
        other => bail!("connectors.offset_store must be log or file, got `{other}`"),
    }
    if let Some(f) = &a.log_format {
        if f != "text" && f != "json" {
            bail!("log.format must be text or json, got `{f}`");
        }
    }
    if let Some(path) = &a.credentials_file {
        exspeed_common::auth::CredentialStore::build(Some(path), a.auth_token.as_deref())
            .map_err(|e| anyhow::anyhow!("credentials: {e}"))?;
    }
    Ok(())
}

/// The resolved settings as TOML, secrets redacted.
pub fn show(a: &ServerArgs) -> String {
    let secret = |v: &Option<String>| match v {
        Some(_) => "\"<redacted>\"".to_string(),
        None => "# unset".to_string(),
    };
    let opt_path = |v: &Option<PathBuf>| match v {
        Some(p) => format!("{:?}", p.display().to_string()),
        None => "# unset".to_string(),
    };
    let opt = |v: &Option<String>| match v {
        Some(s) => format!("{s:?}"),
        None => "# unset".to_string(),
    };
    let sync = match a.storage_sync {
        StorageSyncArg::Sync => "sync",
        StorageSyncArg::Async => "async",
    };
    format!(
        r#"# resolved from: {source}
[server]
bind = {bind:?}
api_bind = {api_bind:?}
data_dir = {data_dir:?}
max_connections = {maxc}
drain_timeout_secs = {drain}

[auth]
token = {token}
credentials_file = {creds}

[tls]
cert = {cert}
key = {key}

[storage]
sync = "{sync}"
flush_window_us = {fw}
flush_threshold_records = {ftr}
flush_threshold_bytes = {ftb}
sync_interval_ms = {si}
sync_bytes = {sb}
dedup_window_secs = {dw}

[cluster]
lease = {lease:?}
postgres_url = {pg}
postgres_schema = {pgs:?}
redis_url = {redis}
redis_key_prefix = {rkp:?}
lease_ttl_secs = {ttl}
lease_heartbeat_secs = {hb}
bind = {cbind:?}
advertise = {adv}
client_advertise = {cadv}
node_id = {nid}
replicator_credential = {repl}
acks = {acks:?}
min_insync_replicas = {misr}
replica_lag_max_ms = {lag}
ack_timeout_ms = {ackt}
unclean_leader_election = {unclean}

[connectors]
offset_store = {os:?}

[log]
format = {lf}
level = {ll}
"#,
        source = a
            .config_file
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "defaults + env + flags".into()),
        bind = a.bind,
        api_bind = a.api_bind,
        data_dir = a.data_dir.display().to_string(),
        maxc = a.max_connections,
        drain = a.drain_timeout_secs,
        token = secret(&a.auth_token),
        creds = opt_path(&a.credentials_file),
        cert = opt_path(&a.tls_cert),
        key = opt_path(&a.tls_key),
        fw = a.storage_flush_window_us,
        ftr = a.storage_flush_threshold_records,
        ftb = a.storage_flush_threshold_bytes,
        si = a.storage_sync_interval_ms,
        sb = a.storage_sync_bytes,
        dw = a.dedup_window_secs,
        lease = a.cluster.lease,
        pg = secret(&a.cluster.postgres_url),
        pgs = a.cluster.postgres_schema,
        redis = secret(&a.cluster.redis_url),
        rkp = a.cluster.redis_key_prefix,
        ttl = a.cluster.lease_ttl_secs,
        hb = a.cluster.lease_heartbeat_secs,
        cbind = a.cluster.bind,
        adv = opt(&a.cluster.advertise),
        cadv = opt(&a.cluster.client_advertise),
        nid = opt(&a.cluster.node_id),
        repl = secret(&a.cluster.replicator_credential),
        acks = a.cluster.acks,
        misr = a.cluster.min_insync_replicas,
        lag = a.cluster.replica_lag_max_ms,
        ackt = a.cluster.ack_timeout_ms,
        unclean = a.cluster.unclean_leader_election,
        os = a.connector_offset_store,
        lf = opt(&a.log_format),
        ll = opt(&a.log_level),
    )
}

/// Printed by `exspeed config print-default`.
pub const DEFAULT_CONFIG: &str = r#"# exspeed.toml: every setting with its default. All keys are optional.
# Precedence: default < this file < environment variable < command-line flag.
# Start with: exspeed server --config exspeed.toml   (or EXSPEED_CONFIG=...)

[server]
bind = "0.0.0.0:5933"            # client protocol (EXSPEED_BIND, --bind)
api_bind = "0.0.0.0:8080"        # HTTP API, probes, metrics (EXSPEED_API_BIND, --api-bind)
data_dir = "./exspeed-data"      # (EXSPEED_DATA_DIR, --data-dir)
max_connections = 1024           # (EXSPEED_MAX_CONNS, --max-connections)
drain_timeout_secs = 10          # time given to open connections on shutdown (EXSPEED_DRAIN_TIMEOUT_SECS)

[auth]
# credentials_file = "/etc/exspeed/credentials.toml"   # (EXSPEED_CREDENTIALS_FILE)
#                    default: {data_dir}/credentials.toml when it exists
# token = "..."      # one shared admin token (EXSPEED_AUTH_TOKEN); prefer credentials_file

[tls]
# cert = "/etc/exspeed/tls.crt"  # PEM chain (EXSPEED_TLS_CERT); requires key
# key = "/etc/exspeed/tls.key"   # (EXSPEED_TLS_KEY)

[storage]
sync = "sync"                    # "sync": fsync per group commit; "async": fsync on a timer (EXSPEED_STORAGE_SYNC)
flush_window_us = 500            # group-commit window (EXSPEED_FLUSH_WINDOW_US)
flush_threshold_records = 256    # (EXSPEED_FLUSH_THRESHOLD_RECORDS)
flush_threshold_bytes = 1048576  # (EXSPEED_FLUSH_THRESHOLD_BYTES)
sync_interval_ms = 10            # async mode only (EXSPEED_SYNC_INTERVAL_MS)
sync_bytes = 4194304             # async mode: fsync early after this many unsynced bytes, 0 = timer only (EXSPEED_SYNC_BYTES)
dedup_window_secs = 300          # default msg_id dedup window for new streams (EXSPEED_DEDUP_WINDOW_SECS)

[cluster]
lease = "none"                   # "none" (single node), "postgres" or "redis" (EXSPEED_LEASE_BACKEND)
# postgres_url = "postgres://user:pass@host/db"   # (EXSPEED_LEASE_POSTGRES_URL)
postgres_schema = "public"       # (EXSPEED_LEASE_POSTGRES_SCHEMA)
# redis_url = "redis://host:6379"                 # (EXSPEED_LEASE_REDIS_URL)
redis_key_prefix = "exspeed:lease:"               # (EXSPEED_LEASE_REDIS_KEY_PREFIX)
lease_ttl_secs = 15              # a dead leader is replaced within about this long (EXSPEED_LEASE_TTL_SECS)
lease_heartbeat_secs = 3         # at most ttl/3 (EXSPEED_LEASE_HEARTBEAT_SECS)
bind = "0.0.0.0:5934"            # replication listener (EXSPEED_CLUSTER_BIND)
# advertise = "exspeed-0.exspeed:5934"            # address peers replicate from; default: bind (EXSPEED_CLUSTER_ADVERTISE)
# client_advertise = "exspeed-0.exspeed:5933"     # address clients are redirected to when this node leads (EXSPEED_CLIENT_ADVERTISE)
# node_id = "..."                # default: generated once and kept in {data_dir}/node_id (EXSPEED_NODE_ID)
# replicator_credential = "..."  # token followers present (needs the `replicate` action); required with auth (EXSPEED_REPLICATOR_CREDENTIAL)
acks = "all"                     # "all": ack once every in-sync replica has the write; "leader": ack after the local write (EXSPEED_ACKS)
min_insync_replicas = 1          # acks=all writes fail with 503 while fewer replicas (leader included) are in sync (EXSPEED_MIN_INSYNC_REPLICAS)
replica_lag_max_ms = 10000       # a follower that hasn't caught up for this long leaves the ISR (EXSPEED_REPLICA_LAG_MAX_MS)
ack_timeout_ms = 10000           # how long an acks=all write waits for replication (EXSPEED_ACK_TIMEOUT_MS)
unclean_leader_election = false  # true: any node may take over, even one missing acknowledged writes (EXSPEED_UNCLEAN_LEADER_ELECTION)

[connectors]
offset_store = "log"             # "log" (__connector_offsets stream) or "file" (EXSPEED_CONNECTOR_OFFSET_STORE)

[log]
format = "text"                  # "text" or "json" (LOG_FORMAT)
level = "info"                   # tracing filter, e.g. "exspeed=debug,warn" (RUST_LOG)
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn default_config_parses_and_matches_defaults() {
        let f: FileConfig = toml::from_str(DEFAULT_CONFIG).unwrap();
        let mut a = ServerArgs::default();
        Layer::from_file(f).unwrap().apply(&mut a);
        let d = ServerArgs::default();
        assert_eq!(a.bind, d.bind);
        assert_eq!(a.max_connections, d.max_connections);
        assert_eq!(a.cluster.lease, d.cluster.lease);
        assert_eq!(a.cluster.lease_ttl_secs, d.cluster.lease_ttl_secs);
        assert_eq!(a.storage_sync_bytes, d.storage_sync_bytes);
        assert_eq!(a.connector_offset_store, d.connector_offset_store);
    }

    #[test]
    fn precedence_is_default_file_env_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exspeed.toml");
        std::fs::write(
            &path,
            r#"
[server]
bind = "127.0.0.1:1000"
api_bind = "127.0.0.1:2000"
max_connections = 7
[storage]
dedup_window_secs = 60
"#,
        )
        .unwrap();
        let flags = ServeArgs {
            config: Some(path.clone()),
            bind: Some("127.0.0.1:3000".into()),
            ..Default::default()
        };
        let a = resolve_with(
            &flags,
            &env(&[("EXSPEED_BIND", "127.0.0.1:9"), ("EXSPEED_MAX_CONNS", "9")]),
        )
        .unwrap();
        assert_eq!(a.bind, "127.0.0.1:3000", "flag beats env and file");
        assert_eq!(a.max_connections, 9, "env beats file");
        assert_eq!(a.api_bind, "127.0.0.1:2000", "file beats default");
        assert_eq!(a.dedup_window_secs, 60);
        assert_eq!(a.data_dir, PathBuf::from("./exspeed-data"), "default");
        assert_eq!(a.config_file, Some(path));
    }

    #[test]
    fn config_path_from_env_and_unknown_keys_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.toml");
        std::fs::write(&path, "[server]\nbnd = \"x\"\n").unwrap();
        let err = resolve_with(
            &ServeArgs::default(),
            &env(&[(CONFIG_ENV, path.to_str().unwrap())]),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("bnd"), "{err:#}");
    }

    #[test]
    fn legacy_env_names_still_work() {
        let a = resolve_with(
            &ServeArgs::default(),
            &env(&[
                ("EXSPEED_CONSUMER_STORE", "postgres"),
                ("EXSPEED_OFFSET_STORE_POSTGRES_URL", "postgres://x"),
            ]),
        )
        .unwrap();
        assert_eq!(a.cluster.lease, "postgres");
        assert_eq!(a.cluster.postgres_url.as_deref(), Some("postgres://x"));
    }

    #[test]
    fn validation_catches_mistakes() {
        let ok = ServerArgs::default();
        validate(&ok).unwrap();

        let a = ServerArgs {
            tls_cert: Some("/nope".into()),
            ..Default::default()
        };
        assert!(format!("{:#}", validate(&a).unwrap_err()).contains("TLS"));

        let mut a = ServerArgs::default();
        a.cluster.lease = "postgres".into();
        assert!(format!("{:#}", validate(&a).unwrap_err()).contains("postgres_url"));

        let mut a = ServerArgs::default();
        a.cluster.lease = "redis".into();
        a.cluster.redis_url = Some("redis://x".into());
        validate(&a).unwrap(); // no auth: no replicator credential needed
        a.auth_token = Some("t".into());
        assert!(format!("{:#}", validate(&a).unwrap_err()).contains("replicator_credential"));
        a.cluster.replicator_credential = Some("r".into());
        validate(&a).unwrap();
        a.cluster.acks = "some".into();
        assert!(format!("{:#}", validate(&a).unwrap_err()).contains("acks"));
        a.cluster.acks = "leader".into();
        a.cluster.lease_heartbeat_secs = 10;
        assert!(format!("{:#}", validate(&a).unwrap_err()).contains("heartbeat"));

        let a = ServerArgs {
            bind: "nonsense".into(),
            ..Default::default()
        };
        assert!(validate(&a).is_err());

        let bad_num = resolve_with(
            &ServeArgs::default(),
            &env(&[("EXSPEED_MAX_CONNS", "lots")]),
        );
        assert!(bad_num.is_err());
    }

    #[test]
    fn show_redacts_secrets() {
        let mut a = ServerArgs {
            auth_token: Some("hunter2".into()),
            ..Default::default()
        };
        a.cluster.postgres_url = Some("postgres://u:pw@h/db".into());
        let s = show(&a);
        assert!(!s.contains("hunter2") && !s.contains("pw@"));
        // And it is valid TOML for the file format.
        let _: FileConfig = toml::from_str(
            &s.replace("# unset", "")
                .lines()
                .filter(|l| !l.trim_end().ends_with('='))
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
    }
}
