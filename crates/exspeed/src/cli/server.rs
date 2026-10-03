use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};

use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::Broker;
use exspeed_common::auth::CredentialStore;
use exspeed_connectors::ConnectorManager;
use exspeed_processing::ExqlEngine;
use exspeed_storage::file::FileStorage;
use exspeed_streams::StorageEngine;

use crate::session::{self, SessionContext};

/// A TLS handshake must finish within this time or the socket is dropped.
const TLS_ACCEPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Default cluster-replication bind address. Matches the advertised default
/// in the replication design doc and the Plan G Wave 5 contract.
const DEFAULT_CLUSTER_BIND: &str = "0.0.0.0:5934";

/// Storage durability mode for the `--storage-sync` CLI flag.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum StorageSyncArg {
    /// Group commit + fsync per batch (default, strongest durability).
    Sync,
    /// Batch writes immediately; fsync fires on a timer. Up to
    /// `--storage-sync-interval-ms` of acked data may be lost on crash.
    Async,
}

/// Resolved server settings. The CLI builds this from defaults, the config
/// file, the environment and flags (see [`crate::config`]); embedders and
/// tests construct it directly, usually as `ServerArgs { .., ..Default::default() }`.
#[derive(Debug, Clone)]
pub struct ServerArgs {
    /// Client protocol listener.
    pub bind: String,
    /// HTTP API listener.
    pub api_bind: String,
    pub data_dir: PathBuf,
    /// Shared admin bearer token. When set (or a credentials file exists),
    /// every TCP and HTTP connection must authenticate.
    pub auth_token: Option<String>,
    /// Credentials file; falls back to `{data_dir}/credentials.toml` when
    /// that exists.
    pub credentials_file: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub storage_sync: StorageSyncArg,
    pub storage_flush_window_us: u64,
    pub storage_flush_threshold_records: usize,
    pub storage_flush_threshold_bytes: usize,
    pub storage_sync_interval_ms: u64,
    pub storage_sync_bytes: usize,
    /// Default `msg_id` dedup window for streams that don't set one.
    pub dedup_window_secs: u64,
    pub max_connections: usize,
    /// How long open connections get to finish on shutdown.
    pub drain_timeout_secs: u64,
    /// `log` or `file`.
    pub connector_offset_store: String,
    pub cluster: ClusterArgs,
    /// `text` / `json`; `None` = the environment decides.
    pub log_format: Option<String>,
    pub log_level: Option<String>,
    /// The config file these settings came from, if any.
    pub config_file: Option<PathBuf>,
}

/// Multi-pod settings (`[cluster]`).
#[derive(Debug, Clone)]
pub struct ClusterArgs {
    /// `none`, `postgres` or `redis` (`memory`: in-process, for tests).
    pub lease: String,
    pub postgres_url: Option<String>,
    pub postgres_schema: String,
    pub redis_url: Option<String>,
    pub redis_key_prefix: String,
    pub lease_ttl_secs: u64,
    pub lease_heartbeat_secs: u64,
    /// Replication listener.
    pub bind: String,
    /// Address peers dial; defaults to `bind`.
    pub advertise: Option<String>,
    /// Client-protocol address sent to clients as the leader hint.
    pub client_advertise: Option<String>,
    /// Stable node id; default: generated once into `{data_dir}/node_id`.
    pub node_id: Option<String>,
    /// Token followers present on the replication handshake.
    pub replicator_credential: Option<String>,
    /// `all` or `leader`.
    pub acks: String,
    /// Number of nodes in the cluster (for `acks = "quorum"`).
    pub size: Option<usize>,
    pub min_insync_replicas: usize,
    pub replica_lag_max_ms: u64,
    pub ack_timeout_ms: u64,
    pub unclean_leader_election: bool,
    /// Serve and require TLS on the cluster port (with the `[tls]` cert).
    pub tls: bool,
    /// Trust roots for peers' certificates; default: the `[tls]` cert.
    pub tls_ca: Option<PathBuf>,
    /// Namespace of the `memory` lease backend (tests).
    pub memory_namespace: String,
    /// Millisecond overrides of the lease TTL / heartbeat (tests).
    pub lease_ttl_ms: Option<u64>,
    pub lease_heartbeat_ms: Option<u64>,
}

impl Default for ClusterArgs {
    fn default() -> Self {
        Self {
            lease: "none".into(),
            postgres_url: None,
            postgres_schema: "public".into(),
            redis_url: None,
            redis_key_prefix: "exspeed:lease:".into(),
            lease_ttl_secs: 15,
            lease_heartbeat_secs: 3,
            bind: DEFAULT_CLUSTER_BIND.into(),
            advertise: None,
            client_advertise: None,
            node_id: None,
            replicator_credential: None,
            acks: "all".into(),
            size: None,
            min_insync_replicas: 1,
            replica_lag_max_ms: 10_000,
            ack_timeout_ms: 10_000,
            unclean_leader_election: false,
            tls: false,
            tls_ca: None,
            memory_namespace: "default".into(),
            lease_ttl_ms: None,
            lease_heartbeat_ms: None,
        }
    }
}

impl ClusterArgs {
    pub fn multi_pod(&self) -> bool {
        self.lease != "none"
    }

    /// Lease TTL and heartbeat interval.
    pub fn lease_timing(&self) -> (std::time::Duration, std::time::Duration) {
        use std::time::Duration;
        (
            self.lease_ttl_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(self.lease_ttl_secs)),
            self.lease_heartbeat_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(self.lease_heartbeat_secs)),
        )
    }

    pub fn lease_config(&self) -> exspeed_broker::lease::LeaseConfig {
        exspeed_broker::lease::LeaseConfig {
            backend: self.lease.clone(),
            postgres_url: self.postgres_url.clone(),
            postgres_schema: self.postgres_schema.clone(),
            redis_url: self.redis_url.clone(),
            redis_key_prefix: self.redis_key_prefix.clone(),
            memory_namespace: self.memory_namespace.clone(),
            call_timeout: std::time::Duration::from_secs(self.lease_heartbeat_secs.clamp(1, 5)),
        }
    }
}

impl Default for ServerArgs {
    fn default() -> Self {
        Self::new("./exspeed-data")
    }
}

impl ServerArgs {
    /// Built-in defaults with the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            bind: "0.0.0.0:5933".into(),
            api_bind: "0.0.0.0:8080".into(),
            data_dir: data_dir.into(),
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            storage_sync: StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            dedup_window_secs: 300,
            max_connections: 1024,
            drain_timeout_secs: 10,
            connector_offset_store: "log".into(),
            cluster: ClusterArgs::default(),
            log_format: None,
            log_level: None,
            config_file: None,
        }
    }
}

pub async fn run(args: ServerArgs) -> Result<()> {
    run_with_shutdown(args, signal_listener()).await
}

/// SIGTERM | SIGINT | Ctrl-C, whichever fires first. Returns when one is received.
async fn signal_listener() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "failed to install SIGTERM handler: {}; falling back to ctrl_c",
                    e
                );
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                error!("failed to install SIGINT handler: {}; SIGTERM only", e);
                let _ = sigterm.recv().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
            _ = sigint.recv() => info!("received SIGINT, shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("received Ctrl-C, shutting down");
    }
}

/// Run the server until `shutdown` resolves (or it fails fatally).
///
/// On shutdown:
/// 1. Cancellation propagates to the leader supervisor and the accept loop.
/// 2. Per-connection child tokens fire so in-flight `select!`s break out.
/// 3. We wait up to 10s for active connections to drain (semaphore permits returned).
pub async fn run_with_shutdown<F>(args: ServerArgs, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    // Install the rustls crypto provider once, before anything else. This
    // ensures both the sync TCP load_tls_config path and the axum-server
    // (HTTP) spawn always see a provider, regardless of scheduler ordering.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    // Cancellation token. Forwarder task waits for shutdown future, then cancels.
    let cancel_token = CancellationToken::new();
    {
        let cancel_for_signal = cancel_token.clone();
        tokio::spawn(async move {
            shutdown.await;
            cancel_for_signal.cancel();
        });
    }

    // Normalize auth token: empty string → unset (guards against shells
    // passing EXSPEED_AUTH_TOKEN="" through).
    let auth_token_raw: Option<String> =
        args.auth_token.as_ref().filter(|v| !v.is_empty()).cloned();

    // Resolve credentials file. Precedence: explicit `--credentials-file` /
    // `EXSPEED_CREDENTIALS_FILE` (clap folds both into `args.credentials_file`)
    // → `{data_dir}/credentials.toml` when it exists on disk.
    // Holding the path in `ServerArgs` instead of reading env at this point
    // lets integration tests point a single in-process server at a per-test
    // tempfile without racing on process-global state.
    let credentials_path: Option<PathBuf> = args
        .credentials_file
        .as_ref()
        .filter(|p| !p.as_os_str().is_empty())
        .cloned()
        .or_else(|| {
            let default = args.data_dir.join("credentials.toml");
            default.exists().then_some(default)
        });

    let credential_store: Option<Arc<CredentialStore>> =
        match (credentials_path.as_deref(), auth_token_raw.as_deref()) {
            (None, None) => None,
            (path, env_tok) => {
                let store = CredentialStore::build(path, env_tok)
                    .map_err(|e| anyhow::anyhow!("failed to load credentials: {e}"))?;
                Some(Arc::new(store))
            }
        };

    let tls_paths = crate::cli::server_tls::TlsPaths::from_args(
        args.tls_cert.as_deref(),
        args.tls_key.as_deref(),
    )?;
    let tls_enabled = tls_paths.is_some();

    if credential_store.is_none() {
        warn!("auth disabled — do not expose broker ports to the public internet");
    } else if credentials_path.is_some() && auth_token_raw.is_some() {
        warn!(
            "EXSPEED_AUTH_TOKEN active alongside credentials.toml as synthetic \
             'legacy-admin' — consider migrating fully to the credentials file"
        );
    }
    if !tls_enabled {
        warn!("TLS disabled — do not expose broker ports to the public internet");
    }

    // Exclusive data-dir lock, held until this function returns (or the
    // process exits) so an embedded server can be restarted in-process.
    let _data_dir_lock = crate::cli::server_lock::acquire_data_dir_lock(&args.data_dir)?;

    // Build storage sync mode from CLI args.
    let storage_sync_mode = match args.storage_sync {
        StorageSyncArg::Sync => exspeed_storage::file::StorageSyncMode::Sync,
        StorageSyncArg::Async => exspeed_storage::file::StorageSyncMode::Async {
            interval: std::time::Duration::from_millis(args.storage_sync_interval_ms),
            threshold_bytes: args.storage_sync_bytes,
        },
    };
    if matches!(args.storage_sync, StorageSyncArg::Async) {
        tracing::warn!(
            interval_ms = args.storage_sync_interval_ms,
            "storage sync mode is async; up to {} ms of acked data may be lost on crash",
            args.storage_sync_interval_ms
        );
    }

    // Build appender config from CLI args.
    let appender_config = exspeed_storage::file::AppenderConfig {
        flush_window: std::time::Duration::from_micros(args.storage_flush_window_us),
        flush_threshold_records: args.storage_flush_threshold_records,
        flush_threshold_bytes: args.storage_flush_threshold_bytes,
    };

    // Create storage
    let file_storage = Arc::new(FileStorage::open_with_mode(
        &args.data_dir,
        storage_sync_mode,
        appender_config,
    )?);
    let storage: Arc<dyn StorageEngine> = file_storage.clone();

    // Create metrics
    let (metrics, prometheus_registry) = exspeed_common::Metrics::new();
    let metrics = Arc::new(metrics);

    let dedup_window_secs = args.dedup_window_secs;
    let broker_append = Arc::new(
        BrokerAppend::new(storage.clone(), dedup_window_secs).with_metrics(metrics.clone()),
    );

    // Apply per-stream dedup config from persisted stream.json files, then
    // spawn parallel per-stream rebuild tasks (snapshot path + tail scan).
    // Use file_storage (concrete FileStorage) to access list_streams() + data_dir().
    // In a cluster the dedup maps are rebuilt on promotion instead (from the
    // replicated log, ignoring local snapshots).
    let multi_pod = args.cluster.multi_pod();
    let mut rebuild_set = tokio::task::JoinSet::new();
    for stream_name_str in file_storage
        .list_streams()
        .into_iter()
        .filter(|_| !multi_pod)
    {
        if let Ok(stream_name) = exspeed_common::StreamName::try_from(stream_name_str.as_str()) {
            let stream_dir = file_storage
                .data_dir()
                .join("streams")
                .join(stream_name_str.as_str());
            let cfg = <exspeed_storage::file::stream_config::StreamConfig as exspeed_storage::file::stream_config::StreamConfigFile>::load(&stream_dir)
                .unwrap_or_default();
            broker_append
                .configure_stream(&stream_name, cfg.dedup_window_secs, cfg.dedup_max_entries)
                .await;
            let ba = broker_append.clone();
            let s = stream_name.clone();
            let sd = stream_dir.clone();
            rebuild_set.spawn(async move { ba.rebuild_stream(&s, &sd).await });
        }
    }

    let lease = exspeed_broker::lease::from_config(&args.cluster.lease_config())
        .await
        .context("failed to initialize the lease backend")?;
    if !multi_pod {
        info!("single-node mode (no cluster.lease backend): this node is always the leader");
    }

    let node_id = match &args.cluster.node_id {
        Some(id) => id.clone(),
        None => exspeed_broker::cluster::load_or_create_node_id(&args.data_dir)
            .context("failed to read or create {data_dir}/node_id")?,
    };
    let (lease_ttl, lease_heartbeat) = args.cluster.lease_timing();

    // Cluster listener (bound once; serves fetches only while leading).
    let cluster_listener: Option<TcpListener> = if multi_pod {
        let bind: SocketAddr =
            args.cluster.bind.parse().with_context(|| {
                format!("cluster.bind `{}` is not host:port", args.cluster.bind)
            })?;
        Some(
            TcpListener::bind(bind)
                .await
                .with_context(|| format!("failed to bind the cluster listener on {bind}"))?,
        )
    } else {
        None
    };
    let replication_advertise: Option<String> = match &cluster_listener {
        Some(l) => Some(match &args.cluster.advertise {
            Some(a) => a.clone(),
            None => {
                let local = l.local_addr()?;
                if local.ip().is_unspecified() {
                    warn!(
                        %local,
                        "cluster.advertise is not set and cluster.bind is a wildcard address; \
                         followers can't reach this node when it leads"
                    );
                }
                local.to_string()
            }
        }),
        None => None,
    };
    let client_advertise: Option<String> = args.cluster.client_advertise.clone().or_else(|| {
        args.bind
            .parse::<SocketAddr>()
            .ok()
            .filter(|a| !a.ip().is_unspecified() && a.port() != 0)
            .map(|a| a.to_string())
    });

    let broker = Arc::new(Broker::new(
        storage.clone(),
        broker_append,
        args.data_dir.clone(),
        lease.clone(),
        metrics.clone(),
    ));

    let cluster: Option<Arc<exspeed_broker::cluster::Cluster>> = if multi_pod {
        let mut cfg = exspeed_broker::cluster::ClusterConfig::new(node_id.clone());
        cfg.acks_all = args.cluster.acks != "leader";
        cfg.min_insync_replicas = args.cluster.min_insync_replicas.max(1);
        if args.cluster.acks == "quorum" {
            // `all`, plus never fewer than a majority of the cluster in sync:
            // an acknowledged write is on a majority of nodes, and only an
            // in-sync node can be elected.
            let majority = args.cluster.size.unwrap_or(1) / 2 + 1;
            cfg.min_insync_replicas = cfg.min_insync_replicas.max(majority);
        }
        cfg.replica_lag_max = std::time::Duration::from_millis(args.cluster.replica_lag_max_ms);
        cfg.ack_timeout = std::time::Duration::from_millis(args.cluster.ack_timeout_ms);
        cfg.replicator_token = args.cluster.replicator_credential.clone();
        if args.cluster.tls {
            let paths = tls_paths
                .as_ref()
                .context("cluster.tls needs the [tls] cert and key")?;
            let ca = args
                .cluster
                .tls_ca
                .clone()
                .unwrap_or_else(|| paths.cert.clone());
            cfg.tls = Some(exspeed_broker::cluster::ClusterTls {
                server: crate::cli::server_tls::load_tls_config(&paths.cert, &paths.key)?,
                client: crate::cli::server_tls::load_client_config(&ca)?,
            });
        }
        Some(
            exspeed_broker::cluster::Cluster::new(
                cfg,
                &args.data_dir,
                storage.clone(),
                broker.log.clone(),
                metrics.clone(),
                credential_store.clone(),
                lease.clone(),
                broker.dedup_ready.clone(),
            )
            .context("failed to open the cluster state")?,
        )
    } else {
        None
    };

    let mut opts = exspeed_broker::leadership::LeadershipOptions::new(node_id.clone());
    opts.ttl = lease_ttl;
    opts.heartbeat = lease_heartbeat;
    opts.replication_endpoint = replication_advertise.clone();
    opts.client_endpoint = client_advertise.clone();
    opts.require_isr = !args.cluster.unclean_leader_election;
    let hooks: Option<Arc<dyn exspeed_broker::leadership::RoleHooks>> = cluster
        .clone()
        .map(|c| Arc::new(c) as Arc<dyn exspeed_broker::leadership::RoleHooks>);
    let leadership = Arc::new(exspeed_broker::leadership::ClusterLeadership::start(
        lease.clone(),
        metrics.clone(),
        opts,
        hooks,
    ));
    // Only the leader accepts writes. Every write path (TCP, HTTP, webhooks,
    // connectors, ExQL) goes through `broker.log`, which enforces this.
    broker.log.set_write_gate(leadership.clone());
    if let (Some(c), Some(l)) = (&cluster, cluster_listener) {
        c.set_leadership((*leadership).clone());
        c.serve(l);
    } else {
        metrics.set_replication_role("standalone");
    }

    // Give the node a moment to win the lease before logging its posture.
    let startup_deadline = std::cmp::min(lease_ttl / 3, std::time::Duration::from_secs(2));
    let mut leadership_rx = leadership.is_leader.clone();
    let _ = tokio::time::timeout(startup_deadline, leadership_rx.wait_for(|&v| v)).await;
    let role = if !multi_pod {
        "standalone"
    } else if leadership.is_currently_leader() {
        "leader"
    } else {
        "follower"
    };
    let (cred_file_count, cred_legacy) = credential_store
        .as_ref()
        .map(|s| s.source_breakdown())
        .unwrap_or((0, false));
    let cred_total = cred_file_count + if cred_legacy { 1 } else { 0 };
    info!(
        auth = if credential_store.is_some() { "on" } else { "off" },
        tls = if tls_enabled { "on" } else { "off" },
        lease = %args.cluster.lease,
        node_id = %node_id,
        role = role,
        bind = %args.bind,
        api_bind = %args.api_bind,
        replication_endpoint = ?replication_advertise,
        client_endpoint = ?client_advertise,
        acks = %args.cluster.acks,
        credentials = cred_total,
        cred_file = cred_file_count,
        cred_legacy_admin = if cred_legacy { 1 } else { 0 },
        "exspeed server starting"
    );

    // Spawn watcher that flips `dedup_ready` once all per-stream rebuild tasks finish.
    if !multi_pod {
        let dedup_ready = broker.dedup_ready.clone();
        tokio::spawn(async move {
            while let Some(r) = rebuild_set.join_next().await {
                match r {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "dedup rebuild returned error"),
                    Err(e) => tracing::error!(error = ?e, "dedup rebuild task panicked"),
                }
            }
            dedup_ready.store(true, std::sync::atomic::Ordering::Release);
            info!("all dedup maps ready");
        });
    }

    // Spawn periodic dedup snapshot task (runs every 60s, final snapshot on
    // shutdown). Single node only: a cluster rebuilds dedup state from the
    // log on promotion.
    let snapshot_handle = (!multi_pod).then(|| {
        exspeed_broker::snapshot_task::spawn_dedup_snapshot_task(
            broker.broker_append.clone(),
            args.data_dir.clone(),
            cancel_token.clone(),
        )
    });

    // Connector offsets: the `__connector_offsets` stream (default, replicates
    // with the log) or atomic files (`EXSPEED_CONNECTOR_OFFSET_STORE=file`).
    let offset_backend = args.connector_offset_store.clone();
    let offset_store = exspeed_connectors::offset_store::build(
        &offset_backend,
        &args.data_dir,
        broker.log.clone(),
    )
    .map_err(|e| anyhow::anyhow!("connector offset store: {e}"))?;
    info!(
        backend = offset_backend.as_str(),
        "connector offset store initialized"
    );

    // Create connector manager
    let connector_manager = Arc::new(ConnectorManager::new(
        storage.clone(),
        broker.log.clone(),
        args.data_dir.clone(),
        metrics.clone(),
        offset_store,
        leadership.clone(),
    ));

    // Ensure connectors.d directory exists
    let _ = std::fs::create_dir_all(args.data_dir.join("connectors.d"));

    // Load persisted + TOML connector configs on startup
    if let Err(e) = connector_manager.load_all().await {
        warn!("failed to load connector configs: {}", e);
    }

    // Spawn TOML file watcher for hot-reload of connectors.d/
    exspeed_connectors::file_watcher::spawn_file_watcher(
        connector_manager.clone(),
        args.data_dir.join("connectors.d"),
    );

    // Create ExQL engine. Reads hit storage directly; query output is written
    // through the broker's write path.
    let exql = Arc::new(
        ExqlEngine::new(
            broker.log.clone(),
            args.data_dir.clone(),
            leadership.clone(),
            metrics.clone(),
            exspeed_processing::ExqlConfig::from_env(),
        )
        .map_err(|e| anyhow::anyhow!("ExQL engine: {e}"))?,
    );
    if let Err(e) = exql.load().await {
        warn!("ExQL load: {e}");
    }
    // resume_all_and_run(token) is called by the leader supervisor (Task 9).

    // Clone before moving into AppState so the leader supervisor can capture them.
    let connector_manager_for_supervisor = connector_manager.clone();
    let connector_manager_for_shutdown = connector_manager.clone();
    let exql_for_supervisor = exql.clone();
    let exql_for_tcp = exql.clone();
    let exql_for_shutdown = exql.clone();

    // Readiness flag, flipped to true after the HTTP API server is spawned.
    // Until then, /readyz returns 503 with {"status":"starting"}.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Create shared AppState. `cluster` lights up `GET /api/v1/cluster`.
    let state = Arc::new(exspeed_api::AppState {
        broker: broker.clone(),
        storage: file_storage.clone(),
        metrics: metrics.clone(),
        start_time: std::time::Instant::now(),
        prometheus_registry,
        connector_manager,
        exql,
        credential_store: credential_store.clone(),
        lease: lease.clone(),
        leadership: leadership.clone(),
        ready: ready.clone(),
        data_dir: args.data_dir.clone(),
        cluster: cluster.clone(),
    });

    let supervisor_handle: tokio::task::JoinHandle<()>;
    // Spawn the leader supervisor: waits for is_leader=true, then runs
    // connectors + continuous queries + retention under the current
    // leader token. Loops so that if we get demoted and re-promoted,
    // we resume. Also observes the process-wide cancel token so SIGTERM
    // unblocks the supervisor and lets the leadership Drop release the lease.
    {
        let leadership_sup = leadership.clone();
        let connector_manager_sup = connector_manager_for_supervisor;
        let exql_sup = exql_for_supervisor;
        let storage_sup = file_storage.clone();
        let consumers_sup = broker.consumers.clone();
        let supervisor_cancel = cancel_token.clone();
        // Leader supervisor. While idle (awaiting promotion or demotion) it
        // exits on `supervisor_cancel`. An active tenure ends only when the
        // leader token is cancelled: on demotion, or when shutdown calls
        // `leadership.resign()` after draining connections and connectors,
        // so leader work stops in order and persists its state.
        supervisor_handle = tokio::spawn(async move {
            let mut is_leader_rx = leadership_sup.is_leader.clone();
            loop {
                tokio::select! {
                    biased;
                    _ = supervisor_cancel.cancelled() => {
                        info!("leader supervisor: shutdown signal received");
                        return;
                    }
                    res = is_leader_rx.wait_for(|&v| v) => {
                        if res.is_err() {
                            return; // watcher closed → shutdown
                        }
                    }
                }
                let token = leadership_sup.current_child_token().await;
                info!("leader supervisor: assuming leadership; starting work");
                // Query, connection and API-connector definitions live in
                // replicated internal streams (`__exql_queries`,
                // `__exql_connections`, `__connectors`); reload them for
                // this tenure, since a promoted follower's copy changed
                // after startup.
                if let Err(e) = exql_sup.load().await {
                    error!(error = %e, "failed to reload the ExQL catalog; resigning tenure");
                    token.cancel();
                }
                if let Err(e) = connector_manager_sup.reload_api_configs().await {
                    error!(error = %e, "failed to reload the connector catalog; resigning tenure");
                    token.cancel();
                }
                // Consumers run only on the leader; they restore their state
                // from `__consumers` and stop when the token is cancelled.
                if let Err(e) = consumers_sup.start(token.clone()).await {
                    error!(error = %e, "failed to start consumers; resigning tenure");
                    token.cancel();
                }

                tokio::select! {
                    _ = connector_manager_sup.run_all(token.clone()) => {}
                    _ = exql_sup.clone().resume_all_and_run(token.clone()) => {}
                    _ = exspeed_broker::retention_task::run(
                            storage_sup.clone(),
                            token.clone(),
                        ) => {}
                    // Shutdown also ends a tenure: the server resigns
                    // leadership (cancelling `token`) only after draining
                    // connections and stopping connectors.
                    _ = token.cancelled() => {}
                }

                info!("leader supervisor: tenure ended; awaiting re-promotion");
                // Wait for demotion before looping so we don't tight-loop.
                // Also bail out early on shutdown.
                tokio::select! {
                    biased;
                    _ = supervisor_cancel.cancelled() => {
                        info!("leader supervisor: shutdown while awaiting re-promotion");
                        return;
                    }
                    _ = is_leader_rx.wait_for(|&v| !v) => {}
                }
            }
        });
    }

    // Spawn dedup eviction task (runs every 60 seconds)
    {
        let ba = broker.broker_append.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                ba.evict_expired().await;
            }
        });
    }

    // Spawn HTTP API server
    let api_addr: SocketAddr = args.api_bind.parse()?;
    let http_tls = tls_paths.clone();

    // Mark ready: all eager startup work (storage open, broker.load_consumers,
    // connector load, ExQL load) completed above; the API task is about to
    // run. /readyz now performs the per-request data_dir writability check
    // on top of this gate.
    ready.store(true, std::sync::atomic::Ordering::Release);

    let api_cancel = cancel_token.clone();
    tokio::spawn(async move {
        let shutdown = async move { api_cancel.cancelled().await };
        if let Err(e) = exspeed_api::serve_with_shutdown(state, api_addr, http_tls, shutdown).await
        {
            error!("HTTP API exited: {}", e);
        }
    });

    // Load TLS config if enabled.
    let tls_config = match &tls_paths {
        Some(paths) => Some(crate::cli::server_tls::load_tls_config(
            &paths.cert,
            &paths.key,
        )?),
        None => None,
    };

    // TCP server
    let tcp_addr: SocketAddr = args.bind.parse()?;
    let listener = TcpListener::bind(tcp_addr).await?;
    info!("exspeed TCP listening on {}", tcp_addr);
    info!("exspeed HTTP API listening on {}", api_addr);

    // Bound concurrent connections. Each accepted connection holds one permit
    // for its lifetime; the OS-level accept queue absorbs short bursts.
    let max_conns = args.max_connections;
    let conn_sem = Arc::new(Semaphore::new(max_conns));
    info!(max_conns, "connection cap configured");

    let session_ctx = Arc::new(SessionContext {
        broker: broker.clone(),
        exql: exql_for_tcp,
        credential_store: credential_store.clone(),
        metrics: metrics.clone(),
        node_id: node_id.clone(),
        leader_hint: {
            let l = leadership.clone();
            Arc::new(move || l.leader_hint())
        },
    });

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("shutdown signal received; stopping accept loop");
                break;
            }
            accept_result = listener.accept() => {
                let (socket, peer) = match accept_result {
                    Ok(v) => v,
                    Err(e) => {
                        error!("accept error: {}", e);
                        continue;
                    }
                };

                let permit = match conn_sem.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        metrics.connection_rejected();
                        warn!(%peer, max_conns, "connection rejected: max_conns reached");
                        drop(socket);
                        continue;
                    }
                };

                info!(%peer, "new connection");
                metrics.connection_opened();

                let session_ctx = session_ctx.clone();
                let metrics_clone = metrics.clone();
                let tls_config_clone = tls_config.clone();
                let conn_token = cancel_token.child_token();
                // Per-connection span: `identity` starts empty and is filled
                // in when Connect succeeds, so every log line inside this
                // connection carries it.
                let conn_span = info_span!(
                    "connection",
                    %peer,
                    identity = tracing::field::Empty,
                );
                tokio::spawn(
                    async move {
                        let _permit = permit; // released when this task ends
                        let result: Result<()> = async move {
                            if let Some(tls_cfg) = tls_config_clone {
                                let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
                                let tls_stream =
                                    tokio::time::timeout(TLS_ACCEPT_TIMEOUT, acceptor.accept(socket))
                                        .await
                                        .map_err(|_| anyhow::anyhow!("TLS handshake timed out"))??;
                                session::run(tls_stream, peer, session_ctx, conn_token).await
                            } else {
                                session::run(socket, peer, session_ctx, conn_token).await
                            }
                        }
                        .await;
                        if let Err(e) = result {
                            error!(%peer, "connection error: {}", e);
                        }
                        metrics_clone.connection_closed();
                        info!(%peer, "connection closed");
                    }
                    .instrument(conn_span),
                );
            }
        }
    }

    // Drain: wait for active connections to finish, up to 10 seconds. Each
    // connection holds one semaphore permit for its lifetime; when permits
    // return to `max_conns` available, all connection tasks have exited.
    let drain_deadline = std::time::Duration::from_secs(args.drain_timeout_secs);
    let drain_start = std::time::Instant::now();
    let active_at_start = max_conns - conn_sem.available_permits();
    info!(
        active = active_at_start,
        "waiting up to {:?} for active connections to drain", drain_deadline
    );
    while conn_sem.available_permits() < max_conns {
        if drain_start.elapsed() >= drain_deadline {
            warn!(
                remaining = max_conns - conn_sem.available_permits(),
                "drain deadline reached, exiting with active connections"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    // Stop connectors: sinks flush and commit, sources finish their batch.
    if tokio::time::timeout(
        std::time::Duration::from_secs(30),
        connector_manager_for_shutdown.shutdown(),
    )
    .await
    .is_err()
    {
        warn!("connectors did not stop within 30s");
    }

    // Stop continuous queries; each writes a final checkpoint.
    if tokio::time::timeout(
        std::time::Duration::from_secs(10),
        exql_for_shutdown.shutdown(),
    )
    .await
    .is_err()
    {
        warn!("continuous queries did not stop within 10s");
    }

    // Step down: cancel the leader token (stopping consumers, continuous
    // queries and retention), let the consumer actors write their final
    // state while writes are still open, then close writes and release the
    // lease so a peer can take over at once.
    let consumers_stop = broker.consumers.clone();
    leadership
        .resign_after(async move {
            if !consumers_stop
                .wait_stopped(std::time::Duration::from_secs(10))
                .await
            {
                warn!("consumers did not stop within 10s");
            }
        })
        .await;
    if let Some(c) = &cluster {
        c.shutdown().await;
    }
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), supervisor_handle).await;

    // Final dedup snapshot (taken by the snapshot task on cancel).
    if let Some(h) = snapshot_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), h).await;
    }

    // Flush and fsync every partition, then release the data-dir lock
    // (dropped when this function returns).
    let storage_for_close = file_storage.clone();
    let _ = tokio::task::spawn_blocking(move || storage_for_close.close()).await;
    // Give the lease release (an async DELETE in the guard's task) a moment.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    info!("server stopped");
    Ok(())
}
