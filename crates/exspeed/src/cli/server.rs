use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};

use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::replication::{ReplicationClient, ReplicationCoordinator, ReplicationServer};
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

/// Parse `EXSPEED_CLUSTER_BIND` (default `0.0.0.0:5934`). Returns `None` on
/// parse failure so the caller can fall back rather than panicking at startup.
fn cluster_bind_addr() -> Option<SocketAddr> {
    let raw =
        std::env::var("EXSPEED_CLUSTER_BIND").unwrap_or_else(|_| DEFAULT_CLUSTER_BIND.to_string());
    match raw.parse() {
        Ok(a) => Some(a),
        Err(e) => {
            warn!(raw = %raw, error = %e, "EXSPEED_CLUSTER_BIND invalid — treating as unset");
            None
        }
    }
}

/// What address followers should dial to reach this pod's cluster listener.
/// Defaults to the bind string; set `EXSPEED_CLUSTER_ADVERTISE` when the
/// container's bind address differs from what followers can route to
/// (k8s service name, external LB host, etc.).
fn cluster_advertise_addr(bind: SocketAddr) -> String {
    std::env::var("EXSPEED_CLUSTER_ADVERTISE").unwrap_or_else(|_| bind.to_string())
}

/// The raw bearer a follower sends on the replication Connect handshake.
/// Must resolve (after sha256) to a credential in the shared credentials
/// store that holds `actions = ["replicate"]`. Required in multi-pod mode;
/// startup hard-fails if missing there.
fn replicator_credential() -> Option<String> {
    std::env::var("EXSPEED_REPLICATOR_CREDENTIAL").ok()
}

/// True when the operator asked for a coordinated backend. That's the
/// single source of truth for "this pod is part of a multi-pod deployment"
/// — every downstream multi-pod decision (build coord, bind cluster
/// listener, run the supervisor) branches on this one check.
fn multi_pod_mode() -> bool {
    matches!(
        std::env::var("EXSPEED_CONSUMER_STORE").as_deref(),
        Ok("postgres") | Ok("redis")
    )
}

/// Per-follower mpsc capacity. 100k records is ~100MB at 1KB records —
/// generous enough that bursty writes don't drop a healthy follower, small
/// enough that a permanently-stuck follower doesn't balloon leader memory.
///
/// Defense-in-depth cap at 10_000_000: a misconfigured env var that
/// reads `100000000` (extra zero) would otherwise reserve ~10GB of
/// heap per follower the moment a session registers. Above that the
/// disk-based follower-seed flow is a better answer than heap.
fn replication_follower_queue_records() -> usize {
    const MAX: usize = 10_000_000;
    std::env::var("EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.min(MAX))
        .unwrap_or(100_000)
}

/// Storage durability mode for the `--storage-sync` CLI flag.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum StorageSyncArg {
    /// Group commit + fsync per batch (default, strongest durability).
    Sync,
    /// Batch writes immediately; fsync fires on a timer. Up to
    /// `--storage-sync-interval-ms` of acked data may be lost on crash.
    Async,
}

#[derive(Args)]
pub struct ServerArgs {
    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:5933")]
    pub bind: String,

    /// Address for the HTTP API
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub api_bind: String,

    /// Directory for persistent data
    #[arg(long, default_value = "./exspeed-data")]
    pub data_dir: PathBuf,

    /// Shared bearer token. When set, required on all TCP and HTTP connections.
    #[arg(long, env = "EXSPEED_AUTH_TOKEN", hide_env_values = true)]
    pub auth_token: Option<String>,

    /// Path to a TOML credentials file. When unset here, falls back to
    /// `EXSPEED_CREDENTIALS_FILE` and then to `{data_dir}/credentials.toml`.
    /// Having a first-class field lets integration tests point at a per-test
    /// tempfile without mutating process-global env vars.
    #[arg(long, env = "EXSPEED_CREDENTIALS_FILE")]
    pub credentials_file: Option<PathBuf>,

    /// Path to PEM-encoded server certificate (full chain). Must be set with --tls-key.
    #[arg(long, env = "EXSPEED_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to PEM-encoded private key. Must be set with --tls-cert.
    #[arg(long, env = "EXSPEED_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Storage durability mode. `sync` = group commit + fsync per batch (safe default).
    /// `async` = batch without fsync, periodic fsync on `--storage-sync-interval-ms`.
    #[arg(long, value_enum, default_value = "sync", env = "EXSPEED_STORAGE_SYNC")]
    pub storage_sync: StorageSyncArg,

    /// Appender flush window in microseconds. Incoming appends are coalesced
    /// into a batch for up to this many µs before being committed to disk.
    #[arg(long, default_value_t = 500, env = "EXSPEED_FLUSH_WINDOW_US")]
    pub storage_flush_window_us: u64,

    /// Flush the appender batch early when this many records are queued.
    #[arg(long, default_value_t = 256, env = "EXSPEED_FLUSH_THRESHOLD_RECORDS")]
    pub storage_flush_threshold_records: usize,

    /// Flush the appender batch early when this many bytes are queued.
    #[arg(
        long,
        default_value_t = 1_048_576,
        env = "EXSPEED_FLUSH_THRESHOLD_BYTES"
    )]
    pub storage_flush_threshold_bytes: usize,

    /// Interval in milliseconds between periodic fsyncs in async sync mode.
    /// Only relevant when `--storage-sync=async`.
    #[arg(long, default_value_t = 10, env = "EXSPEED_SYNC_INTERVAL_MS")]
    pub storage_sync_interval_ms: u64,

    /// Unflushed-bytes threshold to trigger an early fsync in async sync mode.
    /// Currently reserved for future use — timer-only in this release.
    #[arg(long, default_value_t = 4 * 1024 * 1024, env = "EXSPEED_SYNC_BYTES")]
    pub storage_sync_bytes: usize,
}

impl ServerArgs {
    /// Defaults identical to the CLI's, with the given data directory. Handy
    /// for embedding the server (tests, benchmarks).
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
    let appender_config = exspeed_storage::file::segment_appender::AppenderConfig {
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
    // Check for S3 tiered storage
    let storage: Arc<dyn StorageEngine> = match exspeed_storage::s3::config::S3Config::from_env() {
        Ok(Some(s3_config)) => {
            info!("S3 tiered storage enabled");
            let s3_storage = exspeed_storage::s3::S3TieredStorage::new(
                (*file_storage).clone(),
                s3_config.bucket,
                s3_config.prefix,
                s3_config.local_max_bytes,
            )
            .await
            .expect("failed to initialize S3 tiered storage");
            Arc::new(s3_storage)
        }
        Ok(None) => {
            info!("using local file storage");
            file_storage.clone()
        }
        Err(e) => {
            panic!("S3 storage configuration error: {e}");
        }
    };

    // Create metrics
    let (metrics, prometheus_registry) = exspeed_common::Metrics::new();
    let metrics = Arc::new(metrics);

    // Create BrokerAppend with dedup window from env (default 300s)
    let dedup_window_secs: u64 = std::env::var("EXSPEED_DEDUP_WINDOW_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let broker_append = Arc::new(
        BrokerAppend::new(storage.clone(), dedup_window_secs).with_metrics(metrics.clone()),
    );

    // Apply per-stream dedup config from persisted stream.json files, then
    // spawn parallel per-stream rebuild tasks (snapshot path + tail scan).
    // Use file_storage (concrete FileStorage) to access list_streams() + data_dir().
    let mut rebuild_set = tokio::task::JoinSet::new();
    for stream_name_str in file_storage.list_streams() {
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

    // Build lease backend (same env var dispatch as work coordinator).
    let lease = exspeed_broker::lease::from_env()
        .await
        .expect("failed to initialize lease backend");
    info!(
        lease_backend = if lease.supports_coordination() {
            "coordinated"
        } else {
            "noop"
        },
        "lease backend initialized"
    );

    // Warn if file-backed — multi-pod deployments need postgres/redis.
    if !lease.supports_coordination() {
        warn!(
            "lease backend is file — multi-pod deployment not supported; \
             all connectors and continuous queries will run on this pod. \
             Set EXSPEED_CONSUMER_STORE=postgres|redis for multi-pod coordination."
        );
    }

    // Build the replication coordinator + advertise endpoint up front so
    // they can be threaded into both the Broker (for emit-on-append) and
    // `ClusterLeadership::spawn` (which writes the endpoint into the
    // `cluster:leader` lease row for follower discovery).
    //
    // Single-pod deployments get `None` for both and skip every multi-pod
    // branch below — no cluster listener, no follower client, no
    // role-transition supervisor, no `state.replication_coordinator`.
    let multi_pod = multi_pod_mode();
    let replication_coordinator: Option<Arc<ReplicationCoordinator>> = if multi_pod {
        let queue_cap = replication_follower_queue_records();
        Some(ReplicationCoordinator::new(metrics.clone(), queue_cap))
    } else {
        None
    };
    let cluster_bind = cluster_bind_addr();
    let replication_advertise: Option<String> = match (multi_pod, cluster_bind) {
        (true, Some(bind)) => Some(cluster_advertise_addr(bind)),
        _ => None,
    };

    // Spawn cluster-leader leadership state machine. The advertised
    // endpoint is written into the `cluster:leader` lease row on every
    // acquire so followers can discover the current leader via
    // `list_all()` without a separate registry.
    let leadership = Arc::new(
        exspeed_broker::leadership::ClusterLeadership::spawn(
            lease.clone(),
            metrics.clone(),
            replication_advertise.clone(),
        )
        .await,
    );

    // Validate heartbeat vs TTL — heartbeat must be well under TTL or the
    // first heartbeat fires after the lease has already expired and the
    // cluster will thrash.
    let lease_ttl = exspeed_broker::lease::ttl_from_env();
    let lease_hb = exspeed_broker::lease::heartbeat_interval_from_env();
    if lease_hb * 2 >= lease_ttl {
        warn!(
            ttl_secs = lease_ttl.as_secs(),
            heartbeat_secs = lease_hb.as_secs(),
            "EXSPEED_LEASE_HEARTBEAT_SECS should be at most TTL/2 to avoid \
             expiring the lease before the first refresh; current config will \
             cause leader thrashing"
        );
    }

    // Give the retry loop one full tick to race for the lease before we
    // log posture or spawn the supervisor. We wait for is_leader=true with
    // a short deadline (min(TTL/3, 2s)). Under Noop backend the first tick
    // fires immediately so this resolves in <10ms; under Postgres/Redis it
    // takes ~TTL/3 (default 10s, clamped to 2s here).
    let startup_deadline = std::cmp::min(lease_ttl / 3, std::time::Duration::from_secs(2));
    let mut leadership_rx = leadership.is_leader.clone();
    let _ = tokio::time::timeout(startup_deadline, leadership_rx.wait_for(|&v| v)).await;

    // Three-way posture: `standalone` (single-pod), `leader` (multi-pod,
    // holds the cluster:leader lease), `follower` (multi-pod, standby
    // on this pod). The single-pod case collapses the leader/standby
    // distinction: with no coordinated backend there are no peers to
    // fail over from, so the old `role=standby` log line was always a
    // lie in that mode.
    let role = if !multi_pod {
        "standalone"
    } else if leadership.is_currently_leader() {
        "leader"
    } else {
        "follower"
    };

    // Posture log (always). Emitted after lease is built so the backend name
    // appears alongside auth/tls state.
    let lease_backend_name = if lease.supports_coordination() {
        // Read the env var so we show postgres/redis rather than "coordinated"
        std::env::var("EXSPEED_CONSUMER_STORE")
            .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE"))
            .unwrap_or_else(|_| "file".to_string())
    } else {
        "file".to_string()
    };
    let (cred_file_count, cred_legacy) = credential_store
        .as_ref()
        .map(|s| s.source_breakdown())
        .unwrap_or((0, false));
    let cred_total = cred_file_count + if cred_legacy { 1 } else { 0 };
    info!(
        auth = if credential_store.is_some() { "on" } else { "off" },
        tls = if tls_enabled { "on" } else { "off" },
        lease = %lease_backend_name,
        role = role,
        bind = %args.bind,
        api_bind = %args.api_bind,
        cluster_bind = ?cluster_bind,
        replication_endpoint = ?replication_advertise,
        credentials = cred_total,
        cred_file = cred_file_count,
        cred_legacy_admin = if cred_legacy { 1 } else { 0 },
        "exspeed server starting"
    );
    if role == "follower" {
        info!(
            "role=follower — this pod does not serve client traffic. \
             Configure your load balancer to probe GET /healthz and only \
             route to pods returning 200. Replication client will dial \
             the current leader when one is elected."
        );
    }

    // Create broker. In multi-pod mode, attach the replication coordinator
    // so `Broker::append/create_stream/delete_stream` fan out their
    // `ReplicationEvent`s to every connected follower's mpsc channel
    // before returning to the caller.
    let broker_builder = Broker::new(
        storage.clone(),
        broker_append,
        args.data_dir.clone(),
        lease.clone(),
        metrics.clone(),
    );
    let broker = Arc::new(match replication_coordinator.as_ref() {
        Some(coord) => broker_builder.with_replication_coordinator(coord.clone()),
        None => broker_builder,
    });
    // Only the leader accepts writes. Every write path (TCP, HTTP, webhooks,
    // connectors, ExQL) goes through `broker.log`, which enforces this.
    broker.log.set_write_gate(leadership.clone());

    // Spawn watcher that flips `dedup_ready` once all per-stream rebuild tasks finish.
    {
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

    // Spawn periodic dedup snapshot task (runs every 60s, final snapshot on shutdown).
    let _snapshot_handle = exspeed_broker::snapshot_task::spawn_dedup_snapshot_task(
        broker.broker_append.clone(),
        args.data_dir.clone(),
        cancel_token.clone(),
    );

    // Create offset store (backend selected by EXSPEED_OFFSET_STORE env var)
    let offset_backend =
        std::env::var("EXSPEED_OFFSET_STORE").unwrap_or_else(|_| "file".to_string());
    let offset_store = exspeed_connectors::offset_store::from_env(&args.data_dir, storage.clone())
        .await
        .expect("failed to initialize offset store");
    info!(
        backend = offset_backend.as_str(),
        "offset store initialized"
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
    let exql_storage: Arc<dyn StorageEngine> = Arc::new(
        exspeed_broker::log::LogBackedStorage::new(broker.log.clone()),
    );
    let exql = Arc::new(ExqlEngine::new(
        exql_storage,
        args.data_dir.clone(),
        leadership.clone(),
        metrics.clone(),
    ));
    exql.load().unwrap_or_else(|e| warn!("ExQL load: {e}"));
    // resume_all_and_run(token) is called by the leader supervisor (Task 9).

    // Clone before moving into AppState so the leader supervisor can capture them.
    let connector_manager_for_supervisor = connector_manager.clone();
    let exql_for_supervisor = exql.clone();
    let exql_for_tcp = exql.clone();

    // Readiness flag, flipped to true after the HTTP API server is spawned.
    // Until then, /readyz returns 503 with {"status":"starting"}.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Create shared AppState. `replication_coordinator` lights up
    // `GET /api/v1/cluster/followers` in multi-pod mode and stays `None`
    // elsewhere (the endpoint returns 503 in that case, with a hint
    // pointing at EXSPEED_CONSUMER_STORE).
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
        replication_coordinator: replication_coordinator.clone(),
    });

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
        // Retention task emits `RetentionTrimmed` replication events in
        // multi-pod mode; `None` in single-pod short-circuits the emit
        // call inside the task.
        let replication_coordinator_for_retention = replication_coordinator.clone();
        // Leader supervisor: three select-wraps observe `supervisor_cancel` because the
        // supervisor has three idle states (awaiting promotion, active tenure, awaiting
        // demotion). Without the third wrap, SIGTERM during the post-tenure idle window
        // would block until the next promotion or watcher channel close.
        tokio::spawn(async move {
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
                            replication_coordinator_for_retention.clone(),
                        ) => {}
                    _ = token.cancelled() => {}
                    _ = supervisor_cancel.cancelled() => {
                        info!("leader supervisor: cancelled");
                        return;
                    }
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

    // ---- Multi-pod replication wiring --------------------------------
    //
    // In multi-pod mode we bind the cluster listener ONCE at startup and
    // keep the socket alive across leader/follower role flips; the
    // supervisor below gates the accept loop on `is_leader`. Binding
    // here (not inside the supervisor) means:
    //   * `Arc<ReplicationServer>` clones cheaply into the server+client
    //     futures, so `ReplicationServer::run(&self, cancel)` can be
    //     called repeatedly across tenures.
    //   * A bind failure is a hard-fail at startup, not an error that
    //     surfaces only on first promotion minutes later.
    //   * Tests using `:0` can read `local_addr()` once and reuse it.
    //
    // Single-pod mode skips all of it and just sets the role metric to
    // `standalone`.
    let replication_server_handle: Option<Arc<ReplicationServer>> =
        if let (Some(coord), Some(bind)) = (replication_coordinator.as_ref(), cluster_bind) {
            let server = ReplicationServer::bind(
                bind,
                coord.clone(),
                storage.clone(),
                credential_store.clone(),
                leadership.holder_id,
                metrics.clone(),
            )
            .await
            .context("failed to bind cluster listener")?;
            info!(%bind, advertise = ?replication_advertise, "cluster replication listener bound");
            Some(Arc::new(server))
        } else if multi_pod {
            // Multi-pod mode but cluster_bind couldn't be resolved. We
            // warned above; coord is still useful for emit-on-append but
            // no peer can actually dial this pod. Prefer a hard error so
            // the operator doesn't silently run a crippled cluster.
            anyhow::bail!(
                "EXSPEED_CONSUMER_STORE is multi-pod but EXSPEED_CLUSTER_BIND is unparseable — \
                 refusing to start so the misconfiguration is caught at deploy time"
            );
        } else {
            None
        };

    // Spawn the role-transition supervisor. ONE task observes `is_leader`
    // and runs either the leader-side accept loop OR the follower client,
    // never both. On every flip we cancel + await the previous role's
    // task before starting the new one; overlap would risk double-append
    // (a brief period where both client and server apply to local
    // storage). See the cancel-then-await dance below.
    if multi_pod {
        let leadership_for_sup = leadership.clone();
        let metrics_for_sup = metrics.clone();
        let supervisor_cancel = cancel_token.clone();
        let server_handle = replication_server_handle.clone();

        // Follower client is a singleton for the process lifetime — its
        // cursor state must not be re-created on every demotion, or we'd
        // lose the on-disk offset every time we flapped.
        let client = {
            let cursor_path = args.data_dir.join("replication").join("cursor.json");
            match ReplicationClient::new(storage.clone(), cursor_path, metrics.clone()) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    // Hard fail: if we can't load the follower cursor,
                    // the follower path is dead and the role supervisor
                    // has nothing to swap to. Better to surface this at
                    // startup than crash the first time we demote.
                    anyhow::bail!(
                        "failed to initialize replication follower cursor at \
                         {:?}/replication/cursor.json: {e}",
                        args.data_dir
                    );
                }
            }
        };

        // Replicator credential is required in multi-pod mode. It's the
        // bearer the follower sends on the replication Connect handshake,
        // and the leader-side server enforces `Action::Replicate` on the
        // resulting identity. A misconfiguration here would manifest as
        // every follower session failing with 401; fail fast instead.
        let replicator_bearer = replicator_credential().context(
            "EXSPEED_REPLICATOR_CREDENTIAL must be set when EXSPEED_CONSUMER_STORE=postgres|redis \
             (the bearer a follower uses to authenticate its replication session)",
        )?;

        tokio::spawn(async move {
            let mut is_leader_rx = leadership_for_sup.is_leader.clone();
            // Task handle + cancel token for whichever role we're
            // currently running. On every change, cancel the old one,
            // await its exit, then start the new one.
            let mut previous_task: Option<tokio::task::JoinHandle<()>> = None;
            let mut previous_cancel: Option<CancellationToken> = None;

            loop {
                let leader = *is_leader_rx.borrow();

                // Cancel + drain the previous role's task before the
                // new one starts. Awaiting is essential: without it we'd
                // have a brief window where both leader server and
                // follower client ran in parallel, and the follower's
                // `apply` writes to the same storage the leader serves
                // from. Cancel-then-await gives us a strict role-swap
                // boundary.
                //
                // Caveat: `ReplicationServer::handle_follower` spawns a
                // child task per connected follower. We cancel the
                // parent `server.run(...)` via `role_cancel` above, and
                // `ReplicationServer::run` exits promptly, but the
                // per-follower child tasks detect the cancel through
                // their own borrow of the same token and unwind
                // independently. In practice this unwind completes
                // well before the new role's task sends a meaningful
                // frame, but there's no explicit "drain all children"
                // step here — the overlap window, if any, is bounded
                // by how long it takes the kernel to deliver the
                // CancellationToken flip, not by any blocking I/O.
                // Tracked for a possible tighter drain if it shows up
                // in failover race tests.
                if let Some(tok) = previous_cancel.take() {
                    tok.cancel();
                }
                if let Some(handle) = previous_task.take() {
                    let _ = handle.await;
                }

                let role_cancel = CancellationToken::new();

                if leader {
                    // Leader: start the accept loop on the already-bound
                    // listener. `server.run(&self, cancel)` returns when
                    // `cancel` fires.
                    let server = server_handle
                        .clone()
                        .expect("replication server was bound earlier in multi-pod mode");
                    let rc = role_cancel.clone();
                    previous_task = Some(tokio::spawn(async move {
                        server.run(rc).await;
                    }));
                    metrics_for_sup.set_replication_role("leader");
                    info!(
                        role = "leader",
                        endpoint = ?replication_advertise,
                        "exspeed replication: role=leader — serving follower sessions"
                    );
                } else {
                    // Follower: spin up the client loop. Endpoint getter
                    // reads the lease row on every reconnect attempt;
                    // that handles both "no leader yet" and "leader
                    // changed mid-session" transparently.
                    let rc = role_cancel.clone();
                    let client_for_task = client.clone();
                    let leadership_for_getter = leadership_for_sup.clone();
                    let bearer = replicator_bearer.clone();
                    previous_task = Some(tokio::spawn(async move {
                        client_for_task
                            .run(
                                || {
                                    let l = leadership_for_getter.clone();
                                    async move { l.leader_replication_endpoint().await }
                                },
                                bearer,
                                rc,
                            )
                            .await;
                    }));
                    metrics_for_sup.set_replication_role("follower");
                    info!(
                        role = "follower",
                        "exspeed replication: role=follower — dialing leader"
                    );
                }
                previous_cancel = Some(role_cancel);

                // Wait for the next role change or process shutdown.
                // `is_leader_rx.changed()` returning Err means the
                // watcher was closed (ClusterLeadership dropped) — that
                // only happens on process teardown, so exit cleanly.
                tokio::select! {
                    biased;
                    _ = supervisor_cancel.cancelled() => {
                        info!("replication role supervisor: shutdown signal received");
                        if let Some(tok) = previous_cancel.take() { tok.cancel(); }
                        if let Some(handle) = previous_task.take() { let _ = handle.await; }
                        return;
                    }
                    res = is_leader_rx.changed() => {
                        if res.is_err() {
                            if let Some(tok) = previous_cancel.take() { tok.cancel(); }
                            if let Some(handle) = previous_task.take() { let _ = handle.await; }
                            return;
                        }
                    }
                }
            }
        });
    } else {
        // Single-pod mode: pin the role metric to `standalone` so
        // dashboards don't interpret the default (`0/0/0`) as "unknown
        // state". Also makes the posture grep-able.
        metrics.set_replication_role("standalone");
        info!(
            role = "standalone",
            "exspeed replication: single-instance mode"
        );
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
    let max_conns: usize = std::env::var("EXSPEED_MAX_CONNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    let conn_sem = Arc::new(Semaphore::new(max_conns));
    info!(max_conns, "connection cap configured");

    let session_ctx = Arc::new(SessionContext {
        broker: broker.clone(),
        exql: exql_for_tcp,
        credential_store: credential_store.clone(),
        metrics: metrics.clone(),
        node_id: leadership.holder_id.to_string(),
        // Leader hints for clients arrive with the HA work (Phase 6).
        leader_hint: Arc::new(|| None),
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
    let drain_deadline = std::time::Duration::from_secs(10);
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
    info!("server stopped");
    Ok(())
}
