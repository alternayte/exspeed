use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::consumer_state::DeliveryRecord;
use exspeed_broker::Broker;
use exspeed_connectors::ConnectorManager;
use exspeed_processing::ExqlEngine;
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::messages::record_delivery::RecordDelivery;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_storage::file::FileStorage;
use exspeed_streams::StorageEngine;

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

    /// Path to PEM-encoded server certificate (full chain). Must be set with --tls-key.
    #[arg(long, env = "EXSPEED_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to PEM-encoded private key. Must be set with --tls-cert.
    #[arg(long, env = "EXSPEED_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
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
                error!("failed to install SIGTERM handler: {}; falling back to ctrl_c", e);
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
    let auth_token: Option<Arc<String>> = args
        .auth_token
        .as_ref()
        .filter(|v| !v.is_empty())
        .cloned()
        .map(Arc::new);

    let tls_paths = crate::cli::server_tls::TlsPaths::from_args(
        args.tls_cert.as_deref(),
        args.tls_key.as_deref(),
    )?;
    let tls_enabled = tls_paths.is_some();

    if auth_token.is_none() {
        warn!("auth disabled — do not expose broker ports to the public internet");
    }
    if !tls_enabled {
        warn!("TLS disabled — do not expose broker ports to the public internet");
    }

    // Acquire exclusive data-dir lock — held for the lifetime of this process.
    // Leaks intentionally; the OS releases the flock on process exit.
    let lock = crate::cli::server_lock::acquire_data_dir_lock(&args.data_dir)?;
    Box::leak(Box::new(lock));

    // Create storage
    let file_storage = Arc::new(FileStorage::open(&args.data_dir)?);
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
    broker_append.rebuild_from_log().await.unwrap_or_else(|e| {
        warn!("failed to rebuild dedup state from log: {}", e);
    });

    // Build consumer store (selects backend from EXSPEED_CONSUMER_STORE or EXSPEED_OFFSET_STORE)
    let consumer_backend = std::env::var("EXSPEED_CONSUMER_STORE")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE"))
        .unwrap_or_else(|_| "file".to_string());
    let consumer_store = exspeed_broker::consumer_store::from_env(&args.data_dir)
        .await
        .expect("failed to initialize consumer store");
    info!(backend = consumer_backend.as_str(), "consumer store initialized");

    // Build work coordinator (uses same backend as consumer store).
    let work_coordinator = exspeed_broker::work_coordinator::from_env()
        .await
        .expect("failed to initialize work coordinator");
    info!(
        supports_coordination = work_coordinator.supports_coordination(),
        "work coordinator initialized"
    );

    // Build lease backend (same env var dispatch as work coordinator).
    let lease = exspeed_broker::lease::from_env()
        .await
        .expect("failed to initialize lease backend");
    info!(
        lease_backend = if lease.supports_coordination() { "coordinated" } else { "noop" },
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

    // Spawn cluster-leader leadership state machine.
    let leadership = Arc::new(
        exspeed_broker::leadership::ClusterLeadership::spawn(
            lease.clone(),
            metrics.clone(),
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
    let _ = tokio::time::timeout(
        startup_deadline,
        leadership_rx.wait_for(|&v| v),
    )
    .await;

    let role = if leadership.is_currently_leader() { "leader" } else { "standby" };

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
    info!(
        auth = if auth_token.is_some() { "on" } else { "off" },
        tls = if tls_enabled { "on" } else { "off" },
        lease = %lease_backend_name,
        role = role,
        bind = %args.bind,
        api_bind = %args.api_bind,
        "exspeed server starting"
    );
    if role == "standby" {
        info!(
            "role=standby — this pod does not serve client traffic. \
             Configure your load balancer to probe GET /healthz and only \
             route to pods returning 200."
        );
    }

    // Create broker
    let broker_append_for_connectors = broker_append.clone();
    let broker = Arc::new(Broker::new(
        storage.clone(),
        broker_append,
        args.data_dir.clone(),
        consumer_store,
        work_coordinator.clone(),
        lease.clone(),
        metrics.clone(),
    ));
    broker.load_consumers().await.map_err(|e| anyhow::anyhow!(e))?;

    // Warn if grouped consumers exist but the coordinator doesn't support
    // multi-pod coordination (file/s3 backends).
    if !work_coordinator.supports_coordination() {
        let consumers = broker.consumers.read().unwrap();
        let grouped_count = consumers
            .values()
            .filter(|c| !c.config.group.is_empty())
            .count();
        if grouped_count > 0 {
            warn!(
                grouped_consumers = grouped_count,
                "grouped consumers exist but the active backend does not support multi-pod \
                 coordination — running multiple pods will cause duplicate deliveries. \
                 Set EXSPEED_CONSUMER_STORE=postgres or redis for multi-pod safety."
            );
        }
    }

    // Create offset store (backend selected by EXSPEED_OFFSET_STORE env var)
    let offset_backend = std::env::var("EXSPEED_OFFSET_STORE").unwrap_or_else(|_| "file".to_string());
    let offset_store = exspeed_connectors::offset_store::from_env(
        &args.data_dir,
        storage.clone(),
    )
    .await
    .expect("failed to initialize offset store");
    info!(backend = offset_backend.as_str(), "offset store initialized");

    // Create connector manager
    let connector_manager = Arc::new(ConnectorManager::new(
        storage.clone(),
        broker_append_for_connectors,
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

    // Create ExQL engine (use file_storage as the StorageEngine trait object)
    let exql_storage: Arc<dyn StorageEngine> = file_storage.clone();
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

    // Readiness flag, flipped to true after the HTTP API server is spawned.
    // Until then, /readyz returns 503 with {"status":"starting"}.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Create shared AppState
    let state = Arc::new(exspeed_api::AppState {
        broker: broker.clone(),
        storage: file_storage.clone(),
        metrics: metrics.clone(),
        start_time: std::time::Instant::now(),
        prometheus_registry,
        connector_manager,
        exql,
        auth_token: auth_token.clone(),
        lease: lease.clone(),
        leadership: leadership.clone(),
        ready: ready.clone(),
        data_dir: args.data_dir.clone(),
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
        let supervisor_cancel = cancel_token.clone();
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

                tokio::select! {
                    _ = connector_manager_sup.run_all(token.clone()) => {}
                    _ = exql_sup.clone().resume_all_and_run(token.clone()) => {}
                    _ = exspeed_broker::retention_task::run(
                            storage_sup.clone(),
                            token.clone(),
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
        if let Err(e) =
            exspeed_api::serve_with_shutdown(state, api_addr, http_tls, shutdown).await
        {
            error!("HTTP API exited: {}", e);
        }
    });

    // Load TLS config if enabled.
    let tls_config = match &tls_paths {
        Some(paths) => Some(crate::cli::server_tls::load_tls_config(&paths.cert, &paths.key)?),
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

                let broker = broker.clone();
                let metrics_clone = metrics.clone();
                let auth_token_clone = auth_token.clone();
                let tls_config_clone = tls_config.clone();
                let conn_token = cancel_token.child_token();
                tokio::spawn(async move {
                    let _permit = permit; // released when this task ends
                    let result: Result<()> = async move {
                        if let Some(tls_cfg) = tls_config_clone {
                            let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
                            let tls_stream = acceptor.accept(socket).await?;
                            handle_connection(tls_stream, peer, broker, auth_token_clone, conn_token).await
                        } else {
                            handle_connection(socket, peer, broker, auth_token_clone, conn_token).await
                        }
                    }
                    .await;
                    if let Err(e) = result {
                        error!(%peer, "connection error: {}", e);
                    }
                    metrics_clone.connection_closed();
                    info!(%peer, "connection closed");
                });
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
        "waiting up to {:?} for active connections to drain",
        drain_deadline
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

async fn handle_connection<S>(
    socket: S,
    peer: SocketAddr,
    broker: Arc<Broker>,
    auth_token: Option<Arc<String>>,
    cancel: CancellationToken,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(socket);
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

    // Auth state. Pre-authenticated when auth is off.
    let mut authenticated = auth_token.is_none();

    // Per-connection subscription state (single subscription per TCP connection).
    // Tracks (consumer_name, subscriber_id) so disconnect cleanup removes the right subscriber.
    let mut active_subscription: Option<(String, String)> = None;
    let mut delivery_rx: Option<mpsc::Receiver<DeliveryRecord>> = None;
    let mut cancel_tx: Option<oneshot::Sender<()>> = None;

    loop {
        tokio::select! {
            biased;
            // Branch 0: process-wide shutdown (SIGTERM/SIGINT).
            // `biased` ensures cancel wins races against in-flight reads.
            _ = cancel.cancelled() => {
                info!(%peer, "connection cancelled by shutdown");
                break;
            }
            // Branch 1: incoming frame from client
            frame_result = framed_read.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
                        let correlation_id = frame.correlation_id;

                        let parsed = ClientMessage::from_frame(frame);

                        // Auth gate: all non-Connect ops require authentication.
                        if !authenticated {
                            match &parsed {
                                Ok(ClientMessage::Connect(_)) => { /* allowed */ }
                                _ => {
                                    warn!(%peer, "rejected op before auth");
                                    let response = ServerMessage::Error {
                                        code: 401,
                                        message: "unauthorized".into(),
                                    }
                                    .into_frame(correlation_id);
                                    framed_write.send(response).await?;
                                    break;
                                }
                            }
                        }

                        match parsed {
                            Ok(ClientMessage::Connect(req)) => {
                                let result = if let Some(expected) = &auth_token {
                                    if req.auth_type != exspeed_protocol::messages::connect::AuthType::Token {
                                        Err("unauthorized")
                                    } else if !exspeed_common::auth::verify_token(
                                        &req.auth_payload,
                                        expected,
                                    ) {
                                        Err("unauthorized")
                                    } else {
                                        Ok(())
                                    }
                                } else {
                                    Ok(())
                                };
                                match result {
                                    Ok(()) => {
                                        authenticated = true;
                                        info!(%peer, client_id = %req.client_id, "CONNECT authenticated");
                                        let response = ServerMessage::Ok.into_frame(correlation_id);
                                        framed_write.send(response).await?;
                                    }
                                    Err(msg) => {
                                        warn!(%peer, client_id = %req.client_id, "CONNECT rejected");
                                        let response = ServerMessage::Error {
                                            code: 401,
                                            message: msg.to_string(),
                                        }
                                        .into_frame(correlation_id);
                                        framed_write.send(response).await?;
                                        break; // close the socket
                                    }
                                }
                            }
                            Ok(ClientMessage::Ping) => {
                                let response = ServerMessage::Pong.into_frame(correlation_id);
                                framed_write.send(response).await?;
                            }
                            Ok(ClientMessage::Subscribe(req)) => {
                                if active_subscription.is_some() {
                                    let response = ServerMessage::Error {
                                        code: 400,
                                        message: "already subscribed; unsubscribe first".into(),
                                    }
                                    .into_frame(correlation_id);
                                    framed_write.send(response).await?;
                                } else {
                                    // Auto-generate subscriber_id if client didn't provide one
                                    // (legacy client, or new client opting out of explicit IDs).
                                    let subscriber_id = if req.subscriber_id.is_empty() {
                                        uuid::Uuid::new_v4().to_string()
                                    } else {
                                        req.subscriber_id.clone()
                                    };

                                    match broker.subscribe(&req.consumer_name, &subscriber_id) {
                                        Ok((rx, cancel)) => {
                                            active_subscription = Some((req.consumer_name.clone(), subscriber_id));
                                            delivery_rx = Some(rx);
                                            drop(cancel_tx.replace(cancel));
                                            framed_write
                                                .send(ServerMessage::Ok.into_frame(correlation_id))
                                                .await?;
                                        }
                                        Err(e) => {
                                            let response = ServerMessage::Error {
                                                code: 400,
                                                message: e,
                                            }
                                            .into_frame(correlation_id);
                                            framed_write.send(response).await?;
                                        }
                                    }
                                }
                            }
                            Ok(ClientMessage::Unsubscribe(req)) => {
                                // Use the subscriber_id the client sent, or fall back to the one
                                // the server generated on the Subscribe call (stored in active_subscription).
                                let subscriber_id = if !req.subscriber_id.is_empty() {
                                    req.subscriber_id.clone()
                                } else if let Some((_, ref sub_id)) = active_subscription {
                                    sub_id.clone()
                                } else {
                                    String::new()
                                };
                                if !subscriber_id.is_empty() {
                                    let _ = broker.unsubscribe(&req.consumer_name, &subscriber_id);
                                }
                                drop(cancel_tx.take());
                                delivery_rx = None;
                                active_subscription = None;
                                framed_write
                                    .send(ServerMessage::Ok.into_frame(correlation_id))
                                    .await?;
                            }
                            Ok(msg) => {
                                // All other messages: dispatch to broker
                                let response = broker.handle_message(msg).await;
                                framed_write
                                    .send(response.into_frame(correlation_id))
                                    .await?;
                            }
                            Err(e) => {
                                warn!(%peer, "unhandled message: {}", e);
                                let response = ServerMessage::Error {
                                    code: 400,
                                    message: e.to_string(),
                                }
                                .into_frame(correlation_id);
                                framed_write.send(response).await?;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!(%peer, "frame decode error: {}", e);
                        return Err(e.into());
                    }
                    None => break, // client disconnected
                }
            }

            // Branch 2: outgoing delivery record from active subscription
            delivery = async {
                match delivery_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(delivery_record) = delivery {
                    let consumer_name = match active_subscription.as_ref() {
                        Some((name, _)) => name.clone(),
                        None => {
                            warn!(
                                %peer,
                                offset = delivery_record.record.offset.0,
                                "received delivery after subscription was cleared; \
                                 dropping record and tearing down delivery channel"
                            );
                            delivery_rx = None;
                            drop(cancel_tx.take());
                            continue;
                        }
                    };
                    let record_delivery = RecordDelivery {
                        consumer_name,
                        offset: delivery_record.record.offset.0,
                        timestamp: delivery_record.record.timestamp,
                        subject: delivery_record.record.subject.clone(),
                        delivery_attempt: delivery_record.delivery_attempt,
                        key: delivery_record.record.key.clone(),
                        value: delivery_record.record.value.clone(),
                        headers: delivery_record.record.headers.clone(),
                    };
                    let response = ServerMessage::Record(record_delivery);
                    framed_write.send(response.into_frame(0)).await?;
                } else {
                    // Channel closed — delivery task stopped
                    delivery_rx = None;
                    drop(cancel_tx.take());
                    active_subscription = None;
                }
            }
        }
    }

    // Clean up: if this connection had an active subscription, unsubscribe this
    // specific subscriber so other subscribers on the same consumer are unaffected.
    if let Some((ref consumer_name, ref subscriber_id)) = active_subscription {
        let _ = broker.unsubscribe(consumer_name, subscriber_id);
    }

    Ok(())
}
