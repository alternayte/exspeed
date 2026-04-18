use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
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
    // Normalize auth token: empty string → unset (guards against shells
    // passing EXSPEED_AUTH_TOKEN="" through).
    let auth_token: Option<Arc<String>> = args
        .auth_token
        .as_ref()
        .filter(|v| !v.is_empty())
        .cloned()
        .map(Arc::new);

    // Validate TLS: both or neither.
    match (args.tls_cert.as_ref(), args.tls_key.as_ref()) {
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "TLS configuration invalid: EXSPEED_TLS_CERT and EXSPEED_TLS_KEY must both be set or both unset"
            );
        }
        _ => {}
    }
    let tls_enabled = args.tls_cert.is_some();

    // Posture log (always).
    info!(
        auth = if auth_token.is_some() { "on" } else { "off" },
        tls = if tls_enabled { "on" } else { "off" },
        bind = %args.bind,
        api_bind = %args.api_bind,
        "exspeed server starting"
    );
    if auth_token.is_none() {
        warn!("auth disabled — do not expose broker ports to the public internet");
    }
    if !tls_enabled {
        warn!("TLS disabled — do not expose broker ports to the public internet");
    }

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
    let broker_append = Arc::new(BrokerAppend::new(storage.clone(), dedup_window_secs));
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

    // Create broker
    let broker_append_for_connectors = broker_append.clone();
    let broker = Arc::new(Broker::new(
        storage.clone(),
        broker_append,
        args.data_dir.clone(),
        consumer_store,
        work_coordinator.clone(),
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
    let exql = Arc::new(ExqlEngine::new(exql_storage, args.data_dir.clone()));
    exql.load().unwrap_or_else(|e| warn!("ExQL load: {e}"));
    exql.resume_all();

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
    });

    // Spawn retention task
    exspeed_broker::retention_task::spawn_retention_task(file_storage);

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
    tokio::spawn(exspeed_api::serve(state, api_addr));

    // Load TLS config if enabled.
    let tls_config = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(crate::cli::server_tls::load_tls_config(cert, key)?),
        _ => None,
    };

    // TCP server
    let tcp_addr: SocketAddr = args.bind.parse()?;
    let listener = TcpListener::bind(tcp_addr).await?;
    info!("exspeed TCP listening on {}", tcp_addr);
    info!("exspeed HTTP API listening on {}", api_addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        info!(%peer, "new connection");
        metrics.connection_opened();

        let broker = broker.clone();
        let metrics_clone = metrics.clone();
        let auth_token_clone = auth_token.clone();
        let tls_config_clone = tls_config.clone();
        tokio::spawn(async move {
            let result: Result<()> = async move {
                if let Some(tls_cfg) = tls_config_clone {
                    let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
                    let tls_stream = acceptor.accept(socket).await?;
                    handle_connection(tls_stream, peer, broker, auth_token_clone).await
                } else {
                    handle_connection(socket, peer, broker, auth_token_clone).await
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

async fn handle_connection<S>(
    socket: S,
    peer: SocketAddr,
    broker: Arc<Broker>,
    auth_token: Option<Arc<String>>,
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
                    let consumer_name = active_subscription
                        .as_ref()
                        .expect("delivery_rx is Some but active_subscription is None")
                        .0
                        .clone();
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
