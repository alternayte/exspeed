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

use exspeed_broker::consumer_state::DeliveryRecord;
use exspeed_broker::Broker;
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
}

pub async fn run(args: ServerArgs) -> Result<()> {
    // Create storage
    let file_storage = Arc::new(FileStorage::open(&args.data_dir)?);
    let storage: Arc<dyn StorageEngine> = file_storage.clone();

    // Create metrics
    let (metrics, prometheus_registry) = exspeed_common::Metrics::new();
    let metrics = Arc::new(metrics);

    // Create broker
    let broker = Arc::new(Broker::new(storage, args.data_dir.clone()));
    broker.load_consumers()?;

    // Create shared AppState
    let state = Arc::new(exspeed_api::AppState {
        broker: broker.clone(),
        storage: file_storage.clone(),
        metrics: metrics.clone(),
        start_time: std::time::Instant::now(),
        prometheus_registry,
    });

    // Spawn retention task
    exspeed_broker::retention_task::spawn_retention_task(file_storage);

    // Spawn HTTP API server
    let api_addr: SocketAddr = args.api_bind.parse()?;
    tokio::spawn(exspeed_api::serve(state, api_addr));

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
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, broker).await {
                error!(%peer, "connection error: {}", e);
            }
            metrics_clone.connection_closed();
            info!(%peer, "connection closed");
        });
    }
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    peer: SocketAddr,
    broker: Arc<Broker>,
) -> Result<()> {
    let (reader, writer) = socket.into_split();
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

    // Per-connection subscription state (Phase 2b: single subscription per connection).
    let mut active_subscription: Option<String> = None;
    let mut delivery_rx: Option<mpsc::Receiver<DeliveryRecord>> = None;
    // Dropping cancel_tx signals the delivery task to stop (oneshot cancellation).
    let mut cancel_tx: Option<oneshot::Sender<()>> = None;

    loop {
        tokio::select! {
            // Branch 1: incoming frame from client
            frame_result = framed_read.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
                        let correlation_id = frame.correlation_id;

                        match ClientMessage::from_frame(frame) {
                            Ok(ClientMessage::Connect(req)) => {
                                info!(%peer, client_id = %req.client_id, "CONNECT");
                                let response = ServerMessage::Ok.into_frame(correlation_id);
                                framed_write.send(response).await?;
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
                                    match broker.subscribe(&req.consumer_name) {
                                        Ok((rx, cancel)) => {
                                            active_subscription = Some(req.consumer_name.clone());
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
                                let _ = broker.unsubscribe(&req.consumer_name);
                                drop(cancel_tx.take());
                                delivery_rx = None;
                                active_subscription = None;
                                framed_write
                                    .send(ServerMessage::Ok.into_frame(correlation_id))
                                    .await?;
                            }
                            Ok(msg) => {
                                // All other messages: dispatch to broker via spawn_blocking
                                let broker = broker.clone();
                                let response = tokio::task::spawn_blocking(move || {
                                    broker.handle_message(msg)
                                })
                                .await?;
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

    Ok(())
}
