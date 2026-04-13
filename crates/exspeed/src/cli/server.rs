use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info, warn};

use exspeed_broker::Broker;
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_storage::file::FileStorage;
use exspeed_streams::StorageEngine;

#[derive(Args)]
pub struct ServerArgs {
    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:5933")]
    pub bind: String,

    /// Directory for persistent data
    #[arg(long, default_value = "./exspeed-data")]
    pub data_dir: PathBuf,
}

pub async fn run(args: ServerArgs) -> Result<()> {
    let storage: Arc<dyn StorageEngine> = Arc::new(FileStorage::open(&args.data_dir)?);
    let broker = Arc::new(Broker::new(storage, args.data_dir.clone()));

    let addr: SocketAddr = args.bind.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("exspeed server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        info!(%peer, "new connection");

        let broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, broker).await {
                error!(%peer, "connection error: {}", e);
            }
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

    while let Some(result) = framed_read.next().await {
        match result {
            Ok(frame) => {
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
                    Ok(msg) => {
                        let broker = broker.clone();
                        let response =
                            tokio::task::spawn_blocking(move || broker.handle_message(msg)).await?;
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
            Err(e) => {
                error!(%peer, "frame decode error: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}
