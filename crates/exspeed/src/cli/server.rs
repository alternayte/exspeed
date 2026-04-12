use std::net::SocketAddr;

use anyhow::Result;
use clap::Args;
use tokio::net::TcpListener;
use tokio_util::codec::{FramedRead, FramedWrite};
use futures_util::{SinkExt, StreamExt};
use tracing::{info, warn, error};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};

#[derive(Args)]
pub struct ServerArgs {
    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:5933")]
    pub bind: String,
}

pub async fn run(args: ServerArgs) -> Result<()> {
    let addr: SocketAddr = args.bind.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("exspeed server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        info!(%peer, "new connection");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer).await {
                error!(%peer, "connection error: {}", e);
            }
            info!(%peer, "connection closed");
        });
    }
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    peer: SocketAddr,
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
