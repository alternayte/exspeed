use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

pub type Reader = FramedRead<OwnedReadHalf, ExspeedCodec>;
pub type Writer = FramedWrite<OwnedWriteHalf, ExspeedCodec>;

pub struct ExspeedClient {
    pub reader: Reader,
    pub writer: Writer,
    next_corr: u32,
}

impl ExspeedClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.context("tcp connect")?;
        stream.set_nodelay(true).ok();
        let (r, w) = stream.into_split();
        let mut client = Self {
            reader: FramedRead::new(r, ExspeedCodec::new()),
            writer: FramedWrite::new(w, ExspeedCodec::new()),
            next_corr: 1,
        };
        client.connect_handshake().await?;
        Ok(client)
    }

    async fn connect_handshake(&mut self) -> Result<()> {
        let req = ConnectRequest {
            client_id: "exspeed-bench".into(),
            auth_type: AuthType::None,
            auth_payload: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let corr = self.alloc_corr();
        self.writer.send(Frame::new(OpCode::Connect, corr, buf.freeze())).await?;
        let resp = self.reader.next().await.ok_or_else(|| anyhow!("connect closed"))??;
        if resp.opcode != OpCode::ConnectOk {
            return Err(anyhow!("expected ConnectOk, got {:?}", resp.opcode));
        }
        Ok(())
    }

    pub async fn ensure_stream(&mut self, name: &str) -> Result<()> {
        let req = CreateStreamRequest {
            stream_name: name.into(),
            max_age_secs: 0,
            max_bytes: 0,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let corr = self.alloc_corr();
        self.writer.send(Frame::new(OpCode::CreateStream, corr, buf.freeze())).await?;
        let resp = self.reader.next().await.ok_or_else(|| anyhow!("closed"))??;
        match resp.opcode {
            OpCode::Ok => Ok(()),
            // Broker returns Error for "stream already exists" — that's fine here.
            OpCode::Error => Ok(()),
            other => Err(anyhow!("ensure_stream: unexpected opcode {other:?}")),
        }
    }

    pub fn alloc_corr(&mut self) -> u32 {
        let c = self.next_corr;
        self.next_corr = self.next_corr.wrapping_add(1).max(1);
        c
    }
}
