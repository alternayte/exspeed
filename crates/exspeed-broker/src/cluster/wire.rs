//! Replication protocol between cluster nodes (cluster port, default 5934).
//!
//! Frames are `[u32 LE length][bincode payload]` carrying one [`Msg`]. The
//! follower drives the conversation: `Hello` → `HelloOk`, then a loop of
//! `Fetch` → `FetchOk` (or `Error`). One request is in flight at a time.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use exspeed_common::Offset;
use exspeed_streams::StoredRecord;

pub const PROTOCOL_VERSION: u16 = 1;
/// Largest frame accepted (a fetch response is bounded by `max_bytes`).
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    Hello {
        protocol: u16,
        node_id: String,
        token: Option<String>,
    },
    HelloOk {
        node_id: String,
        epoch: u64,
    },
    Fetch(FetchRequest),
    FetchOk(FetchResponse),
    Error {
        code: u16,
        message: String,
    },
}

pub mod code {
    pub const UNAUTHORIZED: u16 = 401;
    pub const BAD_REQUEST: u16 = 400;
    /// The fetcher knows a newer epoch than this node's: this node is a
    /// deposed leader.
    pub const FENCED: u16 = 409;
    pub const NOT_LEADER: u16 = 503;
}

/// The follower's position in one stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPos {
    pub stream: String,
    /// The uid the follower knows the stream by.
    pub uid: u64,
    /// The follower has everything below this offset.
    pub next: u64,
    /// Epoch of the follower's last record (0 if none / unknown).
    pub last_epoch: u64,
    /// The follower's earliest retained offset.
    pub earliest: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    /// The highest leader epoch the follower knows of.
    pub epoch: u64,
    /// The metadata version the follower last applied.
    pub metadata_version: u64,
    pub positions: Vec<FetchPos>,
    pub max_wait_ms: u32,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    pub name: String,
    pub uid: u64,
    /// `StreamConfig` as JSON (it has serde attributes bincode can't carry).
    pub config_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamAction {
    /// Append these records (offsets ascending, gaps allowed).
    Records(Vec<WireRecord>),
    /// Drop everything at or after this offset: it diverged from the
    /// leader's log.
    Truncate(u64),
    /// Nothing new; only `earliest` moved.
    Trim,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    pub stream: String,
    pub action: StreamAction,
    /// Leader's earliest retained offset: the follower trims up to it.
    pub earliest: u64,
    /// Leader's next offset.
    pub high_watermark: u64,
    /// Leader's epoch history for the stream.
    pub epochs: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub epoch: u64,
    pub metadata_version: u64,
    /// Every stream on the leader, when the follower's metadata version is
    /// out of date.
    pub metadata: Option<Vec<StreamMeta>>,
    pub streams: Vec<StreamData>,
}

impl FetchResponse {
    pub fn is_empty(&self) -> bool {
        self.metadata.is_none() && self.streams.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

impl WireRecord {
    pub fn size(&self) -> usize {
        32 + self.subject.len()
            + self.value.len()
            + self.key.as_ref().map_or(0, |k| k.len())
            + self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len() + 8)
                .sum::<usize>()
    }
}

impl From<StoredRecord> for WireRecord {
    fn from(r: StoredRecord) -> Self {
        Self {
            offset: r.offset.0,
            timestamp: r.timestamp,
            subject: r.subject,
            key: r.key,
            value: r.value,
            headers: r.headers,
        }
    }
}

impl From<WireRecord> for StoredRecord {
    fn from(r: WireRecord) -> Self {
        Self {
            offset: Offset(r.offset),
            timestamp: r.timestamp,
            subject: r.subject,
            key: r.key,
            value: r.value,
            headers: r.headers,
        }
    }
}

pub async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, msg: &Msg) -> std::io::Result<()> {
    let body = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut buf = Vec::with_capacity(body.len() + 4);
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body);
    w.write_all(&buf).await?;
    w.flush().await
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Msg> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("replication frame of {len} bytes exceeds the limit"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    bincode::deserialize(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let msg = Msg::FetchOk(FetchResponse {
            epoch: 3,
            metadata_version: 9,
            metadata: Some(vec![StreamMeta {
                name: "a".into(),
                uid: 7,
                config_json: "{}".into(),
            }]),
            streams: vec![StreamData {
                stream: "a".into(),
                action: StreamAction::Records(vec![WireRecord {
                    offset: 5,
                    timestamp: 6,
                    subject: "s".into(),
                    key: Some(Bytes::from_static(b"k")),
                    value: Bytes::from_static(b"v"),
                    headers: vec![("h".into(), "1".into())],
                }]),
                earliest: 0,
                high_watermark: 6,
                epochs: vec![(1, 0), (3, 4)],
            }],
        });
        let mut buf = Vec::new();
        write_msg(&mut buf, &msg).await.unwrap();
        let back = read_msg(&mut buf.as_slice()).await.unwrap();
        match back {
            Msg::FetchOk(r) => {
                assert_eq!(r.epoch, 3);
                let StreamAction::Records(rs) = &r.streams[0].action else {
                    panic!()
                };
                assert_eq!(rs[0].key.as_deref(), Some(&b"k"[..]));
                assert_eq!(r.streams[0].epochs, vec![(1, 0), (3, 4)]);
            }
            other => panic!("{other:?}"),
        }
    }
}
