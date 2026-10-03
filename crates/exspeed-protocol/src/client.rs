//! Client protocol (version 2): every request a client can send and every
//! response or push the server can return, with their binary encodings.
//!
//! Conventions (all integers little-endian):
//! - `str`   = `u16` length + UTF-8 bytes
//! - `lstr`  = `u32` length + UTF-8 bytes (SQL text)
//! - `bytes` = `u32` length + raw bytes
//! - `opt<T>`= `u8` flag (0 = none, 1 = some) + `T`
//! - `headers` = `u16` count + (`str` key, `str` value) pairs
//! - `vec<T>`  = `u32` count + items
//!
//! The data plane (publish, deliver, read, ack) is binary. Admin requests
//! whose shape evolves (consumer config, stream/consumer info, query
//! results) carry JSON.
//!
//! A request sent with correlation id `0` gets no reply on success
//! (fire-and-forget); used for `Ack` and `Credit` on hot paths. Errors are
//! still reported (with correlation id 0).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use exspeed_common::record_format;

use crate::error::ProtocolError;
use crate::frame::{Frame, OutFrame};
use crate::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Error codes (HTTP-like)
// ---------------------------------------------------------------------------

pub mod code {
    pub const BAD_REQUEST: u16 = 400;
    pub const UNAUTHORIZED: u16 = 401;
    pub const FORBIDDEN: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    /// Conflict: stream/consumer exists with a different config, or a
    /// `msg_id` was reused with a different body (`detail.stored_offset`).
    pub const CONFLICT: u16 = 409;
    /// Retry later: dedup map full (`detail.retry_after_secs`), max ack
    /// pending reached, etc.
    pub const TOO_MANY_REQUESTS: u16 = 429;
    pub const INTERNAL: u16 = 500;
    /// Not the leader (`detail.leader` = leader address when known), or the
    /// server is still starting.
    pub const UNAVAILABLE: u16 = 503;
    /// The server's disk is full. Nothing was written; retry once space is
    /// freed.
    pub const INSUFFICIENT_STORAGE: u16 = 507;
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// A record as published by a client.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishRecord {
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
    /// Idempotency key: retries with the same `msg_id` and body return the
    /// original offset instead of writing again.
    pub msg_id: Option<String>,
}

impl PublishRecord {
    pub fn new(subject: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            subject: subject.into(),
            value: value.into(),
            ..Default::default()
        }
    }

    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn header(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.headers.push((k.into(), v.into()));
        self
    }

    pub fn msg_id(mut self, id: impl Into<String>) -> Self {
        self.msg_id = Some(id.into());
        self
    }
}

impl StreamSpec {
    /// A stream with server defaults for every setting.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
}

/// A record as delivered to a client. On the wire it uses the record
/// encoding of segment files ([`exspeed_common::record_format`]): length,
/// CRC32C, delivery count, then the fields below.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WireRecord {
    pub offset: u64,
    /// Append time, nanoseconds since the Unix epoch.
    pub timestamp_ns: u64,
    /// 1 on first delivery, incremented on each redelivery. 0 for stateless
    /// reads.
    pub delivery_count: u16,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

impl WireRecord {
    /// Append time, milliseconds since the Unix epoch.
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ns / 1_000_000
    }

    /// Number of bytes this record takes in a record list (its wire
    /// encoding), or an error if a field is too long to encode.
    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        record_format::encoded_len(&record_format::Fields {
            offset: self.offset,
            timestamp_ns: self.timestamp_ns,
            delivery_count: self.delivery_count,
            subject: &self.subject,
            key: self.key.as_deref(),
            value: &self.value,
            headers: &self.headers,
        })
        .map_err(|e| ProtocolError::Encode(e.0))
    }
}

/// Records already in wire encoding, ready to be written into a
/// `Deliver`, `Messages` or `ReadResult` frame without re-encoding. The
/// bytes are held as a list of chunks, typically zero-copy slices of a
/// segment read, so building a response never decodes a record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EncodedRecords {
    chunks: Vec<Bytes>,
    count: u32,
    len: usize,
}

impl EncodedRecords {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `count` back-to-back encoded records. The caller guarantees
    /// that `chunk` holds exactly that many valid records (they come from
    /// storage, which verified them).
    pub fn push_chunk(&mut self, chunk: Bytes, count: u32) {
        if count == 0 {
            return;
        }
        self.len += chunk.len();
        self.count += count;
        self.chunks.push(chunk);
    }

    /// Encode and append one record.
    pub fn push_record(&mut self, r: &WireRecord) {
        let mut w = Writer::default();
        w.record(r);
        self.push_chunk(w.finish(), 1);
    }

    /// Number of records.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Total encoded size in bytes.
    pub fn byte_len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    /// Decode every record (tests and tools; the server never needs this).
    pub fn decode(&self) -> Result<Vec<WireRecord>, ProtocolError> {
        let mut out = Vec::with_capacity(self.count as usize);
        for c in &self.chunks {
            let mut r = Reader::new(c.clone());
            while r.buf.has_remaining() {
                out.push(r.record()?);
            }
        }
        Ok(out)
    }
}

impl From<&[WireRecord]> for EncodedRecords {
    fn from(records: &[WireRecord]) -> Self {
        let mut w = Writer::default();
        for r in records {
            w.record(r);
        }
        let mut out = Self::new();
        out.push_chunk(w.finish(), records.len() as u32);
        out
    }
}

/// Stream creation/update parameters. Zero means "default" for every field.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamSpec {
    pub name: String,
    pub max_age_secs: u64,
    pub max_bytes: u64,
    pub dedup_window_secs: u64,
    pub dedup_max_entries: u64,
    /// Keep only the latest record per key (log compaction).
    pub compaction: bool,
}

/// Where a consumer starts reading when it is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliverPolicy {
    /// From the first retained record.
    #[default]
    All,
    /// Only records appended after the consumer is created.
    New,
    /// From a specific offset.
    FromOffset(u64),
    /// From the first record at or after this time (ms since epoch).
    FromTime(u64),
}

/// Whether delivered records must be acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AckPolicy {
    /// Each record must be acked; unacked records are redelivered after
    /// `ack_wait_ms`.
    #[default]
    Explicit,
    /// Records count as acked when delivered (at-most-once).
    None,
}

fn default_ack_wait_ms() -> u64 {
    30_000
}
fn default_max_deliver() -> u32 {
    5
}
fn default_max_ack_pending() -> u32 {
    1_000
}

/// A durable (or ephemeral) consumer: a cursor plus delivery state over one
/// stream. Several subscribers on one consumer share its records (a work
/// queue); each record goes to one of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerSpec {
    pub name: String,
    pub stream: String,
    /// NATS-style subject filters (`orders.*`, `orders.>`). Empty = all.
    #[serde(default)]
    pub filter_subjects: Vec<String>,
    #[serde(default)]
    pub deliver: DeliverPolicy,
    #[serde(default)]
    pub ack: AckPolicy,
    /// Redeliver a record if it isn't acked within this time.
    #[serde(default = "default_ack_wait_ms")]
    pub ack_wait_ms: u64,
    /// Give up (dead-letter) after this many deliveries. 0 = never.
    #[serde(default = "default_max_deliver")]
    pub max_deliver: u32,
    /// Redelivery delays after a nack or timeout, indexed by delivery
    /// count (the last value repeats). Empty = redeliver immediately.
    #[serde(default)]
    pub backoff_ms: Vec<u64>,
    /// Stop delivering while this many records await an ack.
    #[serde(default = "default_max_ack_pending")]
    pub max_ack_pending: u32,
    /// Where records go after `max_deliver` attempts or a `Term`. Unset =
    /// they are dropped (and counted).
    #[serde(default)]
    pub dlq_stream: Option<String>,
    /// Deleted automatically when the connection that created it closes.
    #[serde(default)]
    pub ephemeral: bool,
}

impl ConsumerSpec {
    /// A spec with defaults for everything but the name and stream.
    pub fn new(name: impl Into<String>, stream: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stream: stream.into(),
            filter_subjects: Vec::new(),
            deliver: DeliverPolicy::default(),
            ack: AckPolicy::default(),
            ack_wait_ms: default_ack_wait_ms(),
            max_deliver: default_max_deliver(),
            backoff_ms: Vec::new(),
            max_ack_pending: default_max_ack_pending(),
            dlq_stream: None,
            ephemeral: false,
        }
    }
}

/// Target of a consumer seek.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekTo {
    Earliest,
    Latest,
    Offset(u64),
    /// Milliseconds since the Unix epoch.
    Time(u64),
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Connect {
        client_id: String,
        token: Option<String>,
    },
    Ping,
    Metadata,
    Publish {
        stream: String,
        record: PublishRecord,
    },
    PublishBatch {
        stream: String,
        records: Vec<PublishRecord>,
    },
    CreateStream(StreamSpec),
    UpdateStream(StreamSpec),
    DeleteStream {
        name: String,
    },
    StreamInfo {
        name: String,
    },
    ListStreams,
    Query {
        sql: String,
    },
    CreateConsumer(ConsumerSpec),
    DeleteConsumer {
        name: String,
    },
    ConsumerInfo {
        name: String,
    },
    ListConsumers {
        stream: Option<String>,
    },
    SeekConsumer {
        consumer: String,
        to: SeekTo,
    },
    /// Start push delivery from `consumer`. The server sends at most
    /// `credits` records before waiting for more `Credit`.
    Subscribe {
        consumer: String,
        credits: u32,
    },
    Credit {
        sub_id: u32,
        credits: u32,
    },
    Unsubscribe {
        sub_id: u32,
    },
    /// Fetch up to `max_messages` from `consumer`, waiting up to
    /// `expires_ms` for at least one.
    Pull {
        consumer: String,
        max_messages: u32,
        max_bytes: u32,
        expires_ms: u32,
    },
    Ack {
        consumer: String,
        offsets: Vec<u64>,
    },
    /// Redeliver after `delay_ms` (0 = use the consumer's backoff).
    Nack {
        consumer: String,
        offset: u64,
        delay_ms: u32,
    },
    /// Never redeliver: dead-letter now.
    Term {
        consumer: String,
        offset: u64,
        reason: String,
    },
    /// Still working on these; reset their ack deadlines.
    InProgress {
        consumer: String,
        offsets: Vec<u64>,
    },
    /// Stateless read of a stream (no consumer).
    Read {
        stream: String,
        from: u64,
        max_records: u32,
        max_bytes: u32,
        /// Long-poll: wait up to this long for records when caught up.
        wait_ms: u32,
        /// NATS-style subject filter; empty = all.
        filter: String,
    },
}

impl Request {
    pub fn opcode(&self) -> OpCode {
        match self {
            Request::Connect { .. } => OpCode::Connect,
            Request::Ping => OpCode::Ping,
            Request::Metadata => OpCode::Metadata,
            Request::Publish { .. } => OpCode::Publish,
            Request::PublishBatch { .. } => OpCode::PublishBatch,
            Request::CreateStream(_) => OpCode::CreateStream,
            Request::UpdateStream(_) => OpCode::UpdateStream,
            Request::DeleteStream { .. } => OpCode::DeleteStream,
            Request::StreamInfo { .. } => OpCode::StreamInfo,
            Request::ListStreams => OpCode::ListStreams,
            Request::Query { .. } => OpCode::Query,
            Request::CreateConsumer(_) => OpCode::CreateConsumer,
            Request::DeleteConsumer { .. } => OpCode::DeleteConsumer,
            Request::ConsumerInfo { .. } => OpCode::ConsumerInfo,
            Request::ListConsumers { .. } => OpCode::ListConsumers,
            Request::SeekConsumer { .. } => OpCode::SeekConsumer,
            Request::Subscribe { .. } => OpCode::Subscribe,
            Request::Credit { .. } => OpCode::Credit,
            Request::Unsubscribe { .. } => OpCode::Unsubscribe,
            Request::Pull { .. } => OpCode::Pull,
            Request::Ack { .. } => OpCode::Ack,
            Request::Nack { .. } => OpCode::Nack,
            Request::Term { .. } => OpCode::Term,
            Request::InProgress { .. } => OpCode::InProgress,
            Request::Read { .. } => OpCode::Read,
        }
    }

    pub fn into_frame(self, correlation_id: u32) -> Frame {
        let mut w = Writer::default();
        self.encode(&mut w);
        Frame::new(self.opcode(), correlation_id, w.finish())
    }

    pub fn encode(&self, w: &mut Writer) {
        match self {
            Request::Connect { client_id, token } => {
                w.str(client_id);
                w.opt(token.as_ref(), |w, t| w.str(t));
            }
            Request::Ping | Request::Metadata | Request::ListStreams => {}
            Request::Publish { stream, record } => {
                w.str(stream);
                w.publish_record(record);
            }
            Request::PublishBatch { stream, records } => {
                w.str(stream);
                w.u32(records.len() as u32);
                for r in records {
                    w.publish_record(r);
                }
            }
            Request::CreateStream(s) | Request::UpdateStream(s) => w.stream_spec(s),
            Request::DeleteStream { name }
            | Request::StreamInfo { name }
            | Request::DeleteConsumer { name }
            | Request::ConsumerInfo { name } => w.str(name),
            Request::Query { sql } => w.lstr(sql),
            Request::CreateConsumer(spec) => {
                w.bytes(&serde_json::to_vec(spec).expect("ConsumerSpec serializes"))
            }
            Request::ListConsumers { stream } => w.opt(stream.as_ref(), |w, s| w.str(s)),
            Request::SeekConsumer { consumer, to } => {
                w.str(consumer);
                let (kind, value) = match to {
                    SeekTo::Earliest => (0, 0),
                    SeekTo::Latest => (1, 0),
                    SeekTo::Offset(o) => (2, *o),
                    SeekTo::Time(t) => (3, *t),
                };
                w.u8(kind);
                w.u64(value);
            }
            Request::Subscribe { consumer, credits } => {
                w.str(consumer);
                w.u32(*credits);
            }
            Request::Credit { sub_id, credits } => {
                w.u32(*sub_id);
                w.u32(*credits);
            }
            Request::Unsubscribe { sub_id } => w.u32(*sub_id),
            Request::Pull {
                consumer,
                max_messages,
                max_bytes,
                expires_ms,
            } => {
                w.str(consumer);
                w.u32(*max_messages);
                w.u32(*max_bytes);
                w.u32(*expires_ms);
            }
            Request::Ack { consumer, offsets } | Request::InProgress { consumer, offsets } => {
                w.str(consumer);
                w.u32(offsets.len() as u32);
                for o in offsets {
                    w.u64(*o);
                }
            }
            Request::Nack {
                consumer,
                offset,
                delay_ms,
            } => {
                w.str(consumer);
                w.u64(*offset);
                w.u32(*delay_ms);
            }
            Request::Term {
                consumer,
                offset,
                reason,
            } => {
                w.str(consumer);
                w.u64(*offset);
                w.str(reason);
            }
            Request::Read {
                stream,
                from,
                max_records,
                max_bytes,
                wait_ms,
                filter,
            } => {
                w.str(stream);
                w.u64(*from);
                w.u32(*max_records);
                w.u32(*max_bytes);
                w.u32(*wait_ms);
                w.str(filter);
            }
        }
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, ProtocolError> {
        Self::decode(frame.opcode, frame.payload.clone())
    }

    pub fn decode(opcode: OpCode, payload: Bytes) -> Result<Self, ProtocolError> {
        let mut r = Reader::new(payload);
        let req = match opcode {
            OpCode::Connect => Request::Connect {
                client_id: r.str()?,
                token: r.opt(|r| r.str())?,
            },
            OpCode::Ping => Request::Ping,
            OpCode::Metadata => Request::Metadata,
            OpCode::Publish => Request::Publish {
                stream: r.str()?,
                record: r.publish_record()?,
            },
            OpCode::PublishBatch => {
                let stream = r.str()?;
                let n = r.count(10)?;
                let mut records = Vec::with_capacity(n);
                for _ in 0..n {
                    records.push(r.publish_record()?);
                }
                Request::PublishBatch { stream, records }
            }
            OpCode::CreateStream => Request::CreateStream(r.stream_spec()?),
            OpCode::UpdateStream => Request::UpdateStream(r.stream_spec()?),
            OpCode::DeleteStream => Request::DeleteStream { name: r.str()? },
            OpCode::StreamInfo => Request::StreamInfo { name: r.str()? },
            OpCode::ListStreams => Request::ListStreams,
            OpCode::Query => Request::Query { sql: r.lstr()? },
            OpCode::CreateConsumer => {
                let raw = r.bytes()?;
                let spec: ConsumerSpec = serde_json::from_slice(&raw)
                    .map_err(|e| ProtocolError::Decode(format!("invalid consumer spec: {e}")))?;
                Request::CreateConsumer(spec)
            }
            OpCode::DeleteConsumer => Request::DeleteConsumer { name: r.str()? },
            OpCode::ConsumerInfo => Request::ConsumerInfo { name: r.str()? },
            OpCode::ListConsumers => Request::ListConsumers {
                stream: r.opt(|r| r.str())?,
            },
            OpCode::SeekConsumer => {
                let consumer = r.str()?;
                let kind = r.u8()?;
                let value = r.u64()?;
                let to = match kind {
                    0 => SeekTo::Earliest,
                    1 => SeekTo::Latest,
                    2 => SeekTo::Offset(value),
                    3 => SeekTo::Time(value),
                    k => return Err(ProtocolError::Decode(format!("unknown seek kind {k}"))),
                };
                Request::SeekConsumer { consumer, to }
            }
            OpCode::Subscribe => Request::Subscribe {
                consumer: r.str()?,
                credits: r.u32()?,
            },
            OpCode::Credit => Request::Credit {
                sub_id: r.u32()?,
                credits: r.u32()?,
            },
            OpCode::Unsubscribe => Request::Unsubscribe { sub_id: r.u32()? },
            OpCode::Pull => Request::Pull {
                consumer: r.str()?,
                max_messages: r.u32()?,
                max_bytes: r.u32()?,
                expires_ms: r.u32()?,
            },
            OpCode::Ack | OpCode::InProgress => {
                let consumer = r.str()?;
                let n = r.count(8)?;
                let mut offsets = Vec::with_capacity(n);
                for _ in 0..n {
                    offsets.push(r.u64()?);
                }
                if opcode == OpCode::Ack {
                    Request::Ack { consumer, offsets }
                } else {
                    Request::InProgress { consumer, offsets }
                }
            }
            OpCode::Nack => Request::Nack {
                consumer: r.str()?,
                offset: r.u64()?,
                delay_ms: r.u32()?,
            },
            OpCode::Term => Request::Term {
                consumer: r.str()?,
                offset: r.u64()?,
                reason: r.str()?,
            },
            OpCode::Read => Request::Read {
                stream: r.str()?,
                from: r.u64()?,
                max_records: r.u32()?,
                max_bytes: r.u32()?,
                wait_ms: r.u32()?,
                filter: r.str()?,
            },
            other => {
                return Err(ProtocolError::Decode(format!(
                    "opcode {other:?} is not a client request"
                )))
            }
        };
        r.finish()?;
        Ok(req)
    }
}

// ---------------------------------------------------------------------------
// Responses and pushes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Error {
        code: u16,
        message: String,
        /// Optional machine-readable JSON (e.g. `{"leader": "host:port"}`,
        /// `{"stored_offset": 7}`, `{"retry_after_secs": 30}`).
        detail: Option<Bytes>,
    },
    ConnectOk {
        server_version: String,
        node_id: String,
        /// Address of the leader when this node is not it.
        leader: Option<String>,
    },
    Pong,
    PublishOk {
        offset: u64,
        duplicate: bool,
    },
    PublishBatchOk {
        results: Vec<(u64, bool)>,
    },
    SubscribeOk {
        sub_id: u32,
    },
    /// Push delivery for a subscription (correlation id 0).
    Deliver {
        sub_id: u32,
        records: Vec<WireRecord>,
    },
    /// The subscription ended server-side (consumer deleted, leadership
    /// lost, …). No more `Deliver` frames will follow for `sub_id`.
    SubscriptionEnded {
        sub_id: u32,
        code: u16,
        message: String,
    },
    /// Reply to `Pull`.
    Messages {
        records: Vec<WireRecord>,
    },
    /// Reply to `Read`.
    ReadResult {
        next_offset: u64,
        high_watermark: u64,
        records: Vec<WireRecord>,
    },
    /// JSON reply (stream/consumer info and lists, query results, metadata).
    Json(Bytes),
}

impl Response {
    pub fn error(code: u16, message: impl Into<String>) -> Self {
        Response::Error {
            code,
            message: message.into(),
            detail: None,
        }
    }

    pub fn error_with(code: u16, message: impl Into<String>, detail: serde_json::Value) -> Self {
        Response::Error {
            code,
            message: message.into(),
            detail: Some(Bytes::from(detail.to_string())),
        }
    }

    /// A `Deliver` push (correlation id 0) carrying already-encoded records.
    pub fn deliver_frame(sub_id: u32, records: EncodedRecords) -> OutFrame {
        let mut w = Writer::default();
        w.u32(sub_id);
        w.u32(records.count());
        OutFrame::new(OpCode::Deliver, 0, w.finish(), records.chunks)
    }

    /// A `Messages` reply carrying already-encoded records.
    pub fn messages_frame(correlation_id: u32, records: EncodedRecords) -> OutFrame {
        let mut w = Writer::default();
        w.u32(records.count());
        OutFrame::new(OpCode::Messages, correlation_id, w.finish(), records.chunks)
    }

    /// A `ReadResult` reply carrying already-encoded records.
    pub fn read_result_frame(
        correlation_id: u32,
        next_offset: u64,
        high_watermark: u64,
        records: EncodedRecords,
    ) -> OutFrame {
        let mut w = Writer::default();
        w.u64(next_offset);
        w.u64(high_watermark);
        w.u32(records.count());
        OutFrame::new(
            OpCode::ReadResult,
            correlation_id,
            w.finish(),
            records.chunks,
        )
    }

    pub fn json(value: &impl Serialize) -> Self {
        Response::Json(Bytes::from(
            serde_json::to_vec(value).expect("JSON-serializable response"),
        ))
    }

    pub fn opcode(&self) -> OpCode {
        match self {
            Response::Ok => OpCode::Ok,
            Response::Error { .. } => OpCode::Error,
            Response::ConnectOk { .. } => OpCode::ConnectOk,
            Response::Pong => OpCode::Pong,
            Response::PublishOk { .. } => OpCode::PublishOk,
            Response::PublishBatchOk { .. } => OpCode::PublishBatchOk,
            Response::SubscribeOk { .. } => OpCode::SubscribeOk,
            Response::Deliver { .. } => OpCode::Deliver,
            Response::SubscriptionEnded { .. } => OpCode::SubscriptionEnded,
            Response::Messages { .. } => OpCode::Messages,
            Response::ReadResult { .. } => OpCode::ReadResult,
            Response::Json(_) => OpCode::Json,
        }
    }

    pub fn into_frame(self, correlation_id: u32) -> Frame {
        let mut w = Writer::default();
        self.encode(&mut w);
        Frame::new(self.opcode(), correlation_id, w.finish())
    }

    pub fn encode(&self, w: &mut Writer) {
        match self {
            Response::Ok | Response::Pong => {}
            Response::Error {
                code,
                message,
                detail,
            } => {
                w.u16(*code);
                w.str_lossy(message);
                w.opt(detail.as_ref(), |w, d| w.bytes(d));
            }
            Response::ConnectOk {
                server_version,
                node_id,
                leader,
            } => {
                w.str(server_version);
                w.str(node_id);
                w.opt(leader.as_ref(), |w, l| w.str(l));
            }
            Response::PublishOk { offset, duplicate } => {
                w.u64(*offset);
                w.u8(*duplicate as u8);
            }
            Response::PublishBatchOk { results } => {
                w.u32(results.len() as u32);
                for (o, d) in results {
                    w.u64(*o);
                    w.u8(*d as u8);
                }
            }
            Response::SubscribeOk { sub_id } => w.u32(*sub_id),
            Response::Deliver { sub_id, records } => {
                w.u32(*sub_id);
                w.records(records);
            }
            Response::SubscriptionEnded {
                sub_id,
                code,
                message,
            } => {
                w.u32(*sub_id);
                w.u16(*code);
                w.str_lossy(message);
            }
            Response::Messages { records } => w.records(records),
            Response::ReadResult {
                next_offset,
                high_watermark,
                records,
            } => {
                w.u64(*next_offset);
                w.u64(*high_watermark);
                w.records(records);
            }
            Response::Json(b) => w.raw(b),
        }
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, ProtocolError> {
        Self::decode(frame.opcode, frame.payload.clone())
    }

    pub fn decode(opcode: OpCode, payload: Bytes) -> Result<Self, ProtocolError> {
        if opcode == OpCode::Json {
            return Ok(Response::Json(payload));
        }
        let mut r = Reader::new(payload);
        let resp = match opcode {
            OpCode::Ok => Response::Ok,
            OpCode::Pong => Response::Pong,
            OpCode::Error => Response::Error {
                code: r.u16()?,
                message: r.str()?,
                detail: r.opt(|r| r.bytes())?,
            },
            OpCode::ConnectOk => Response::ConnectOk {
                server_version: r.str()?,
                node_id: r.str()?,
                leader: r.opt(|r| r.str())?,
            },
            OpCode::PublishOk => Response::PublishOk {
                offset: r.u64()?,
                duplicate: r.u8()? != 0,
            },
            OpCode::PublishBatchOk => {
                let n = r.count(9)?;
                let mut results = Vec::with_capacity(n);
                for _ in 0..n {
                    results.push((r.u64()?, r.u8()? != 0));
                }
                Response::PublishBatchOk { results }
            }
            OpCode::SubscribeOk => Response::SubscribeOk { sub_id: r.u32()? },
            OpCode::Deliver => Response::Deliver {
                sub_id: r.u32()?,
                records: r.records()?,
            },
            OpCode::SubscriptionEnded => Response::SubscriptionEnded {
                sub_id: r.u32()?,
                code: r.u16()?,
                message: r.str()?,
            },
            OpCode::Messages => Response::Messages {
                records: r.records()?,
            },
            OpCode::ReadResult => Response::ReadResult {
                next_offset: r.u64()?,
                high_watermark: r.u64()?,
                records: r.records()?,
            },
            other => {
                return Err(ProtocolError::Decode(format!(
                    "opcode {other:?} is not a server response"
                )))
            }
        };
        r.finish()?;
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Encoding primitives
// ---------------------------------------------------------------------------

/// Little-endian writer. String/byte lengths that don't fit their prefix are
/// truncated only for error messages (`str_lossy`); callers must validate
/// other lengths before encoding (the broker's write path does).
#[derive(Default)]
pub struct Writer {
    buf: BytesMut,
}

impl Writer {
    pub fn finish(self) -> Bytes {
        self.buf.freeze()
    }
    pub fn u8(&mut self, v: u8) {
        self.buf.put_u8(v);
    }
    pub fn u16(&mut self, v: u16) {
        self.buf.put_u16_le(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.buf.put_u32_le(v);
    }
    pub fn u64(&mut self, v: u64) {
        self.buf.put_u64_le(v);
    }
    pub fn raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
    pub fn str(&mut self, s: &str) {
        debug_assert!(s.len() <= u16::MAX as usize, "string too long for str");
        let len = s.len().min(u16::MAX as usize);
        self.u16(len as u16);
        self.raw(&s.as_bytes()[..len]);
    }
    /// Like `str` but truncates at a char boundary instead of asserting.
    pub fn str_lossy(&mut self, s: &str) {
        let mut end = s.len().min(u16::MAX as usize);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        self.str(&s[..end]);
    }
    pub fn lstr(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.raw(b);
    }
    pub fn opt<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                f(self, v);
            }
        }
    }
    pub fn headers(&mut self, h: &[(String, String)]) {
        self.u16(h.len().min(u16::MAX as usize) as u16);
        for (k, v) in h.iter().take(u16::MAX as usize) {
            self.str(k);
            self.str(v);
        }
    }
    fn publish_record(&mut self, r: &PublishRecord) {
        self.str(&r.subject);
        self.opt(r.key.as_ref(), |w, k| w.bytes(k));
        self.bytes(&r.value);
        self.headers(&r.headers);
        self.opt(r.msg_id.as_ref(), |w, m| w.str(m));
    }
    fn stream_spec(&mut self, s: &StreamSpec) {
        self.str(&s.name);
        self.u64(s.max_age_secs);
        self.u64(s.max_bytes);
        self.u64(s.dedup_window_secs);
        self.u64(s.dedup_max_entries);
        self.u8(s.compaction as u8);
    }
    /// Encode one record. Panics if a field exceeds its width (the
    /// broker's write path rejects such records before they are stored).
    pub fn record(&mut self, r: &WireRecord) {
        record_format::encode(
            &mut self.buf,
            &record_format::Fields {
                offset: r.offset,
                timestamp_ns: r.timestamp_ns,
                delivery_count: r.delivery_count,
                subject: &r.subject,
                key: r.key.as_deref(),
                value: &r.value,
                headers: &r.headers,
            },
        )
        .expect("record fits the wire format");
    }
    fn records(&mut self, records: &[WireRecord]) {
        self.u32(records.len() as u32);
        for r in records {
            self.record(r);
        }
    }
}

/// Bounds-checked reader. Never allocates more than the remaining payload
/// can justify, so a hostile length or count can't exhaust memory.
pub struct Reader {
    buf: Bytes,
}

impl Reader {
    pub fn new(buf: Bytes) -> Self {
        Self { buf }
    }

    fn need(&self, n: usize) -> Result<(), ProtocolError> {
        if self.buf.remaining() < n {
            Err(ProtocolError::Decode(format!(
                "truncated payload: need {n} bytes, have {}",
                self.buf.remaining()
            )))
        } else {
            Ok(())
        }
    }

    /// Fail if bytes are left over (catches encoder/decoder drift).
    pub fn finish(&self) -> Result<(), ProtocolError> {
        if self.buf.has_remaining() {
            Err(ProtocolError::Decode(format!(
                "{} trailing bytes",
                self.buf.remaining()
            )))
        } else {
            Ok(())
        }
    }

    pub fn u8(&mut self) -> Result<u8, ProtocolError> {
        self.need(1)?;
        Ok(self.buf.get_u8())
    }
    pub fn u16(&mut self) -> Result<u16, ProtocolError> {
        self.need(2)?;
        Ok(self.buf.get_u16_le())
    }
    pub fn u32(&mut self) -> Result<u32, ProtocolError> {
        self.need(4)?;
        Ok(self.buf.get_u32_le())
    }
    pub fn u64(&mut self) -> Result<u64, ProtocolError> {
        self.need(8)?;
        Ok(self.buf.get_u64_le())
    }
    fn take(&mut self, n: usize) -> Result<Bytes, ProtocolError> {
        self.need(n)?;
        Ok(self.buf.split_to(n))
    }
    fn utf8(b: Bytes) -> Result<String, ProtocolError> {
        String::from_utf8(b.to_vec()).map_err(|_| ProtocolError::Decode("invalid UTF-8".into()))
    }
    pub fn str(&mut self) -> Result<String, ProtocolError> {
        let n = self.u16()? as usize;
        Self::utf8(self.take(n)?)
    }
    pub fn lstr(&mut self) -> Result<String, ProtocolError> {
        Self::utf8(self.bytes()?)
    }
    pub fn bytes(&mut self) -> Result<Bytes, ProtocolError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn opt<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ProtocolError>,
    ) -> Result<Option<T>, ProtocolError> {
        match self.u8()? {
            0 => Ok(None),
            1 => f(self).map(Some),
            v => Err(ProtocolError::Decode(format!("invalid option flag {v}"))),
        }
    }
    /// Read a `u32` element count, rejecting counts the remaining payload
    /// cannot possibly hold (each element is at least `min_size` bytes).
    pub fn count(&mut self, min_size: usize) -> Result<usize, ProtocolError> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_size.max(1)) > self.buf.remaining() {
            return Err(ProtocolError::Decode(format!(
                "count {n} exceeds payload size"
            )));
        }
        Ok(n)
    }
    pub fn headers(&mut self) -> Result<Vec<(String, String)>, ProtocolError> {
        let n = self.u16()? as usize;
        if n * 4 > self.buf.remaining() {
            return Err(ProtocolError::Decode("header count exceeds payload".into()));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push((self.str()?, self.str()?));
        }
        Ok(out)
    }
    fn publish_record(&mut self) -> Result<PublishRecord, ProtocolError> {
        Ok(PublishRecord {
            subject: self.str()?,
            key: self.opt(|r| r.bytes())?,
            value: self.bytes()?,
            headers: self.headers()?,
            msg_id: self.opt(|r| r.str())?,
        })
    }
    fn stream_spec(&mut self) -> Result<StreamSpec, ProtocolError> {
        Ok(StreamSpec {
            name: self.str()?,
            max_age_secs: self.u64()?,
            max_bytes: self.u64()?,
            dedup_window_secs: self.u64()?,
            dedup_max_entries: self.u64()?,
            compaction: self.u8()? != 0,
        })
    }
    /// Decode one record, verifying its length field, structure and CRC.
    pub fn record(&mut self) -> Result<WireRecord, ProtocolError> {
        let bad = |e: record_format::FormatError| ProtocolError::Decode(format!("record: {}", e.0));
        self.need(4)?;
        let size = record_format::record_size(&self.buf[..4]).map_err(bad)?;
        let rec = self.take(size)?;
        record_format::check_crc(&rec).map_err(bad)?;
        let l = record_format::layout(&rec).map_err(bad)?;
        Ok(WireRecord {
            offset: l.offset,
            timestamp_ns: l.timestamp_ns,
            delivery_count: l.delivery_count,
            subject: l.subject(&rec).to_owned(),
            key: l.key.clone().map(|r| rec.slice(r)),
            value: rec.slice(l.value.clone()),
            headers: l.headers(&rec),
        })
    }
    fn records(&mut self) -> Result<Vec<WireRecord>, ProtocolError> {
        let n = self.count(record_format::MIN_RECORD_LEN)?;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.record()?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(i: u64) -> WireRecord {
        WireRecord {
            offset: i,
            timestamp_ns: 1_700_000_000_000_000_000 + i,
            delivery_count: (i % 3) as u16,
            subject: format!("orders.{i}"),
            key: if i.is_multiple_of(2) {
                Some(Bytes::from(format!("k{i}")))
            } else {
                None
            },
            value: Bytes::from(vec![i as u8; i as usize % 7]),
            headers: vec![("h".into(), format!("v{i}"))],
        }
    }

    fn roundtrip_req(req: Request) {
        let frame = req.clone().into_frame(42);
        assert_eq!(frame.correlation_id, 42);
        assert_eq!(Request::from_frame(&frame).unwrap(), req);
    }

    fn roundtrip_resp(resp: Response) {
        let frame = resp.clone().into_frame(7);
        assert_eq!(Response::from_frame(&frame).unwrap(), resp);
    }

    #[test]
    fn every_request_roundtrips() {
        let pr = PublishRecord {
            subject: "a.b".into(),
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"{\"x\":1}"),
            headers: vec![("h1".into(), "v1".into())],
            msg_id: Some("m-1".into()),
        };
        let spec = StreamSpec {
            name: "s".into(),
            max_age_secs: 1,
            max_bytes: 2,
            dedup_window_secs: 3,
            dedup_max_entries: 4,
            compaction: true,
        };
        let mut cs = ConsumerSpec::new("c", "s");
        cs.filter_subjects = vec!["orders.>".into()];
        cs.deliver = DeliverPolicy::FromTime(123);
        cs.backoff_ms = vec![100, 1000];
        cs.dlq_stream = Some("s-dlq".into());
        for req in [
            Request::Connect {
                client_id: "c".into(),
                token: Some("t".into()),
            },
            Request::Connect {
                client_id: "c".into(),
                token: None,
            },
            Request::Ping,
            Request::Metadata,
            Request::Publish {
                stream: "s".into(),
                record: pr.clone(),
            },
            Request::PublishBatch {
                stream: "s".into(),
                records: vec![pr.clone(), PublishRecord::default()],
            },
            Request::CreateStream(spec.clone()),
            Request::UpdateStream(spec),
            Request::DeleteStream { name: "s".into() },
            Request::StreamInfo { name: "s".into() },
            Request::ListStreams,
            Request::Query {
                sql: "SELECT 1".into(),
            },
            Request::CreateConsumer(cs),
            Request::DeleteConsumer { name: "c".into() },
            Request::ConsumerInfo { name: "c".into() },
            Request::ListConsumers { stream: None },
            Request::ListConsumers {
                stream: Some("s".into()),
            },
            Request::SeekConsumer {
                consumer: "c".into(),
                to: SeekTo::Time(9),
            },
            Request::SeekConsumer {
                consumer: "c".into(),
                to: SeekTo::Latest,
            },
            Request::Subscribe {
                consumer: "c".into(),
                credits: 100,
            },
            Request::Credit {
                sub_id: 3,
                credits: 10,
            },
            Request::Unsubscribe { sub_id: 3 },
            Request::Pull {
                consumer: "c".into(),
                max_messages: 10,
                max_bytes: 1024,
                expires_ms: 500,
            },
            Request::Ack {
                consumer: "c".into(),
                offsets: vec![1, 2, 3],
            },
            Request::Nack {
                consumer: "c".into(),
                offset: 4,
                delay_ms: 100,
            },
            Request::Term {
                consumer: "c".into(),
                offset: 5,
                reason: "bad".into(),
            },
            Request::InProgress {
                consumer: "c".into(),
                offsets: vec![6],
            },
            Request::Read {
                stream: "s".into(),
                from: 7,
                max_records: 100,
                max_bytes: 1 << 20,
                wait_ms: 1000,
                filter: "a.*".into(),
            },
        ] {
            roundtrip_req(req);
        }
    }

    #[test]
    fn every_response_roundtrips() {
        for resp in [
            Response::Ok,
            Response::Pong,
            Response::error(404, "nope"),
            Response::error_with(503, "not leader", serde_json::json!({"leader": "h:1"})),
            Response::ConnectOk {
                server_version: "0.6.0".into(),
                node_id: "n1".into(),
                leader: Some("h:5933".into()),
            },
            Response::PublishOk {
                offset: 9,
                duplicate: true,
            },
            Response::PublishBatchOk {
                results: vec![(1, false), (1, true)],
            },
            Response::SubscribeOk { sub_id: 2 },
            Response::Deliver {
                sub_id: 2,
                records: (0..5).map(rec).collect(),
            },
            Response::SubscriptionEnded {
                sub_id: 2,
                code: 404,
                message: "consumer deleted".into(),
            },
            Response::Messages {
                records: vec![rec(1)],
            },
            Response::ReadResult {
                next_offset: 10,
                high_watermark: 12,
                records: (0..3).map(rec).collect(),
            },
            Response::Json(Bytes::from_static(b"{\"a\":1}")),
        ] {
            roundtrip_resp(resp);
        }
    }

    #[test]
    fn encoded_record_frames_match_decoded_responses() {
        let records: Vec<WireRecord> = (0..4).map(rec).collect();
        // Split over several chunks, as the server does.
        let mut enc = EncodedRecords::new();
        enc.push_chunk(EncodedRecords::from(&records[..1]).chunks[0].clone(), 1);
        enc.push_chunk(EncodedRecords::from(&records[1..]).chunks[0].clone(), 3);
        assert_eq!(enc.count(), 4);
        assert_eq!(enc.decode().unwrap(), records);

        let a = Response::deliver_frame(3, enc.clone()).into_frame();
        let b = Response::Deliver {
            sub_id: 3,
            records: records.clone(),
        }
        .into_frame(0);
        assert_eq!(a, b);
        let a = Response::messages_frame(5, enc.clone()).into_frame();
        assert_eq!(
            a,
            Response::Messages {
                records: records.clone()
            }
            .into_frame(5)
        );
        let a = Response::read_result_frame(6, 4, 9, enc).into_frame();
        assert_eq!(
            Response::from_frame(&a).unwrap(),
            Response::ReadResult {
                next_offset: 4,
                high_watermark: 9,
                records,
            }
        );
    }

    #[test]
    fn record_encoding_matches_the_typescript_fixture() {
        // Same bytes as the `Deliver` fixture in
        // sdks/typescript/test/unit/protocol.test.ts.
        let mut w = Writer::default();
        w.record(&rec(2));
        let hex: String = w.finish().iter().map(|b| format!("{b:02x}")).collect();
        let want = "36000000 06760cef 0200 0200000000000000 02002a36fe9c9717 \
                    0800 6f72646572732e32 01 02000000 6b32 02000000 0202 \
                    0100 0100 68 0200 7632";
        assert_eq!(hex, want.replace(' ', ""));
    }

    #[test]
    fn record_crc_and_delivery_count_patch() {
        let mut w = Writer::default();
        w.u32(1);
        w.record(&rec(2));
        let mut payload = BytesMut::from(&w.finish()[..]);
        // Patching delivery_count (bytes 8..10 of the record) keeps the CRC valid.
        let at = 4 + record_format::DELIVERY_COUNT_AT;
        payload[at..at + 2].copy_from_slice(&9u16.to_le_bytes());
        match Response::decode(OpCode::Messages, payload.clone().freeze()).unwrap() {
            Response::Messages { records } => assert_eq!(records[0].delivery_count, 9),
            other => panic!("{other:?}"),
        }
        // Any other bit flip is caught.
        let last = payload.len() - 1;
        payload[last] ^= 1;
        let err = Response::decode(OpCode::Messages, payload.freeze()).unwrap_err();
        assert!(err.to_string().contains("CRC"), "{err}");
    }

    #[test]
    fn wire_record_encoded_len_is_exact() {
        for r in [
            WireRecord::default(),
            rec(7),
            WireRecord {
                key: Some(Bytes::from_static(b"key")),
                headers: vec![("a".into(), "bb".into()), ("ccc".into(), String::new())],
                ..rec(3)
            },
        ] {
            let mut w = Writer::default();
            w.record(&r);
            assert_eq!(w.finish().len(), r.encoded_len().unwrap(), "{r:?}");
        }
    }

    #[test]
    fn hostile_counts_are_rejected_without_allocating() {
        let mut w = Writer::default();
        w.str("s");
        w.u32(u32::MAX); // claims 4 billion records
        let err = Request::decode(OpCode::PublishBatch, w.finish()).unwrap_err();
        assert!(err.to_string().contains("exceeds payload"), "{err}");

        let mut w = Writer::default();
        w.u64(0);
        w.u64(0);
        w.u32(u32::MAX);
        assert!(Response::decode(OpCode::ReadResult, w.finish()).is_err());
    }

    #[test]
    fn truncated_and_trailing_payloads_fail() {
        let frame = Request::Ping.into_frame(1);
        let mut payload = BytesMut::from(&frame.payload[..]);
        payload.put_u8(9);
        assert!(Request::decode(OpCode::Ping, payload.freeze()).is_err());

        let frame = Request::Subscribe {
            consumer: "c".into(),
            credits: 1,
        }
        .into_frame(1);
        let short = frame.payload.slice(..frame.payload.len() - 1);
        assert!(Request::decode(OpCode::Subscribe, short).is_err());
    }

    #[test]
    fn consumer_spec_json_defaults() {
        let spec: ConsumerSpec = serde_json::from_str(r#"{"name":"c","stream":"s"}"#).unwrap();
        assert_eq!(spec, ConsumerSpec::new("c", "s"));
        let spec: ConsumerSpec = serde_json::from_str(
            r#"{"name":"c","stream":"s","deliver":{"from_offset":5},"ack":"none"}"#,
        )
        .unwrap();
        assert_eq!(spec.deliver, DeliverPolicy::FromOffset(5));
        assert_eq!(spec.ack, AckPolicy::None);
    }
}
