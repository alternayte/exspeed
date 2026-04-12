pub mod connect;
pub mod fetch;
pub mod ping;
pub mod publish;
pub mod records_batch;
pub mod stream_mgmt;

pub use connect::{AuthType, ConnectRequest};
pub use fetch::FetchRequest;
pub use ping::{Ping, Pong};
pub use publish::PublishRequest;
pub use records_batch::{BatchRecord, RecordsBatch};
pub use stream_mgmt::CreateStreamRequest;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::frame::Frame;
use crate::opcodes::OpCode;

/// A decoded client -> server message.
#[derive(Debug)]
pub enum ClientMessage {
    Connect(ConnectRequest),
    Ping,
    CreateStream(CreateStreamRequest),
    Publish(PublishRequest),
    Fetch(FetchRequest),
}

impl ClientMessage {
    pub fn from_frame(frame: Frame) -> Result<Self, ProtocolError> {
        match frame.opcode {
            OpCode::Connect => {
                let req = ConnectRequest::decode(frame.payload)?;
                Ok(ClientMessage::Connect(req))
            }
            OpCode::Ping => Ok(ClientMessage::Ping),
            OpCode::CreateStream => {
                let req = CreateStreamRequest::decode(frame.payload)?;
                Ok(ClientMessage::CreateStream(req))
            }
            OpCode::Publish => {
                let req = PublishRequest::decode(frame.payload)?;
                Ok(ClientMessage::Publish(req))
            }
            OpCode::Fetch => {
                let req = FetchRequest::decode(frame.payload)?;
                Ok(ClientMessage::Fetch(req))
            }
            other => Err(ProtocolError::Decode(format!(
                "unhandled client opcode: {:?}",
                other
            ))),
        }
    }
}

/// A server -> client response.
#[derive(Debug)]
pub enum ServerMessage {
    Ok,
    Error { code: u16, message: String },
    Pong,
    PublishOk { offset: u64 },
    RecordsBatch(RecordsBatch),
}

impl ServerMessage {
    pub fn into_frame(self, correlation_id: u32) -> Frame {
        match self {
            ServerMessage::Ok => Frame::empty(OpCode::Ok, correlation_id),
            ServerMessage::Pong => Frame::empty(OpCode::Pong, correlation_id),
            ServerMessage::Error { code, message } => {
                let mut payload = Vec::with_capacity(2 + message.len());
                payload.extend_from_slice(&code.to_le_bytes());
                payload.extend_from_slice(message.as_bytes());
                Frame::new(OpCode::Error, correlation_id, Bytes::from(payload))
            }
            ServerMessage::PublishOk { offset } => {
                let mut payload = BytesMut::with_capacity(8);
                payload.put_u64_le(offset);
                Frame::new(OpCode::Ok, correlation_id, payload.freeze())
            }
            ServerMessage::RecordsBatch(batch) => {
                let mut payload = BytesMut::new();
                batch.encode(&mut payload);
                Frame::new(OpCode::RecordsBatch, correlation_id, payload.freeze())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn ping_frame_to_client_message() {
        let frame = Frame::empty(OpCode::Ping, 5);
        let msg = ClientMessage::from_frame(frame).unwrap();
        assert!(matches!(msg, ClientMessage::Ping));
    }

    #[test]
    fn connect_frame_to_client_message() {
        let req = ConnectRequest {
            client_id: "test".into(),
            auth_type: AuthType::None,
            auth_payload: Bytes::new(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Connect, 1, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        assert!(matches!(msg, ClientMessage::Connect(_)));
    }

    #[test]
    fn server_ok_to_frame() {
        let frame = ServerMessage::Ok.into_frame(42);
        assert_eq!(frame.opcode, OpCode::Ok);
        assert_eq!(frame.correlation_id, 42);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn server_error_to_frame() {
        let frame = ServerMessage::Error {
            code: 404,
            message: "stream not found".into(),
        }
        .into_frame(1);
        assert_eq!(frame.opcode, OpCode::Error);
        assert!(frame.payload.len() > 2);
    }

    #[test]
    fn pong_to_frame() {
        let frame = ServerMessage::Pong.into_frame(99);
        assert_eq!(frame.opcode, OpCode::Pong);
        assert_eq!(frame.correlation_id, 99);
    }

    // --- New tests for Phase 2a message types ---

    #[test]
    fn create_stream_frame_to_client_message() {
        let req = CreateStreamRequest {
            stream_name: "test-stream".into(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::CreateStream, 10, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::CreateStream(r) => assert_eq!(r.stream_name, "test-stream"),
            other => panic!("expected CreateStream, got {:?}", other),
        }
    }

    #[test]
    fn publish_frame_to_client_message() {
        let req = PublishRequest {
            stream: "events".into(),
            subject: "events.click".into(),
            key: None,
            value: Bytes::from_static(b"data"),
            headers: vec![],
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Publish, 20, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Publish(r) => {
                assert_eq!(r.stream, "events");
                assert_eq!(r.subject, "events.click");
                assert!(r.key.is_none());
                assert_eq!(r.value, Bytes::from_static(b"data"));
            }
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn fetch_frame_to_client_message() {
        let req = FetchRequest {
            stream: "logs".into(),
            offset: 42,
            max_records: 100,
            subject_filter: "logs.error".into(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Fetch, 30, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Fetch(r) => {
                assert_eq!(r.stream, "logs");
                assert_eq!(r.offset, 42);
                assert_eq!(r.max_records, 100);
                assert_eq!(r.subject_filter, "logs.error");
            }
            other => panic!("expected Fetch, got {:?}", other),
        }
    }

    #[test]
    fn publish_ok_to_frame() {
        let frame = ServerMessage::PublishOk { offset: 999 }.into_frame(50);
        assert_eq!(frame.opcode, OpCode::Ok);
        assert_eq!(frame.correlation_id, 50);
        assert_eq!(frame.payload.len(), 8);
        let mut payload = frame.payload;
        let offset = payload.get_u64_le();
        assert_eq!(offset, 999);
    }

    #[test]
    fn records_batch_to_frame() {
        let batch = RecordsBatch {
            records: vec![BatchRecord {
                offset: 0,
                timestamp: 12345,
                subject: "test.sub".into(),
                key: None,
                value: Bytes::from_static(b"val"),
                headers: vec![],
            }],
        };
        let frame = ServerMessage::RecordsBatch(batch).into_frame(60);
        assert_eq!(frame.opcode, OpCode::RecordsBatch);
        assert_eq!(frame.correlation_id, 60);

        // Decode the batch back from the frame payload
        let decoded = RecordsBatch::decode(frame.payload).unwrap();
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].offset, 0);
        assert_eq!(decoded.records[0].subject, "test.sub");
    }
}
