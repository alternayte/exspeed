pub mod ack;
pub mod connect;
pub mod consumer;
pub mod fetch;
pub mod ping;
pub mod publish;
pub mod record_delivery;
pub mod records_batch;
pub mod seek;
pub mod stream_mgmt;

pub use ack::{AckRequest, NackRequest};
pub use connect::{AuthType, ConnectRequest};
pub use consumer::{
    CreateConsumerRequest, DeleteConsumerRequest, StartFrom, SubscribeRequest, UnsubscribeRequest,
};
pub use fetch::FetchRequest;
pub use ping::{Ping, Pong};
pub use publish::PublishRequest;
pub use record_delivery::RecordDelivery;
pub use records_batch::{BatchRecord, RecordsBatch};
pub use seek::SeekRequest;
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
    CreateConsumer(CreateConsumerRequest),
    DeleteConsumer(DeleteConsumerRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    Ack(AckRequest),
    Nack(NackRequest),
    Seek(SeekRequest),
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
            OpCode::CreateConsumer => Ok(Self::CreateConsumer(CreateConsumerRequest::decode(
                frame.payload,
            )?)),
            OpCode::DeleteConsumer => Ok(Self::DeleteConsumer(DeleteConsumerRequest::decode(
                frame.payload,
            )?)),
            OpCode::Subscribe => Ok(Self::Subscribe(SubscribeRequest::decode(frame.payload)?)),
            OpCode::Unsubscribe => Ok(Self::Unsubscribe(UnsubscribeRequest::decode(
                frame.payload,
            )?)),
            OpCode::Ack => Ok(Self::Ack(AckRequest::decode(frame.payload)?)),
            OpCode::Nack => Ok(Self::Nack(NackRequest::decode(frame.payload)?)),
            OpCode::Seek => Ok(Self::Seek(SeekRequest::decode(frame.payload)?)),
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
    Record(RecordDelivery),
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
            ServerMessage::Record(delivery) => {
                let mut payload = BytesMut::new();
                delivery.encode(&mut payload);
                Frame::new(OpCode::Record, 0, payload.freeze()) // correlation_id=0 for push
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
            max_age_secs: 0,
            max_bytes: 0,
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

    // --- Phase 2b: consumer message frame tests ---

    #[test]
    fn create_consumer_frame_to_client_message() {
        let req = CreateConsumerRequest {
            name: "my-consumer".into(),
            stream: "orders".into(),
            group: "workers".into(),
            subject_filter: "orders.created".into(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::CreateConsumer, 100, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::CreateConsumer(r) => {
                assert_eq!(r.name, "my-consumer");
                assert_eq!(r.stream, "orders");
                assert_eq!(r.group, "workers");
                assert_eq!(r.subject_filter, "orders.created");
                assert_eq!(r.start_from, StartFrom::Earliest);
                assert_eq!(r.start_offset, 0);
            }
            other => panic!("expected CreateConsumer, got {:?}", other),
        }
    }

    #[test]
    fn delete_consumer_frame_to_client_message() {
        let req = DeleteConsumerRequest {
            name: "old-consumer".into(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::DeleteConsumer, 101, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::DeleteConsumer(r) => assert_eq!(r.name, "old-consumer"),
            other => panic!("expected DeleteConsumer, got {:?}", other),
        }
    }

    #[test]
    fn subscribe_frame_to_client_message() {
        let req = SubscribeRequest {
            consumer_name: "my-consumer".into(),
            subscriber_id: String::new(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Subscribe, 102, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Subscribe(r) => assert_eq!(r.consumer_name, "my-consumer"),
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn unsubscribe_frame_to_client_message() {
        let req = UnsubscribeRequest {
            consumer_name: "my-consumer".into(),
            subscriber_id: String::new(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Unsubscribe, 103, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Unsubscribe(r) => assert_eq!(r.consumer_name, "my-consumer"),
            other => panic!("expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn ack_frame_to_client_message() {
        let req = AckRequest {
            consumer_name: "acker".into(),
            offset: 555,
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Ack, 104, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Ack(r) => {
                assert_eq!(r.consumer_name, "acker");
                assert_eq!(r.offset, 555);
            }
            other => panic!("expected Ack, got {:?}", other),
        }
    }

    #[test]
    fn nack_frame_to_client_message() {
        let req = NackRequest {
            consumer_name: "nacker".into(),
            offset: 777,
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Nack, 105, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        match msg {
            ClientMessage::Nack(r) => {
                assert_eq!(r.consumer_name, "nacker");
                assert_eq!(r.offset, 777);
            }
            other => panic!("expected Nack, got {:?}", other),
        }
    }

    #[test]
    fn record_delivery_to_frame() {
        let delivery = RecordDelivery {
            consumer_name: "my-consumer".into(),
            offset: 42,
            timestamp: 1_700_000_000,
            subject: "orders.created".into(),
            delivery_attempt: 1,
            key: Some(Bytes::from_static(b"key-1")),
            value: Bytes::from_static(b"payload-data"),
            headers: vec![("trace".into(), "t1".into())],
        };
        let frame = ServerMessage::Record(delivery).into_frame(0);
        assert_eq!(frame.opcode, OpCode::Record);
        assert_eq!(frame.correlation_id, 0);

        let decoded = RecordDelivery::decode(frame.payload).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp, 1_700_000_000);
        assert_eq!(decoded.subject, "orders.created");
        assert_eq!(decoded.delivery_attempt, 1);
        assert_eq!(decoded.key, Some(Bytes::from_static(b"key-1")));
        assert_eq!(decoded.value, Bytes::from_static(b"payload-data"));
        assert_eq!(decoded.headers, vec![("trace".into(), "t1".into())]);
    }
}
