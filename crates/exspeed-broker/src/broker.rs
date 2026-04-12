use std::sync::Arc;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_streams::StorageEngine;
use crate::handlers;

pub struct Broker {
    storage: Arc<dyn StorageEngine>,
}

impl Broker {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
    }

    pub fn handle_message(&self, msg: ClientMessage) -> ServerMessage {
        match msg {
            ClientMessage::CreateStream(req) => handlers::handle_create_stream(&self.storage, req),
            ClientMessage::Publish(req) => handlers::handle_publish(&self.storage, req),
            ClientMessage::Fetch(req) => handlers::handle_fetch(&self.storage, req),
            ClientMessage::Connect(_) | ClientMessage::Ping => {
                ServerMessage::Error {
                    code: 500,
                    message: "message should be handled by connection layer".into(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_protocol::messages::{
        CreateStreamRequest, FetchRequest, PublishRequest, RecordsBatch, ServerMessage,
    };
    use exspeed_storage::memory::MemoryStorage;

    // --- Helpers ---

    fn make_broker() -> Broker {
        let storage = Arc::new(MemoryStorage::new());
        Broker::new(storage)
    }

    fn create_stream(broker: &Broker, name: &str) -> ServerMessage {
        broker.handle_message(ClientMessage::CreateStream(CreateStreamRequest {
            stream_name: name.into(),
        }))
    }

    fn publish(
        broker: &Broker,
        stream: &str,
        subject: &str,
        value: &[u8],
    ) -> ServerMessage {
        broker.handle_message(ClientMessage::Publish(PublishRequest {
            stream: stream.into(),
            subject: subject.into(),
            key: None,
            value: Bytes::copy_from_slice(value),
            headers: vec![],
        }))
    }

    fn publish_with_key_headers(
        broker: &Broker,
        stream: &str,
        subject: &str,
        key: Option<Bytes>,
        value: &[u8],
        headers: Vec<(String, String)>,
    ) -> ServerMessage {
        broker.handle_message(ClientMessage::Publish(PublishRequest {
            stream: stream.into(),
            subject: subject.into(),
            key,
            value: Bytes::copy_from_slice(value),
            headers,
        }))
    }

    fn fetch(broker: &Broker, stream: &str, offset: u64, max: u32, filter: &str) -> ServerMessage {
        broker.handle_message(ClientMessage::Fetch(FetchRequest {
            stream: stream.into(),
            offset,
            max_records: max,
            subject_filter: filter.into(),
        }))
    }

    fn unwrap_batch(msg: ServerMessage) -> RecordsBatch {
        match msg {
            ServerMessage::RecordsBatch(b) => b,
            other => panic!("expected RecordsBatch, got {:?}", other),
        }
    }

    fn unwrap_publish_offset(msg: ServerMessage) -> u64 {
        match msg {
            ServerMessage::PublishOk { offset } => offset,
            other => panic!("expected PublishOk, got {:?}", other),
        }
    }

    // --- Tests ---

    #[test]
    fn create_stream_ok() {
        let broker = make_broker();
        let resp = create_stream(&broker, "events");
        assert!(matches!(resp, ServerMessage::Ok), "expected Ok, got {:?}", resp);
    }

    #[test]
    fn create_duplicate_stream_error() {
        let broker = make_broker();
        create_stream(&broker, "events");
        let resp = create_stream(&broker, "events");
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error on duplicate create, got {:?}",
            resp
        );
    }

    #[test]
    fn publish_returns_offset() {
        let broker = make_broker();
        create_stream(&broker, "events");
        let offset = unwrap_publish_offset(publish(&broker, "events", "events.created", b"data"));
        assert_eq!(offset, 0);
    }

    #[test]
    fn publish_sequential_offsets() {
        let broker = make_broker();
        create_stream(&broker, "orders");
        let o0 = unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"a"));
        let o1 = unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"b"));
        let o2 = unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"c"));
        assert_eq!(o0, 0);
        assert_eq!(o1, 1);
        assert_eq!(o2, 2);
    }

    #[test]
    fn publish_to_nonexistent_stream_error() {
        let broker = make_broker();
        let resp = publish(&broker, "ghost", "ghost.event", b"data");
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error publishing to nonexistent stream, got {:?}",
            resp
        );
    }

    #[test]
    fn fetch_after_publish() {
        let broker = make_broker();
        create_stream(&broker, "logs");
        publish(&broker, "logs", "logs.info", b"msg1");
        publish(&broker, "logs", "logs.warn", b"msg2");
        publish(&broker, "logs", "logs.error", b"msg3");

        let batch = unwrap_batch(fetch(&broker, "logs", 0, 10, ""));
        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.records[0].subject, "logs.info");
        assert_eq!(batch.records[0].value, Bytes::from_static(b"msg1"));
        assert_eq!(batch.records[1].subject, "logs.warn");
        assert_eq!(batch.records[1].value, Bytes::from_static(b"msg2"));
        assert_eq!(batch.records[2].subject, "logs.error");
        assert_eq!(batch.records[2].value, Bytes::from_static(b"msg3"));
    }

    #[test]
    fn fetch_with_subject_filter() {
        let broker = make_broker();
        create_stream(&broker, "orders");
        publish(&broker, "orders", "order.eu.created", b"1");
        publish(&broker, "orders", "order.us.created", b"2");
        publish(&broker, "orders", "order.eu.shipped", b"3");
        publish(&broker, "orders", "payment.captured", b"4");

        // "order.eu.*" should match order.eu.created and order.eu.shipped → 2 records
        let batch_eu = unwrap_batch(fetch(&broker, "orders", 0, 100, "order.eu.*"));
        assert_eq!(
            batch_eu.records.len(),
            2,
            "expected 2 records for order.eu.*, got {:?}",
            batch_eu.records.iter().map(|r| &r.subject).collect::<Vec<_>>()
        );

        // "order.>" should match all three order.* subjects → 3 records
        let batch_order = unwrap_batch(fetch(&broker, "orders", 0, 100, "order.>"));
        assert_eq!(
            batch_order.records.len(),
            3,
            "expected 3 records for order.>, got {:?}",
            batch_order.records.iter().map(|r| &r.subject).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fetch_empty_stream() {
        let broker = make_broker();
        create_stream(&broker, "empty");
        let batch = unwrap_batch(fetch(&broker, "empty", 0, 10, ""));
        assert!(batch.records.is_empty(), "expected empty batch");
    }

    #[test]
    fn fetch_nonexistent_stream_error() {
        let broker = make_broker();
        let resp = fetch(&broker, "ghost", 0, 10, "");
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error fetching from nonexistent stream, got {:?}",
            resp
        );
    }

    #[test]
    fn publish_with_key_and_headers() {
        let broker = make_broker();
        create_stream(&broker, "events");

        let key = Bytes::from_static(b"user-42");
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("trace-id".to_string(), "xyz-999".to_string()),
        ];

        publish_with_key_headers(
            &broker,
            "events",
            "user.created",
            Some(key.clone()),
            b"{\"id\":42}",
            headers.clone(),
        );

        let batch = unwrap_batch(fetch(&broker, "events", 0, 10, ""));
        assert_eq!(batch.records.len(), 1);
        let record = &batch.records[0];
        assert_eq!(record.key, Some(key), "key should be preserved");
        assert_eq!(record.headers, headers, "headers should be preserved");
        assert_eq!(record.value, Bytes::from_static(b"{\"id\":42}"));
    }
}
