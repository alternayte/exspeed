use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, oneshot, watch};

use crate::consumer_state::{ConsumerGroup, ConsumerState, DeliveryRecord};
use crate::delivery::{run_delivery, DeliveryConfig};
use crate::handlers;
use crate::persistence;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_streams::StorageEngine;

pub struct Broker {
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) consumers: RwLock<HashMap<String, ConsumerState>>,
    pub(crate) groups: RwLock<HashMap<String, ConsumerGroup>>,
    pub(crate) data_dir: PathBuf,
    pub(crate) max_delivery_attempts: u16,
    pub(crate) nack_attempts: RwLock<HashMap<(String, u64), u16>>,
}

impl Broker {
    pub fn new(storage: Arc<dyn StorageEngine>, data_dir: PathBuf) -> Self {
        Self {
            storage,
            consumers: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            data_dir,
            max_delivery_attempts: 5,
            nack_attempts: RwLock::new(HashMap::new()),
        }
    }

    /// Load all persisted consumers from disk and rebuild in-memory state.
    pub fn load_consumers(&self) -> io::Result<()> {
        let configs = persistence::load_all_consumers(&self.data_dir)?;
        let mut consumers = self.consumers.write().unwrap();
        let mut groups = self.groups.write().unwrap();

        for config in configs {
            let group_name = config.group.clone();
            let consumer_name = config.name.clone();

            consumers.insert(
                consumer_name.clone(),
                ConsumerState {
                    config,
                    delivery_tx: None,
                },
            );

            if !group_name.is_empty() {
                groups
                    .entry(group_name.clone())
                    .or_insert_with(|| ConsumerGroup::new(&group_name))
                    .add_member(&consumer_name);
            }
        }

        Ok(())
    }

    /// Subscribe a consumer: start the delivery task, return the channel receiver.
    /// Called by the connection handler (not by handle_message — needs async context).
    pub fn subscribe(
        &self,
        consumer_name: &str,
    ) -> Result<(mpsc::Receiver<DeliveryRecord>, oneshot::Sender<()>), String> {
        // 1. Look up the consumer.
        let mut consumers = self.consumers.write().unwrap();
        let consumer = consumers
            .get_mut(consumer_name)
            .ok_or_else(|| format!("consumer '{}' not found", consumer_name))?;

        // 2. Must not already be subscribed.
        if consumer.delivery_tx.is_some() {
            return Err(format!(
                "consumer '{}' is already subscribed",
                consumer_name
            ));
        }

        // 3. Create mpsc channel for delivery records.
        let (tx, rx) = mpsc::channel::<DeliveryRecord>(1000);

        // 4. Create oneshot for cancellation.
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        // 5. Store tx clone in consumer state.
        consumer.delivery_tx = Some(tx.clone());

        // 6. Build DeliveryConfig from consumer config.
        let delivery_config = DeliveryConfig {
            consumer_name: consumer.config.name.clone(),
            stream_name: consumer.config.stream.clone(),
            subject_filter: consumer.config.subject_filter.clone(),
            start_offset: consumer.config.offset,
            group_name: if consumer.config.group.is_empty() {
                None
            } else {
                Some(consumer.config.group.clone())
            },
        };

        // 7. Build group_members watch channel if consumer is in a group.
        let group_members = if let Some(ref group_name) = delivery_config.group_name {
            let groups = self.groups.read().unwrap();
            let members = groups
                .get(group_name)
                .map(|g| g.members.clone())
                .unwrap_or_default();
            let (watch_tx, watch_rx) = watch::channel(members);
            // Leak the sender so the receiver stays valid.
            // Proper rebalance would store it for later broadcasts.
            std::mem::forget(watch_tx);
            Some(watch_rx)
        } else {
            None
        };

        let storage = Arc::clone(&self.storage);

        // 8. Spawn the delivery task.
        tokio::spawn(async move {
            run_delivery(delivery_config, storage, tx, cancel_rx, group_members).await;
        });

        // 9. Return the receiver and cancel sender.
        Ok((rx, cancel_tx))
    }

    /// Unsubscribe a consumer: stop the delivery task.
    pub fn unsubscribe(&self, consumer_name: &str) -> Result<(), String> {
        let mut consumers = self.consumers.write().unwrap();
        let consumer = consumers
            .get_mut(consumer_name)
            .ok_or_else(|| format!("consumer '{}' not found", consumer_name))?;

        // Dropping the sender signals the delivery task to stop.
        consumer.delivery_tx = None;
        Ok(())
    }

    pub fn handle_message(&self, msg: ClientMessage) -> ServerMessage {
        match msg {
            ClientMessage::CreateStream(req) => handlers::handle_create_stream(self, req),
            ClientMessage::Publish(req) => handlers::handle_publish(self, req),
            ClientMessage::Fetch(req) => handlers::handle_fetch(self, req),
            ClientMessage::CreateConsumer(req) => handlers::handle_create_consumer(self, req),
            ClientMessage::DeleteConsumer(req) => handlers::handle_delete_consumer(self, req),
            ClientMessage::Ack(req) => handlers::handle_ack(self, req),
            ClientMessage::Nack(req) => handlers::handle_nack(self, req),
            ClientMessage::Seek(req) => handlers::handle_seek(self, req),
            ClientMessage::Connect(_) | ClientMessage::Ping => ServerMessage::Error {
                code: 500,
                message: "message should be handled by connection layer".into(),
            },
            ClientMessage::Subscribe(_) | ClientMessage::Unsubscribe(_) => ServerMessage::Error {
                code: 500,
                message: "subscribe/unsubscribe handled by connection layer".into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_protocol::messages::{
        AckRequest, CreateConsumerRequest, CreateStreamRequest, DeleteConsumerRequest,
        FetchRequest, NackRequest, PublishRequest, RecordsBatch, ServerMessage, StartFrom,
    };
    use exspeed_storage::memory::MemoryStorage;
    use tempfile::TempDir;

    // --- Helpers ---

    fn make_broker() -> (Broker, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(MemoryStorage::new());
        let broker = Broker::new(storage, dir.path().to_path_buf());
        (broker, dir)
    }

    fn create_stream(broker: &Broker, name: &str) -> ServerMessage {
        broker.handle_message(ClientMessage::CreateStream(CreateStreamRequest {
            stream_name: name.into(),
        }))
    }

    fn publish(broker: &Broker, stream: &str, subject: &str, value: &[u8]) -> ServerMessage {
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
        let (broker, _dir) = make_broker();
        let resp = create_stream(&broker, "events");
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );
    }

    #[test]
    fn create_duplicate_stream_error() {
        let (broker, _dir) = make_broker();
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
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");
        let offset = unwrap_publish_offset(publish(&broker, "events", "events.created", b"data"));
        assert_eq!(offset, 0);
    }

    #[test]
    fn publish_sequential_offsets() {
        let (broker, _dir) = make_broker();
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
        let (broker, _dir) = make_broker();
        let resp = publish(&broker, "ghost", "ghost.event", b"data");
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error publishing to nonexistent stream, got {:?}",
            resp
        );
    }

    #[test]
    fn fetch_after_publish() {
        let (broker, _dir) = make_broker();
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
        let (broker, _dir) = make_broker();
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
            batch_eu
                .records
                .iter()
                .map(|r| &r.subject)
                .collect::<Vec<_>>()
        );

        // "order.>" should match all three order.* subjects → 3 records
        let batch_order = unwrap_batch(fetch(&broker, "orders", 0, 100, "order.>"));
        assert_eq!(
            batch_order.records.len(),
            3,
            "expected 3 records for order.>, got {:?}",
            batch_order
                .records
                .iter()
                .map(|r| &r.subject)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn fetch_empty_stream() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "empty");
        let batch = unwrap_batch(fetch(&broker, "empty", 0, 10, ""));
        assert!(batch.records.is_empty(), "expected empty batch");
    }

    #[test]
    fn fetch_nonexistent_stream_error() {
        let (broker, _dir) = make_broker();
        let resp = fetch(&broker, "ghost", 0, 10, "");
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error fetching from nonexistent stream, got {:?}",
            resp
        );
    }

    #[test]
    fn publish_with_key_and_headers() {
        let (broker, _dir) = make_broker();
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

    // --- Consumer CRUD tests ---

    #[test]
    fn create_consumer_ok() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");

        let resp = broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "my-consumer".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );

        // Verify consumer is in state
        let consumers = broker.consumers.read().unwrap();
        assert!(consumers.contains_key("my-consumer"));
    }

    #[test]
    fn create_consumer_nonexistent_stream() {
        let (broker, _dir) = make_broker();

        let resp = broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "orphan".into(),
            stream: "ghost".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error for non-existent stream, got {:?}",
            resp
        );
    }

    #[test]
    fn create_duplicate_consumer() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");

        let req = CreateConsumerRequest {
            name: "dup".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        };
        let resp1 = broker.handle_message(ClientMessage::CreateConsumer(req.clone()));
        assert!(matches!(resp1, ServerMessage::Ok));

        let resp2 = broker.handle_message(ClientMessage::CreateConsumer(req));
        assert!(
            matches!(resp2, ServerMessage::Error { .. }),
            "expected Error on duplicate consumer, got {:?}",
            resp2
        );
    }

    #[test]
    fn delete_consumer_ok() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");

        broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "doomed".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));

        let resp = broker.handle_message(ClientMessage::DeleteConsumer(DeleteConsumerRequest {
            name: "doomed".into(),
        }));
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );

        let consumers = broker.consumers.read().unwrap();
        assert!(!consumers.contains_key("doomed"));
    }

    #[test]
    fn delete_nonexistent_consumer() {
        let (broker, _dir) = make_broker();

        let resp = broker.handle_message(ClientMessage::DeleteConsumer(DeleteConsumerRequest {
            name: "ghost".into(),
        }));
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error deleting non-existent consumer, got {:?}",
            resp
        );
    }

    #[test]
    fn ack_advances_offset() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");

        // Publish some records
        for i in 0..5 {
            publish(
                &broker,
                "events",
                "events.tick",
                format!("msg-{i}").as_bytes(),
            );
        }

        // Create consumer
        broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "acker".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));

        // ACK offset 3
        let resp = broker.handle_message(ClientMessage::Ack(AckRequest {
            consumer_name: "acker".into(),
            offset: 3,
        }));
        assert!(matches!(resp, ServerMessage::Ok));

        // Verify offset advanced
        let consumers = broker.consumers.read().unwrap();
        let state = consumers.get("acker").unwrap();
        assert_eq!(state.config.offset, 3);
    }

    #[test]
    fn nack_increments_attempts() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");
        publish(&broker, "events", "events.fail", b"bad-record");

        broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "nacker".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));

        // NACK once — should not create DLQ yet
        let resp = broker.handle_message(ClientMessage::Nack(NackRequest {
            consumer_name: "nacker".into(),
            offset: 0,
        }));
        assert!(matches!(resp, ServerMessage::Ok));

        // Verify attempts incremented
        let nack_attempts = broker.nack_attempts.read().unwrap();
        assert_eq!(*nack_attempts.get(&("nacker".into(), 0u64)).unwrap(), 1);

        // DLQ stream should NOT exist yet (only 1 attempt < max_delivery_attempts=5)
        let dlq_resp = fetch(&broker, "events-dlq", 0, 10, "");
        assert!(
            matches!(dlq_resp, ServerMessage::Error { .. }),
            "DLQ should not exist yet"
        );
    }

    #[test]
    fn nack_max_attempts_sends_to_dlq() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events");
        publish(&broker, "events", "events.fail", b"poison-pill");

        broker.handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
            name: "nacker".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        }));

        // NACK 5 times (max_delivery_attempts = 5)
        for _ in 0..5 {
            let resp = broker.handle_message(ClientMessage::Nack(NackRequest {
                consumer_name: "nacker".into(),
                offset: 0,
            }));
            assert!(matches!(resp, ServerMessage::Ok));
        }

        // DLQ stream should now exist and contain the record
        let dlq_batch = unwrap_batch(fetch(&broker, "events-dlq", 0, 10, ""));
        assert_eq!(dlq_batch.records.len(), 1, "DLQ should have 1 record");
        assert_eq!(
            dlq_batch.records[0].value,
            Bytes::from_static(b"poison-pill")
        );

        // DLQ record should have diagnostic headers
        let headers = &dlq_batch.records[0].headers;
        let header_map: HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(header_map.get("x-exspeed-original-stream"), Some(&"events"));
        assert_eq!(header_map.get("x-exspeed-original-offset"), Some(&"0"));
        assert_eq!(header_map.get("x-exspeed-consumer"), Some(&"nacker"));
        assert_eq!(header_map.get("x-exspeed-failure-count"), Some(&"5"));

        // Consumer offset should have advanced past the NACKed record
        let consumers = broker.consumers.read().unwrap();
        let state = consumers.get("nacker").unwrap();
        assert_eq!(
            state.config.offset, 1,
            "consumer offset should advance past DLQ'd record"
        );

        // nack_attempts entry should be cleaned up
        let nack_attempts = broker.nack_attempts.read().unwrap();
        assert!(
            !nack_attempts.contains_key(&("nacker".into(), 0u64)),
            "nack_attempts should be cleaned up after DLQ"
        );
    }
}
