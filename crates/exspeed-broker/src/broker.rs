use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, oneshot};

use crate::broker_append::BrokerAppend;
use crate::consumer_state::{ConsumerGroup, ConsumerState, DeliveryRecord};
use crate::delivery::{run_delivery, DeliveryConfig};
use crate::handlers;
use crate::lease::LeaderLease;
use crate::replication::ReplicationCoordinator;
use exspeed_common::Metrics;
use exspeed_protocol::messages::{ClientMessage, ServerMessage};
use exspeed_streams::StorageEngine;

/// Default delivery mpsc buffer capacity. Callers that want to accept the
/// default pass this constant; callers with an explicit user-facing setting
/// (e.g. the `--delivery-buffer` CLI flag) pass their value directly.
pub const DEFAULT_DELIVERY_BUFFER: usize = 8192;

pub struct Broker {
    pub storage: Arc<dyn StorageEngine>,
    pub broker_append: Arc<BrokerAppend>,
    pub consumers: RwLock<HashMap<String, ConsumerState>>,
    pub(crate) groups: RwLock<HashMap<String, ConsumerGroup>>,
    pub data_dir: PathBuf,
    pub consumer_store: Arc<dyn crate::consumer_store::ConsumerStore>,
    pub work_coordinator: Arc<dyn crate::work_coordinator::WorkCoordinator>,
    pub lease: Arc<dyn LeaderLease>,
    pub(crate) max_delivery_attempts: u16,
    pub(crate) nack_attempts: RwLock<HashMap<(String, u64), u16>>,
    pub metrics: Arc<Metrics>,
    /// mpsc buffer capacity for per-subscription delivery channels.
    pub(crate) delivery_buffer: usize,
    /// Set to `true` once all startup dedup rebuild tasks complete.
    pub dedup_ready: Arc<AtomicBool>,
    /// Leader-side replication coordinator. `None` on single-pod / file-
    /// backed deployments; `Some(_)` when multi-pod mode is configured.
    pub(crate) replication_coordinator: Option<Arc<ReplicationCoordinator>>,
}

impl Broker {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker_append: Arc<BrokerAppend>,
        data_dir: PathBuf,
        consumer_store: Arc<dyn crate::consumer_store::ConsumerStore>,
        work_coordinator: Arc<dyn crate::work_coordinator::WorkCoordinator>,
        lease: Arc<dyn LeaderLease>,
        metrics: Arc<Metrics>,
        delivery_buffer: usize,
    ) -> Self {
        Self {
            storage,
            broker_append,
            consumers: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            data_dir,
            consumer_store,
            work_coordinator,
            lease,
            max_delivery_attempts: 5,
            nack_attempts: RwLock::new(HashMap::new()),
            metrics,
            delivery_buffer,
            dedup_ready: Arc::new(AtomicBool::new(false)),
            replication_coordinator: None,
        }
    }

    /// Attach a `ReplicationCoordinator` so Broker mutation paths fan
    /// out events to connected followers. Called by the multi-pod server
    /// startup (Wave 5). Consumes and returns `self` for builder ergonomics.
    pub fn with_replication_coordinator(
        mut self,
        coordinator: Arc<ReplicationCoordinator>,
    ) -> Self {
        self.replication_coordinator = Some(coordinator);
        self
    }

    /// Access the attached replication coordinator, if any. Used by the
    /// leader-side replication server to register connecting followers.
    pub fn replication_coordinator(&self) -> Option<&Arc<ReplicationCoordinator>> {
        self.replication_coordinator.as_ref()
    }

    /// Emit a replication event to any attached coordinator. The event is
    /// constructed lazily — the `FnOnce` is only invoked when a coordinator
    /// is present, so single-pod deployments pay no event-construction cost.
    pub(crate) fn emit_replication_event(
        &self,
        make_event: impl FnOnce() -> crate::replication::ReplicationEvent,
    ) {
        if let Some(coord) = &self.replication_coordinator {
            coord.emit(make_event());
        }
    }

    /// Returns `true` once all startup dedup rebuild tasks have completed.
    pub fn is_dedup_ready(&self) -> bool {
        self.dedup_ready.load(Ordering::Acquire)
    }

    /// Delete a stream and emit a `StreamDeleted` replication event. Thin
    /// wrapper around the storage trait that exists so multi-pod leaders
    /// can fan the deletion out to followers. Returns `Err` if storage
    /// says the stream does not exist (or any other I/O error).
    pub async fn delete_stream(
        &self,
        stream: &exspeed_common::StreamName,
    ) -> Result<(), exspeed_streams::StorageError> {
        self.storage.delete_stream(stream).await?;
        self.emit_replication_event(|| {
            use crate::replication::ReplicationEvent;
            use exspeed_protocol::messages::replicate::StreamDeletedEvent;
            ReplicationEvent::StreamDeleted(StreamDeletedEvent {
                name: stream.as_str().to_string(),
            })
        });
        Ok(())
    }

    /// Load all persisted consumers from the configured ConsumerStore and
    /// rebuild in-memory state.
    pub async fn load_consumers(&self) -> Result<(), String> {
        let configs = self
            .consumer_store
            .load_all()
            .await
            .map_err(|e| format!("failed to load consumers: {e}"))?;
        let mut consumers = self.consumers.write().unwrap();
        let mut groups = self.groups.write().unwrap();

        for config in configs {
            let group_name = config.group.clone();
            let consumer_name = config.name.clone();

            consumers.insert(
                consumer_name.clone(),
                ConsumerState {
                    config,
                    subscribers: std::collections::HashMap::new(),
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

    /// Subscribe a specific `subscriber_id` to a consumer. Starts a delivery task
    /// for this subscriber and returns the channel receiver plus a cancel sender
    /// (dropping the cancel sender stops the delivery task).
    ///
    /// - Non-grouped consumer: only one subscriber allowed. Second call returns
    ///   "consumer already subscribed".
    /// - Grouped consumer: multiple subscribers allowed, keyed by `subscriber_id`.
    ///   A duplicate `subscriber_id` returns "subscriber_id already active".
    pub fn subscribe(
        &self,
        consumer_name: &str,
        subscriber_id: &str,
    ) -> Result<(mpsc::Receiver<DeliveryRecord>, oneshot::Sender<()>), String> {
        if subscriber_id.is_empty() {
            return Err("subscriber_id is required".to_string());
        }

        let mut consumers = self.consumers.write().unwrap();
        let consumer = consumers
            .get_mut(consumer_name)
            .ok_or_else(|| format!("consumer '{}' not found", consumer_name))?;

        let is_grouped = !consumer.config.group.is_empty();

        if !is_grouped && !consumer.subscribers.is_empty() {
            return Err(format!("consumer '{}' is already subscribed", consumer_name));
        }

        if consumer.subscribers.contains_key(subscriber_id) {
            return Err(format!(
                "subscriber_id '{}' already active for consumer '{}'",
                subscriber_id, consumer_name
            ));
        }

        // Create mpsc channel for delivery records.
        let (tx, rx) = mpsc::channel::<DeliveryRecord>(self.delivery_buffer);
        // Cancel channel kept inside the broker (dropped on unsubscribe).
        let (cancel_tx_store, cancel_rx_store) = oneshot::channel::<()>();
        // Cancel channel returned to the caller (dropped on disconnect).
        let (caller_cancel_tx, caller_cancel_rx) = oneshot::channel::<()>();

        // Build DeliveryConfig.
        let delivery_config = DeliveryConfig {
            consumer_name: consumer.config.name.clone(),
            stream_name: consumer.config.stream.clone(),
            subject_filter: consumer.config.subject_filter.clone(),
            start_offset: consumer.config.offset,
            group_name: if is_grouped {
                Some(consumer.config.group.clone())
            } else {
                None
            },
            subscriber_id: subscriber_id.to_string(),
            work_coordinator: Arc::clone(&self.work_coordinator),
            metrics: Arc::clone(&self.metrics),
        };

        let storage = Arc::clone(&self.storage);

        // Store the subscriber state in the consumer's subscribers map.
        consumer.subscribers.insert(
            subscriber_id.to_string(),
            crate::consumer_state::SubscriberState {
                delivery_tx: tx.clone(),
                cancel_tx: cancel_tx_store,
            },
        );

        // Spawn delivery task. Completes when either cancel channel fires, or when
        // run_delivery itself returns (e.g., client dropped the rx).
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel_rx_store => {}
                _ = caller_cancel_rx => {}
                _ = run_delivery(delivery_config, storage, tx) => {}
            }
        });

        Ok((rx, caller_cancel_tx))
    }

    /// Unsubscribe a specific subscriber from a consumer.
    /// No-op if the subscriber_id is not active (idempotent).
    pub fn unsubscribe(&self, consumer_name: &str, subscriber_id: &str) -> Result<(), String> {
        let mut consumers = self.consumers.write().unwrap();
        let consumer = consumers
            .get_mut(consumer_name)
            .ok_or_else(|| format!("consumer '{}' not found", consumer_name))?;

        // Removing drops the SubscriberState, which drops cancel_tx, which
        // signals the delivery task via the tokio::select! to exit.
        consumer.subscribers.remove(subscriber_id);
        Ok(())
    }

    pub async fn handle_message(&self, msg: ClientMessage) -> ServerMessage {
        match msg {
            ClientMessage::CreateStream(req) => handlers::handle_create_stream(self, req).await,
            ClientMessage::Publish(req) => handlers::handle_publish(self, req).await,
            ClientMessage::Fetch(req) => handlers::handle_fetch(self, req).await,
            ClientMessage::CreateConsumer(req) => handlers::handle_create_consumer(self, req).await,
            ClientMessage::DeleteConsumer(req) => handlers::handle_delete_consumer(self, req).await,
            ClientMessage::Ack(req) => handlers::handle_ack(self, req).await,
            ClientMessage::Nack(req) => handlers::handle_nack(self, req).await,
            ClientMessage::Seek(req) => handlers::handle_seek(self, req).await,
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
    use crate::broker_append::BrokerAppend;
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
        let broker_append = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let consumer_store = Arc::new(crate::consumer_store::file::FileConsumerStore::new(
            dir.path().to_path_buf(),
        ));
        let work_coordinator = Arc::new(crate::work_coordinator::noop::NoopWorkCoordinator);
        let lease = Arc::new(crate::lease::NoopLeaderLease::new());
        let metrics = Arc::new(exspeed_common::Metrics::new().0);
        let broker = Broker::new(
            storage,
            broker_append,
            dir.path().to_path_buf(),
            consumer_store,
            work_coordinator,
            lease,
            metrics,
            DEFAULT_DELIVERY_BUFFER,
        );
        (broker, dir)
    }

    async fn create_stream(broker: &Broker, name: &str) -> ServerMessage {
        broker
            .handle_message(ClientMessage::CreateStream(CreateStreamRequest {
                stream_name: name.into(),
                max_age_secs: 0,
                max_bytes: 0,
            }))
            .await
    }

    async fn publish(
        broker: &Broker,
        stream: &str,
        subject: &str,
        value: &[u8],
    ) -> ServerMessage {
        broker
            .handle_message(ClientMessage::Publish(PublishRequest {
                stream: stream.into(),
                subject: subject.into(),
                key: None,
                msg_id: None,
                value: Bytes::copy_from_slice(value),
                headers: vec![],
            }))
            .await
    }

    async fn publish_with_key_headers(
        broker: &Broker,
        stream: &str,
        subject: &str,
        key: Option<Bytes>,
        value: &[u8],
        headers: Vec<(String, String)>,
    ) -> ServerMessage {
        broker
            .handle_message(ClientMessage::Publish(PublishRequest {
                stream: stream.into(),
                subject: subject.into(),
                key,
                msg_id: None,
                value: Bytes::copy_from_slice(value),
                headers,
            }))
            .await
    }

    async fn fetch(
        broker: &Broker,
        stream: &str,
        offset: u64,
        max: u32,
        filter: &str,
    ) -> ServerMessage {
        broker
            .handle_message(ClientMessage::Fetch(FetchRequest {
                stream: stream.into(),
                offset,
                max_records: max,
                subject_filter: filter.into(),
            }))
            .await
    }

    fn unwrap_batch(msg: ServerMessage) -> RecordsBatch {
        match msg {
            ServerMessage::RecordsBatch(b) => b,
            other => panic!("expected RecordsBatch, got {:?}", other),
        }
    }

    fn unwrap_publish_offset(msg: ServerMessage) -> u64 {
        match msg {
            ServerMessage::PublishOk { offset, .. } => offset,
            other => panic!("expected PublishOk, got {:?}", other),
        }
    }

    // --- Tests ---

    #[tokio::test]
    async fn create_stream_ok() {
        let (broker, _dir) = make_broker();
        let resp = create_stream(&broker, "events").await;
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn create_duplicate_stream_error() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;
        let resp = create_stream(&broker, "events").await;
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error on duplicate create, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn publish_returns_offset() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;
        let offset =
            unwrap_publish_offset(publish(&broker, "events", "events.created", b"data").await);
        assert_eq!(offset, 0);
    }

    #[tokio::test]
    async fn publish_sequential_offsets() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "orders").await;
        let o0 =
            unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"a").await);
        let o1 =
            unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"b").await);
        let o2 =
            unwrap_publish_offset(publish(&broker, "orders", "orders.created", b"c").await);
        assert_eq!(o0, 0);
        assert_eq!(o1, 1);
        assert_eq!(o2, 2);
    }

    #[tokio::test]
    async fn publish_to_nonexistent_stream_error() {
        let (broker, _dir) = make_broker();
        let resp = publish(&broker, "ghost", "ghost.event", b"data").await;
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error publishing to nonexistent stream, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn fetch_after_publish() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "logs").await;
        publish(&broker, "logs", "logs.info", b"msg1").await;
        publish(&broker, "logs", "logs.warn", b"msg2").await;
        publish(&broker, "logs", "logs.error", b"msg3").await;

        let batch = unwrap_batch(fetch(&broker, "logs", 0, 10, "").await);
        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.records[0].subject, "logs.info");
        assert_eq!(batch.records[0].value, Bytes::from_static(b"msg1"));
        assert_eq!(batch.records[1].subject, "logs.warn");
        assert_eq!(batch.records[1].value, Bytes::from_static(b"msg2"));
        assert_eq!(batch.records[2].subject, "logs.error");
        assert_eq!(batch.records[2].value, Bytes::from_static(b"msg3"));
    }

    #[tokio::test]
    async fn fetch_with_subject_filter() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "orders").await;
        publish(&broker, "orders", "order.eu.created", b"1").await;
        publish(&broker, "orders", "order.us.created", b"2").await;
        publish(&broker, "orders", "order.eu.shipped", b"3").await;
        publish(&broker, "orders", "payment.captured", b"4").await;

        // "order.eu.*" should match order.eu.created and order.eu.shipped → 2 records
        let batch_eu = unwrap_batch(fetch(&broker, "orders", 0, 100, "order.eu.*").await);
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
        let batch_order = unwrap_batch(fetch(&broker, "orders", 0, 100, "order.>").await);
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

    #[tokio::test]
    async fn fetch_empty_stream() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "empty").await;
        let batch = unwrap_batch(fetch(&broker, "empty", 0, 10, "").await);
        assert!(batch.records.is_empty(), "expected empty batch");
    }

    #[tokio::test]
    async fn fetch_nonexistent_stream_error() {
        let (broker, _dir) = make_broker();
        let resp = fetch(&broker, "ghost", 0, 10, "").await;
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error fetching from nonexistent stream, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn publish_with_key_and_headers() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;

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
        )
        .await;

        let batch = unwrap_batch(fetch(&broker, "events", 0, 10, "").await);
        assert_eq!(batch.records.len(), 1);
        let record = &batch.records[0];
        assert_eq!(record.key, Some(key), "key should be preserved");
        assert_eq!(record.headers, headers, "headers should be preserved");
        assert_eq!(record.value, Bytes::from_static(b"{\"id\":42}"));
    }

    // --- Consumer CRUD tests ---

    #[tokio::test]
    async fn create_consumer_ok() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;

        let resp = broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "my-consumer".into(),
                stream: "events".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );

        // Verify consumer is in state
        let consumers = broker.consumers.read().unwrap();
        assert!(consumers.contains_key("my-consumer"));
    }

    #[tokio::test]
    async fn create_consumer_nonexistent_stream() {
        let (broker, _dir) = make_broker();

        let resp = broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "orphan".into(),
                stream: "ghost".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error for non-existent stream, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn create_duplicate_consumer() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;

        let req = CreateConsumerRequest {
            name: "dup".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        };
        let resp1 = broker
            .handle_message(ClientMessage::CreateConsumer(req.clone()))
            .await;
        assert!(matches!(resp1, ServerMessage::Ok));

        let resp2 = broker
            .handle_message(ClientMessage::CreateConsumer(req))
            .await;
        assert!(
            matches!(resp2, ServerMessage::Error { .. }),
            "expected Error on duplicate consumer, got {:?}",
            resp2
        );
    }

    #[tokio::test]
    async fn delete_consumer_ok() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;

        broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "doomed".into(),
                stream: "events".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;

        let resp = broker
            .handle_message(ClientMessage::DeleteConsumer(DeleteConsumerRequest {
                name: "doomed".into(),
            }))
            .await;
        assert!(
            matches!(resp, ServerMessage::Ok),
            "expected Ok, got {:?}",
            resp
        );

        let consumers = broker.consumers.read().unwrap();
        assert!(!consumers.contains_key("doomed"));
    }

    #[tokio::test]
    async fn delete_nonexistent_consumer() {
        let (broker, _dir) = make_broker();

        let resp = broker
            .handle_message(ClientMessage::DeleteConsumer(DeleteConsumerRequest {
                name: "ghost".into(),
            }))
            .await;
        assert!(
            matches!(resp, ServerMessage::Error { .. }),
            "expected Error deleting non-existent consumer, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn ack_advances_offset() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;

        // Publish some records
        for i in 0..5 {
            publish(
                &broker,
                "events",
                "events.tick",
                format!("msg-{i}").as_bytes(),
            )
            .await;
        }

        // Create consumer
        broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "acker".into(),
                stream: "events".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;

        // ACK offset 3
        let resp = broker
            .handle_message(ClientMessage::Ack(AckRequest {
                consumer_name: "acker".into(),
                offset: 3,
            }))
            .await;
        assert!(matches!(resp, ServerMessage::Ok));

        // Verify offset advanced
        let consumers = broker.consumers.read().unwrap();
        let state = consumers.get("acker").unwrap();
        assert_eq!(state.config.offset, 3);
    }

    #[tokio::test]
    async fn nack_increments_attempts() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;
        publish(&broker, "events", "events.fail", b"bad-record").await;

        broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "nacker".into(),
                stream: "events".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;

        // NACK once — should not create DLQ yet
        let resp = broker
            .handle_message(ClientMessage::Nack(NackRequest {
                consumer_name: "nacker".into(),
                offset: 0,
            }))
            .await;
        assert!(matches!(resp, ServerMessage::Ok));

        // Verify attempts incremented
        let nack_attempts = broker.nack_attempts.read().unwrap();
        assert_eq!(*nack_attempts.get(&("nacker".into(), 0u64)).unwrap(), 1);

        // DLQ stream should NOT exist yet (only 1 attempt < max_delivery_attempts=5)
        let dlq_resp = fetch(&broker, "events-dlq", 0, 10, "").await;
        assert!(
            matches!(dlq_resp, ServerMessage::Error { .. }),
            "DLQ should not exist yet"
        );
    }

    #[tokio::test]
    async fn nack_max_attempts_sends_to_dlq() {
        let (broker, _dir) = make_broker();
        create_stream(&broker, "events").await;
        publish(&broker, "events", "events.fail", b"poison-pill").await;

        broker
            .handle_message(ClientMessage::CreateConsumer(CreateConsumerRequest {
                name: "nacker".into(),
                stream: "events".into(),
                group: String::new(),
                subject_filter: String::new(),
                start_from: StartFrom::Earliest,
                start_offset: 0,
            }))
            .await;

        // NACK 5 times (max_delivery_attempts = 5)
        for _ in 0..5 {
            let resp = broker
                .handle_message(ClientMessage::Nack(NackRequest {
                    consumer_name: "nacker".into(),
                    offset: 0,
                }))
                .await;
            assert!(matches!(resp, ServerMessage::Ok));
        }

        // DLQ stream should now exist and contain the record
        let dlq_batch = unwrap_batch(fetch(&broker, "events-dlq", 0, 10, "").await);
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
