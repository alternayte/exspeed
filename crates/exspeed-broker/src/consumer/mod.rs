//! Durable consumers (JetStream-style).
//!
//! A consumer is a named cursor over one stream plus delivery state. Clients
//! receive its records by **push** (`subscribe` with credit-based flow
//! control) or **pull** (`pull` with a batch size and long-poll timeout);
//! both draw from the same state. Several subscribers on one consumer form a
//! work queue: each record goes to exactly one of them. Records must be
//! acked (unless `AckPolicy::None`); unacked records are redelivered after
//! `ack_wait`, nacked records after their backoff, and after `max_deliver`
//! attempts (or a `term`) records go to the consumer's DLQ stream.
//!
//! Consumer state lives in the internal `__consumers` stream (see
//! [`store`]), so it replicates with the log and survives restarts and
//! failover. Consumers only run on the leader: [`ConsumerManager::start`]
//! loads and starts them under the leadership token.

mod actor;
pub mod core;
pub mod store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use exspeed_common::{validate_resource_name, Metrics, StreamName, SubjectFilters};
use exspeed_protocol::client::{ConsumerSpec, DeliverPolicy, EncodedRecords, SeekTo};
use exspeed_streams::StorageError;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

use self::actor::{Actor, Cmd};
use self::core::{ConsumerStats, Core};
use self::store::ConsumerStore;
use crate::log::Log;

/// Event sent to a push subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubEvent {
    /// Records in wire encoding, delivery counts already set.
    Deliver(EncodedRecords),
    /// No more deliveries for this subscription.
    Ended { code: u16, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConsumerError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("not the leader")]
    NotLeader,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("{message}")]
    Ended { code: u16, message: String },
}

impl ConsumerError {
    /// HTTP-like status code for protocol responses.
    pub fn code(&self) -> u16 {
        use exspeed_protocol::client::code;
        match self {
            ConsumerError::NotFound(_) => code::NOT_FOUND,
            ConsumerError::Conflict(_) => code::CONFLICT,
            ConsumerError::Invalid(_) => code::BAD_REQUEST,
            ConsumerError::NotLeader => code::UNAVAILABLE,
            ConsumerError::Storage(_) => code::INTERNAL,
            ConsumerError::Ended { code, .. } => *code,
        }
    }
}

/// Consumer status, as returned by info/list.
#[derive(Debug, Clone, Serialize)]
pub struct ConsumerInfo {
    pub spec: ConsumerSpec,
    /// Next stream offset to be delivered for the first time.
    pub next_offset: u64,
    /// Everything below this offset is acked (or filtered out).
    pub ack_floor: u64,
    pub num_unacked: u64,
    pub num_in_flight: u64,
    /// Records not yet delivered (approximate: includes filtered-out ones).
    pub num_waiting: u64,
    /// `high_watermark - ack_floor`.
    pub lag: u64,
    pub subscribers: u64,
    pub pull_waiters: u64,
    pub stats: ConsumerStats,
}

struct Handle {
    spec: ConsumerSpec,
    tx: mpsc::Sender<Cmd>,
}

/// A push subscription. Dropping the receiver detaches it on the next
/// delivery attempt; call [`ConsumerManager::unsubscribe`] to detach now.
pub struct Subscription {
    pub sub_id: u32,
    pub consumer: String,
    pub events: mpsc::UnboundedReceiver<SubEvent>,
}

pub struct ConsumerManager {
    log: Arc<Log>,
    store: Arc<ConsumerStore>,
    metrics: Arc<Metrics>,
    consumers: RwLock<HashMap<String, Handle>>,
    /// `Some` while this node is the leader and consumers are running.
    token: RwLock<Option<CancellationToken>>,
    next_sub_id: AtomicU32,
    /// Every actor task, so shutdown can wait for final persists.
    tasks: tokio_util::task::TaskTracker,
}

impl ConsumerManager {
    pub fn new(log: Arc<Log>, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            store: Arc::new(ConsumerStore::new(log.clone())),
            log,
            metrics,
            consumers: RwLock::new(HashMap::new()),
            token: RwLock::new(None),
            next_sub_id: AtomicU32::new(1),
            tasks: tokio_util::task::TaskTracker::new(),
        })
    }

    /// Wait (up to `timeout`) for every consumer actor to exit after its
    /// token was cancelled; each persists its final state on the way out.
    pub async fn wait_stopped(&self, timeout: std::time::Duration) -> bool {
        self.tasks.close();
        let done = tokio::time::timeout(timeout, self.tasks.wait())
            .await
            .is_ok();
        self.tasks.reopen();
        done
    }

    /// Load every persisted consumer and start it under `token` (cancelled
    /// on demotion/shutdown). Call once per leadership tenure.
    pub async fn start(self: &Arc<Self>, token: CancellationToken) -> Result<usize, ConsumerError> {
        let snapshots = self
            .store
            .load_all()
            .await
            .map_err(|e| ConsumerError::Storage(e.to_string()))?;
        let mut map = self.consumers.write().await;
        map.clear();
        let now = Instant::now();
        let mut started = 0;
        for (name, snap) in snapshots {
            if snap.spec.ephemeral {
                // Its connection died with the previous leader.
                let _ = self.store.delete(&name).await;
                continue;
            }
            let core = Core::restore(snap, now);
            match self.spawn(core, token.child_token()) {
                Ok(h) => {
                    map.insert(name, h);
                    started += 1;
                }
                Err(e) => tracing::error!(consumer = %name, error = %e, "cannot start consumer"),
            }
        }
        *self.token.write().await = Some(token.clone());
        drop(map);

        // Forget everything when leadership ends.
        let this = self.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            this.consumers.write().await.clear();
            *this.token.write().await = None;
        });
        Ok(started)
    }

    fn spawn(&self, core: Core, token: CancellationToken) -> Result<Handle, ConsumerError> {
        let spec = core.spec.clone();
        let actor = Actor::new(
            core,
            self.log.clone(),
            self.store.clone(),
            self.metrics.clone(),
        )?;
        let (tx, rx) = mpsc::channel(4096);
        self.tasks.spawn(actor.run(rx, token));
        Ok(Handle { spec, tx })
    }

    async fn running_token(&self) -> Result<CancellationToken, ConsumerError> {
        self.token
            .read()
            .await
            .clone()
            .ok_or(ConsumerError::NotLeader)
    }

    /// Create a consumer. Creating one that already exists with the same
    /// spec succeeds (idempotent); a different spec is a conflict.
    pub async fn create(&self, spec: ConsumerSpec) -> Result<ConsumerInfo, ConsumerError> {
        let token = self.running_token().await?;
        validate_resource_name(&spec.name, "consumer name")
            .map_err(|e| ConsumerError::Invalid(e.to_string()))?;
        let stream = StreamName::try_from(spec.stream.as_str())
            .map_err(|e| ConsumerError::Invalid(e.to_string()))?;
        SubjectFilters::parse(&spec.filter_subjects).map_err(ConsumerError::Invalid)?;
        if let Some(dlq) = &spec.dlq_stream {
            let dlq = StreamName::try_from(dlq.as_str())
                .map_err(|e| ConsumerError::Invalid(format!("dlq_stream: {e}")))?;
            if dlq == stream {
                return Err(ConsumerError::Invalid(
                    "dlq_stream must differ from the consumer's stream".into(),
                ));
            }
        }

        {
            let map = self.consumers.read().await;
            if let Some(h) = map.get(&spec.name) {
                return if h.spec == spec {
                    drop(map);
                    self.info(&spec.name).await
                } else {
                    Err(ConsumerError::Conflict(format!(
                        "consumer '{}' exists with a different config",
                        spec.name
                    )))
                };
            }
        }

        let storage = self.log.storage();
        let (earliest, hwm) = storage.stream_bounds(&stream).await.map_err(|e| match e {
            StorageError::StreamNotFound(_) => {
                ConsumerError::NotFound(format!("stream '{}'", spec.stream))
            }
            other => ConsumerError::Storage(other.to_string()),
        })?;
        let start = match spec.deliver {
            DeliverPolicy::All => earliest.0,
            DeliverPolicy::New => hwm.0,
            DeliverPolicy::FromOffset(o) => o.max(earliest.0),
            DeliverPolicy::FromTime(ms) => storage
                .seek_by_time(&stream, ms.saturating_mul(1_000_000))
                .await
                .map_err(|e| ConsumerError::Storage(e.to_string()))?
                .0
                .max(earliest.0),
        };
        let core = Core::new(spec.clone(), start);
        self.store
            .save(&core.snapshot())
            .await
            .map_err(|e| ConsumerError::Storage(e.to_string()))?;

        let mut map = self.consumers.write().await;
        if map.contains_key(&spec.name) {
            return Err(ConsumerError::Conflict(format!(
                "consumer '{}' was created concurrently",
                spec.name
            )));
        }
        let handle = self.spawn(core, token.child_token())?;
        map.insert(spec.name.clone(), handle);
        drop(map);
        self.info(&spec.name).await
    }

    async fn tx(&self, name: &str) -> Result<mpsc::Sender<Cmd>, ConsumerError> {
        if self.token.read().await.is_none() {
            return Err(ConsumerError::NotLeader);
        }
        self.consumers
            .read()
            .await
            .get(name)
            .map(|h| h.tx.clone())
            .ok_or_else(|| ConsumerError::NotFound(format!("consumer '{name}'")))
    }

    async fn send(&self, name: &str, cmd: Cmd) -> Result<(), ConsumerError> {
        self.tx(name)
            .await?
            .send(cmd)
            .await
            .map_err(|_| ConsumerError::NotFound(format!("consumer '{name}' stopped")))
    }

    pub async fn delete(&self, name: &str) -> Result<(), ConsumerError> {
        let tx = self.tx(name).await?;
        let (reply, rx) = oneshot::channel();
        let _ = tx.send(Cmd::Delete { reply }).await;
        let r = rx
            .await
            .unwrap_or_else(|_| Err(ConsumerError::Storage("consumer task ended".into())));
        self.consumers.write().await.remove(name);
        r
    }

    pub async fn info(&self, name: &str) -> Result<ConsumerInfo, ConsumerError> {
        let (reply, rx) = oneshot::channel();
        self.send(name, Cmd::Info { reply }).await?;
        rx.await
            .map_err(|_| ConsumerError::NotFound(format!("consumer '{name}' stopped")))
    }

    /// All consumers, optionally only those on `stream`.
    pub async fn list(&self, stream: Option<&str>) -> Result<Vec<ConsumerInfo>, ConsumerError> {
        if self.token.read().await.is_none() {
            return Err(ConsumerError::NotLeader);
        }
        let names: Vec<String> = self
            .consumers
            .read()
            .await
            .iter()
            .filter(|(_, h)| stream.is_none_or(|s| h.spec.stream == s))
            .map(|(n, _)| n.clone())
            .collect();
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            if let Ok(i) = self.info(&n).await {
                out.push(i);
            }
        }
        out.sort_by(|a, b| a.spec.name.cmp(&b.spec.name));
        Ok(out)
    }

    /// The stream a consumer reads (for authorization). `None` if unknown.
    pub async fn stream_of(&self, name: &str) -> Option<String> {
        self.consumers
            .read()
            .await
            .get(name)
            .map(|h| h.spec.stream.clone())
    }

    /// Spec of a consumer, if it exists.
    pub async fn spec(&self, name: &str) -> Option<ConsumerSpec> {
        self.consumers
            .read()
            .await
            .get(name)
            .map(|h| h.spec.clone())
    }

    /// Names of consumers reading `stream`.
    pub async fn consumers_of(&self, stream: &str) -> Vec<String> {
        self.consumers
            .read()
            .await
            .iter()
            .filter(|(_, h)| h.spec.stream == stream)
            .map(|(n, _)| n.clone())
            .collect()
    }

    pub async fn seek(&self, name: &str, to: SeekTo) -> Result<(), ConsumerError> {
        let (reply, rx) = oneshot::channel();
        self.send(name, Cmd::Seek { to, reply }).await?;
        rx.await
            .unwrap_or_else(|_| Err(ConsumerError::NotFound(format!("consumer '{name}'"))))
    }

    /// Start push delivery with `credits` initial credit.
    pub async fn subscribe(&self, name: &str, credits: u32) -> Result<Subscription, ConsumerError> {
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (tx, events) = mpsc::unbounded_channel();
        let (ready, attached) = oneshot::channel();
        self.send(
            name,
            Cmd::Attach {
                sub_id,
                credits,
                tx,
                ready,
            },
        )
        .await?;
        attached
            .await
            .map_err(|_| ConsumerError::NotFound(format!("consumer '{name}' stopped")))?;
        Ok(Subscription {
            sub_id,
            consumer: name.to_string(),
            events,
        })
    }

    pub async fn credit(&self, name: &str, sub_id: u32, credits: u32) -> Result<(), ConsumerError> {
        self.send(name, Cmd::Credit { sub_id, credits }).await
    }

    pub async fn unsubscribe(&self, name: &str, sub_id: u32) {
        let _ = self.send(name, Cmd::Detach { sub_id }).await;
    }

    /// Fetch up to `max_messages`, waiting up to `expires` for at least one.
    pub async fn pull(
        &self,
        name: &str,
        max_messages: u32,
        max_bytes: u32,
        expires: Duration,
    ) -> Result<EncodedRecords, ConsumerError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            name,
            Cmd::Pull {
                max_messages,
                max_bytes,
                expires,
                reply,
            },
        )
        .await?;
        rx.await
            .unwrap_or_else(|_| Err(ConsumerError::NotFound(format!("consumer '{name}'"))))
    }

    pub async fn ack(&self, name: &str, offsets: Vec<u64>) -> Result<(), ConsumerError> {
        self.send(name, Cmd::Ack { offsets }).await
    }

    pub async fn nack(&self, name: &str, offset: u64, delay_ms: u32) -> Result<(), ConsumerError> {
        self.send(name, Cmd::Nack { offset, delay_ms }).await
    }

    pub async fn term(&self, name: &str, offset: u64, reason: String) -> Result<(), ConsumerError> {
        self.send(name, Cmd::Term { offset, reason }).await
    }

    pub async fn in_progress(&self, name: &str, offsets: Vec<u64>) -> Result<(), ConsumerError> {
        self.send(name, Cmd::InProgress { offsets }).await
    }
}

#[cfg(test)]
mod tests;
