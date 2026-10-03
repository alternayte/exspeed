//! Top-level broker state shared by the TCP session layer and the HTTP API.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use exspeed_common::Metrics;
use exspeed_streams::StorageEngine;

use crate::broker_append::BrokerAppend;
use crate::consumer::ConsumerManager;
use crate::lease::LeaderLease;
use crate::log::Log;

pub struct Broker {
    pub storage: Arc<dyn StorageEngine>,
    pub broker_append: Arc<BrokerAppend>,
    /// The single write path. All appends and stream-metadata changes go
    /// through it.
    pub log: Arc<Log>,
    /// Durable consumers (push and pull delivery, acks, redelivery, DLQ).
    pub consumers: Arc<ConsumerManager>,
    pub data_dir: PathBuf,
    pub lease: Arc<dyn LeaderLease>,
    pub metrics: Arc<Metrics>,
    /// Set to `true` once all startup dedup rebuild tasks complete.
    pub dedup_ready: Arc<AtomicBool>,
}

impl Broker {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker_append: Arc<BrokerAppend>,
        data_dir: PathBuf,
        lease: Arc<dyn LeaderLease>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let dedup_ready = Arc::new(AtomicBool::new(false));
        let log = Arc::new(Log::new(
            storage.clone(),
            broker_append.clone(),
            metrics.clone(),
            dedup_ready.clone(),
        ));
        let consumers = ConsumerManager::new(log.clone(), metrics.clone());
        Self {
            storage,
            broker_append,
            log,
            consumers,
            data_dir,
            lease,
            metrics,
            dedup_ready,
        }
    }

    /// Returns `true` once all startup dedup rebuild tasks have completed.
    pub fn is_dedup_ready(&self) -> bool {
        self.dedup_ready.load(Ordering::Acquire)
    }

    /// Delete a stream (through the write path, so it replicates and drops
    /// the stream's dedup state).
    pub async fn delete_stream(
        &self,
        stream: &exspeed_common::StreamName,
    ) -> Result<(), crate::log::LogError> {
        self.log.delete_stream(stream).await
    }
}
