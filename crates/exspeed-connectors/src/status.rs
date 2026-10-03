//! Live status of one connector, shared by its supervisor (writer), the
//! manager and the HTTP API (readers), and mirrored into Prometheus.

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use exspeed_common::Metrics;
use opentelemetry::KeyValue;
use serde::Serialize;

use crate::traits::Lag;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Starting,
    Running,
    Backoff,
    Failed,
    Stopped,
}

impl Status {
    pub const ALL: [Status; 5] = [
        Status::Starting,
        Status::Running,
        Status::Backoff,
        Status::Failed,
        Status::Stopped,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Starting => "starting",
            Status::Running => "running",
            Status::Backoff => "backoff",
            Status::Failed => "failed",
            Status::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
struct Inner {
    status: Status,
    last_error: Option<String>,
    restart_count: u64,
    lag: Option<Lag>,
    last_success: Option<SystemTime>,
    since: Instant,
    checkpoint: Option<String>,
    records: u64,
    /// Sink: the first offset of a write that exhausted its in-place retries,
    /// and how many times in a row that happened (across restarts).
    stuck: Option<(u64, u32)>,
}

/// Snapshot of a connector's status.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub status: Status,
    pub last_error: Option<String>,
    pub restart_count: u64,
    pub lag: Option<u64>,
    pub lag_unit: Option<&'static str>,
    /// Unix milliseconds of the last successful batch.
    pub last_success_ms: Option<u64>,
    /// Seconds in the current status.
    pub status_secs: u64,
    /// Last persisted position (source checkpoint or sink offset).
    pub checkpoint: Option<String>,
    /// Records moved since the server started.
    pub records: u64,
}

pub struct ConnectorState {
    name: String,
    metrics: Arc<Metrics>,
    inner: Mutex<Inner>,
}

impl ConnectorState {
    pub fn new(name: &str, metrics: Arc<Metrics>) -> Arc<Self> {
        let s = Arc::new(Self {
            name: name.to_string(),
            metrics,
            inner: Mutex::new(Inner {
                status: Status::Stopped,
                last_error: None,
                restart_count: 0,
                lag: None,
                last_success: None,
                since: Instant::now(),
                checkpoint: None,
                records: 0,
                stuck: None,
            }),
        });
        s.publish_status(Status::Stopped);
        s
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn label(&self) -> KeyValue {
        KeyValue::new("connector", self.name.clone())
    }

    fn publish_status(&self, current: Status) {
        for s in Status::ALL {
            self.metrics.connector_state.record(
                i64::from(s == current),
                &[self.label(), KeyValue::new("state", s.as_str())],
            );
        }
    }

    pub fn status(&self) -> Status {
        self.inner.lock().unwrap().status
    }

    pub fn set_status(&self, status: Status) {
        {
            let mut g = self.inner.lock().unwrap();
            if g.status == status {
                return;
            }
            g.status = status;
            g.since = Instant::now();
            if status == Status::Running {
                // Keep last_error visible while starting/backing off; clear
                // it once the connector runs again.
                g.last_error = None;
            }
        }
        self.publish_status(status);
    }

    /// Enter `failed` (or `backoff`) with an error message.
    pub fn set_error(&self, status: Status, error: impl Into<String>) {
        {
            let mut g = self.inner.lock().unwrap();
            g.last_error = Some(error.into());
            if g.status != status {
                g.status = status;
                g.since = Instant::now();
            }
        }
        self.publish_status(status);
    }

    /// Record an error without changing status.
    pub fn note_error(&self, error: impl Into<String>) {
        self.inner.lock().unwrap().last_error = Some(error.into());
    }

    /// Record a transient error that is being retried in place.
    pub fn note_retry(&self, error: impl Into<String>) {
        self.note_error(error);
        self.metrics
            .connector_retry_attempts_total
            .add(1, &[self.label(), KeyValue::new("outcome", "retried")]);
    }

    /// Sink: a write starting at `offset` exhausted its retries. Returns how
    /// many consecutive exhaustions started at that same offset.
    pub fn note_stuck(&self, offset: u64) -> u32 {
        let mut g = self.inner.lock().unwrap();
        let n = match g.stuck {
            Some((o, n)) if o == offset => n.saturating_add(1),
            _ => 1,
        };
        g.stuck = Some((offset, n));
        n
    }

    /// Sink: the stuck write got past its first record.
    pub fn clear_stuck(&self) {
        self.inner.lock().unwrap().stuck = None;
    }

    pub fn record_restart(&self) {
        self.inner.lock().unwrap().restart_count += 1;
        self.metrics
            .connector_restarts_total
            .add(1, &[self.label()]);
    }

    pub fn record_success(
        &self,
        records: u64,
        direction: &'static str,
        checkpoint: Option<String>,
    ) {
        let now = SystemTime::now();
        {
            let mut g = self.inner.lock().unwrap();
            g.last_success = Some(now);
            g.records += records;
            if checkpoint.is_some() {
                g.checkpoint = checkpoint;
            }
        }
        if records > 0 {
            self.metrics.connector_records_total.add(
                records,
                &[self.label(), KeyValue::new("direction", direction)],
            );
        }
        let secs = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        self.metrics
            .connector_last_success_timestamp_seconds
            .record(secs, &[self.label()]);
    }

    pub fn set_checkpoint(&self, checkpoint: Option<String>) {
        self.inner.lock().unwrap().checkpoint = checkpoint;
    }

    pub fn last_success(&self) -> Option<SystemTime> {
        self.inner.lock().unwrap().last_success
    }

    pub fn set_lag(&self, lag: Option<Lag>) {
        self.inner.lock().unwrap().lag = lag;
        if let Some(l) = lag {
            self.metrics.connector_lag.record(
                l.value.min(i64::MAX as u64) as i64,
                &[self.label(), KeyValue::new("unit", l.unit.as_str())],
            );
        }
    }

    pub fn restart_count(&self) -> u64 {
        self.inner.lock().unwrap().restart_count
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        let g = self.inner.lock().unwrap().clone();
        StatusSnapshot {
            status: g.status,
            last_error: g.last_error,
            restart_count: g.restart_count,
            lag: g.lag.map(|l| l.value),
            lag_unit: g.lag.map(|l| l.unit.as_str()),
            last_success_ms: g
                .last_success
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64),
            status_secs: g.since.elapsed().as_secs(),
            checkpoint: g.checkpoint,
            records: g.records,
        }
    }

    /// Zero the per-connector gauges (connector deleted).
    pub fn retire(&self) {
        for s in Status::ALL {
            self.metrics
                .connector_state
                .record(0, &[self.label(), KeyValue::new("state", s.as_str())]);
        }
    }
}
