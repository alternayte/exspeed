//! Server-wide Prometheus metrics.
//!
//! Every series is named `exspeed_*`; counters end in `_total` exactly once.
//! Instruments keep the `add` / `record` shape of OpenTelemetry instruments
//! (attributes as [`KeyValue`]s) so call sites stay terse, but they are plain
//! `prometheus` vectors underneath. That gives exact names and lets the
//! server drop series when the stream, consumer, connector or query they
//! describe is deleted ([`Metrics::forget_stream`] and friends).

use std::borrow::Cow;
use std::collections::HashMap;

use prometheus::core::{Collector, MetricVec, MetricVecBuilder};
use prometheus::{
    GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
};

pub use opentelemetry::KeyValue;

/// Label values in `names` order; a label missing from `attrs` is empty.
fn label_values<'a>(names: &[&str], attrs: &'a [KeyValue]) -> Vec<Cow<'a, str>> {
    names
        .iter()
        .map(|n| {
            attrs
                .iter()
                .find(|kv| kv.key.as_str() == *n)
                .map(|kv| kv.value.as_str())
                .unwrap_or(Cow::Borrowed(""))
        })
        .collect()
}

/// Remove every series of `vec` for which `keep` returns false.
fn retain_series<T: MetricVecBuilder>(
    vec: &MetricVec<T>,
    names: &[&str],
    keep: impl Fn(&HashMap<&str, &str>) -> bool,
) {
    for family in vec.collect() {
        for m in family.get_metric() {
            let labels: HashMap<&str, &str> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            if !keep(&labels) {
                let values: Vec<&str> = names
                    .iter()
                    .map(|n| labels.get(n).copied().unwrap_or(""))
                    .collect();
                let _ = vec.remove_label_values(&values);
            }
        }
    }
}

macro_rules! instrument {
    ($(#[$doc:meta])* $name:ident, $vec:ty, $method:ident($v:ty) => $apply:ident) => {
        $(#[$doc])*
        pub struct $name {
            vec: $vec,
            labels: &'static [&'static str],
        }

        impl $name {
            pub fn $method(&self, value: $v, attrs: &[KeyValue]) {
                let values = label_values(self.labels, attrs);
                let refs: Vec<&str> = values.iter().map(|v| v.as_ref()).collect();
                self.vec.with_label_values(&refs).$apply(value);
            }

            /// Drop every series whose `label` equals `value`.
            pub fn forget(&self, label: &str, value: &str) {
                if self.labels.contains(&label) {
                    retain_series(&self.vec, self.labels, |l| l.get(label) != Some(&value));
                }
            }

            /// Keep only the series for which `keep` returns true.
            pub fn retain(&self, keep: impl Fn(&HashMap<&str, &str>) -> bool) {
                retain_series(&self.vec, self.labels, keep);
            }
        }
    };
}

instrument!(
    /// A monotonic counter.
    Counter, IntCounterVec, add(u64) => inc_by
);
instrument!(
    /// An integer gauge set to a value.
    Gauge, IntGaugeVec, record(i64) => set
);
instrument!(
    /// A floating-point gauge set to a value.
    FloatGauge, GaugeVec, record(f64) => set
);
instrument!(
    /// An integer gauge moved up and down.
    UpDownCounter, IntGaugeVec, add(i64) => add
);
instrument!(
    /// A histogram (seconds; Prometheus default buckets).
    Histogram, HistogramVec, record(f64) => observe
);

struct Builder {
    registry: Registry,
}

impl Builder {
    fn opts(name: &str, help: &str) -> Opts {
        debug_assert!(name.starts_with("exspeed_"), "{name}");
        Opts::new(name, help)
    }

    fn register<C: Collector + Clone + 'static>(&self, c: C) -> C {
        self.registry
            .register(Box::new(c.clone()))
            .expect("metric names are unique");
        c
    }

    fn counter(&self, name: &str, help: &str, labels: &'static [&'static str]) -> Counter {
        debug_assert!(name.ends_with("_total") && !name.ends_with("_total_total"));
        let vec = IntCounterVec::new(Self::opts(name, help), labels).expect("valid counter");
        Counter {
            vec: self.register(vec),
            labels,
        }
    }

    fn gauge(&self, name: &str, help: &str, labels: &'static [&'static str]) -> Gauge {
        let vec = IntGaugeVec::new(Self::opts(name, help), labels).expect("valid gauge");
        Gauge {
            vec: self.register(vec),
            labels,
        }
    }

    fn float_gauge(&self, name: &str, help: &str, labels: &'static [&'static str]) -> FloatGauge {
        let vec = GaugeVec::new(Self::opts(name, help), labels).expect("valid gauge");
        FloatGauge {
            vec: self.register(vec),
            labels,
        }
    }

    fn up_down(&self, name: &str, help: &str, labels: &'static [&'static str]) -> UpDownCounter {
        let vec = IntGaugeVec::new(Self::opts(name, help), labels).expect("valid gauge");
        UpDownCounter {
            vec: self.register(vec),
            labels,
        }
    }

    fn histogram(&self, name: &str, help: &str, labels: &'static [&'static str]) -> Histogram {
        debug_assert!(name.starts_with("exspeed_"), "{name}");
        let vec =
            HistogramVec::new(HistogramOpts::new(name, help), labels).expect("valid histogram");
        Histogram {
            vec: self.register(vec),
            labels,
        }
    }
}

/// Server-wide metrics. Create with [`Metrics::new`], which also returns
/// the [`prometheus::Registry`] the HTTP `/metrics` endpoint renders.
pub struct Metrics {
    /// `exspeed_records_published_total{stream}`: records written through
    /// the log (duplicates not counted).
    pub records_published: Counter,
    /// `exspeed_consumer_lag{stream,consumer}`: records the consumer has
    /// not yet acknowledged (stream end minus ack floor). Set on scrape.
    pub consumer_lag: Gauge,
    /// `exspeed_storage_bytes{stream}`: bytes on disk. Set on scrape.
    pub storage_bytes: Gauge,
    /// `exspeed_partition_failed{stream}`: 1 while the stream's partition
    /// is fenced read-only after an IO error it could not roll back (until
    /// a restart runs recovery). Set on scrape.
    pub partition_failed: Gauge,
    /// `exspeed_connections_active`: open client (TCP) connections.
    pub connections_active: UpDownCounter,
    /// `exspeed_connections_rejected_total`: connections refused because
    /// `max_connections` was reached.
    pub connections_rejected: Counter,
    /// `exspeed_uptime_seconds`.
    pub uptime_seconds: FloatGauge,
    /// `exspeed_lease_held{name}`: 1 while this node holds the lease.
    pub lease_held: Gauge,
    /// `exspeed_lease_acquire_total{name,result}`: `result` is `acquired`,
    /// `rejected` (another node holds it) or `error` (backend failure).
    pub lease_acquire_total: Counter,
    /// `exspeed_lease_lost_total{name}`: involuntary losses (another holder,
    /// or the heartbeat missed its local deadline).
    pub lease_lost_total: Counter,
    /// `exspeed_is_leader`: 1 on the leader, 0 elsewhere.
    pub is_leader: Gauge,
    /// `exspeed_leader_transitions_total{direction}`: `acquired`, `lost`,
    /// `stepped_down` or `resigned`.
    pub leader_transitions_total: Counter,
    /// `exspeed_publish_latency_seconds{stream}`: successful publishes.
    pub publish_latency_seconds: Histogram,
    /// `exspeed_storage_write_errors_total{stream,kind}`: `kind` is
    /// `storage_full` or `other`.
    pub storage_write_errors: Counter,

    // -- connectors ----------------------------------------------------------
    /// `exspeed_connector_records_skipped_total{connector,stream,reason}`.
    pub connector_records_skipped_total: Counter,
    /// `exspeed_connector_write_errors_total{connector,stream,sqlstate}`.
    pub connector_write_errors_total: Counter,
    /// `exspeed_connector_start_errors_total{connector,stream}`.
    pub connector_start_errors_total: Counter,
    /// `exspeed_connector_dlq_total{connector,reason}`.
    pub connector_dlq_total: Counter,
    /// `exspeed_connector_dlq_failures_total{connector}`.
    pub connector_dlq_failures_total: Counter,
    /// `exspeed_connector_retry_attempts_total{connector,outcome}`.
    pub connector_retry_attempts_total: Counter,
    /// `exspeed_connector_transient_exhausted_total{connector,action}`.
    pub connector_transient_exhausted_total: Counter,
    /// `exspeed_consumer_dead_letters_total{consumer,outcome}`: `outcome`
    /// is `dlq` or `dropped`.
    pub consumer_dead_letters_total: Counter,
    /// `exspeed_connector_state{connector,state}`: 1 for the current state.
    pub connector_state: Gauge,
    /// `exspeed_connector_restarts_total{connector}`.
    pub connector_restarts_total: Counter,
    /// `exspeed_connector_lag{connector,unit}`.
    pub connector_lag: Gauge,
    /// `exspeed_connector_last_success_timestamp_seconds{connector}`.
    pub connector_last_success_timestamp_seconds: FloatGauge,
    /// `exspeed_connector_records_total{connector,direction}`.
    pub connector_records_total: Counter,

    // -- dedup ---------------------------------------------------------------
    /// `exspeed_dedup_map_entries{stream}`.
    pub dedup_map_entries: Gauge,
    /// `exspeed_dedup_writes_total{stream,result}`.
    pub dedup_writes_total: Counter,
    /// `exspeed_dedup_collisions_total{stream}`.
    pub dedup_collisions_total: Counter,
    /// `exspeed_dedup_map_full_total{stream}`.
    pub dedup_map_full_total: Counter,
    /// `exspeed_dedup_snapshot_write_duration_seconds`.
    pub dedup_snapshot_write_duration_seconds: Histogram,
    /// `exspeed_dedup_rebuild_duration_seconds{stream,source}`.
    pub dedup_rebuild_duration_seconds: Histogram,
    /// `exspeed_dedup_window_secs{stream}`.
    pub dedup_window_secs: Gauge,

    // -- auth ----------------------------------------------------------------
    /// `exspeed_auth_denied_total{reason,transport,op}`: `op` is the opcode
    /// (TCP) or the route template (HTTP, e.g. `/api/v1/streams/{name}`).
    pub auth_denied_total: Counter,

    // -- replication ---------------------------------------------------------
    /// `exspeed_replication_role{role}`: 1 for this node's role.
    pub replication_role: Gauge,
    /// `exspeed_replication_lag_records{follower_id}` (follower side).
    pub replication_lag_records: Gauge,
    /// `exspeed_replication_records_applied_total{stream}` (follower side).
    pub replication_records_applied_total: Counter,
    /// `exspeed_replication_bytes_total{direction}`.
    pub replication_bytes_total: Counter,
    /// `exspeed_replication_truncated_records_total{stream}`.
    pub replication_truncated_records_total: Counter,
    /// `exspeed_replication_reseed_total{stream}`: a follower behind the
    /// leader's earliest offset dropped its copy and re-replicated.
    pub replication_reseed_total: Counter,
    /// `exspeed_replication_apply_errors_total` (follower side).
    pub replication_apply_errors_total: Counter,
    /// `exspeed_replication_connect_attempts_total{result}`.
    pub replication_connect_attempts_total: Counter,
    /// `exspeed_exql_late_records_total{query}`.
    pub exql_late_records_total: Counter,
}

impl Metrics {
    /// Build every instrument and the registry `/metrics` renders.
    pub fn new() -> (Self, Registry) {
        let b = Builder {
            registry: Registry::new(),
        };
        let m = Metrics {
            records_published: b.counter(
                "exspeed_records_published_total",
                "Records written through the log",
                &["stream"],
            ),
            consumer_lag: b.gauge(
                "exspeed_consumer_lag",
                "Records a consumer has not acknowledged yet (stream end minus ack floor)",
                &["stream", "consumer"],
            ),
            storage_bytes: b.gauge("exspeed_storage_bytes", "Bytes on disk per stream", &["stream"]),
            partition_failed: b.gauge(
                "exspeed_partition_failed",
                "1 while the stream's partition is fenced read-only after an unrecoverable IO error",
                &["stream"],
            ),
            connections_active: b.up_down(
                "exspeed_connections_active",
                "Open client connections",
                &[],
            ),
            connections_rejected: b.counter(
                "exspeed_connections_rejected_total",
                "Connections rejected because max_connections was reached",
                &[],
            ),
            uptime_seconds: b.float_gauge("exspeed_uptime_seconds", "Server uptime", &[]),
            lease_held: b.gauge(
                "exspeed_lease_held",
                "1 while this node holds the lease",
                &["name"],
            ),
            lease_acquire_total: b.counter(
                "exspeed_lease_acquire_total",
                "Lease acquire attempts (result: acquired, rejected, error)",
                &["name", "result"],
            ),
            lease_lost_total: b.counter(
                "exspeed_lease_lost_total",
                "Leases lost involuntarily",
                &["name"],
            ),
            is_leader: b.gauge("exspeed_is_leader", "1 on the cluster leader", &[]),
            leader_transitions_total: b.counter(
                "exspeed_leader_transitions_total",
                "Leadership transitions (direction: acquired, lost, stepped_down, resigned)",
                &["direction"],
            ),
            publish_latency_seconds: b.histogram(
                "exspeed_publish_latency_seconds",
                "Latency of a successful publish",
                &["stream"],
            ),
            storage_write_errors: b.counter(
                "exspeed_storage_write_errors_total",
                "Storage write failures (kind: storage_full, other)",
                &["stream", "kind"],
            ),
            connector_records_skipped_total: b.counter(
                "exspeed_connector_records_skipped_total",
                "Records dropped by a sink connector (by reason)",
                &["connector", "stream", "reason"],
            ),
            connector_write_errors_total: b.counter(
                "exspeed_connector_write_errors_total",
                "SQL-side write errors from sink connectors",
                &["connector", "stream", "sqlstate"],
            ),
            connector_start_errors_total: b.counter(
                "exspeed_connector_start_errors_total",
                "Connector start failures (connect or CREATE TABLE)",
                &["connector", "stream"],
            ),
            connector_dlq_total: b.counter(
                "exspeed_connector_dlq_total",
                "Records routed to a connector DLQ stream",
                &["connector", "reason"],
            ),
            connector_dlq_failures_total: b.counter(
                "exspeed_connector_dlq_failures_total",
                "DLQ append failures (record lost)",
                &["connector"],
            ),
            connector_retry_attempts_total: b.counter(
                "exspeed_connector_retry_attempts_total",
                "Retry attempt outcomes on transient failures",
                &["connector", "outcome"],
            ),
            connector_transient_exhausted_total: b.counter(
                "exspeed_connector_transient_exhausted_total",
                "Transient-exhaustion events and the action taken",
                &["connector", "action"],
            ),
            consumer_dead_letters_total: b.counter(
                "exspeed_consumer_dead_letters_total",
                "Records dead-lettered (or dropped) by consumers",
                &["consumer", "outcome"],
            ),
            connector_state: b.gauge(
                "exspeed_connector_state",
                "Connector supervisor state (1 = current)",
                &["connector", "state"],
            ),
            connector_restarts_total: b.counter(
                "exspeed_connector_restarts_total",
                "Connector restarts by the supervisor",
                &["connector"],
            ),
            connector_lag: b.gauge(
                "exspeed_connector_lag",
                "Connector lag (unit: records, bytes or rows)",
                &["connector", "unit"],
            ),
            connector_last_success_timestamp_seconds: b.float_gauge(
                "exspeed_connector_last_success_timestamp_seconds",
                "Unix time of the connector's last successful batch",
                &["connector"],
            ),
            connector_records_total: b.counter(
                "exspeed_connector_records_total",
                "Records appended by sources (in) or committed by sinks (out)",
                &["connector", "direction"],
            ),
            dedup_map_entries: b.gauge(
                "exspeed_dedup_map_entries",
                "Live dedup entries per stream",
                &["stream"],
            ),
            dedup_writes_total: b.counter(
                "exspeed_dedup_writes_total",
                "Idempotent publish outcomes (result: written, duplicate)",
                &["stream", "result"],
            ),
            dedup_collisions_total: b.counter(
                "exspeed_dedup_collisions_total",
                "msg_id reused with a different body",
                &["stream"],
            ),
            dedup_map_full_total: b.counter(
                "exspeed_dedup_map_full_total",
                "Publishes rejected because the dedup map was full",
                &["stream"],
            ),
            dedup_snapshot_write_duration_seconds: b.histogram(
                "exspeed_dedup_snapshot_write_duration_seconds",
                "Time spent writing a dedup snapshot",
                &[],
            ),
            dedup_rebuild_duration_seconds: b.histogram(
                "exspeed_dedup_rebuild_duration_seconds",
                "Dedup map rebuild duration (source: snapshot, full_scan)",
                &["stream", "source"],
            ),
            dedup_window_secs: b.gauge(
                "exspeed_dedup_window_secs",
                "Configured dedup window per stream",
                &["stream"],
            ),
            auth_denied_total: b.counter(
                "exspeed_auth_denied_total",
                "Auth denials (reason: unauthorized, forbidden; transport: tcp, http; op: opcode or route)",
                &["reason", "transport", "op"],
            ),
            replication_role: b.gauge(
                "exspeed_replication_role",
                "1 for this node's replication role",
                &["role"],
            ),
            replication_lag_records: b.gauge(
                "exspeed_replication_lag_records",
                "Records this follower is behind the leader",
                &["follower_id"],
            ),
            replication_records_applied_total: b.counter(
                "exspeed_replication_records_applied_total",
                "Records a follower applied to its log",
                &["stream"],
            ),
            replication_bytes_total: b.counter(
                "exspeed_replication_bytes_total",
                "Replication wire bytes (direction: in, out)",
                &["direction"],
            ),
            replication_truncated_records_total: b.counter(
                "exspeed_replication_truncated_records_total",
                "Records a follower truncated because they diverged from the leader",
                &["stream"],
            ),
            replication_reseed_total: b.counter(
                "exspeed_replication_reseed_total",
                "Streams a follower re-replicated because it was behind the leader's earliest offset",
                &["stream"],
            ),
            replication_apply_errors_total: b.counter(
                "exspeed_replication_apply_errors_total",
                "Follower errors applying replicated records",
                &[],
            ),
            replication_connect_attempts_total: b.counter(
                "exspeed_replication_connect_attempts_total",
                "Follower dials to the leader's cluster port (result: ok, err)",
                &["result"],
            ),
            exql_late_records_total: b.counter(
                "exspeed_exql_late_records_total",
                "Records dropped by continuous queries for arriving after the watermark",
                &["query"],
            ),
        };

        // Zero-initialize series operators alert on, so they exist before
        // the first event (alerting on absence or `rate() > 0` works).
        m.connections_active.add(0, &[]);
        m.connections_rejected.add(0, &[]);
        m.is_leader.record(0, &[]);
        for d in ["acquired", "lost"] {
            m.leader_transitions_total
                .add(0, &[KeyValue::new("direction", d)]);
        }
        for r in ["leader", "follower", "standalone"] {
            m.replication_role
                .record(i64::from(r == "standalone"), &[KeyValue::new("role", r)]);
        }
        for d in ["in", "out"] {
            m.replication_bytes_total
                .add(0, &[KeyValue::new("direction", d)]);
        }
        for r in ["ok", "err"] {
            m.replication_connect_attempts_total
                .add(0, &[KeyValue::new("result", r)]);
        }
        m.replication_apply_errors_total.add(0, &[]);

        let registry = b.registry;
        (m, registry)
    }

    // -- forgetting deleted objects -------------------------------------------

    /// Drop every series labelled with a deleted stream.
    pub fn forget_stream(&self, stream: &str) {
        let s = ("stream", stream);
        self.records_published.forget(s.0, s.1);
        self.consumer_lag.forget(s.0, s.1);
        self.storage_bytes.forget(s.0, s.1);
        self.partition_failed.forget(s.0, s.1);
        self.publish_latency_seconds.forget(s.0, s.1);
        self.storage_write_errors.forget(s.0, s.1);
        self.dedup_map_entries.forget(s.0, s.1);
        self.dedup_writes_total.forget(s.0, s.1);
        self.dedup_collisions_total.forget(s.0, s.1);
        self.dedup_map_full_total.forget(s.0, s.1);
        self.dedup_rebuild_duration_seconds.forget(s.0, s.1);
        self.dedup_window_secs.forget(s.0, s.1);
        self.replication_records_applied_total.forget(s.0, s.1);
        self.replication_truncated_records_total.forget(s.0, s.1);
        self.replication_reseed_total.forget(s.0, s.1);
    }

    /// Drop every series labelled with a deleted consumer.
    pub fn forget_consumer(&self, consumer: &str) {
        self.consumer_lag.forget("consumer", consumer);
        self.consumer_dead_letters_total
            .forget("consumer", consumer);
    }

    /// Drop every series labelled with a deleted connector.
    pub fn forget_connector(&self, connector: &str) {
        let c = ("connector", connector);
        self.connector_records_skipped_total.forget(c.0, c.1);
        self.connector_write_errors_total.forget(c.0, c.1);
        self.connector_start_errors_total.forget(c.0, c.1);
        self.connector_dlq_total.forget(c.0, c.1);
        self.connector_dlq_failures_total.forget(c.0, c.1);
        self.connector_retry_attempts_total.forget(c.0, c.1);
        self.connector_transient_exhausted_total.forget(c.0, c.1);
        self.connector_state.forget(c.0, c.1);
        self.connector_restarts_total.forget(c.0, c.1);
        self.connector_lag.forget(c.0, c.1);
        self.connector_last_success_timestamp_seconds
            .forget(c.0, c.1);
        self.connector_records_total.forget(c.0, c.1);
    }

    /// Drop every series labelled with a deleted continuous query.
    pub fn forget_query(&self, query: &str) {
        self.exql_late_records_total.forget("query", query);
    }

    // -- helper methods -------------------------------------------------------

    /// Count a record a consumer dead-lettered (`outcome` = `dlq`) or dropped.
    pub fn record_consumer_dead_letter(&self, consumer: &str, outcome: &'static str) {
        self.consumer_dead_letters_total.add(
            1,
            &[
                KeyValue::new("consumer", consumer.to_owned()),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    /// Count one record written to `stream`.
    pub fn record_publish(&self, stream: &str) {
        self.records_published
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Set a consumer's lag.
    pub fn set_consumer_lag(&self, stream: &str, consumer: &str, lag: i64) {
        self.consumer_lag.record(
            lag,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("consumer", consumer.to_owned()),
            ],
        );
    }

    /// Set a stream's size on disk.
    pub fn set_storage_bytes(&self, stream: &str, bytes: i64) {
        self.storage_bytes
            .record(bytes, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Set whether a stream's partition is fenced.
    pub fn set_partition_failed(&self, stream: &str, failed: bool) {
        self.partition_failed.record(
            i64::from(failed),
            &[KeyValue::new("stream", stream.to_owned())],
        );
    }

    pub fn connection_opened(&self) {
        self.connections_active.add(1, &[]);
    }

    pub fn connection_closed(&self) {
        self.connections_active.add(-1, &[]);
    }

    /// A connection was refused at accept time (`max_connections` reached).
    pub fn connection_rejected(&self) {
        self.connections_rejected.add(1, &[]);
    }

    pub fn set_uptime(&self, seconds: f64) {
        self.uptime_seconds.record(seconds, &[]);
    }

    /// `held = true` on acquire, `false` on release or loss.
    pub fn set_lease_held(&self, name: &str, held: bool) {
        self.lease_held
            .record(i64::from(held), &[KeyValue::new("name", name.to_owned())]);
    }

    /// `result` is `acquired`, `rejected` or `error`.
    pub fn record_lease_acquire_attempt(&self, name: &str, result: &'static str) {
        self.lease_acquire_total.add(
            1,
            &[
                KeyValue::new("name", name.to_owned()),
                KeyValue::new("result", result),
            ],
        );
    }

    /// Involuntary lease loss only; a clean release must not call this.
    pub fn record_lease_lost(&self, name: &str) {
        self.lease_lost_total
            .add(1, &[KeyValue::new("name", name.to_owned())]);
    }

    pub fn set_is_leader(&self, leader: bool) {
        self.is_leader.record(i64::from(leader), &[]);
    }

    /// `direction`: `acquired`, `lost`, `stepped_down` or `resigned`.
    pub fn record_leader_transition(&self, direction: &'static str) {
        self.leader_transitions_total
            .add(1, &[KeyValue::new("direction", direction)]);
    }

    pub fn record_publish_latency(&self, stream: &str, secs: f64) {
        self.publish_latency_seconds
            .record(secs, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// `kind` is `storage_full` or `other`.
    pub fn record_storage_write_error(&self, stream: &str, kind: &'static str) {
        self.storage_write_errors.add(
            1,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("kind", kind),
            ],
        );
    }

    // -- dedup helpers --------------------------------------------------------

    /// `result` is `written` or `duplicate`.
    pub fn record_dedup_write(&self, stream: &str, result: &str) {
        self.dedup_writes_total.add(
            1,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("result", result.to_owned()),
            ],
        );
    }

    pub fn record_dedup_collision(&self, stream: &str) {
        self.dedup_collisions_total
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    pub fn record_dedup_map_full(&self, stream: &str) {
        self.dedup_map_full_total
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    pub fn set_dedup_map_entries(&self, stream: &str, n: i64) {
        self.dedup_map_entries
            .record(n, &[KeyValue::new("stream", stream.to_owned())]);
    }

    pub fn observe_dedup_snapshot_write_duration(&self, secs: f64) {
        self.dedup_snapshot_write_duration_seconds.record(secs, &[]);
    }

    /// `source` is `snapshot` or `full_scan`.
    pub fn observe_dedup_rebuild_duration(&self, stream: &str, source: &str, secs: f64) {
        self.dedup_rebuild_duration_seconds.record(
            secs,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("source", source.to_owned()),
            ],
        );
    }

    pub fn set_dedup_window_secs(&self, stream: &str, secs: i64) {
        self.dedup_window_secs
            .record(secs, &[KeyValue::new("stream", stream.to_owned())]);
    }

    // -- auth helpers ---------------------------------------------------------

    /// `reason`: `unauthorized` or `forbidden`; `transport`: `tcp` or `http`;
    /// `op`: the opcode (TCP) or the route template (HTTP). Never pass a raw
    /// request path: it would make the label set unbounded.
    pub fn auth_denied(&self, reason: &str, transport: &str, op: &str) {
        self.auth_denied_total.add(
            1,
            &[
                KeyValue::new("reason", reason.to_owned()),
                KeyValue::new("transport", transport.to_owned()),
                KeyValue::new("op", op.to_owned()),
            ],
        );
    }

    // -- replication helpers --------------------------------------------------

    /// Exactly one role is 1 at a time.
    pub fn set_replication_role(&self, role: &'static str) {
        for candidate in ["leader", "follower", "standalone"] {
            self.replication_role.record(
                i64::from(candidate == role),
                &[KeyValue::new("role", candidate)],
            );
        }
    }

    pub fn inc_replication_truncated_records(&self, stream: &str, count: u64) {
        self.replication_truncated_records_total
            .add(count, &[KeyValue::new("stream", stream.to_string())]);
    }

    pub fn inc_replication_reseed(&self, stream: &str) {
        self.replication_reseed_total
            .add(1, &[KeyValue::new("stream", stream.to_string())]);
    }

    pub fn record_replication_connect_attempt(&self, ok: bool) {
        self.replication_connect_attempts_total
            .add(1, &[KeyValue::new("result", if ok { "ok" } else { "err" })]);
    }

    pub fn inc_replication_records_applied(&self, stream: &str, count: u64) {
        self.replication_records_applied_total
            .add(count, &[KeyValue::new("stream", stream.to_string())]);
    }

    /// Count records a continuous query dropped as late.
    pub fn record_exql_late(&self, query: &str, n: u64) {
        self.exql_late_records_total
            .add(n, &[KeyValue::new("query", query.to_string())]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    fn render(r: &Registry) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&r.gather(), &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn every_series_is_prefixed_and_counters_end_in_total_once() {
        let (m, r) = Metrics::new();
        m.record_publish("s");
        m.record_publish_latency("s", 0.01);
        m.set_consumer_lag("s", "c", 3);
        let text = render(&r);
        for line in text
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            assert!(line.starts_with("exspeed_"), "unprefixed series: {line}");
            assert!(!line.contains("_total_total"), "doubled suffix: {line}");
        }
        for line in text.lines().filter(|l| l.starts_with("# TYPE ")) {
            let mut parts = line.split_whitespace().skip(2);
            let (name, kind) = (parts.next().unwrap(), parts.next().unwrap());
            if kind == "counter" {
                assert!(name.ends_with("_total"), "{name}");
            }
        }
        assert!(text.contains("exspeed_records_published_total{stream=\"s\"} 1"));
        assert!(text.contains("exspeed_consumer_lag{consumer=\"c\",stream=\"s\"} 3"));
    }

    #[test]
    fn forgetting_a_stream_drops_its_series_only() {
        let (m, r) = Metrics::new();
        for s in ["gone", "kept"] {
            m.record_publish(s);
            m.record_storage_write_error(s, "other");
            m.set_consumer_lag(s, "c", 1);
            m.set_storage_bytes(s, 10);
        }
        m.forget_stream("gone");
        let text = render(&r);
        assert!(!text.contains("\"gone\""), "{text}");
        assert!(text.contains("exspeed_records_published_total{stream=\"kept\"} 1"));
        assert!(
            text.contains("exspeed_storage_write_errors_total{kind=\"other\",stream=\"kept\"} 1")
        );
    }

    #[test]
    fn forgetting_consumers_and_connectors() {
        let (m, r) = Metrics::new();
        m.set_consumer_lag("s", "c1", 1);
        m.set_consumer_lag("s", "c2", 2);
        m.record_consumer_dead_letter("c1", "dlq");
        m.connector_state.record(
            1,
            &[
                KeyValue::new("connector", "k"),
                KeyValue::new("state", "running"),
            ],
        );
        m.forget_consumer("c1");
        m.forget_connector("k");
        let text = render(&r);
        assert!(!text.contains("\"c1\""), "{text}");
        assert!(text.contains("consumer=\"c2\""));
        assert!(!text.contains("connector=\"k\""), "{text}");
    }

    #[test]
    fn missing_labels_are_empty() {
        let (m, r) = Metrics::new();
        m.connector_dlq_total
            .add(1, &[KeyValue::new("connector", "x")]);
        assert!(render(&r).contains("exspeed_connector_dlq_total{connector=\"x\",reason=\"\"} 1"));
    }
}
