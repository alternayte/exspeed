use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider, UpDownCounter};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Server-wide metrics backed by OpenTelemetry instruments and exported via
/// Prometheus.
///
/// Create with [`Metrics::new`], which returns the struct together with a
/// [`prometheus::Registry`] that should be wired into the HTTP `/metrics`
/// endpoint.
pub struct Metrics {
    pub records_published: Counter<u64>,
    pub records_consumed: Counter<u64>,
    pub consumer_lag: Gauge<i64>,
    pub storage_bytes: Gauge<i64>,
    pub connections_active: UpDownCounter<i64>,
    /// Counts every TCP connection rejected because the per-process
    /// `EXSPEED_MAX_CONNS` cap was reached at accept time. Operators can
    /// alert on the rate of this counter to detect saturation.
    pub connections_rejected: Counter<u64>,
    pub uptime_seconds: Gauge<f64>,
    /// Whether this pod currently holds the named lease. Gauge value is 1
    /// when held, 0 when released/lost. Labeled by `name` (e.g.
    /// `connector:orders-source` or `query:abc123`).
    pub lease_held: Gauge<i64>,
    /// Counts every `try_acquire` attempt this pod made. Labeled by `name`
    /// and `result`, where `result` is one of `"acquired"`, `"rejected"`
    /// (another pod holds the lease), or `"error"` (backend failure).
    pub lease_acquire_total: Counter<u64>,
    /// Counts every lease lost involuntarily (heartbeat failure, TTL
    /// expiry). Labeled by `name`. Does not fire on clean release/shutdown.
    pub lease_lost_total: Counter<u64>,
    /// Whether this pod currently holds the cluster leader lease.
    /// `1` = leader, `0` = standby. Exactly one pod in a cluster should
    /// observe `1` at any given time.
    pub is_leader: Gauge<i64>,
    /// Counts every promotion / demotion event this pod has observed,
    /// labeled `direction = "acquired" | "lost"`.
    pub leader_transitions_total: Counter<u64>,
    /// End-to-end latency of a successful publish, in seconds. Labeled by
    /// `stream`.
    pub publish_latency_seconds: Histogram<f64>,
    /// Time from a record's write timestamp to its delivery to a subscriber,
    /// in seconds. Labeled by `stream` and `consumer`.
    pub consume_latency_seconds: Histogram<f64>,
    /// Storage write failures. Labeled by `stream` and `kind`
    /// (`"storage_full"` or `"other"`).
    pub storage_write_errors: Counter<u64>,
    /// Fill ratio (0.0–1.0) of the per-subscription delivery mpsc channel.
    /// Labeled by `consumer` and `subscriber`.
    pub subscription_queue_fill_ratio: Gauge<f64>,

    // -- dedup observability ------------------------------------------------
    /// Current number of live dedup entries per stream. Labeled by `stream`.
    pub dedup_map_entries: Gauge<i64>,
    /// Counts idempotent publish outcomes. Labeled by `stream` and `result`
    /// (`"written"` | `"duplicate"`).
    pub dedup_writes_total: Counter<u64>,
    /// Counts key-collision events (same msg_id, different body) per stream.
    /// Labeled by `stream`.
    pub dedup_collisions_total: Counter<u64>,
    /// Counts events where the dedup map was full and a publish was rejected.
    /// Labeled by `stream`.
    pub dedup_map_full_total: Counter<u64>,
    /// Histogram of time spent writing a dedup snapshot to disk, in seconds.
    pub dedup_snapshot_write_duration_seconds: Histogram<f64>,
    /// Histogram of dedup map rebuild duration per stream. Labeled by
    /// `stream` and `source` (`"snapshot"` | `"full_scan"`).
    pub dedup_rebuild_duration_seconds: Histogram<f64>,
    /// Configured dedup window in seconds per stream. Labeled by `stream`.
    pub dedup_window_secs: Gauge<i64>,

    // -- auth observability --------------------------------------------------
    /// Counts every auth denial. Labeled by:
    /// - `reason`: `"unauthorized"` (no/invalid credential) | `"forbidden"`
    ///   (authenticated identity lacked the required permission).
    /// - `transport`: `"tcp"` | `"http"`.
    /// - `op`: opcode name on TCP (e.g. `"Publish"`) or request path on HTTP
    ///   (e.g. `"/api/v1/streams"`).
    pub auth_denied_total: Counter<u64>,

    // -- replication observability ------------------------------------------
    /// Labeled {role=leader|follower|standalone}. Exactly one label is 1 at
    /// a time.
    pub replication_role: Gauge<i64>,
    /// Leader-only: number of followers currently holding live replication
    /// connections.
    pub replication_connected_followers: Gauge<i64>,
    /// Wall-clock lag observed by each follower, labeled by `follower_id`.
    pub replication_lag_seconds: Gauge<f64>,
    /// Offset lag observed by each follower, labeled by `follower_id`.
    pub replication_lag_records: Gauge<i64>,
    /// Follower-side: records successfully applied to local storage.
    pub replication_records_applied_total: Counter<u64>,
    /// Replication wire bytes, labeled `direction=in|out`.
    pub replication_bytes_total: Counter<u64>,
    /// Records truncated on a follower due to divergent-history recovery,
    /// labeled by `stream`.
    pub replication_truncated_records_total: Counter<u64>,
    /// Stream-reseed events (follower cursor earlier than leader's
    /// `earliest_offset`), labeled by `stream`.
    pub replication_reseed_total: Counter<u64>,
    /// Leader-side: followers dropped because their send queue overflowed,
    /// labeled by `follower_id`.
    pub replication_follower_queue_drops_total: Counter<u64>,
    /// Replication protocol / decode errors.
    pub replication_protocol_errors_total: Counter<u64>,
    /// Follower-side errors applying a replicated record to storage.
    pub replication_apply_errors_total: Counter<u64>,
    /// Follower dial attempts to the leader's cluster port, labeled
    /// `result=ok|err`.
    pub replication_connect_attempts_total: Counter<u64>,
}

impl Metrics {
    /// Build all OTel instruments and a Prometheus registry that can serve
    /// them over HTTP.
    pub fn new() -> (Self, prometheus::Registry) {
        let registry = prometheus::Registry::new();

        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .expect("prometheus exporter should build");

        let provider = SdkMeterProvider::builder().with_reader(exporter).build();

        let meter = provider.meter("exspeed");

        let records_published = meter.u64_counter("records_published").build();
        let records_consumed = meter.u64_counter("records_consumed").build();
        let consumer_lag = meter.i64_gauge("consumer_lag").build();
        let storage_bytes = meter.i64_gauge("storage_bytes").build();
        let connections_active = meter.i64_up_down_counter("connections_active").build();
        let connections_rejected = meter
            .u64_counter("connections_rejected_total")
            .with_description("Connections rejected because EXSPEED_MAX_CONNS was reached")
            .build();
        let uptime_seconds = meter.f64_gauge("uptime_seconds").build();
        let lease_held = meter.i64_gauge("exspeed_lease_held").build();
        let lease_acquire_total = meter.u64_counter("exspeed_lease_acquire_total").build();
        let lease_lost_total = meter.u64_counter("exspeed_lease_lost_total").build();

        // Zero-initialize the lease metrics so the descriptor shows up in
        // /metrics output even before any acquire attempt (e.g., on pods
        // with no connectors/queries configured). Operator dashboards can
        // then alert on absence; we don't want the metric to vanish simply
        // because nothing has fired yet. The `__init__` label is a sentinel
        // distinct from any real lease name.
        let init_label = [KeyValue::new("name", "__init__")];
        lease_held.record(0, &init_label);
        lease_acquire_total.add(
            0,
            &[
                KeyValue::new("name", "__init__"),
                KeyValue::new("result", "acquired"),
            ],
        );
        lease_lost_total.add(0, &init_label);

        let is_leader = meter.i64_gauge("exspeed_is_leader").build();
        let leader_transitions_total = meter
            .u64_counter("exspeed_leader_transitions_total")
            .build();

        // Zero-initialize so the descriptor appears in /metrics before any
        // transition fires.
        is_leader.record(0, &[]);
        leader_transitions_total.add(0, &[KeyValue::new("direction", "acquired")]);
        leader_transitions_total.add(0, &[KeyValue::new("direction", "lost")]);

        let publish_latency_seconds = meter
            .f64_histogram("publish_latency_seconds")
            .with_description("End-to-end latency of a successful publish in seconds")
            .build();
        let consume_latency_seconds = meter
            .f64_histogram("consume_latency_seconds")
            .with_description("Time from record write to delivery to a subscriber, in seconds")
            .build();
        let storage_write_errors = meter
            .u64_counter("storage_write_errors_total")
            .with_description("Storage write failures (kind label: storage_full, other)")
            .build();
        let subscription_queue_fill_ratio = meter
            .f64_gauge("subscription_queue_fill_ratio")
            .with_description("Fill ratio (0.0–1.0) of the per-subscription delivery channel")
            .build();

        // -- dedup instruments -----------------------------------------------

        let dedup_map_entries = meter
            .i64_gauge("exspeed_dedup_map_entries")
            .with_description("Current number of live dedup entries per stream")
            .build();
        let dedup_writes_total = meter
            .u64_counter("exspeed_dedup_writes_total")
            .with_description("Idempotent publish outcomes (written or duplicate) per stream")
            .build();
        let dedup_collisions_total = meter
            .u64_counter("exspeed_dedup_collisions_total")
            .with_description("Key-collision events (same msg_id, different body) per stream")
            .build();
        let dedup_map_full_total = meter
            .u64_counter("exspeed_dedup_map_full_total")
            .with_description("Publishes rejected because the dedup map was full, per stream")
            .build();
        let dedup_snapshot_write_duration_seconds = meter
            .f64_histogram("exspeed_dedup_snapshot_write_duration_seconds")
            .with_description("Time spent writing a dedup snapshot to disk, in seconds")
            .build();
        let dedup_rebuild_duration_seconds = meter
            .f64_histogram("exspeed_dedup_rebuild_duration_seconds")
            .with_description("Dedup map rebuild duration per stream, in seconds")
            .build();
        let dedup_window_secs = meter
            .i64_gauge("exspeed_dedup_window_secs")
            .with_description("Configured dedup window in seconds per stream")
            .build();

        // -- auth instruments ------------------------------------------------

        let auth_denied_total = meter
            .u64_counter("exspeed_auth_denied_total")
            .with_description(
                "Auth denials. Labels: reason=unauthorized|forbidden, transport=tcp|http, op=<opcode or path>",
            )
            .build();

        // Zero-initialize a representative label set so the descriptor is
        // visible in /metrics before any denial fires. Operators alerting on
        // `rate(exspeed_auth_denied_total[...]) > 0` want the series to exist.
        auth_denied_total.add(
            0,
            &[
                KeyValue::new("reason", "unauthorized"),
                KeyValue::new("transport", "tcp"),
                KeyValue::new("op", "__init__"),
            ],
        );

        // -- replication instruments -----------------------------------------

        let replication_role = meter.i64_gauge("exspeed_replication_role").build();
        let replication_connected_followers = meter
            .i64_gauge("exspeed_replication_connected_followers")
            .build();
        let replication_lag_seconds = meter.f64_gauge("exspeed_replication_lag_seconds").build();
        let replication_lag_records = meter.i64_gauge("exspeed_replication_lag_records").build();
        let replication_records_applied_total = meter
            .u64_counter("exspeed_replication_records_applied_total")
            .build();
        let replication_bytes_total = meter.u64_counter("exspeed_replication_bytes_total").build();
        let replication_truncated_records_total = meter
            .u64_counter("exspeed_replication_truncated_records_total")
            .build();
        let replication_reseed_total = meter.u64_counter("exspeed_replication_reseed_total").build();
        let replication_follower_queue_drops_total = meter
            .u64_counter("exspeed_replication_follower_queue_drops_total")
            .build();
        let replication_protocol_errors_total = meter
            .u64_counter("exspeed_replication_protocol_errors_total")
            .build();
        let replication_apply_errors_total = meter
            .u64_counter("exspeed_replication_apply_errors_total")
            .build();
        let replication_connect_attempts_total = meter
            .u64_counter("exspeed_replication_connect_attempts_total")
            .build();

        // Descriptor-visibility zero-init: ensures `/metrics` always lists each
        // series even before any replication events.
        replication_role.record(1, &[KeyValue::new("role", "standalone")]);
        replication_role.record(0, &[KeyValue::new("role", "leader")]);
        replication_role.record(0, &[KeyValue::new("role", "follower")]);
        replication_connected_followers.record(0, &[]);
        replication_bytes_total.add(0, &[KeyValue::new("direction", "in")]);
        replication_bytes_total.add(0, &[KeyValue::new("direction", "out")]);
        replication_connect_attempts_total.add(0, &[KeyValue::new("result", "ok")]);
        replication_connect_attempts_total.add(0, &[KeyValue::new("result", "err")]);

        // Keep the provider alive — dropping it shuts down the metrics pipeline.
        std::mem::forget(provider);

        let metrics = Metrics {
            records_published,
            records_consumed,
            consumer_lag,
            storage_bytes,
            connections_active,
            connections_rejected,
            uptime_seconds,
            lease_held,
            lease_acquire_total,
            lease_lost_total,
            is_leader,
            leader_transitions_total,
            publish_latency_seconds,
            consume_latency_seconds,
            storage_write_errors,
            subscription_queue_fill_ratio,
            dedup_map_entries,
            dedup_writes_total,
            dedup_collisions_total,
            dedup_map_full_total,
            dedup_snapshot_write_duration_seconds,
            dedup_rebuild_duration_seconds,
            dedup_window_secs,
            auth_denied_total,
            replication_role,
            replication_connected_followers,
            replication_lag_seconds,
            replication_lag_records,
            replication_records_applied_total,
            replication_bytes_total,
            replication_truncated_records_total,
            replication_reseed_total,
            replication_follower_queue_drops_total,
            replication_protocol_errors_total,
            replication_apply_errors_total,
            replication_connect_attempts_total,
        };

        (metrics, registry)
    }

    // -- helper methods -----------------------------------------------------

    /// Increment `records_published` by 1 for the given stream.
    pub fn record_publish(&self, stream: &str) {
        self.records_published
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Increment `records_consumed` by 1 for the given stream and consumer.
    pub fn record_consume(&self, stream: &str, consumer: &str) {
        self.records_consumed.add(
            1,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("consumer", consumer.to_owned()),
            ],
        );
    }

    /// Record the current consumer lag for a stream/consumer pair.
    pub fn set_consumer_lag(&self, stream: &str, consumer: &str, lag: i64) {
        self.consumer_lag.record(
            lag,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("consumer", consumer.to_owned()),
            ],
        );
    }

    /// Record the current storage size in bytes for a stream.
    pub fn set_storage_bytes(&self, stream: &str, bytes: i64) {
        self.storage_bytes
            .record(bytes, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Increment the active-connections gauge by 1.
    pub fn connection_opened(&self) {
        self.connections_active.add(1, &[]);
    }

    /// Decrement the active-connections gauge by 1.
    pub fn connection_closed(&self) {
        self.connections_active.add(-1, &[]);
    }

    /// Increment `connections_rejected_total` by 1. Called from the TCP
    /// accept loop when a new connection is dropped because the
    /// `EXSPEED_MAX_CONNS` semaphore is exhausted.
    pub fn connection_rejected(&self) {
        self.connections_rejected.add(1, &[]);
    }

    /// Record the server uptime in seconds.
    pub fn set_uptime(&self, seconds: f64) {
        self.uptime_seconds.record(seconds, &[]);
    }

    /// Flip the `exspeed_lease_held` gauge for the named lease. Call with
    /// `held=true` on successful acquire and `held=false` on release or loss.
    pub fn set_lease_held(&self, name: &str, held: bool) {
        self.lease_held.record(
            if held { 1 } else { 0 },
            &[KeyValue::new("name", name.to_owned())],
        );
    }

    /// Increment `exspeed_lease_acquire_total` with a `result` label. The
    /// accepted values are exactly `"acquired"`, `"rejected"`, or `"error"`
    /// — keep these stable as operator dashboards depend on them.
    pub fn record_lease_acquire_attempt(&self, name: &str, result: &'static str) {
        self.lease_acquire_total.add(
            1,
            &[
                KeyValue::new("name", name.to_owned()),
                KeyValue::new("result", result),
            ],
        );
    }

    /// Increment `exspeed_lease_lost_total` — fired only on involuntary
    /// lease loss (heartbeat failure / TTL expiry). Clean release must not
    /// call this.
    pub fn record_lease_lost(&self, name: &str) {
        self.lease_lost_total
            .add(1, &[KeyValue::new("name", name.to_owned())]);
    }

    /// Flip the `exspeed_is_leader` gauge. `true` on promotion, `false` on
    /// demotion.
    pub fn set_is_leader(&self, leader: bool) {
        self.is_leader.record(if leader { 1 } else { 0 }, &[]);
    }

    /// Record a leadership transition. `direction` must be either
    /// `"acquired"` or `"lost"` — operator dashboards depend on these labels.
    pub fn record_leader_transition(&self, direction: &'static str) {
        self.leader_transitions_total
            .add(1, &[KeyValue::new("direction", direction)]);
    }

    /// Record the latency of a successful publish, in seconds.
    pub fn record_publish_latency(&self, stream: &str, secs: f64) {
        self.publish_latency_seconds
            .record(secs, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Record the time from a record's write timestamp to when it was
    /// delivered to a consumer, in seconds.
    pub fn record_consume_latency(&self, stream: &str, consumer: &str, secs: f64) {
        self.consume_latency_seconds.record(
            secs,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("consumer", consumer.to_owned()),
            ],
        );
    }

    /// Increment the storage_write_errors counter. `kind` is one of
    /// `"storage_full"` or `"other"`.
    pub fn record_storage_write_error(&self, stream: &str, kind: &'static str) {
        self.storage_write_errors.add(
            1,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("kind", kind),
            ],
        );
    }

    /// Set the fill ratio (0.0–1.0) of a subscription's delivery channel.
    pub fn set_subscription_queue_fill(&self, consumer: &str, subscriber: &str, ratio: f64) {
        self.subscription_queue_fill_ratio.record(
            ratio,
            &[
                KeyValue::new("consumer", consumer.to_owned()),
                KeyValue::new("subscriber", subscriber.to_owned()),
            ],
        );
    }

    // -- dedup helpers -------------------------------------------------------

    /// Increment `exspeed_dedup_writes_total`. `result` is `"written"` or
    /// `"duplicate"`.
    pub fn record_dedup_write(&self, stream: &str, result: &str) {
        self.dedup_writes_total.add(
            1,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("result", result.to_owned()),
            ],
        );
    }

    /// Increment `exspeed_dedup_collisions_total` for the given stream.
    pub fn record_dedup_collision(&self, stream: &str) {
        self.dedup_collisions_total
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Increment `exspeed_dedup_map_full_total` for the given stream.
    pub fn record_dedup_map_full(&self, stream: &str) {
        self.dedup_map_full_total
            .add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Set the current live dedup entry count for a stream.
    pub fn set_dedup_map_entries(&self, stream: &str, n: i64) {
        self.dedup_map_entries
            .record(n, &[KeyValue::new("stream", stream.to_owned())]);
    }

    /// Record the duration of a dedup snapshot write, in seconds.
    pub fn observe_dedup_snapshot_write_duration(&self, secs: f64) {
        self.dedup_snapshot_write_duration_seconds.record(secs, &[]);
    }

    /// Record the duration of a dedup map rebuild, in seconds. `source` is
    /// `"snapshot"` or `"full_scan"`.
    pub fn observe_dedup_rebuild_duration(&self, stream: &str, source: &str, secs: f64) {
        self.dedup_rebuild_duration_seconds.record(
            secs,
            &[
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("source", source.to_owned()),
            ],
        );
    }

    /// Set the configured dedup window in seconds for a stream.
    pub fn set_dedup_window_secs(&self, stream: &str, secs: i64) {
        self.dedup_window_secs
            .record(secs, &[KeyValue::new("stream", stream.to_owned())]);
    }

    // -- auth helpers --------------------------------------------------------

    /// Increment `exspeed_auth_denied_total`. `reason` must be
    /// `"unauthorized"` or `"forbidden"`; `transport` is `"tcp"` or `"http"`;
    /// `op` is the opcode name (TCP) or request path (HTTP). Operator
    /// dashboards depend on this label set — keep it stable.
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

    // -- replication helpers ------------------------------------------------

    /// Set the current replication role. Exactly one label is 1 at a time;
    /// the previous two are reset to 0.
    pub fn set_replication_role(&self, role: &'static str) {
        for candidate in ["leader", "follower", "standalone"] {
            self.replication_role.record(
                if candidate == role { 1 } else { 0 },
                &[KeyValue::new("role", candidate)],
            );
        }
    }

    pub fn inc_replication_follower_queue_drop(&self, follower_id: &str) {
        self.replication_follower_queue_drops_total.add(
            1,
            &[KeyValue::new("follower_id", follower_id.to_string())],
        );
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
        self.replication_connect_attempts_total.add(
            1,
            &[KeyValue::new("result", if ok { "ok" } else { "err" })],
        );
    }
}
