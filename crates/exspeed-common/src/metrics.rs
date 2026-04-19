use opentelemetry::metrics::{Counter, Gauge, MeterProvider, UpDownCounter};
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

        // Keep the provider alive — dropping it shuts down the metrics pipeline.
        std::mem::forget(provider);

        let metrics = Metrics {
            records_published,
            records_consumed,
            consumer_lag,
            storage_bytes,
            connections_active,
            uptime_seconds,
            lease_held,
            lease_acquire_total,
            lease_lost_total,
            is_leader,
            leader_transitions_total,
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
}
