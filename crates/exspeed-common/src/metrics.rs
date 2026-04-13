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

        // Keep the provider alive — dropping it shuts down the metrics pipeline.
        std::mem::forget(provider);

        let metrics = Metrics {
            records_published,
            records_consumed,
            consumer_lag,
            storage_bytes,
            connections_active,
            uptime_seconds,
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
}
