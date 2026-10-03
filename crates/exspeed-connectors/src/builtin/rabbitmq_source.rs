//! `rabbitmq` source: consume a queue into a stream.
//!
//! Deliveries are acknowledged (`basic_ack` with `multiple = true`) only in
//! [`SourceConnector::ack`], i.e. after the batch is durable in the log. A
//! crash or lost connection before that leaves the deliveries unacked, and
//! RabbitMQ redelivers them to the next consumer: **at-least-once**.
//!
//! With `dedup_on_message_id = true`, the AMQP `message_id` becomes the
//! record's `x-idempotency-key`, so redeliveries inside the stream's dedup
//! window are dropped by the broker (effectively-once for publishers that
//! set unique message ids).
//!
//! A closed channel or connection surfaces as a `Connection` error; the
//! supervisor then restarts the connector (new connection, new channel).

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    Connection, ConnectionProperties, Consumer,
};
use serde::Deserialize;
use tracing::warn;

use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RabbitmqSourceSettings {
    pub url: String,
    pub queue: String,
    #[serde(default)]
    pub consumer_tag: Option<String>,
    #[serde(default = "default_prefetch", deserialize_with = "de::u16")]
    pub prefetch_count: u16,
    /// Declare the queue on start (idempotent if it exists with the same
    /// arguments).
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub declare_queue: bool,
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub queue_durable: bool,
    #[serde(default, deserialize_with = "de::bool")]
    pub queue_auto_delete: bool,
    /// Use the AMQP `message_id` as the record's idempotency key.
    #[serde(default, deserialize_with = "de::bool")]
    pub dedup_on_message_id: bool,
    /// How long one poll waits for the first delivery.
    #[serde(default = "default_wait", deserialize_with = "de::u64")]
    pub poll_wait_ms: u64,
}

fn default_prefetch() -> u16 {
    100
}
fn default_true() -> bool {
    true
}
fn default_wait() -> u64 {
    500
}

pub struct RabbitmqSource {
    settings: RabbitmqSourceSettings,
    consumer_tag: String,
    subject_template: String,
    connection: Option<Connection>,
    channel: Option<lapin::Channel>,
    consumer: Option<Consumer>,
    /// Highest delivery tag returned by `poll()` and not yet acked.
    pending_tag: Option<u64>,
}

impl RabbitmqSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: RabbitmqSourceSettings = settings::parse("rabbitmq", &init.settings)?;
        if !(s.url.starts_with("amqp://") || s.url.starts_with("amqps://")) {
            return Err(ConnectorError::config(
                "rabbitmq: url must start with amqp:// or amqps://",
            ));
        }
        if s.queue.trim().is_empty() {
            return Err(ConnectorError::config("rabbitmq: queue must not be empty"));
        }
        if s.prefetch_count == 0 {
            return Err(ConnectorError::config(
                "rabbitmq: prefetch_count must be > 0",
            ));
        }
        let consumer_tag = s
            .consumer_tag
            .clone()
            .unwrap_or_else(|| format!("exspeed-{}", init.config.name));
        Ok(Self {
            settings: s,
            consumer_tag,
            subject_template: init.config.subject_template.clone(),
            connection: None,
            channel: None,
            consumer: None,
            pending_tag: None,
        })
    }

    fn to_record(&self, delivery: &lapin::message::Delivery) -> SourceRecord {
        let routing_key = delivery.routing_key.as_str();
        let exchange = delivery.exchange.as_str();
        let props = &delivery.properties;

        let mut headers: Vec<(String, String)> = Vec::new();
        if let Some(table) = props.headers() {
            for (k, v) in table.inner() {
                if let Some(v) = amqp_value_to_string(v) {
                    headers.push((k.as_str().to_string(), v));
                }
            }
        }
        if let Some(ct) = props.content_type() {
            headers.push(("content-type".into(), ct.as_str().to_string()));
        }
        if let Some(c) = props.correlation_id() {
            headers.push(("x-correlation-id".into(), c.as_str().to_string()));
        }
        if let Some(id) = props.message_id() {
            headers.push(("x-message-id".into(), id.as_str().to_string()));
            if self.settings.dedup_on_message_id {
                headers.retain(|(k, _)| !k.eq_ignore_ascii_case("x-idempotency-key"));
                headers.push(("x-idempotency-key".into(), id.as_str().to_string()));
            }
        }
        headers.push(("x-amqp-routing-key".into(), routing_key.to_string()));
        if !exchange.is_empty() {
            headers.push(("x-amqp-exchange".into(), exchange.to_string()));
        }
        if delivery.redelivered {
            headers.push(("x-amqp-redelivered".into(), "true".into()));
        }

        let template = if self.subject_template.is_empty() {
            "{routing_key}"
        } else {
            &self.subject_template
        };
        let json: Option<serde_json::Value> = if template.contains("{$.") {
            serde_json::from_slice(&delivery.data).ok()
        } else {
            None
        };
        let rk = if routing_key.is_empty() {
            self.settings.queue.as_str()
        } else {
            routing_key
        };
        let subject = crate::subject::render(
            template,
            &[
                ("routing_key", rk),
                ("exchange", exchange),
                ("queue", &self.settings.queue),
            ],
            json.as_ref(),
        );
        SourceRecord {
            key: None,
            value: Bytes::from(delivery.data.clone()),
            subject,
            headers,
        }
    }
}

fn amqp_value_to_string(v: &AMQPValue) -> Option<String> {
    Some(match v {
        AMQPValue::Boolean(b) => b.to_string(),
        AMQPValue::ShortShortInt(n) => n.to_string(),
        AMQPValue::ShortShortUInt(n) => n.to_string(),
        AMQPValue::ShortInt(n) => n.to_string(),
        AMQPValue::ShortUInt(n) => n.to_string(),
        AMQPValue::LongInt(n) => n.to_string(),
        AMQPValue::LongUInt(n) => n.to_string(),
        AMQPValue::LongLongInt(n) => n.to_string(),
        AMQPValue::Float(n) => n.to_string(),
        AMQPValue::Double(n) => n.to_string(),
        AMQPValue::ShortString(s) => s.as_str().to_string(),
        AMQPValue::LongString(s) => String::from_utf8_lossy(s.as_bytes()).into_owned(),
        AMQPValue::Timestamp(t) => t.to_string(),
        AMQPValue::Void => return None,
        other => format!("{other:?}"),
    })
}

fn conn_err(what: &str, e: lapin::Error) -> ConnectorError {
    match e {
        lapin::Error::ProtocolError(ref p) if p.get_id() == 403 => {
            ConnectorError::fatal(format!("rabbitmq {what}: access refused: {e}"))
        }
        _ => ConnectorError::connection(format!("rabbitmq {what}: {e}")),
    }
}

#[async_trait]
impl SourceConnector for RabbitmqSource {
    async fn start(&mut self, _checkpoint: Option<String>) -> Result<(), ConnectorError> {
        let conn = Connection::connect(&self.settings.url, ConnectionProperties::default())
            .await
            .map_err(|e| conn_err("connect", e))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| conn_err("create channel", e))?;
        channel
            .basic_qos(self.settings.prefetch_count, BasicQosOptions::default())
            .await
            .map_err(|e| conn_err("basic_qos", e))?;
        if self.settings.declare_queue {
            channel
                .queue_declare(
                    &self.settings.queue,
                    QueueDeclareOptions {
                        durable: self.settings.queue_durable,
                        auto_delete: self.settings.queue_auto_delete,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| conn_err("queue_declare", e))?;
        }
        let consumer = channel
            .basic_consume(
                &self.settings.queue,
                &self.consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| conn_err("basic_consume", e))?;
        self.connection = Some(conn);
        self.channel = Some(channel);
        self.consumer = Some(consumer);
        self.pending_tag = None;
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let first_wait = Duration::from_millis(self.settings.poll_wait_ms.max(1));
        let mut deliveries = Vec::new();
        {
            let consumer = self
                .consumer
                .as_mut()
                .ok_or_else(|| ConnectorError::connection("rabbitmq: not started"))?;
            while deliveries.len() < max_batch.max(1) {
                let wait = if deliveries.is_empty() {
                    first_wait
                } else {
                    Duration::from_millis(5)
                };
                match tokio::time::timeout(wait, consumer.next()).await {
                    Err(_) => break,
                    Ok(None) => return Err(ConnectorError::connection(
                        "rabbitmq: consumer stream ended (channel closed or consumer cancelled)",
                    )),
                    Ok(Some(Err(e))) => return Err(conn_err("delivery", e)),
                    Ok(Some(Ok(d))) => deliveries.push(d),
                }
            }
        }
        let mut records = Vec::with_capacity(deliveries.len());
        for d in &deliveries {
            records.push(self.to_record(d));
            self.pending_tag = Some(
                self.pending_tag
                    .map_or(d.delivery_tag, |t| t.max(d.delivery_tag)),
            );
        }
        // No durable cursor: the queue itself holds the position.
        Ok(SourceBatch {
            records,
            checkpoint: None,
        })
    }

    async fn ack(&mut self, _checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        let Some(tag) = self.pending_tag else {
            return Ok(());
        };
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| ConnectorError::connection("rabbitmq: channel not open"))?;
        channel
            .basic_ack(tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|e| conn_err("basic_ack", e))?;
        self.pending_tag = None;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let (Some(channel), Some(consumer)) = (self.channel.as_ref(), self.consumer.as_ref()) {
            let tag = consumer.tag().as_str().to_string();
            if let Err(e) = channel
                .basic_cancel(&tag, BasicCancelOptions::default())
                .await
            {
                warn!("rabbitmq basic_cancel failed (ignored on stop): {e}");
            }
        }
        self.consumer = None;
        // Closing the channel returns unacked deliveries to the queue.
        if let Some(channel) = self.channel.take() {
            let _ = channel.close(200, "exspeed connector stop").await;
        }
        if let Some(conn) = self.connection.take() {
            let _ = conn.close(200, "exspeed connector stop").await;
        }
        self.pending_tag = None;
        Ok(())
    }

    async fn dry_run(&mut self, max: usize) -> Result<Vec<SourceRecord>, ConnectorError> {
        // Consume without acking; stop() closes the channel, which requeues
        // every delivery.
        self.start(None).await?;
        let polled = self.poll(max).await;
        let _ = self.stop().await;
        Ok(polled?.records.into_iter().take(max).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use serde_json::json;

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new(
                "test-rabbitmq",
                ConnectorType::Source,
                "rabbitmq",
                "events",
            ),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    #[test]
    fn settings_defaults() {
        let s = RabbitmqSource::new(&init(json!({
            "url": "amqp://guest:guest@localhost:5672/%2f", "queue": "q"
        })))
        .unwrap();
        assert_eq!(s.consumer_tag, "exspeed-test-rabbitmq");
        assert_eq!(s.settings.prefetch_count, 100);
        assert!(s.settings.queue_durable);
        assert!(!s.settings.queue_auto_delete);
        assert!(!s.settings.dedup_on_message_id);
    }

    #[test]
    fn settings_native_and_string_overrides() {
        let a = RabbitmqSource::new(&init(json!({
            "url": "amqp://localhost", "queue": "q", "consumer_tag": "t",
            "prefetch_count": 50, "queue_durable": false, "queue_auto_delete": true
        })))
        .unwrap();
        let b = RabbitmqSource::new(&init(json!({
            "url": "amqp://localhost", "queue": "q", "consumer_tag": "t",
            "prefetch_count": "50", "queue_durable": "false", "queue_auto_delete": "true"
        })))
        .unwrap();
        for s in [a, b] {
            assert_eq!(s.consumer_tag, "t");
            assert_eq!(s.settings.prefetch_count, 50);
            assert!(!s.settings.queue_durable);
            assert!(s.settings.queue_auto_delete);
        }
    }

    #[test]
    fn settings_errors() {
        assert!(RabbitmqSource::new(&init(json!({"queue": "q"}))).is_err());
        assert!(RabbitmqSource::new(&init(json!({"url": "amqp://x"}))).is_err());
        assert!(RabbitmqSource::new(&init(json!({"url": "http://x", "queue": "q"}))).is_err());
        let e = RabbitmqSource::new(&init(
            json!({"url": "amqp://x", "queue": "q", "prefech": 1}),
        ))
        .err()
        .unwrap();
        assert!(e.to_string().contains("prefech"), "{e}");
    }

    #[test]
    fn header_values() {
        assert_eq!(
            amqp_value_to_string(&AMQPValue::LongInt(5)).as_deref(),
            Some("5")
        );
        assert_eq!(
            amqp_value_to_string(&AMQPValue::LongString("abc".into())).as_deref(),
            Some("abc")
        );
        assert_eq!(amqp_value_to_string(&AMQPValue::Void), None);
    }
}
