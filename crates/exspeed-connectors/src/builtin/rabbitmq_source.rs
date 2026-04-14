use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, Consumer,
};
use std::time::Duration;
use tracing::{error, warn};

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug)]
pub struct RabbitmqSource {
    url: String,
    queue: String,
    consumer_tag: String,
    prefetch_count: u16,
    queue_durable: bool,
    queue_auto_delete: bool,

    connection: Option<Connection>,
    channel: Option<lapin::Channel>,
    consumer: Option<Consumer>,
    /// Last delivery tag seen in poll(); ACKed in commit().
    pending_delivery_tag: Option<u64>,
}

impl RabbitmqSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .setting("url")
            .map_err(ConnectorError::Config)?
            .to_string();

        let queue = config
            .setting("queue")
            .map_err(ConnectorError::Config)?
            .to_string();

        let default_tag = format!("exspeed-{}", config.name);
        let consumer_tag = config.setting_or("consumer_tag", &default_tag);

        let prefetch_count: u16 = config
            .setting_or("prefetch_count", "100")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid prefetch_count: {e}")))?;

        let queue_durable: bool = config
            .setting_or("queue_durable", "true")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid queue_durable: {e}")))?;

        let queue_auto_delete: bool = config
            .setting_or("queue_auto_delete", "false")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid queue_auto_delete: {e}")))?;

        Ok(Self {
            url,
            queue,
            consumer_tag,
            prefetch_count,
            queue_durable,
            queue_auto_delete,
            connection: None,
            channel: None,
            consumer: None,
            pending_delivery_tag: None,
        })
    }
}

#[async_trait]
impl SourceConnector for RabbitmqSource {
    async fn start(&mut self, _last_position: Option<String>) -> Result<(), ConnectorError> {
        let conn = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| ConnectorError::Connection(format!("RabbitMQ connect error: {e}")))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| ConnectorError::Connection(format!("create channel error: {e}")))?;

        channel
            .basic_qos(self.prefetch_count, BasicQosOptions::default())
            .await
            .map_err(|e| ConnectorError::Connection(format!("basic_qos error: {e}")))?;

        channel
            .queue_declare(
                &self.queue,
                QueueDeclareOptions {
                    durable: self.queue_durable,
                    auto_delete: self.queue_auto_delete,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| ConnectorError::Connection(format!("queue_declare error: {e}")))?;

        let consumer = channel
            .basic_consume(
                &self.queue,
                &self.consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| ConnectorError::Connection(format!("basic_consume error: {e}")))?;

        self.connection = Some(conn);
        self.channel = Some(channel);
        self.consumer = Some(consumer);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let consumer = match self.consumer.as_mut() {
            Some(c) => c,
            None => {
                return Err(ConnectorError::Connection(
                    "consumer not started".to_string(),
                ))
            }
        };

        let mut records = Vec::new();
        let mut last_delivery_tag: Option<u64> = None;
        let poll_timeout = Duration::from_millis(50);

        for _ in 0..max_batch {
            match tokio::time::timeout(poll_timeout, consumer.next()).await {
                // Timeout — no more messages available right now
                Err(_) => break,
                // Consumer was canceled or closed
                Ok(None) => {
                    warn!("RabbitMQ consumer stream ended");
                    break;
                }
                Ok(Some(delivery_result)) => match delivery_result {
                    Err(e) => {
                        error!("RabbitMQ delivery error: {e}");
                        break;
                    }
                    Ok(delivery) => {
                        let routing_key = delivery.routing_key.as_str().to_string();

                        let key = if routing_key.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(routing_key.clone().into_bytes()))
                        };

                        let subject = routing_key.clone();
                        let value = Bytes::from(delivery.data.clone());

                        let mut headers: Vec<(String, String)> = Vec::new();
                        if let Some(ct) = delivery.properties.content_type() {
                            headers.push(("content-type".to_string(), ct.as_str().to_string()));
                        }
                        if let Some(corr_id) = delivery.properties.correlation_id() {
                            headers.push((
                                "x-correlation-id".to_string(),
                                corr_id.as_str().to_string(),
                            ));
                        }
                        if let Some(msg_id) = delivery.properties.message_id() {
                            headers.push(("x-message-id".to_string(), msg_id.as_str().to_string()));
                        }

                        last_delivery_tag = Some(delivery.delivery_tag);
                        records.push(SourceRecord {
                            key,
                            value,
                            subject,
                            headers,
                        });
                    }
                },
            }
        }

        self.pending_delivery_tag = last_delivery_tag;

        let position = last_delivery_tag.map(|t| t.to_string());
        Ok(SourceBatch { records, position })
    }

    async fn commit(&mut self, position: String) -> Result<(), ConnectorError> {
        let delivery_tag: u64 = position
            .parse()
            .map_err(|e| ConnectorError::Data(format!("invalid delivery_tag position: {e}")))?;

        let channel = match self.channel.as_ref() {
            Some(ch) => ch,
            None => return Err(ConnectorError::Connection("channel not open".to_string())),
        };

        channel
            .basic_ack(delivery_tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|e| ConnectorError::Connection(format!("basic_ack error: {e}")))?;

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        // Cancel consumer
        if let (Some(channel), Some(consumer)) = (self.channel.as_ref(), self.consumer.as_ref()) {
            let tag = consumer.tag().as_str().to_string();
            if let Err(e) = channel
                .basic_cancel(&tag, BasicCancelOptions::default())
                .await
            {
                warn!("RabbitMQ basic_cancel error (ignored on stop): {e}");
            }
        }
        self.consumer = None;

        // Close channel
        if let Some(channel) = self.channel.take() {
            if let Err(e) = channel.close(0, "normal shutdown").await {
                warn!("RabbitMQ channel close error (ignored): {e}");
            }
        }

        // Close connection
        if let Some(conn) = self.connection.take() {
            if let Err(e) = conn.close(0, "normal shutdown").await {
                warn!("RabbitMQ connection close error (ignored): {e}");
            }
        }

        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.connection.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("not connected".to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectorConfig;
    use std::collections::HashMap;

    fn make_config(settings: Vec<(&str, &str)>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-rabbitmq".to_string(),
            connector_type: "source".to_string(),
            plugin: "rabbitmq".to_string(),
            stream: "events".to_string(),
            subject_template: String::new(),
            subject_filter: String::new(),
            settings: settings
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            batch_size: 100,
            poll_interval_ms: 50,
        }
    }

    #[test]
    fn config_parses_required_fields() {
        let config = make_config(vec![
            ("url", "amqp://guest:guest@localhost:5672/%2f"),
            ("queue", "my-queue"),
        ]);
        let source = RabbitmqSource::new(&config).expect("should parse");
        assert_eq!(source.url, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(source.queue, "my-queue");
        assert_eq!(source.consumer_tag, "exspeed-test-rabbitmq");
        assert_eq!(source.prefetch_count, 100);
        assert!(source.queue_durable);
        assert!(!source.queue_auto_delete);
    }

    #[test]
    fn config_applies_overrides() {
        let config = make_config(vec![
            ("url", "amqp://localhost"),
            ("queue", "q"),
            ("consumer_tag", "my-tag"),
            ("prefetch_count", "50"),
            ("queue_durable", "false"),
            ("queue_auto_delete", "true"),
        ]);
        let source = RabbitmqSource::new(&config).expect("should parse");
        assert_eq!(source.consumer_tag, "my-tag");
        assert_eq!(source.prefetch_count, 50);
        assert!(!source.queue_durable);
        assert!(source.queue_auto_delete);
    }

    #[test]
    fn config_missing_url_returns_error() {
        let config = make_config(vec![("queue", "q")]);
        let err = RabbitmqSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("url"));
    }

    #[test]
    fn config_missing_queue_returns_error() {
        let config = make_config(vec![("url", "amqp://localhost")]);
        let err = RabbitmqSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("queue"));
    }

    #[test]
    fn initial_health_is_unhealthy() {
        let config = make_config(vec![("url", "amqp://localhost"), ("queue", "q")]);
        let source = RabbitmqSource::new(&config).expect("should parse");
        // Before start(), connection is None
        assert!(source.connection.is_none());
    }
}
