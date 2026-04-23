use async_trait::async_trait;
use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use tracing::warn;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SinkBatch, SinkConnector, WriteResult};

#[derive(Debug)]
pub struct RabbitmqSink {
    url: String,
    exchange: String,
    exchange_type: String,
    routing_key_from: String,
    exchange_durable: bool,
    publisher_confirms: bool,

    connection: Option<Connection>,
    channel: Option<lapin::Channel>,
}

impl RabbitmqSink {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .setting("url")
            .map_err(ConnectorError::Config)?
            .to_string();

        let exchange = config
            .setting("exchange")
            .map_err(ConnectorError::Config)?
            .to_string();

        let exchange_type = config.setting_or("exchange_type", "topic");
        let routing_key_from = config.setting_or("routing_key_from", "subject");

        let exchange_durable: bool = config
            .setting_or("exchange_durable", "true")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid exchange_durable: {e}")))?;

        let publisher_confirms: bool = config
            .setting_or("publisher_confirms", "true")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid publisher_confirms: {e}")))?;

        Ok(Self {
            url,
            exchange,
            exchange_type,
            routing_key_from,
            exchange_durable,
            publisher_confirms,
            connection: None,
            channel: None,
        })
    }

    fn exchange_kind(&self) -> ExchangeKind {
        match self.exchange_type.as_str() {
            "topic" => ExchangeKind::Topic,
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            other => ExchangeKind::Custom(other.to_string()),
        }
    }
}

#[async_trait]
impl SinkConnector for RabbitmqSink {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let conn = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| ConnectorError::Connection(format!("RabbitMQ connect error: {e}")))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| ConnectorError::Connection(format!("create channel error: {e}")))?;

        if self.publisher_confirms {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| ConnectorError::Connection(format!("confirm_select error: {e}")))?;
        }

        channel
            .exchange_declare(
                &self.exchange,
                self.exchange_kind(),
                ExchangeDeclareOptions {
                    durable: self.exchange_durable,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| ConnectorError::Connection(format!("exchange_declare error: {e}")))?;

        self.connection = Some(conn);
        self.channel = Some(channel);
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        let channel = match self.channel.as_ref() {
            Some(ch) => ch,
            None => return Err(ConnectorError::Connection("channel not open".to_string())),
        };

        let mut last_successful_offset: Option<u64> = None;

        for record in &batch.records {
            let routing_key = match self.routing_key_from.as_str() {
                "subject" => record.subject.clone(),
                "key" => record
                    .key
                    .as_ref()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .unwrap_or("")
                    .to_string(),
                literal => literal.to_string(),
            };

            let publish_result = channel
                .basic_publish(
                    &self.exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &record.value,
                    BasicProperties::default(),
                )
                .await;

            match publish_result {
                Err(e) => {
                    let msg = format!("basic_publish error: {e}");
                    return Ok(match last_successful_offset {
                        Some(offset) => WriteResult::PartialSuccess {
                            last_successful_offset: offset,
                        },
                        None => WriteResult::AllFailed(msg),
                    });
                }
                Ok(confirm_handle) => {
                    if self.publisher_confirms {
                        match confirm_handle.await {
                            Err(e) => {
                                let msg = format!("publisher confirm error: {e}");
                                return Ok(match last_successful_offset {
                                    Some(offset) => WriteResult::PartialSuccess {
                                        last_successful_offset: offset,
                                    },
                                    None => WriteResult::AllFailed(msg),
                                });
                            }
                            Ok(confirm) => {
                                if !confirm.is_ack() {
                                    let msg = "message nacked by broker".to_string();
                                    return Ok(match last_successful_offset {
                                        Some(offset) => WriteResult::PartialSuccess {
                                            last_successful_offset: offset,
                                        },
                                        None => WriteResult::AllFailed(msg),
                                    });
                                }
                            }
                        }
                    }
                    last_successful_offset = Some(record.offset);
                }
            }
        }

        Ok(WriteResult::AllSuccess)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        // No-op: lapin publishes are submitted immediately.
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(channel) = self.channel.take() {
            if let Err(e) = channel.close(0, "normal shutdown").await {
                warn!("RabbitMQ sink channel close error (ignored): {e}");
            }
        }

        if let Some(conn) = self.connection.take() {
            if let Err(e) = conn.close(0, "normal shutdown").await {
                warn!("RabbitMQ sink connection close error (ignored): {e}");
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
    use bytes::Bytes;
    use std::collections::HashMap;

    fn make_config(settings: Vec<(&str, &str)>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-rabbitmq-sink".to_string(),
            connector_type: "sink".to_string(),
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
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
            on_transient_exhausted: crate::config::OnTransientExhausted::default(),
            retry: crate::retry::RetryPolicy::default_transient(),
        }
    }

    #[test]
    fn config_parses_required_fields() {
        let config = make_config(vec![
            ("url", "amqp://guest:guest@localhost:5672/%2f"),
            ("exchange", "my-exchange"),
        ]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        assert_eq!(sink.url, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(sink.exchange, "my-exchange");
        assert_eq!(sink.exchange_type, "topic");
        assert_eq!(sink.routing_key_from, "subject");
        assert!(sink.exchange_durable);
        assert!(sink.publisher_confirms);
    }

    #[test]
    fn config_applies_overrides() {
        let config = make_config(vec![
            ("url", "amqp://localhost"),
            ("exchange", "ex"),
            ("exchange_type", "direct"),
            ("routing_key_from", "key"),
            ("exchange_durable", "false"),
            ("publisher_confirms", "false"),
        ]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        assert_eq!(sink.exchange_type, "direct");
        assert_eq!(sink.routing_key_from, "key");
        assert!(!sink.exchange_durable);
        assert!(!sink.publisher_confirms);
    }

    #[test]
    fn config_missing_url_returns_error() {
        let config = make_config(vec![("exchange", "ex")]);
        let err = RabbitmqSink::new(&config).unwrap_err();
        assert!(err.to_string().contains("url"));
    }

    #[test]
    fn config_missing_exchange_returns_error() {
        let config = make_config(vec![("url", "amqp://localhost")]);
        let err = RabbitmqSink::new(&config).unwrap_err();
        assert!(err.to_string().contains("exchange"));
    }

    #[test]
    fn initial_health_is_unhealthy() {
        let config = make_config(vec![("url", "amqp://localhost"), ("exchange", "ex")]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        assert!(sink.connection.is_none());
    }

    #[test]
    fn exchange_kind_mapping() {
        let cases = vec![
            ("topic", ExchangeKind::Topic),
            ("direct", ExchangeKind::Direct),
            ("fanout", ExchangeKind::Fanout),
            ("headers", ExchangeKind::Headers),
            ("x-custom", ExchangeKind::Custom("x-custom".to_string())),
        ];
        for (input, expected) in cases {
            let config = make_config(vec![
                ("url", "amqp://localhost"),
                ("exchange", "ex"),
                ("exchange_type", input),
            ]);
            let sink = RabbitmqSink::new(&config).expect("should parse");
            assert_eq!(sink.exchange_kind(), expected, "exchange_type={input}");
        }
    }

    #[test]
    fn routing_key_from_subject() {
        let config = make_config(vec![
            ("url", "amqp://localhost"),
            ("exchange", "ex"),
            ("routing_key_from", "subject"),
        ]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        // Simulate the routing key derivation logic
        let subject = "orders.created".to_string();
        let key: Option<Bytes> = None;
        let routing_key = match sink.routing_key_from.as_str() {
            "subject" => subject.clone(),
            "key" => key
                .as_ref()
                .and_then(|k| std::str::from_utf8(k).ok())
                .unwrap_or("")
                .to_string(),
            literal => literal.to_string(),
        };
        assert_eq!(routing_key, "orders.created");
    }

    #[test]
    fn routing_key_from_key_bytes() {
        let config = make_config(vec![
            ("url", "amqp://localhost"),
            ("exchange", "ex"),
            ("routing_key_from", "key"),
        ]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        let subject = "ignored".to_string();
        let key: Option<Bytes> = Some(Bytes::from("user.123"));
        let routing_key = match sink.routing_key_from.as_str() {
            "subject" => subject.clone(),
            "key" => key
                .as_ref()
                .and_then(|k| std::str::from_utf8(k).ok())
                .unwrap_or("")
                .to_string(),
            literal => literal.to_string(),
        };
        assert_eq!(routing_key, "user.123");
    }

    #[test]
    fn routing_key_from_literal() {
        let config = make_config(vec![
            ("url", "amqp://localhost"),
            ("exchange", "ex"),
            ("routing_key_from", "events.all"),
        ]);
        let sink = RabbitmqSink::new(&config).expect("should parse");
        let subject = "ignored".to_string();
        let key: Option<Bytes> = None;
        let routing_key = match sink.routing_key_from.as_str() {
            "subject" => subject.clone(),
            "key" => key
                .as_ref()
                .and_then(|k| std::str::from_utf8(k).ok())
                .unwrap_or("")
                .to_string(),
            literal => literal.to_string(),
        };
        assert_eq!(routing_key, "events.all");
    }
}
