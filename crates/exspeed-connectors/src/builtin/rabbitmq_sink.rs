//! `rabbitmq` sink: publish records to an exchange.
//!
//! - Publisher confirms are always on; a record counts as written only once
//!   the broker acks it, so the framework commits the stream offset only
//!   after RabbitMQ has the message: **at-least-once**.
//! - Messages are persistent (`delivery_mode = 2`) by default.
//! - `mandatory = true` (default): a message no queue is bound for is
//!   returned by the broker and treated as **poison** (DLQ, or dropped with
//!   a metric), instead of vanishing silently. The records published after
//!   it in the same batch are not published again.
//! - Record headers become AMQP headers; the record's idempotency key
//!   (`x-idempotency-key`, or `<stream>:<offset>`) becomes `message_id`, so
//!   consumers can drop the duplicates a retry may produce.
//! - A lost connection is a `Connection` error: the supervisor reconnects.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions},
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable, ShortString},
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use serde::Deserialize;
use tracing::warn;

use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, PoisonReason, SinkConnector, SinkRecord, WriteResult};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RabbitmqSinkSettings {
    pub url: String,
    pub exchange: String,
    #[serde(default = "default_exchange_type")]
    pub exchange_type: String,
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub declare_exchange: bool,
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub exchange_durable: bool,
    /// Routing key template: `{subject}`, `{key}`, `{stream}` or `{$.field}`
    /// from the JSON value. Literal text is kept as is.
    #[serde(default = "default_routing_key")]
    pub routing_key: String,
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub persistent: bool,
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub mandatory: bool,
    /// Copy record headers into AMQP headers.
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub propagate_headers: bool,
}

fn default_exchange_type() -> String {
    "topic".into()
}
fn default_routing_key() -> String {
    "{subject}".into()
}
fn default_true() -> bool {
    true
}

pub struct RabbitmqSink {
    settings: RabbitmqSinkSettings,
    stream: String,
    connection: Option<Connection>,
    channel: Option<lapin::Channel>,
    /// Broker outcomes of the records published after a returned
    /// (unroutable) one in the same `write()`. The framework sends those
    /// records again after handling the poison one; they are settled from
    /// here instead of being published a second time.
    settled: VecDeque<(u64, Settled)>,
}

enum Settled {
    Confirmed,
    Returned(String),
}

/// The broker's answer to one publish.
enum Outcome {
    Confirmed,
    Returned(String),
    Nacked,
    Lost(lapin::Error),
}

impl RabbitmqSink {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: RabbitmqSinkSettings = settings::parse("rabbitmq", &init.settings)?;
        if !(s.url.starts_with("amqp://") || s.url.starts_with("amqps://")) {
            return Err(ConnectorError::config(
                "rabbitmq: url must start with amqp:// or amqps://",
            ));
        }
        Ok(Self {
            settings: s,
            stream: init.config.stream.clone(),
            connection: None,
            channel: None,
            settled: VecDeque::new(),
        })
    }

    fn exchange_kind(&self) -> ExchangeKind {
        match self.settings.exchange_type.as_str() {
            "topic" => ExchangeKind::Topic,
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            other => ExchangeKind::Custom(other.to_string()),
        }
    }

    fn routing_key(&self, r: &SinkRecord) -> String {
        let t = &self.settings.routing_key;
        if !t.contains('{') {
            return t.clone();
        }
        let key = r
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .unwrap_or_default();
        let json: Option<serde_json::Value> = if t.contains("{$.") {
            serde_json::from_slice(&r.value).ok()
        } else {
            None
        };
        crate::subject::render(
            t,
            &[
                ("subject", &r.subject),
                ("key", &key),
                ("stream", &self.stream),
            ],
            json.as_ref(),
        )
    }

    fn properties(&self, r: &SinkRecord) -> BasicProperties {
        let mut table = FieldTable::default();
        let mut content_type: Option<String> = None;
        if self.settings.propagate_headers {
            for (k, v) in &r.headers {
                if k.eq_ignore_ascii_case("content-type") {
                    content_type = Some(v.clone());
                    continue;
                }
                if k.len() <= 255 {
                    table.insert(
                        ShortString::from(k.clone()),
                        AMQPValue::LongString(v.clone().into()),
                    );
                }
            }
        }
        table.insert(
            "x-exspeed-offset".into(),
            AMQPValue::LongLongInt(r.offset.min(i64::MAX as u64) as i64),
        );
        table.insert(
            "x-exspeed-subject".into(),
            AMQPValue::LongString(r.subject.clone().into()),
        );
        let mut p = BasicProperties::default()
            .with_headers(table)
            .with_timestamp(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            );
        if self.settings.persistent {
            p = p.with_delivery_mode(2);
        }
        let id = r.idempotency_key(&self.stream);
        if id.len() <= 255 {
            p = p.with_message_id(id.into());
        }
        if let Some(ct) = content_type.filter(|c| c.len() <= 255) {
            p = p.with_content_type(ct.into());
        }
        p
    }
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
impl SinkConnector for RabbitmqSink {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let conn = Connection::connect(&self.settings.url, ConnectionProperties::default())
            .await
            .map_err(|e| conn_err("connect", e))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| conn_err("create channel", e))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| conn_err("confirm_select", e))?;
        if self.settings.declare_exchange {
            channel
                .exchange_declare(
                    &self.settings.exchange,
                    self.exchange_kind(),
                    ExchangeDeclareOptions {
                        durable: self.settings.exchange_durable,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| conn_err("exchange_declare", e))?;
        }
        self.connection = Some(conn);
        self.channel = Some(channel);
        self.settled.clear();
        Ok(())
    }

    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError> {
        // Records already published by the previous call (after a returned
        // one): settle them without publishing again.
        let mut base = 0;
        while let Some(r) = records.get(base) {
            match self.settled.front() {
                Some((offset, _)) if *offset == r.offset => {}
                _ => break,
            }
            match self.settled.pop_front().map(|(_, s)| s) {
                Some(Settled::Returned(detail)) => {
                    return Ok(WriteResult::Poison {
                        index: base,
                        reason: PoisonReason::SinkRejected { detail },
                    })
                }
                _ => base += 1,
            }
        }
        self.settled.clear();
        let records = &records[base..];
        if records.is_empty() {
            return Ok(WriteResult::Accepted);
        }

        let channel = self
            .channel
            .clone()
            .ok_or_else(|| ConnectorError::connection("rabbitmq: channel not open"))?;
        let opts = BasicPublishOptions {
            mandatory: self.settings.mandatory,
            ..Default::default()
        };

        // Pipeline the publishes, then wait for the confirms in order.
        let mut confirms = Vec::with_capacity(records.len());
        for r in records {
            let rk = self.routing_key(r);
            match channel
                .basic_publish(
                    &self.settings.exchange,
                    &rk,
                    opts,
                    &r.value,
                    self.properties(r),
                )
                .await
            {
                Ok(c) => confirms.push(c),
                Err(e) => {
                    // Earlier publishes may still be confirmed; count them.
                    let accepted = await_confirms(confirms).await.unwrap_or(0);
                    return Ok(WriteResult::Failed {
                        accepted: base + accepted,
                        error: conn_err("publish", e),
                    });
                }
            }
        }
        // Await every confirm, also past a returned message: those records
        // are already published, and re-sending them would duplicate them.
        let mut outcomes = Vec::with_capacity(confirms.len());
        for c in confirms {
            outcomes.push(match c.await {
                Ok(Confirmation::Ack(None)) | Ok(Confirmation::NotRequested) => Outcome::Confirmed,
                Ok(Confirmation::Ack(Some(ret))) => Outcome::Returned(format!(
                    "unroutable: exchange '{}' returned the message ({} {})",
                    self.settings.exchange,
                    ret.reply_code,
                    ret.reply_text.as_str()
                )),
                Ok(Confirmation::Nack(_)) => Outcome::Nacked,
                Err(e) => Outcome::Lost(e),
            });
        }
        let mut outcomes = outcomes.into_iter().enumerate();
        while let Some((i, o)) = outcomes.next() {
            match o {
                Outcome::Confirmed => {}
                Outcome::Returned(detail) => {
                    // Remember the outcomes after it, up to the first one
                    // that isn't settled (that one and later are re-sent).
                    for (j, o) in outcomes.by_ref() {
                        let s = match o {
                            Outcome::Confirmed => Settled::Confirmed,
                            Outcome::Returned(d) => Settled::Returned(d),
                            Outcome::Nacked | Outcome::Lost(_) => break,
                        };
                        self.settled.push_back((records[j].offset, s));
                    }
                    return Ok(WriteResult::Poison {
                        index: base + i,
                        reason: PoisonReason::SinkRejected { detail },
                    });
                }
                Outcome::Nacked => {
                    return Ok(WriteResult::Failed {
                        accepted: base + i,
                        error: ConnectorError::transient("rabbitmq: broker nacked the message"),
                    })
                }
                Outcome::Lost(e) => {
                    return Ok(WriteResult::Failed {
                        accepted: base + i,
                        error: conn_err("publisher confirm", e),
                    })
                }
            }
        }
        Ok(WriteResult::Accepted)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        // Every accepted record was already confirmed by the broker.
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(channel) = self.channel.take() {
            if let Err(e) = channel.close(200, "exspeed connector stop").await {
                warn!("rabbitmq sink channel close failed (ignored): {e}");
            }
        }
        if let Some(conn) = self.connection.take() {
            let _ = conn.close(200, "exspeed connector stop").await;
        }
        Ok(())
    }

    fn default_flush_interval(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
}

/// Count the leading confirmed publishes.
async fn await_confirms(
    confirms: Vec<lapin::publisher_confirm::PublisherConfirm>,
) -> Option<usize> {
    let mut n = 0;
    for c in confirms {
        match c.await {
            Ok(Confirmation::Ack(None)) => n += 1,
            _ => break,
        }
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use bytes::Bytes;
    use serde_json::json;

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new(
                "test-rabbitmq-sink",
                ConnectorType::Sink,
                "rabbitmq",
                "events",
            ),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    fn record(subject: &str, key: Option<&str>, value: &str) -> SinkRecord {
        SinkRecord {
            offset: 42,
            timestamp: 0,
            subject: subject.into(),
            key: key.map(|k| Bytes::from(k.to_string())),
            value: Bytes::from(value.to_string()),
            headers: vec![("trace-id".into(), "t1".into())],
        }
    }

    #[test]
    fn settings_defaults_and_errors() {
        let s =
            RabbitmqSink::new(&init(json!({"url": "amqp://localhost", "exchange": "ex"}))).unwrap();
        assert_eq!(s.settings.exchange_type, "topic");
        assert!(s.settings.persistent && s.settings.mandatory && s.settings.exchange_durable);
        assert!(RabbitmqSink::new(&init(json!({"exchange": "ex"}))).is_err());
        assert!(RabbitmqSink::new(&init(json!({"url": "amqp://x"}))).is_err());
        let e = RabbitmqSink::new(&init(
            json!({"url": "amqp://x", "exchange": "e", "routing_key_from": "key"}),
        ))
        .err()
        .unwrap();
        assert!(e.to_string().contains("routing_key_from"), "{e}");
    }

    #[test]
    fn exchange_kind_mapping() {
        for (input, expected) in [
            ("topic", ExchangeKind::Topic),
            ("direct", ExchangeKind::Direct),
            ("fanout", ExchangeKind::Fanout),
            ("headers", ExchangeKind::Headers),
            ("x-custom", ExchangeKind::Custom("x-custom".to_string())),
        ] {
            let s = RabbitmqSink::new(&init(
                json!({"url": "amqp://localhost", "exchange": "ex", "exchange_type": input}),
            ))
            .unwrap();
            assert_eq!(s.exchange_kind(), expected, "{input}");
        }
    }

    #[test]
    fn routing_key_templates() {
        let r = record("orders.created", Some("user.123"), r#"{"region":"eu"}"#);
        let rk = |t: &str| {
            RabbitmqSink::new(&init(
                json!({"url": "amqp://l", "exchange": "e", "routing_key": t}),
            ))
            .unwrap()
            .routing_key(&r)
        };
        assert_eq!(rk("{subject}"), "orders.created");
        assert_eq!(rk("{key}"), "user.123");
        assert_eq!(rk("events.all"), "events.all");
        assert_eq!(rk("{$.region}.{subject}"), "eu.orders.created");
    }

    #[test]
    fn properties_are_persistent_with_headers_and_message_id() {
        let s = RabbitmqSink::new(&init(json!({"url": "amqp://l", "exchange": "e"}))).unwrap();
        let p = s.properties(&record("a", None, "{}"));
        assert_eq!(*p.delivery_mode(), Some(2));
        assert_eq!(
            p.message_id().as_ref().map(|m| m.as_str()),
            Some("events:42")
        );
        let h = p.headers().as_ref().unwrap().inner();
        assert!(h.contains_key("trace-id"));
        assert!(h.contains_key("x-exspeed-offset"));
    }
}
