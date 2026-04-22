pub mod http_poll;
pub mod http_sink;
pub mod http_webhook;
pub mod jdbc;
pub mod pgoutput;
pub mod postgres;
pub mod postgres_outbox;
pub mod rabbitmq_sink;
pub mod rabbitmq_source;
pub mod s3_sink;

use std::sync::Arc;

use exspeed_common::Metrics;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, SinkConnector, SourceConnector};

pub fn create_source(
    plugin: &str,
    config: &ConnectorConfig,
) -> Result<Box<dyn SourceConnector>, ConnectorError> {
    match plugin {
        "postgres_outbox" => Ok(Box::new(postgres_outbox::PostgresOutboxSource::new(config)?)),
        "postgres" => Ok(Box::new(postgres::PostgresSource::new(config)?)),
        "rabbitmq" => Ok(Box::new(rabbitmq_source::RabbitmqSource::new(config)?)),
        "http_poll" => Ok(Box::new(http_poll::HttpPollSource::new(config)?)),
        other => Err(ConnectorError::Config(format!(
            "unknown source plugin: {other}"
        ))),
    }
}

pub fn create_sink(
    plugin: &str,
    config: &ConnectorConfig,
    metrics: Arc<Metrics>,
) -> Result<Box<dyn SinkConnector>, ConnectorError> {
    match plugin {
        "http_sink" => Ok(Box::new(http_sink::HttpSinkConnector::new(config)?)),
        "jdbc" => Ok(Box::new(
            jdbc::JdbcSinkConnector::new(config)?.with_metrics(metrics),
        )),
        "rabbitmq" => Ok(Box::new(rabbitmq_sink::RabbitmqSink::new(config)?)),
        "s3" => Ok(Box::new(s3_sink::S3SinkConnector::new(config)?)),
        other => Err(ConnectorError::Config(format!(
            "unknown sink plugin: {other}"
        ))),
    }
}
