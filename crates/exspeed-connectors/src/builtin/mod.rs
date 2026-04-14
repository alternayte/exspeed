pub mod http_sink;
pub mod http_webhook;
pub mod postgres_outbox;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, SinkConnector, SourceConnector};

pub fn create_source(
    plugin: &str,
    config: &ConnectorConfig,
) -> Result<Box<dyn SourceConnector>, ConnectorError> {
    match plugin {
        "postgres_outbox" => Ok(Box::new(postgres_outbox::PostgresOutboxSource::new(
            config,
        )?)),
        other => Err(ConnectorError::Config(format!(
            "unknown source plugin: {other}"
        ))),
    }
}

pub fn create_sink(
    plugin: &str,
    config: &ConnectorConfig,
) -> Result<Box<dyn SinkConnector>, ConnectorError> {
    match plugin {
        "http_sink" => Ok(Box::new(http_sink::HttpSinkConnector::new(config)?)),
        other => Err(ConnectorError::Config(format!(
            "unknown sink plugin: {other}"
        ))),
    }
}
