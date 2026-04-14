use async_trait::async_trait;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector};

pub struct PostgresOutboxSource;

impl PostgresOutboxSource {
    pub fn new(_config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        Ok(Self)
    }
}

#[async_trait]
impl SourceConnector for PostgresOutboxSource {
    async fn start(&mut self, _last_position: Option<String>) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll(&mut self, _max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        Ok(SourceBatch {
            records: vec![],
            position: None,
        })
    }

    async fn commit(&mut self, _position: String) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus::Healthy
    }
}
