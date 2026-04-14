use async_trait::async_trait;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SinkBatch, SinkConnector, WriteResult};

pub struct HttpSinkConnector;

impl HttpSinkConnector {
    pub fn new(_config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        Ok(Self)
    }
}

#[async_trait]
impl SinkConnector for HttpSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write(&mut self, _batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::AllSuccess)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus::Healthy
    }
}
