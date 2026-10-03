//! Built-in plugins.

pub mod http;
pub mod http_poll;
pub mod http_sink;
pub mod http_webhook;
pub mod jdbc;
pub mod jdbc_poll;
pub mod mssql_cdc;
pub mod pg;
pub mod pgoutput;
pub mod postgres_cdc;
pub mod postgres_outbox;
pub mod postgres_poll;
pub mod rabbitmq_sink;
pub mod rabbitmq_source;
pub mod s3_sink;

use crate::registry::Registry;

/// Register every built-in plugin.
pub fn register(r: &mut Registry) {
    // Sources
    r.register_passive_source("http_webhook", |init| {
        http_webhook::WebhookEndpoint::from_init(init).map(drop)
    });
    r.register_source("postgres_cdc", |init| {
        Ok(Box::new(postgres_cdc::PostgresCdcSource::new(init)?))
    });
    r.register_source("postgres_poll", |init| {
        Ok(Box::new(postgres_poll::PostgresPollSource::new(init)?))
    });
    r.register_source("postgres_outbox", |init| {
        Ok(Box::new(postgres_outbox::PostgresOutboxSource::new(init)?))
    });
    r.register_source("jdbc_poll", |init| {
        Ok(Box::new(jdbc_poll::JdbcPollSource::new(init)?))
    });
    r.register_source("mssql_cdc", |init| {
        Ok(Box::new(mssql_cdc::MssqlCdcSource::new(init)?))
    });
    r.register_source("rabbitmq", |init| {
        Ok(Box::new(rabbitmq_source::RabbitmqSource::new(init)?))
    });
    r.register_source("http_poll", |init| {
        Ok(Box::new(http_poll::HttpPollSource::new(init)?))
    });

    // Sinks
    r.register_sink("jdbc", |init| {
        Ok(Box::new(jdbc::JdbcSinkConnector::new(init)?))
    });
    r.register_sink("http_sink", |init| {
        Ok(Box::new(http_sink::HttpSinkConnector::new(init)?))
    });
    r.register_sink("rabbitmq", |init| {
        Ok(Box::new(rabbitmq_sink::RabbitmqSink::new(init)?))
    });
    r.register_sink("s3", |init| {
        Ok(Box::new(s3_sink::S3SinkConnector::new(init)?))
    });
}
