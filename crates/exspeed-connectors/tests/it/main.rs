//! Integration tests for the connector framework and plugins (one binary).
//!
//! - `framework_test`: the checkpoint protocol and the supervisor, driven by
//!   fake sources and sinks against an in-memory log.
//! - `http_test`: `http_poll` and `http_sink` against an in-process server.
//! - `postgres_test`: `postgres_cdc`, `postgres_outbox` and `postgres_poll`
//!   against a real Postgres (`EXSPEED_POSTGRES_URL`, `#[ignore]`d).
//! - `rabbitmq_test`: the `rabbitmq` source and sink against a real RabbitMQ
//!   (`EXSPEED_RABBITMQ_URL`, `#[ignore]`d).
//! - `s3_test`: the `s3` sink against MinIO or another S3-compatible store
//!   (`EXSPEED_S3_ENDPOINT`, `#[ignore]`d).
//! - `jdbc_test`: the `jdbc` sink and `jdbc_poll` against MySQL
//!   (`EXSPEED_MYSQL_URL`) and SQL Server (`EXSPEED_MSSQL_URL`), and
//!   `mssql_cdc` against SQL Server (`#[ignore]`d).
//!
//! The service-backed tests crash the connector mid-stream (a panic between
//! the durable append or write and the checkpoint, or before the external
//! ack), let the supervisor restart it, and check the delivery guarantee.

mod common;
mod framework_test;
mod http_test;
mod jdbc_test;
mod postgres_test;
mod rabbitmq_test;
mod s3_test;
