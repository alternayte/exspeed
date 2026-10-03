//! Integration tests for the connector framework and plugins (one binary).
//!
//! - `framework_test`: the checkpoint protocol and the supervisor, driven by
//!   fake sources and sinks against an in-memory log.
//! - `http_test`: `http_poll` and `http_sink` against an in-process server.
//! - `postgres_test`: `postgres_cdc`, `postgres_outbox` and `postgres_poll`
//!   against a real Postgres (`EXSPEED_POSTGRES_URL`, `#[ignore]`d).

mod common;
mod framework_test;
mod http_test;
mod postgres_test;
