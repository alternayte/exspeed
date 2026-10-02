//! Single integration-test binary for this crate (one link step instead of
//! one per file). Each submodule is one former `tests/*.rs` file.

mod common;
mod api_test;
mod broker_test;
mod cluster_followers_endpoint_test;
mod connect_test;
mod connector_dlq_test;
mod connector_test;
mod consumer_test;
mod dedup_test;
mod exql_test;
mod exql_windows_test;
mod jdbc_poll_test;
mod jdbc_sink_mssql_test;
mod jdbc_sink_mysql_test;
mod jdbc_sink_postgres_test;
mod jdbc_sink_sqlite_test;
mod multi_subscriber_test;
mod multi_tenant_auth_test;
mod observability_test;
mod publish_batch_test;
mod seek_test;
mod stream_delete_test;
mod tls_test;
