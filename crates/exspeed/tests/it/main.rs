//! Single integration-test binary for this crate (one link step instead of
//! one per file). Each submodule is one former `tests/*.rs` file.

mod api_test;
mod backup_test;
mod catalog_test;
mod cluster_test;
mod common;
mod connector_dlq_test;
mod connector_test;
mod consumer_test;
mod crash_test;
mod dedup_test;
mod exql_test;
mod exql_windows_test;
mod jdbc_poll_test;
mod jdbc_sink_mssql_test;
mod jdbc_sink_mysql_test;
mod jdbc_sink_postgres_test;
mod jdbc_sink_sqlite_test;
mod multi_tenant_auth_test;
mod observability_test;
mod protocol_test;
mod stream_delete_test;
mod tls_test;
