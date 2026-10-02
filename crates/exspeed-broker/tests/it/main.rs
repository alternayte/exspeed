//! Single integration-test binary for this crate (one link step instead of
//! one per file). Each submodule is one former `tests/*.rs` file.

mod replication_coordinator_test;
mod replication_cursor_test;
mod replication_emission_test;
mod replication_retention_emission_test;
mod replication_server_test;
mod replication_wire_test;
