//! Single integration-test binary for this crate (one link step instead of
//! one per file). Each submodule is one former `tests/*.rs` file.

mod cli_help_test;
mod driver_exspeed_test;
mod embedded_server;
mod renderer_test;
mod scenarios_test;
