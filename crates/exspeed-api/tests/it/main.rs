//! Single integration-test binary for this crate (one link step instead of
//! one per file). Each submodule is one former `tests/*.rs` file.

mod common;
mod healthz_leadership_test;
mod leader_gate_test;
mod openapi_test;
mod readyz_test;
