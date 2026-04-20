//! Shared auth primitives. See design doc
//! `docs/superpowers/specs/2026-04-20-multi-tenant-auth-design.md`.

pub mod compare;

pub use compare::verify_token;
