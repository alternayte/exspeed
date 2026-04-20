//! Shared auth primitives. See design doc
//! `docs/superpowers/specs/2026-04-20-multi-tenant-auth-design.md`.

pub mod compare;
pub mod error;
pub mod glob;

pub use compare::verify_token;
pub use error::AuthError;
pub use glob::StreamGlob;
