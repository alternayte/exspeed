//! Shared auth primitives. See design doc
//! `docs/superpowers/specs/2026-04-20-multi-tenant-auth-design.md`.

pub mod compare;
pub mod error;
pub mod glob;
pub mod store;
pub mod types;

pub use compare::verify_token;
pub use error::AuthError;
pub use glob::StreamGlob;
pub use store::{sha256_hex, CredentialStore, LEGACY_ADMIN_NAME};
pub use types::{Action, Identity, IdentityRef, Permission};
