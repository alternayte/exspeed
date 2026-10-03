//! External databases, reachable from bounded queries through registered
//! connections only.

pub mod connections;
pub mod postgres;

pub use connections::{ConnectionConfig, ConnectionRegistry};
pub use postgres::{ExternalConfig, ExternalTables};
