//! Replication protocol messages (cluster port). The client protocol lives
//! in [`crate::client`]; this module will be replaced in Phase 6.

pub mod connect;
pub mod replicate;

pub use connect::{AuthType, ConnectRequest, ConnectResponse, WIRE_VERSION};
