//! Cluster replication — async follower-pull streaming.
//!
//! # Overview
//!
//! One leader, N followers. Followers dial the leader on the cluster
//! port (`EXSPEED_CLUSTER_BIND`, default 0.0.0.0:5934) and receive a
//! continuous stream of `ReplicationEvent`s: record batches + stream
//! metadata events + heartbeats. Each follower applies via its local
//! `StorageEngine::append` / `create_stream` / `delete_stream` /
//! `trim_up_to` — no internal-file-format coupling.
//!
//! Writes on the leader ack as soon as they hit local disk (pure async
//! replication). RPO on an unclean crash equals replication lag at the
//! moment of failure. See `docs/superpowers/specs/2026-04-20-replication-design.md`.

pub mod client;
pub mod coordinator;
pub mod cursor;
pub mod errors;
pub mod server;
pub mod wire;

pub use client::ReplicationClient;
pub use coordinator::{FollowerSnapshot, ReplicationCoordinator};
pub use errors::ReplicationError;
pub use server::ReplicationServer;

use exspeed_protocol::messages::replicate::{
    RecordsAppended, RetentionTrimmedEvent, RetentionUpdatedEvent, StreamCreatedEvent,
    StreamDeletedEvent,
};

/// Events emitted by the leader's Broker/Retention paths and fanned out
/// to all connected followers. These are the *internal* event shape —
/// the coordinator serializes them to the corresponding opcode payload
/// before sending.
#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    StreamCreated(StreamCreatedEvent),
    StreamDeleted(StreamDeletedEvent),
    RetentionUpdated(RetentionUpdatedEvent),
    RetentionTrimmed(RetentionTrimmedEvent),
    RecordsAppended(RecordsAppended),
}
