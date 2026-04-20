//! Payload types for the cluster-replication opcodes defined in
//! `crate::opcodes`. Encoded/decoded via `bincode` + the existing frame
//! codec. Designed to be additive: any future field lands at the end of
//! a struct; followers rejecting unknown trailing bytes is a version-skew
//! error, not a silent data loss.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Current replication wire version. Increment on breaking changes.
pub const REPLICATION_WIRE_VERSION: u16 = 1;

/// Follower -> Leader handshake.
///
/// Sent immediately after the TCP accept + auth handshake. The leader
/// responds with `ClusterManifest`, then streams events from each
/// cursor entry forward.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateResume {
    pub follower_id: Uuid,
    pub wire_version: u16,
    /// `{stream_name -> next_offset}`. Empty map = fresh follower.
    pub cursor: BTreeMap<String, u64>,
}

/// Leader -> Follower handshake response. Sent exactly once per
/// connection, immediately after receiving a valid `ReplicateResume`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterManifest {
    pub streams: Vec<StreamSummary>,
    /// Leader's `holder_id` at the moment of handshake. Followers use this
    /// to detect failover mid-session (if it changes on reconnect, promotion
    /// happened).
    pub leader_holder_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamSummary {
    pub name: String,
    pub max_age_secs: u64,
    pub max_bytes: u64,
    pub earliest_offset: u64,
    pub latest_offset: u64,
}

/// Leader -> Follower: a batch of records appended to `stream`, starting
/// at `base_offset`. `records.len()` is the count; offsets are
/// `base_offset, base_offset+1, ...`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordsAppended {
    pub stream: String,
    pub base_offset: u64,
    pub records: Vec<ReplicatedRecord>,
}

/// Same shape as `exspeed_streams::Record` but serialized for the wire.
/// Includes `msg_id` so follower's dedup map stays synced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicatedRecord {
    pub subject: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub timestamp_ms: u64,
    pub msg_id: Option<String>,
}

/// Leader -> Follower: stream metadata events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamCreatedEvent {
    pub name: String,
    pub max_age_secs: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamDeletedEvent {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionUpdatedEvent {
    pub stream: String,
    pub max_age_secs: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionTrimmedEvent {
    pub stream: String,
    /// Records earlier than this offset are deleted on the leader; follower
    /// applies `trim_up_to(stream, new_earliest_offset)`.
    pub new_earliest_offset: u64,
}

/// Leader -> Follower: the follower's cursor for `stream` is earlier than
/// the leader's `earliest_offset` (retention advanced past the cursor).
/// Follower must drop local data for `stream`, recreate it empty, and
/// resume from `new_earliest_offset`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReseedEvent {
    pub stream: String,
    pub new_earliest_offset: u64,
}

/// Bidirectional keepalive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplicationHeartbeat {}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(v: &T) {
        let bytes = bincode::serialize(v).expect("serialize");
        let back: T = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(v, &back);
    }

    #[test]
    fn replicate_resume_roundtrip() {
        let mut cursor = BTreeMap::new();
        cursor.insert("orders".to_string(), 42u64);
        cursor.insert("payments".to_string(), 0u64);
        let v = ReplicateResume {
            follower_id: Uuid::new_v4(),
            wire_version: REPLICATION_WIRE_VERSION,
            cursor,
        };
        roundtrip(&v);
    }

    #[test]
    fn cluster_manifest_roundtrip() {
        let v = ClusterManifest {
            streams: vec![StreamSummary {
                name: "orders".into(),
                max_age_secs: 86400,
                max_bytes: 1_000_000,
                earliest_offset: 100,
                latest_offset: 500,
            }],
            leader_holder_id: Uuid::new_v4(),
        };
        roundtrip(&v);
    }

    #[test]
    fn records_appended_roundtrip() {
        let v = RecordsAppended {
            stream: "orders".into(),
            base_offset: 1000,
            records: vec![
                ReplicatedRecord {
                    subject: "order.placed".into(),
                    payload: vec![1, 2, 3],
                    headers: vec![("trace-id".into(), "abc".into())],
                    timestamp_ms: 1_700_000_000_000,
                    msg_id: Some("msg-1".into()),
                },
                ReplicatedRecord {
                    subject: "order.placed".into(),
                    payload: vec![4, 5, 6],
                    headers: vec![],
                    timestamp_ms: 1_700_000_000_001,
                    msg_id: None,
                },
            ],
        };
        roundtrip(&v);
    }

    #[test]
    fn metadata_events_roundtrip() {
        roundtrip(&StreamCreatedEvent {
            name: "orders".into(),
            max_age_secs: 3600,
            max_bytes: 500_000,
        });
        roundtrip(&StreamDeletedEvent {
            name: "orders".into(),
        });
        roundtrip(&RetentionUpdatedEvent {
            stream: "orders".into(),
            max_age_secs: 7200,
            max_bytes: 1_000_000,
        });
        roundtrip(&RetentionTrimmedEvent {
            stream: "orders".into(),
            new_earliest_offset: 50,
        });
        roundtrip(&StreamReseedEvent {
            stream: "orders".into(),
            new_earliest_offset: 51004,
        });
        roundtrip(&ReplicationHeartbeat::default());
    }

    #[test]
    fn wire_version_constant_is_1() {
        assert_eq!(REPLICATION_WIRE_VERSION, 1);
    }
}
