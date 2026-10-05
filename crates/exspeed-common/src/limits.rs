//! Stream limit and lifetime settings shared by the wire protocol, the HTTP
//! API and the storage config.

use serde::{Deserialize, Serialize};

/// What happens when a stream reaches `max_msgs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DiscardPolicy {
    /// Drop the oldest records to make room.
    #[default]
    Old,
    /// Reject new records until there is room again.
    New,
}

/// When records leave a stream besides the age, size and count limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RetentionPolicy {
    /// Records stay until a limit removes them.
    #[default]
    Limits,
    /// A queue: the stream has at most one consumer, and a record is removed
    /// once that consumer acked it.
    WorkQueue,
    /// A record is removed once every consumer of the stream acked it (and
    /// immediately when the stream has no consumers).
    Interest,
}

impl RetentionPolicy {
    pub fn is_limits(&self) -> bool {
        *self == RetentionPolicy::Limits
    }
}

/// The limit and lifetime settings of a stream (see `StreamConfig` for what
/// each does). Every field defaults to "off".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StreamLimits {
    pub max_msgs: u64,
    pub discard: DiscardPolicy,
    pub max_msgs_per_subject: u64,
    pub allow_msg_ttl: bool,
    pub msg_ttl_ms: u64,
    pub allow_delayed: bool,
    pub retention: RetentionPolicy,
    /// Core messages (Exspeed `CorePublish` or NATS `PUB`) published to a
    /// subject matching one of these filters are also appended to the
    /// stream. No two streams may capture overlapping subjects.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capture_subjects: Vec<String>,
}

impl StreamLimits {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_subjects_are_serialized_only_when_set() {
        let plain = StreamLimits {
            allow_delayed: true,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,"msg_ttl_ms":0,"allow_delayed":true,"retention":"limits"}"#
        );
        let c = StreamLimits {
            capture_subjects: vec!["orders.>".into()],
            ..Default::default()
        };
        // The TypeScript SDK's unit tests pin this exact encoding.
        assert_eq!(
            serde_json::to_string(&c).unwrap(),
            r#"{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,"msg_ttl_ms":0,"allow_delayed":false,"retention":"limits","capture_subjects":["orders.>"]}"#
        );
        assert!(!c.is_default());
    }
}
