use std::fmt;

/// A validated stream name. Must be non-empty, ASCII alphanumeric + hyphens + underscores.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamName(String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidName(pub String);

impl fmt::Display for InvalidName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid name: '{}'", self.0)
    }
}

impl std::error::Error for InvalidName {}

impl StreamName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for StreamName {
    type Error = InvalidName;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.is_empty() {
            return Err(InvalidName("stream name cannot be empty".into()));
        }
        if s.len() > MAX_NAME_LEN {
            return Err(InvalidName(format!(
                "stream name length {} exceeds max {}",
                s.len(),
                MAX_NAME_LEN
            )));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(InvalidName(format!(
                "stream name '{}' contains invalid characters (allowed: a-z, A-Z, 0-9, -, _)",
                s
            )));
        }
        Ok(StreamName(s))
    }
}

impl TryFrom<&str> for StreamName {
    type Error = InvalidName;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        StreamName::try_from(s.to_string())
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Partition identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u32);

/// Record offset within a partition. Monotonically increasing, starts at 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

/// Protocol version.
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Default server port.
pub const DEFAULT_PORT: u16 = 5933;

/// Maximum frame payload size (16MB).
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// Frame header size in bytes.
pub const FRAME_HEADER_SIZE: usize = 10;

/// Maximum length, in bytes, for any user-provided resource name
/// (stream, consumer, view, group, subject filter).
pub const MAX_NAME_LEN: usize = 255;

/// Validate a resource name (consumer/view/group). Less restrictive than
/// `StreamName` (used for subject filters, group names, etc).
pub fn validate_resource_name(name: &str, kind: &str) -> Result<(), InvalidName> {
    if name.is_empty() {
        return Err(InvalidName(format!("{kind} cannot be empty")));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(InvalidName(format!(
            "{kind} length {} exceeds max {}",
            name.len(),
            MAX_NAME_LEN
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_stream_name() {
        let name = StreamName::try_from("order-events").unwrap();
        assert_eq!(name.as_str(), "order-events");
    }

    #[test]
    fn stream_name_with_underscores() {
        let name = StreamName::try_from("order_events_v2").unwrap();
        assert_eq!(name.as_str(), "order_events_v2");
    }

    #[test]
    fn empty_stream_name_rejected() {
        let result = StreamName::try_from("");
        assert!(result.is_err());
    }

    #[test]
    fn stream_name_with_dots_rejected() {
        let result = StreamName::try_from("order.events");
        assert!(result.is_err());
    }

    #[test]
    fn stream_name_with_spaces_rejected() {
        let result = StreamName::try_from("order events");
        assert!(result.is_err());
    }

    #[test]
    fn stream_name_display() {
        let name = StreamName::try_from("my-stream").unwrap();
        assert_eq!(format!("{}", name), "my-stream");
    }

    #[test]
    fn stream_name_over_max_len_rejected() {
        let too_long = "a".repeat(MAX_NAME_LEN + 1);
        assert!(StreamName::try_from(too_long).is_err());
    }

    #[test]
    fn stream_name_at_max_len_accepted() {
        let exactly_max = "a".repeat(MAX_NAME_LEN);
        assert!(StreamName::try_from(exactly_max).is_ok());
    }

    #[test]
    fn validate_resource_name_rejects_empty() {
        assert!(validate_resource_name("", "consumer").is_err());
    }

    #[test]
    fn validate_resource_name_accepts_normal() {
        assert!(validate_resource_name("my-consumer", "consumer").is_ok());
    }

    #[test]
    fn validate_resource_name_rejects_too_long() {
        let too_long = "x".repeat(MAX_NAME_LEN + 1);
        assert!(validate_resource_name(&too_long, "consumer").is_err());
    }
}
