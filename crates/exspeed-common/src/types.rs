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
}
