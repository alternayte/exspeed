use thiserror::Error;

/// Top-level error type for the exspeed system.
#[derive(Debug, Error)]
pub enum ExspeedError {
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("invalid name: {0}")]
    InvalidName(#[from] crate::types::InvalidName),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StreamName;

    #[test]
    fn invalid_name_converts_to_exspeed_error() {
        fn try_create() -> Result<StreamName, ExspeedError> {
            Ok(StreamName::try_from("")?)
        }
        let err = try_create().unwrap_err();
        assert!(matches!(err, ExspeedError::InvalidName(_)));
    }

    #[test]
    fn error_display() {
        let err = ExspeedError::Protocol("bad frame".into());
        assert_eq!(err.to_string(), "protocol error: bad frame");
    }
}
