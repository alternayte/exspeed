use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unknown opcode: 0x{0:02x}")]
    UnknownOpCode(u8),

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: u32, max: u32 },

    #[error("incomplete frame: need {needed} bytes, have {have}")]
    IncompleteFrame { needed: usize, have: usize },

    #[error("decode error: {0}")]
    Decode(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
