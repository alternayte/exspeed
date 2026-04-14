use crate::parser::error::ParseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExqlError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("storage error: {0}")]
    Storage(String),
}
