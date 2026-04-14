use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    Sql(String),

    #[error("unsupported SQL feature: {0}")]
    Unsupported(String),

    #[error("transform error: {0}")]
    Transform(String),
}
