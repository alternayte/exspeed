use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use exspeed_broker::log::LogError;
use exspeed_streams::StorageError;
use serde_json::{json, Value as JsonValue};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExqlError {
    #[error("parse error: {message}")]
    Parse {
        message: String,
        line: Option<u64>,
        column: Option<u64>,
    },

    #[error("not supported: {feature}")]
    Unsupported { feature: String, hint: String },

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("query exceeded its memory limit: {0}")]
    ResourcesExhausted(String),

    #[error("query timed out after {0} ms")]
    Timeout(u64),

    #[error("query cancelled")]
    Cancelled,

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    Conflict(String),

    #[error("not the leader; continuous queries run on the leader")]
    NotLeader,

    #[error("storage error: {0}")]
    Storage(String),

    /// A write failed in a way that is expected to succeed on retry
    /// (not leader yet, dedup state still loading, transient I/O).
    #[error("transient error: {0}")]
    Transient(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl ExqlError {
    pub fn unsupported(feature: impl Into<String>, hint: impl Into<String>) -> Self {
        ExqlError::Unsupported {
            feature: feature.into(),
            hint: hint.into(),
        }
    }

    pub fn parse(message: impl Into<String>) -> Self {
        ExqlError::Parse {
            message: message.into(),
            line: None,
            column: None,
        }
    }

    /// Machine-readable error code.
    pub fn code(&self) -> &'static str {
        match self {
            ExqlError::Parse { .. } => "PARSE_ERROR",
            ExqlError::Unsupported { .. } => "UNSUPPORTED",
            ExqlError::Plan(_) => "PLAN_ERROR",
            ExqlError::Execution(_) => "EXECUTION_ERROR",
            ExqlError::ResourcesExhausted(_) => "RESOURCES_EXHAUSTED",
            ExqlError::Timeout(_) => "TIMEOUT",
            ExqlError::Cancelled => "CANCELLED",
            ExqlError::NotFound(_) => "NOT_FOUND",
            ExqlError::Conflict(_) => "CONFLICT",
            ExqlError::NotLeader => "NOT_LEADER",
            ExqlError::Storage(_) => "STORAGE_ERROR",
            ExqlError::Transient(_) => "TRANSIENT_ERROR",
            ExqlError::Internal(_) => "INTERNAL_ERROR",
        }
    }

    /// Suggested HTTP status.
    pub fn http_status(&self) -> u16 {
        match self {
            ExqlError::Parse { .. }
            | ExqlError::Unsupported { .. }
            | ExqlError::Plan(_)
            | ExqlError::Execution(_) => 400,
            ExqlError::ResourcesExhausted(_) => 422,
            ExqlError::Timeout(_) => 408,
            ExqlError::Cancelled => 499,
            ExqlError::NotFound(_) => 404,
            ExqlError::Conflict(_) => 409,
            ExqlError::NotLeader | ExqlError::Transient(_) => 503,
            ExqlError::Storage(_) | ExqlError::Internal(_) => 500,
        }
    }

    /// Whether retrying the same operation later may succeed.
    pub fn is_transient(&self) -> bool {
        matches!(self, ExqlError::Transient(_) | ExqlError::NotLeader)
    }

    pub fn to_json(&self) -> JsonValue {
        let mut v = json!({
            "error": self.to_string(),
            "code": self.code(),
        });
        match self {
            ExqlError::Parse { line, column, .. } => {
                if let Some(l) = line {
                    v["line"] = json!(l);
                }
                if let Some(c) = column {
                    v["column"] = json!(c);
                }
            }
            ExqlError::Unsupported { hint, .. } if !hint.is_empty() => {
                v["hint"] = json!(hint);
            }
            _ => {}
        }
        v
    }
}

/// Extract `Line: X, Column: Y` from a sqlparser message.
fn parse_location(msg: &str) -> (Option<u64>, Option<u64>) {
    let find = |tag: &str| -> Option<u64> {
        let i = msg.find(tag)? + tag.len();
        let digits: String = msg[i..]
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse().ok()
    };
    (find("Line:"), find("Column:"))
}

impl From<ParserError> for ExqlError {
    fn from(e: ParserError) -> Self {
        let message = match &e {
            ParserError::ParserError(m) | ParserError::TokenizerError(m) => m.clone(),
            ParserError::RecursionLimitExceeded => "query is nested too deeply".to_string(),
        };
        let (line, column) = parse_location(&message);
        ExqlError::Parse {
            message,
            line,
            column,
        }
    }
}

impl From<DataFusionError> for ExqlError {
    fn from(e: DataFusionError) -> Self {
        match e.find_root() {
            DataFusionError::SQL(pe, _) => ExqlError::from(match pe.as_ref() {
                ParserError::ParserError(m) => ParserError::ParserError(m.clone()),
                ParserError::TokenizerError(m) => ParserError::TokenizerError(m.clone()),
                ParserError::RecursionLimitExceeded => ParserError::RecursionLimitExceeded,
            }),
            DataFusionError::Plan(m) => ExqlError::Plan(m.clone()),
            DataFusionError::SchemaError(..) => ExqlError::Plan(e.find_root().to_string()),
            DataFusionError::NotImplemented(m) => ExqlError::unsupported(m.clone(), ""),
            DataFusionError::ResourcesExhausted(m) => ExqlError::ResourcesExhausted(m.clone()),
            DataFusionError::Execution(m) => ExqlError::Execution(m.clone()),
            DataFusionError::External(inner) => {
                if let Some(x) = inner.downcast_ref::<ExqlErrorBox>() {
                    x.0.clone_shallow()
                } else {
                    ExqlError::Execution(inner.to_string())
                }
            }
            other => ExqlError::Execution(other.to_string()),
        }
    }
}

impl From<ArrowError> for ExqlError {
    fn from(e: ArrowError) -> Self {
        ExqlError::Execution(e.to_string())
    }
}

impl From<StorageError> for ExqlError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::StreamNotFound(s) => ExqlError::NotFound(format!("stream '{s}' not found")),
            StorageError::Io(_) | StorageError::ChannelClosed => ExqlError::Transient(e.to_string()),
            other => ExqlError::Storage(other.to_string()),
        }
    }
}

impl From<LogError> for ExqlError {
    fn from(e: LogError) -> Self {
        if e.is_retryable() {
            ExqlError::Transient(e.to_string())
        } else {
            ExqlError::Storage(e.to_string())
        }
    }
}

impl ExqlError {
    /// A copy that keeps the variant and message (errors are not `Clone`
    /// because the source error types aren't).
    pub fn clone_shallow(&self) -> ExqlError {
        match self {
            ExqlError::Parse {
                message,
                line,
                column,
            } => ExqlError::Parse {
                message: message.clone(),
                line: *line,
                column: *column,
            },
            ExqlError::Unsupported { feature, hint } => ExqlError::Unsupported {
                feature: feature.clone(),
                hint: hint.clone(),
            },
            ExqlError::Plan(m) => ExqlError::Plan(m.clone()),
            ExqlError::Execution(m) => ExqlError::Execution(m.clone()),
            ExqlError::ResourcesExhausted(m) => ExqlError::ResourcesExhausted(m.clone()),
            ExqlError::Timeout(ms) => ExqlError::Timeout(*ms),
            ExqlError::Cancelled => ExqlError::Cancelled,
            ExqlError::NotFound(m) => ExqlError::NotFound(m.clone()),
            ExqlError::Conflict(m) => ExqlError::Conflict(m.clone()),
            ExqlError::NotLeader => ExqlError::NotLeader,
            ExqlError::Storage(m) => ExqlError::Storage(m.clone()),
            ExqlError::Transient(m) => ExqlError::Transient(m.clone()),
            ExqlError::Internal(m) => ExqlError::Internal(m.clone()),
        }
    }

    /// Wrap into a DataFusion error that converts back losslessly.
    pub fn into_df(self) -> DataFusionError {
        DataFusionError::External(Box::new(ExqlErrorBox(self)))
    }
}

/// Carries an [`ExqlError`] through DataFusion.
#[derive(Debug)]
pub struct ExqlErrorBox(pub ExqlError);

impl std::fmt::Display for ExqlErrorBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ExqlErrorBox {}

pub type Result<T, E = ExqlError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_json_has_location() {
        let e = ExqlError::from(ParserError::ParserError(
            "Expected: end of statement, found: FORM at Line: 1, Column: 10".into(),
        ));
        let j = e.to_json();
        assert_eq!(j["code"], "PARSE_ERROR");
        assert_eq!(j["line"], 1);
        assert_eq!(j["column"], 10);
    }

    #[test]
    fn unsupported_has_hint() {
        let j = ExqlError::unsupported("CREATE INDEX", "indexes were removed").to_json();
        assert_eq!(j["code"], "UNSUPPORTED");
        assert_eq!(j["hint"], "indexes were removed");
    }

    #[test]
    fn external_round_trips() {
        let df = ExqlError::NotFound("stream 'x' not found".into()).into_df();
        let back = ExqlError::from(df);
        assert_eq!(back.code(), "NOT_FOUND");
    }
}
