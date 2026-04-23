use crate::parser::error::ParseError;
use serde_json::{json, Value as JsonValue};
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

impl ExqlError {
    pub fn to_json(&self) -> JsonValue {
        match self {
            ExqlError::Parse(pe) => match pe {
                ParseError::Sql { message: _, line, column } => json!({
                    "error": self.to_string(),
                    "code": "PARSE_ERROR",
                    "line": *line,
                    "column": *column,
                }),
                ParseError::Unsupported { feature: _, hint } => json!({
                    "error": self.to_string(),
                    "code": "UNSUPPORTED",
                    "hint": hint,
                }),
                ParseError::Transform(_) => json!({
                    "error": self.to_string(),
                    "code": "PARSE_ERROR",
                }),
            },
            ExqlError::Plan(_) => json!({
                "error": self.to_string(),
                "code": "PLAN_ERROR",
            }),
            ExqlError::Execution(_) => json!({
                "error": self.to_string(),
                "code": "EXECUTION_ERROR",
            }),
            ExqlError::Storage(_) => json!({
                "error": self.to_string(),
                "code": "STORAGE_ERROR",
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::error::ParseError;

    #[test]
    fn parse_error_to_json() {
        let err = ExqlError::Parse(ParseError::Sql {
            message: "Expected SELECT".into(),
            line: 1,
            column: 5,
        });
        let json = err.to_json();
        assert_eq!(json["code"], "PARSE_ERROR");
        assert_eq!(json["line"], 1);
        assert_eq!(json["column"], 5);
        assert!(json["error"].as_str().unwrap().contains("Expected SELECT"));
    }

    #[test]
    fn unsupported_error_to_json() {
        let err = ExqlError::Parse(ParseError::Unsupported {
            feature: "UNION".into(),
            hint: "ExQL supports SELECT".into(),
        });
        let json = err.to_json();
        assert_eq!(json["code"], "UNSUPPORTED");
        assert_eq!(json["hint"], "ExQL supports SELECT");
        assert!(json["error"].as_str().unwrap().contains("UNION"));
    }

    #[test]
    fn plan_error_to_json() {
        let err = ExqlError::Plan("stream 'orders' not found".into());
        let json = err.to_json();
        assert_eq!(json["code"], "PLAN_ERROR");
        assert!(json["error"].as_str().unwrap().contains("orders"));
        assert!(json.get("line").is_none());
    }

    #[test]
    fn execution_error_to_json() {
        let err = ExqlError::Execution("timeout".into());
        let json = err.to_json();
        assert_eq!(json["code"], "EXECUTION_ERROR");
    }

    #[test]
    fn storage_error_to_json() {
        let err = ExqlError::Storage("disk full".into());
        let json = err.to_json();
        assert_eq!(json["code"], "STORAGE_ERROR");
    }
}
