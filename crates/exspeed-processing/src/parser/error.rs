use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("SQL parse error at line {line}, column {column}: {message}")]
    Sql {
        message: String,
        line: usize,
        column: usize,
    },

    #[error("unsupported: {feature}")]
    Unsupported {
        feature: String,
        hint: String,
    },

    #[error("transform error: {0}")]
    Transform(String),
}

/// Extract `at Line: N, Column: M` from sqlparser's error text.
/// Returns (cleaned_message, line, column). Falls back to (1, 0) if no match.
pub fn extract_position(msg: &str) -> (String, usize, usize) {
    if let Some(at_pos) = msg.find(" at Line: ") {
        let suffix = &msg[at_pos + " at Line: ".len()..];
        if let Some(comma) = suffix.find(", Column: ") {
            let line: usize = suffix[..comma].parse().unwrap_or(1);
            let col: usize = suffix[comma + ", Column: ".len()..].parse().unwrap_or(0);
            return (msg[..at_pos].to_string(), line, col);
        }
    }
    (msg.to_string(), 1, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_position_from_sqlparser_error() {
        let msg = "Expected end of statement, found: UNION at Line: 1, Column: 42";
        let (clean, line, col) = extract_position(msg);
        assert_eq!(clean, "Expected end of statement, found: UNION");
        assert_eq!(line, 1);
        assert_eq!(col, 42);
    }

    #[test]
    fn extract_position_no_match_falls_back() {
        let msg = "some weird error without position";
        let (clean, line, col) = extract_position(msg);
        assert_eq!(clean, "some weird error without position");
        assert_eq!(line, 1);
        assert_eq!(col, 0);
    }

    #[test]
    fn display_sql_error() {
        let err = ParseError::Sql {
            message: "Expected SELECT".into(),
            line: 1,
            column: 5,
        };
        assert_eq!(
            err.to_string(),
            "SQL parse error at line 1, column 5: Expected SELECT"
        );
    }

    #[test]
    fn display_unsupported_error() {
        let err = ParseError::Unsupported {
            feature: "UNION".into(),
            hint: "ExQL supports SELECT with WHERE, JOIN, GROUP BY, ORDER BY, LIMIT".into(),
        };
        assert_eq!(err.to_string(), "unsupported: UNION");
    }
}
