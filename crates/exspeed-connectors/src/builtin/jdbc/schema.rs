//! Typed-schema DSL + JSON→SQL type coercion for the typed mode of the JDBC sink.
//!
//! Syntax: `"col:type[?], col:type[?], ..."`.
//! Types: text | bigint | double | boolean | timestamptz | jsonb
//! `?` suffix marks nullable (default: NOT NULL).

use crate::builtin::jdbc::dialect::{ColumnSpec, JsonType};

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("empty schema")]
    Empty,
    #[error("column #{idx}: missing ':' between name and type in '{fragment}'")]
    MissingColon { idx: usize, fragment: String },
    #[error("column #{idx}: invalid identifier '{name}' (must match [A-Za-z_][A-Za-z0-9_]*)")]
    BadIdent { idx: usize, name: String },
    #[error("column #{idx}: unknown type '{got}' (want one of text|bigint|double|boolean|timestamptz|jsonb)")]
    UnknownType { idx: usize, got: String },
    #[error("duplicate column name '{name}'")]
    Duplicate { name: String },
}

/// Validate that `s` matches `^[A-Za-z_][A-Za-z0-9_]*$` — safe to embed in
/// dialect-quoted SQL.
pub fn is_valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn parse_schema(input: &str) -> Result<Vec<ColumnSpec>, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }

    let mut out: Vec<ColumnSpec> = Vec::new();
    for (idx, frag) in trimmed.split(',').enumerate() {
        let frag_trim = frag.trim();
        if frag_trim.is_empty() {
            continue;
        }
        let colon = frag_trim.find(':').ok_or_else(|| ParseError::MissingColon {
            idx,
            fragment: frag_trim.to_string(),
        })?;

        let name = frag_trim[..colon].trim();
        let rest = frag_trim[colon + 1..].trim();

        if !is_valid_ident(name) {
            return Err(ParseError::BadIdent { idx, name: name.to_string() });
        }

        let (type_str, nullable) = if let Some(stripped) = rest.strip_suffix('?') {
            (stripped.trim(), true)
        } else {
            (rest, false)
        };

        let json_type = JsonType::parse(type_str).ok_or_else(|| ParseError::UnknownType {
            idx,
            got: type_str.to_string(),
        })?;

        if out.iter().any(|c| c.name == name) {
            return Err(ParseError::Duplicate { name: name.to_string() });
        }

        out.push(ColumnSpec { name: name.to_string(), json_type, nullable });
    }

    if out.is_empty() {
        return Err(ParseError::Empty);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_column() {
        let cols = parse_schema("id:bigint").unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].json_type, JsonType::Bigint);
        assert!(!cols[0].nullable);
    }

    #[test]
    fn parses_multiple_columns_with_nullable() {
        let cols = parse_schema("id:bigint, email:text?, price:double").unwrap();
        assert_eq!(cols.len(), 3);
        assert!(!cols[0].nullable);
        assert!(cols[1].nullable);
        assert!(!cols[2].nullable);
    }

    #[test]
    fn accepts_whitespace_and_tabs() {
        let cols = parse_schema("  id : bigint , email :  text ? ").unwrap();
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[1].name, "email");
        assert!(cols[1].nullable);
    }

    #[test]
    fn rejects_unknown_type() {
        let err = parse_schema("id:smallint").unwrap_err();
        assert!(matches!(err, ParseError::UnknownType { .. }));
    }

    #[test]
    fn rejects_bad_ident() {
        let err = parse_schema("1bad:text").unwrap_err();
        assert!(matches!(err, ParseError::BadIdent { .. }));
        let err = parse_schema("a b:text").unwrap_err();
        assert!(matches!(err, ParseError::BadIdent { .. }));
    }

    #[test]
    fn rejects_missing_colon() {
        let err = parse_schema("id bigint").unwrap_err();
        assert!(matches!(err, ParseError::MissingColon { .. }));
    }

    #[test]
    fn rejects_duplicate_columns() {
        let err = parse_schema("id:bigint, id:text").unwrap_err();
        assert!(matches!(err, ParseError::Duplicate { .. }));
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(parse_schema("").unwrap_err(), ParseError::Empty));
        assert!(matches!(parse_schema("   ").unwrap_err(), ParseError::Empty));
        assert!(matches!(parse_schema(",,,").unwrap_err(), ParseError::Empty));
    }

    #[test]
    fn is_valid_ident_rules() {
        assert!(is_valid_ident("a"));
        assert!(is_valid_ident("_x"));
        assert!(is_valid_ident("col_1"));
        assert!(!is_valid_ident(""));
        assert!(!is_valid_ident("1col"));
        assert!(!is_valid_ident("col-1"));
        assert!(!is_valid_ident("col;drop"));
    }
}
