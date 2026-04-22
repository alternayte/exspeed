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

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("field '{field}': missing required value")]
    MissingRequired { field: String },
    #[error("field '{field}': expected {expected}, got {got}")]
    TypeMismatch { field: String, expected: &'static str, got: &'static str },
    #[error("field '{field}': could not parse as RFC3339 timestamp: {source}")]
    TimestampParse { field: String, #[source] source: chrono::ParseError },
}

fn json_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Convert a JSON value to a `Param` per the declared `ColumnSpec`,
/// enforcing strict type coercion.
///
/// `Timestamptz` returns `Param::Timestamptz` (parsed via RFC3339).
/// `Jsonb` returns `Param::JsonText` containing `serde_json::to_string(v)`.
pub fn bind_json_as_type(
    spec: &ColumnSpec,
    value: Option<&serde_json::Value>,
) -> Result<crate::builtin::jdbc::backend::Param, BindError> {
    use crate::builtin::jdbc::backend::Param;
    use serde_json::Value as V;
    let field = &spec.name;

    if matches!(value, None | Some(V::Null)) {
        if !spec.nullable {
            return Err(BindError::MissingRequired { field: field.clone() });
        }
        return Ok(Param::Null);
    }

    let v = value.unwrap();

    match spec.json_type {
        JsonType::Bigint => match v {
            V::Number(n) if n.is_i64() => Ok(Param::I64(n.as_i64().unwrap())),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "bigint",
                got: json_kind(v),
            }),
        },
        JsonType::Double => match v {
            V::Number(n) => Ok(Param::F64(n.as_f64().unwrap_or(0.0))),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "double",
                got: json_kind(v),
            }),
        },
        JsonType::Boolean => match v {
            V::Bool(b) => Ok(Param::Bool(*b)),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "boolean",
                got: json_kind(v),
            }),
        },
        JsonType::Text => match v {
            V::String(s) => Ok(Param::Text(s.clone())),
            V::Number(n) => Ok(Param::Text(n.to_string())),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "text",
                got: json_kind(v),
            }),
        },
        JsonType::Timestamptz => match v {
            V::String(s) => match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(dt) => Ok(Param::Timestamptz(dt.with_timezone(&chrono::Utc))),
                Err(e) => Err(BindError::TimestampParse { field: field.clone(), source: e }),
            },
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "timestamptz",
                got: json_kind(v),
            }),
        },
        JsonType::Jsonb => Ok(Param::JsonText(
            serde_json::to_string(v).expect("serde_json::Value always serializes"),
        )),
    }
}

#[cfg(test)]
mod bind_tests {
    use super::*;
    use crate::builtin::jdbc::backend::Param;
    use serde_json::json;

    fn spec(name: &str, t: JsonType, nullable: bool) -> ColumnSpec {
        ColumnSpec { name: name.to_string(), json_type: t, nullable }
    }

    #[test]
    fn bigint_accepts_integer() {
        let s = spec("n", JsonType::Bigint, false);
        let v = json!(42);
        match bind_json_as_type(&s, Some(&v)).unwrap() {
            Param::I64(42) => {}
            p => panic!("expected I64(42), got {p:?}"),
        }
    }

    #[test]
    fn bigint_rejects_string() {
        let s = spec("n", JsonType::Bigint, false);
        let v = json!("42");
        match bind_json_as_type(&s, Some(&v)) {
            Err(BindError::TypeMismatch { expected: "bigint", got: "string", .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn missing_required_errors() {
        let s = spec("n", JsonType::Bigint, false);
        match bind_json_as_type(&s, None) {
            Err(BindError::MissingRequired { .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn missing_nullable_is_null_param() {
        let s = spec("n", JsonType::Text, true);
        match bind_json_as_type(&s, None).unwrap() {
            Param::Null => {}
            p => panic!("expected Null, got {p:?}"),
        }
    }

    #[test]
    fn null_value_on_nullable_is_null_param() {
        let s = spec("n", JsonType::Text, true);
        let v = json!(null);
        match bind_json_as_type(&s, Some(&v)).unwrap() {
            Param::Null => {}
            p => panic!("expected Null, got {p:?}"),
        }
    }

    #[test]
    fn timestamptz_parses_rfc3339() {
        let s = spec("t", JsonType::Timestamptz, false);
        let v = json!("2026-04-22T10:00:00Z");
        match bind_json_as_type(&s, Some(&v)).unwrap() {
            Param::Timestamptz(_) => {}
            p => panic!("expected Timestamptz, got {p:?}"),
        }
    }

    #[test]
    fn timestamptz_rejects_garbage() {
        let s = spec("t", JsonType::Timestamptz, false);
        let v = json!("not-a-date");
        match bind_json_as_type(&s, Some(&v)) {
            Err(BindError::TimestampParse { .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn jsonb_accepts_any() {
        let s = spec("j", JsonType::Jsonb, false);
        for v in [json!({"a":1}), json!([1,2,3]), json!("str")] {
            match bind_json_as_type(&s, Some(&v)).unwrap() {
                Param::JsonText(_) => {}
                p => panic!("expected JsonText, got {p:?}"),
            }
        }
    }

    #[test]
    fn text_accepts_number_by_stringifying() {
        let s = spec("t", JsonType::Text, false);
        match bind_json_as_type(&s, Some(&json!(42))).unwrap() {
            Param::Text(s) if s == "42" => {}
            p => panic!("expected Text(\"42\"), got {p:?}"),
        }
    }
}
