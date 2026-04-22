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

/// Bind a JSON value into a prepared `sqlx::Any` query, enforcing strict
/// type coercion per the declared `ColumnSpec`.
///
/// `sqlx::Any` only encodes primitive scalars (bool, i16/32/64, f32/64, String,
/// Vec<u8>), so `Timestamptz` and `Jsonb` degrade to strings:
///   - Timestamptz is parsed as RFC3339 (strict validation) then re-emitted as
///     an ISO-8601 UTC string, which Postgres/MySQL both accept as a timestamp
///     literal.
///   - Jsonb is serialized via `serde_json::to_string`, which Postgres accepts
///     as a `jsonb` literal; MySQL's `JSON` column accepts the same.
pub fn bind_json_as_type<'q>(
    query: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
    spec: &ColumnSpec,
    value: Option<&serde_json::Value>,
) -> Result<sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>, BindError> {
    use serde_json::Value as V;
    let field = &spec.name;

    if matches!(value, None | Some(V::Null)) {
        if !spec.nullable {
            return Err(BindError::MissingRequired { field: field.clone() });
        }
        return Ok(match spec.json_type {
            JsonType::Text | JsonType::Timestamptz | JsonType::Jsonb => {
                query.bind(None::<String>)
            }
            JsonType::Bigint => query.bind(None::<i64>),
            JsonType::Double => query.bind(None::<f64>),
            JsonType::Boolean => query.bind(None::<bool>),
        });
    }

    let v = value.unwrap();

    match spec.json_type {
        JsonType::Bigint => match v {
            V::Number(n) if n.is_i64() => Ok(query.bind(n.as_i64().unwrap())),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "bigint",
                got: json_kind(v),
            }),
        },
        JsonType::Double => match v {
            V::Number(n) => Ok(query.bind(n.as_f64().unwrap_or(0.0))),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "double",
                got: json_kind(v),
            }),
        },
        JsonType::Boolean => match v {
            V::Bool(b) => Ok(query.bind(*b)),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "boolean",
                got: json_kind(v),
            }),
        },
        JsonType::Text => match v {
            V::String(s) => Ok(query.bind(s.clone())),
            V::Number(n) => Ok(query.bind(n.to_string())),
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "text",
                got: json_kind(v),
            }),
        },
        JsonType::Timestamptz => match v {
            V::String(s) => match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(dt) => Ok(query.bind(
                    dt.with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                )),
                Err(e) => Err(BindError::TimestampParse {
                    field: field.clone(),
                    source: e,
                }),
            },
            _ => Err(BindError::TypeMismatch {
                field: field.clone(),
                expected: "timestamptz",
                got: json_kind(v),
            }),
        },
        JsonType::Jsonb => Ok(query.bind(
            serde_json::to_string(v).expect("serde_json::Value always serializes"),
        )),
    }
}

#[cfg(test)]
mod bind_tests {
    use super::*;
    use serde_json::json;

    fn spec(name: &str, t: JsonType, nullable: bool) -> ColumnSpec {
        ColumnSpec { name: name.to_string(), json_type: t, nullable }
    }

    fn pg_query() -> sqlx::query::Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>> {
        sqlx::query("SELECT 1")
    }

    #[test]
    fn bigint_accepts_integer() {
        let s = spec("n", JsonType::Bigint, false);
        let v = json!(42);
        assert!(bind_json_as_type(pg_query(), &s, Some(&v)).is_ok());
    }

    #[test]
    fn bigint_rejects_string() {
        let s = spec("n", JsonType::Bigint, false);
        let v = json!("42");
        match bind_json_as_type(pg_query(), &s, Some(&v)) {
            Err(BindError::TypeMismatch { expected: "bigint", got: "string", .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn missing_required_errors() {
        let s = spec("n", JsonType::Bigint, false);
        match bind_json_as_type(pg_query(), &s, None) {
            Err(BindError::MissingRequired { .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn missing_nullable_is_ok() {
        let s = spec("n", JsonType::Text, true);
        assert!(bind_json_as_type(pg_query(), &s, None).is_ok());
    }

    #[test]
    fn null_value_on_nullable_is_ok() {
        let s = spec("n", JsonType::Text, true);
        let v = json!(null);
        assert!(bind_json_as_type(pg_query(), &s, Some(&v)).is_ok());
    }

    #[test]
    fn timestamptz_parses_rfc3339() {
        let s = spec("t", JsonType::Timestamptz, false);
        let v = json!("2026-04-22T10:00:00Z");
        assert!(bind_json_as_type(pg_query(), &s, Some(&v)).is_ok());
    }

    #[test]
    fn timestamptz_rejects_garbage() {
        let s = spec("t", JsonType::Timestamptz, false);
        let v = json!("not-a-date");
        match bind_json_as_type(pg_query(), &s, Some(&v)) {
            Err(BindError::TimestampParse { .. }) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn jsonb_accepts_any() {
        let s = spec("j", JsonType::Jsonb, false);
        assert!(bind_json_as_type(pg_query(), &s, Some(&json!({"a":1}))).is_ok());
        assert!(bind_json_as_type(pg_query(), &s, Some(&json!([1,2,3]))).is_ok());
        assert!(bind_json_as_type(pg_query(), &s, Some(&json!("str"))).is_ok());
    }

    #[test]
    fn text_accepts_number_by_stringifying() {
        let s = spec("t", JsonType::Text, false);
        assert!(bind_json_as_type(pg_query(), &s, Some(&json!(42))).is_ok());
    }
}
