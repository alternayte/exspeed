use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A dynamically-typed value that can flow through ExQL pipelines.
#[derive(Debug, Clone, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    /// Raw JSON bytes held before parsing. The custom `Serialize` impl emits
    /// the bytes as bare JSON (not wrapped in a `{"Json": ...}` envelope like
    /// `Value::Json`); `PartialEq` normalizes whitespace and compares equal
    /// to the equivalent `Json` variant. Only constructed by the row builder
    /// in `runtime::row_builder` — `Deserialize` (derived) never produces it.
    RawJson(Bytes),
    /// Epoch milliseconds.
    Timestamp(u64),
}

impl Value {
    /// Returns `true` if the value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to extract a string slice.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to extract an `i64`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract an `f64`.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract a JSON value reference.
    ///
    /// Returns `Cow::Borrowed` for `Json` (zero-copy) and `Cow::Owned` for
    /// `RawJson` (parses on demand).
    pub fn as_json(&self) -> Option<std::borrow::Cow<'_, serde_json::Value>> {
        match self {
            Value::Json(v) => Some(std::borrow::Cow::Borrowed(v)),
            Value::RawJson(b) => {
                serde_json::from_slice::<serde_json::Value>(b).ok().map(std::borrow::Cow::Owned)
            }
            _ => None,
        }
    }

    /// Best-effort conversion to `f64`.
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            Value::Timestamp(v) => Some(*v as f64),
            Value::Text(s) => s.parse::<f64>().ok(),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Best-effort conversion to `i64`.
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            Value::Float(v) => Some(*v as i64),
            Value::Timestamp(v) => Some(*v as i64),
            Value::Text(s) => s.parse::<i64>().ok(),
            Value::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }

    /// Produce a byte-string suitable for deterministic ordering.
    ///
    /// The key is **not** intended for human consumption; it exists so that
    /// rows can be sorted without allocating trait objects.
    pub fn to_sort_key(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![0],
            Value::Bool(b) => vec![1, u8::from(*b)],
            Value::Int(v) => {
                let mut key = vec![2];
                key.extend_from_slice(&v.to_be_bytes());
                key
            }
            Value::Float(v) => {
                let mut key = vec![3];
                // IEEE-754 total-order encoding: flip sign bit, then flip all
                // bits if originally negative so that ordering is preserved.
                let bits = v.to_bits();
                let encoded = if bits >> 63 == 1 {
                    !bits
                } else {
                    bits ^ (1u64 << 63)
                };
                key.extend_from_slice(&encoded.to_be_bytes());
                key
            }
            Value::Text(s) => {
                let mut key = vec![4];
                key.extend_from_slice(s.as_bytes());
                key
            }
            Value::Json(v) => {
                let mut key = vec![5];
                key.extend_from_slice(v.to_string().as_bytes());
                key
            }
            Value::Timestamp(v) => {
                let mut key = vec![6];
                key.extend_from_slice(&v.to_be_bytes());
                key
            }
            Value::RawJson(b) => {
                let mut key = vec![7];
                key.extend_from_slice(b);
                key
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Json(v) => write!(f, "{v}"),
            Value::RawJson(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "{s}"),
                Err(_) => write!(f, "<invalid utf8>"),
            },
            Value::Timestamp(v) => write!(f, "{v}"),
        }
    }
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error as _;
        match self {
            Value::Null => serializer.serialize_unit_variant("Value", 0, "Null"),
            Value::Bool(b) => serializer.serialize_newtype_variant("Value", 1, "Bool", b),
            Value::Int(v) => serializer.serialize_newtype_variant("Value", 2, "Int", v),
            Value::Float(v) => serializer.serialize_newtype_variant("Value", 3, "Float", v),
            Value::Text(s) => serializer.serialize_newtype_variant("Value", 4, "Text", s),
            Value::Json(v) => serializer.serialize_newtype_variant("Value", 5, "Json", v),
            Value::RawJson(bytes) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| S::Error::custom(format!("RawJson non-utf8: {e}")))?;
                let raw = serde_json::value::RawValue::from_string(s.to_string())
                    .map_err(|e| S::Error::custom(format!("RawJson invalid JSON: {e}")))?;
                raw.serialize(serializer)
            }
            Value::Timestamp(v) => serializer.serialize_newtype_variant("Value", 6, "Timestamp", v),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,
            (Json(a), Json(b)) => a == b,
            // RawJson ↔ RawJson: parse both so whitespace differences don't matter.
            (RawJson(a), RawJson(b)) => {
                match (
                    serde_json::from_slice::<serde_json::Value>(a),
                    serde_json::from_slice::<serde_json::Value>(b),
                ) {
                    (Ok(av), Ok(bv)) => av == bv,
                    _ => a == b, // fall back to byte equality on invalid JSON
                }
            }
            // RawJson ↔ Json (either direction): parse the raw side and compare.
            (RawJson(raw), Json(j)) | (Json(j), RawJson(raw)) => {
                matches!(serde_json::from_slice::<serde_json::Value>(raw), Ok(v) if &v == j)
            }
            _ => false,
        }
    }
}

/// A single row of values with named columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

impl Row {
    /// Look up a value by column name.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|idx| self.values.get(idx))
    }

    /// Look up a value by column index.
    pub fn get_idx(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }
}

/// Convert a query Value to a native JSON value for API responses.
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::json!(n),
        Value::Float(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
        Value::RawJson(b) => serde_json::from_slice(b).unwrap_or(serde_json::Value::Null),
        Value::Timestamp(ts) => serde_json::json!(ts),
    }
}

/// The result of executing an ExQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub execution_time_ms: u64,
}

impl ResultSet {
    /// Create an empty result set (no columns, no rows, zero elapsed time).
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            execution_time_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_conversions() {
        // Int → f64 / i64
        let v = Value::Int(42);
        assert_eq!(v.to_f64(), Some(42.0));
        assert_eq!(v.to_i64(), Some(42));
        assert_eq!(v.as_int(), Some(42));

        // Float → f64 / i64
        let v = Value::Float(3.14);
        assert_eq!(v.to_f64(), Some(3.14));
        assert_eq!(v.to_i64(), Some(3)); // truncation
        assert_eq!(v.as_float(), Some(3.14));

        // Text → f64 / i64
        let v = Value::Text("100".into());
        assert_eq!(v.to_f64(), Some(100.0));
        assert_eq!(v.to_i64(), Some(100));
        assert_eq!(v.as_text(), Some("100"));

        // Bool → f64 / i64
        let v = Value::Bool(true);
        assert_eq!(v.to_f64(), Some(1.0));
        assert_eq!(v.to_i64(), Some(1));

        // Null
        let v = Value::Null;
        assert!(v.is_null());
        assert_eq!(v.to_f64(), None);
        assert_eq!(v.to_i64(), None);
        assert_eq!(v.as_text(), None);

        // Json
        let j = serde_json::json!({"key": "val"});
        let v = Value::Json(j.clone());
        assert_eq!(v.as_json().as_deref(), Some(&j));

        // Timestamp
        let v = Value::Timestamp(1_700_000_000_000);
        assert_eq!(v.to_f64(), Some(1_700_000_000_000.0));
        assert_eq!(v.to_i64(), Some(1_700_000_000_000));
    }

    #[test]
    fn value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Bool(false).to_string(), "false");
        assert_eq!(Value::Int(7).to_string(), "7");
        assert_eq!(Value::Float(2.5).to_string(), "2.5");
        assert_eq!(Value::Text("hello".into()).to_string(), "hello");
        assert_eq!(Value::Timestamp(123).to_string(), "123");
    }

    #[test]
    fn raw_json_display() {
        let v = Value::RawJson(bytes::Bytes::from_static(b"{\"a\":1}"));
        assert_eq!(v.to_string(), "{\"a\":1}");
    }

    #[test]
    fn raw_json_invalid_utf8_display_is_placeholder() {
        let v = Value::RawJson(bytes::Bytes::from_static(&[0xFF, 0xFE]));
        assert_eq!(v.to_string(), "<invalid utf8>");
    }

    #[test]
    fn raw_json_sort_key_stable() {
        let a = Value::RawJson(bytes::Bytes::from_static(b"{\"a\":1}"));
        let b = Value::RawJson(bytes::Bytes::from_static(b"{\"a\":2}"));
        assert_ne!(a.to_sort_key(), b.to_sort_key());
        assert!(!a.to_sort_key().is_empty());
    }

    #[test]
    fn raw_json_is_not_null() {
        let v = Value::RawJson(bytes::Bytes::from_static(b"null"));
        assert!(!v.is_null()); // holds JSON "null" literal, not the Null variant
    }

    #[test]
    fn raw_json_serialize_object() {
        let v = Value::RawJson(bytes::Bytes::from_static(br#"{"a":1}"#));
        assert_eq!(serde_json::to_string(&v).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn raw_json_serialize_null_literal() {
        let v = Value::RawJson(bytes::Bytes::from_static(b"null"));
        assert_eq!(serde_json::to_string(&v).unwrap(), "null");
    }

    #[test]
    fn raw_json_serialize_invalid_utf8_errors() {
        let v = Value::RawJson(bytes::Bytes::from_static(&[0xFF, 0xFE]));
        assert!(serde_json::to_string(&v).is_err());
    }

    #[test]
    fn raw_json_serialize_invalid_json_errors() {
        let v = Value::RawJson(bytes::Bytes::from_static(b"not json"));
        assert!(serde_json::to_string(&v).is_err());
    }

    #[test]
    fn raw_json_eq_json() {
        let raw = Value::RawJson(bytes::Bytes::from_static(b"1"));
        let parsed = Value::Json(serde_json::Value::from(1));
        assert_eq!(raw, parsed);
    }

    #[test]
    fn raw_json_eq_raw_json() {
        // Same JSON value with different whitespace should still compare equal.
        let a = Value::RawJson(bytes::Bytes::from_static(br#"{"a":1}"#));
        let b = Value::RawJson(bytes::Bytes::from_static(br#"{"a": 1}"#));
        assert_eq!(a, b);
    }

    #[test]
    fn raw_json_eq_across_other_variants_still_false() {
        let raw = Value::RawJson(bytes::Bytes::from_static(b"\"x\""));
        assert_ne!(raw, Value::Text("x".into())); // Text is not JSON
    }

    #[test]
    fn row_get() {
        let row = Row {
            columns: vec!["id".into(), "name".into(), "score".into()],
            values: vec![
                Value::Int(1),
                Value::Text("Alice".into()),
                Value::Float(95.5),
            ],
        };

        assert_eq!(row.get("id"), Some(&Value::Int(1)));
        assert_eq!(row.get("name"), Some(&Value::Text("Alice".into())));
        assert_eq!(row.get("score"), Some(&Value::Float(95.5)));
        assert_eq!(row.get("missing"), None);

        assert_eq!(row.get_idx(0), Some(&Value::Int(1)));
        assert_eq!(row.get_idx(3), None);
    }
}
