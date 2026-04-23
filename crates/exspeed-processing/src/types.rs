use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A dynamically-typed value that can flow through ExQL pipelines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    /// Raw JSON bytes held before parsing. NOTE: the currently-derived
    /// Serialize emits a u8 array, not JSON — Task 2 replaces the derive
    /// with a custom impl. Until then, don't construct RawJson values in
    /// paths that flow to HTTP responses.
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
