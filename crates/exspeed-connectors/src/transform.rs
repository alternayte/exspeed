use bytes::Bytes;

use exspeed_processing::parser::ast::{Expr, ExqlStatement, SelectItem};
use exspeed_processing::runtime::eval::eval_expr;
use exspeed_processing::types::{Row, Value};

use crate::traits::SourceRecord;

/// A compiled transform that can filter and project source records using ExQL
/// expressions parsed from a user-provided SQL fragment.
pub struct Transform {
    select_items: Vec<SelectItem>,
    filter: Option<Expr>,
}

impl Transform {
    /// Compile a SQL fragment into a Transform.
    ///
    /// The input `sql` should look like `SELECT ... WHERE ...` (no FROM clause).
    /// We inject a dummy FROM clause so the parser can handle it, then extract
    /// the SELECT items and optional WHERE predicate.
    pub fn compile(sql: &str) -> Result<Self, String> {
        // Inject a dummy FROM clause so the ExQL parser is happy.
        let upper = sql.to_uppercase();
        let full_sql = if let Some(pos) = find_top_level_where(&upper) {
            // Insert FROM "__transform__" before WHERE
            format!("{} FROM \"__transform__\" {}", &sql[..pos], &sql[pos..])
        } else {
            // No WHERE clause — append FROM
            format!("{sql} FROM \"__transform__\"")
        };

        let stmt = exspeed_processing::parser::parse(&full_sql)
            .map_err(|e| format!("transform SQL parse error: {e}"))?;

        match stmt {
            ExqlStatement::Query(query) => Ok(Transform {
                select_items: query.select,
                filter: query.filter,
            }),
            _ => Err("transform SQL must be a SELECT statement".into()),
        }
    }

    /// Apply this transform to a source record.
    ///
    /// Returns `None` if the record is filtered out by the WHERE predicate.
    /// Returns `Some(record)` with projected values otherwise.
    pub fn apply(&self, record: &SourceRecord) -> Option<SourceRecord> {
        let row = record_to_row(record);

        // Evaluate WHERE predicate
        if let Some(ref filter) = self.filter {
            let result = eval_expr(filter, &row);
            match result {
                Value::Bool(true) => {}
                _ => return None,
            }
        }

        // Check if all select items are wildcards
        let all_wildcard = self
            .select_items
            .iter()
            .all(|item| matches!(item.expr, Expr::Wildcard { .. }));

        if all_wildcard {
            return Some(record.clone());
        }

        // Evaluate each select item and build an output JSON object
        let mut output = serde_json::Map::new();
        for (i, item) in self.select_items.iter().enumerate() {
            let val = eval_expr(&item.expr, &row);
            let col_name = item
                .alias
                .clone()
                .unwrap_or_else(|| expr_default_name(&item.expr, i));
            output.insert(col_name, value_to_json(&val));
        }

        let new_value = serde_json::to_vec(&serde_json::Value::Object(output)).unwrap_or_default();

        Some(SourceRecord {
            key: record.key.clone(),
            value: Bytes::from(new_value),
            subject: record.subject.clone(),
            headers: record.headers.clone(),
        })
    }
}

/// Convert a SourceRecord into a Row with columns: "key", "subject", "payload", "headers".
fn record_to_row(record: &SourceRecord) -> Row {
    let key_val = match &record.key {
        Some(k) => Value::Text(String::from_utf8_lossy(k).to_string()),
        None => Value::Null,
    };

    let subject_val = Value::Text(record.subject.clone());

    // Try parsing payload as JSON; fall back to text
    let payload_str = String::from_utf8_lossy(&record.value);
    let payload_val = match serde_json::from_str::<serde_json::Value>(&payload_str) {
        Ok(j) => Value::Json(j),
        Err(_) => Value::Text(payload_str.to_string()),
    };

    // Headers as JSON object
    let headers_json: serde_json::Value = record
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let headers_val = Value::Json(headers_json);

    Row {
        columns: vec![
            "key".into(),
            "subject".into(),
            "payload".into(),
            "headers".into(),
        ],
        values: vec![key_val, subject_val, payload_val, headers_val],
    }
}

/// Derive a default column name from an expression when no alias is provided.
fn expr_default_name(expr: &Expr, index: usize) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::JsonAccess { field, .. } => field.clone(),
        _ => format!("col{index}"),
    }
}

/// Convert a Value to a serde_json::Value for output serialization.
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
        Value::Timestamp(t) => serde_json::json!(t),
    }
}

/// Find the position of a top-level WHERE keyword, skipping any WHERE that
/// might appear inside parenthesized subexpressions.
fn find_top_level_where(upper: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'W' if depth == 0 => {
                if upper[i..].starts_with("WHERE") {
                    // Make sure it's a word boundary (not part of ELSEWHERE etc.)
                    let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                    let after_ok = i + 5 >= bytes.len() || !bytes[i + 5].is_ascii_alphanumeric();
                    if before_ok && after_ok {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_record(key: Option<&str>, payload: &str, subject: &str) -> SourceRecord {
        SourceRecord {
            key: key.map(|k| Bytes::from(k.to_string())),
            value: Bytes::from(payload.to_string()),
            subject: subject.to_string(),
            headers: vec![("content-type".into(), "application/json".into())],
        }
    }

    #[test]
    fn filter_passes_matching_record() {
        let transform = Transform::compile("SELECT * WHERE subject = 'orders.created'").unwrap();

        let record = make_record(Some("k1"), r#"{"amount": 100}"#, "orders.created");

        let result = transform.apply(&record);
        assert!(result.is_some(), "matching record should pass filter");
        let out = result.unwrap();
        assert_eq!(out.subject, "orders.created");
        assert_eq!(out.value, record.value); // wildcard = unchanged
    }

    #[test]
    fn filter_rejects_non_matching_record() {
        let transform = Transform::compile("SELECT * WHERE subject = 'orders.created'").unwrap();

        let record = make_record(Some("k2"), r#"{"amount": 50}"#, "orders.cancelled");

        let result = transform.apply(&record);
        assert!(
            result.is_none(),
            "non-matching record should be filtered out"
        );
    }

    #[test]
    fn projection_transforms_columns() {
        let transform =
            Transform::compile("SELECT payload->>'name' AS customer_name, subject AS topic")
                .unwrap();

        let record = make_record(
            Some("k3"),
            r#"{"name": "Alice", "age": 30}"#,
            "customers.updated",
        );

        let result = transform.apply(&record);
        assert!(result.is_some());

        let out = result.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out.value).unwrap();
        assert_eq!(parsed["customer_name"], "Alice");
        assert_eq!(parsed["topic"], "customers.updated");
        // Key, subject, headers should be preserved
        assert_eq!(out.subject, "customers.updated");
        assert_eq!(out.key, Some(Bytes::from("k3")));
    }

    #[test]
    fn passthrough_with_select_star() {
        let transform = Transform::compile("SELECT *").unwrap();

        let record = make_record(Some("k4"), r#"{"data": true}"#, "events.test");

        let result = transform.apply(&record);
        assert!(result.is_some());
        let out = result.unwrap();
        // Record should be completely unchanged
        assert_eq!(out.value, record.value);
        assert_eq!(out.subject, record.subject);
        assert_eq!(out.key, record.key);
        assert_eq!(out.headers, record.headers);
    }
}
