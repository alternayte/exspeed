use bytes::Bytes;
use exspeed_streams::StoredRecord;

use crate::planner::column_set::ColumnSet;
use crate::types::{Row, Value};

/// Build a `Row` from a stored record, emitting only the columns required
/// by `cs`. The payload (when required) lands as `Value::RawJson` so
/// downstream evaluators can do path-aware partial parsing.
///
/// If `cs.select_star` is true, every column is emitted regardless of the
/// other flags (matching the semantics of `SELECT *`).
pub fn stored_record_to_row(
    record: &StoredRecord,
    alias: Option<&str>,
    cs: &ColumnSet,
) -> Row {
    let prefix = alias.map(|a| format!("{a}.")).unwrap_or_default();
    let mut columns = Vec::with_capacity(5);
    let mut values = Vec::with_capacity(5);

    let want = |col: &str| -> bool {
        cs.select_star || cs.virtual_cols.contains(col)
    };

    if want("offset") {
        columns.push(format!("{prefix}offset"));
        values.push(Value::Int(record.offset.0 as i64));
    }
    if want("timestamp") {
        columns.push(format!("{prefix}timestamp"));
        // StoredRecord.timestamp is epoch-nanoseconds (as stamped by storage);
        // Value::Timestamp is documented as epoch-milliseconds (types.rs).
        values.push(Value::Timestamp(record.timestamp / 1_000_000));
    }
    if want("key") {
        columns.push(format!("{prefix}key"));
        values.push(match &record.key {
            Some(k) => match std::str::from_utf8(k) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Text(String::from_utf8_lossy(k).into()),
            },
            None => Value::Null,
        });
    }
    if want("subject") {
        columns.push(format!("{prefix}subject"));
        values.push(Value::Text(record.subject.clone()));
    }

    if cs.select_star || cs.payload_referenced {
        columns.push(format!("{prefix}payload"));
        // Validate utf-8 at row-build; store raw bytes so downstream parsing
        // can be partial. Invalid utf-8 falls back to Value::Text lossy
        // conversion (matches the pre-existing fallback in bounded.rs).
        values.push(match std::str::from_utf8(&record.value) {
            Ok(_) => Value::RawJson(record.value.clone()),
            Err(_) => Value::Text(String::from_utf8_lossy(&record.value).into()),
        });
    }

    Row { columns, values }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exspeed_common::Offset;
    use exspeed_streams::StoredRecord;

    fn rec(value: &[u8]) -> StoredRecord {
        StoredRecord {
            offset: Offset(7),
            // StoredRecord.timestamp is epoch-nanoseconds (as stamped by storage).
            // 1_700_000_000_000_000_000 ns == 1_700_000_000_000 ms (2023-11-14 UTC).
            timestamp: 1_700_000_000_000_000_000,
            key: Some(Bytes::from_static(b"k1")),
            subject: "s.a".into(),
            value: Bytes::copy_from_slice(value),
            headers: vec![],
        }
    }

    #[test]
    fn select_star_emits_everything() {
        let cs = ColumnSet::needs_everything();
        let r = stored_record_to_row(&rec(br#"{"a":1}"#), None, &cs);
        assert_eq!(r.columns, vec!["offset", "timestamp", "key", "subject", "payload"]);
        assert_eq!(r.values[0], Value::Int(7));
        // Fixture is 1_700_000_000_000_000_000 ns; divided by 1_000_000 → ms.
        assert_eq!(r.values[1], Value::Timestamp(1_700_000_000_000));
        assert!(matches!(r.values[4], Value::RawJson(_)));
    }

    #[test]
    fn only_virtual_columns_skips_payload() {
        let mut cs = ColumnSet::default();
        cs.virtual_cols.insert("offset".into());
        let r = stored_record_to_row(&rec(br#"{"a":1}"#), None, &cs);
        assert_eq!(r.columns, vec!["offset"]);
        assert_eq!(r.values, vec![Value::Int(7)]);
    }

    #[test]
    fn payload_referenced_without_star_includes_only_payload() {
        let mut cs = ColumnSet::default();
        cs.payload_referenced = true;
        let r = stored_record_to_row(&rec(br#"{"a":1}"#), None, &cs);
        assert_eq!(r.columns, vec!["payload"]);
        assert!(matches!(r.values[0], Value::RawJson(_)));
    }

    #[test]
    fn non_utf8_payload_falls_back_to_text() {
        let mut cs = ColumnSet::default();
        cs.payload_referenced = true;
        let r = stored_record_to_row(&rec(&[0xFF, 0xFE]), None, &cs);
        assert!(matches!(r.values[0], Value::Text(_)));
    }

    #[test]
    fn alias_prefixes_column_names() {
        let cs = ColumnSet::needs_everything();
        let r = stored_record_to_row(&rec(br#"{"a":1}"#), Some("o"), &cs);
        assert_eq!(r.columns, vec!["o.offset", "o.timestamp", "o.key", "o.subject", "o.payload"]);
    }

    #[test]
    fn empty_column_set_emits_empty_row() {
        let r = stored_record_to_row(&rec(br#"{"a":1}"#), None, &ColumnSet::default());
        assert!(r.columns.is_empty());
        assert!(r.values.is_empty());
    }

    #[test]
    fn null_key_emits_value_null() {
        let mut record = rec(br#"{"a":1}"#);
        record.key = None;
        let mut cs = ColumnSet::default();
        cs.virtual_cols.insert("key".into());
        let r = stored_record_to_row(&record, None, &cs);
        assert_eq!(r.columns, vec!["key"]);
        assert_eq!(r.values[0], Value::Null);
    }
}
