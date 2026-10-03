use exspeed_processing::transform::RecordTransform;
use tracing::warn;

use crate::traits::SourceRecord;

/// A compiled transform that filters and projects source records with an
/// ExQL `SELECT … [WHERE …]` fragment (no FROM clause). Columns: `key`,
/// `subject`, `payload`, `headers`, `offset`, `timestamp`.
pub struct Transform {
    inner: RecordTransform,
}

impl Transform {
    /// Compile a SQL fragment such as `SELECT payload->>'id' AS id WHERE
    /// subject = 'orders.created'`.
    pub fn compile(sql: &str) -> Result<Self, String> {
        RecordTransform::compile(sql)
            .map(|inner| Transform { inner })
            .map_err(|e| format!("transform SQL error: {e}"))
    }

    /// Apply this transform to a source record.
    ///
    /// Returns `None` if the record is filtered out by the WHERE predicate
    /// (or the expressions fail on it). Returns `Some(record)` with the
    /// projected payload otherwise; key, subject and headers are kept.
    pub fn apply(&self, record: &SourceRecord) -> Option<SourceRecord> {
        match self.inner.apply(
            record.key.as_ref(),
            &record.subject,
            &record.value,
            &record.headers,
        ) {
            Ok(Some(value)) => Some(SourceRecord {
                key: record.key.clone(),
                value,
                subject: record.subject.clone(),
                headers: record.headers.clone(),
            }),
            Ok(None) => None,
            Err(e) => {
                warn!("transform failed on a record, dropping it: {e}");
                None
            }
        }
    }
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
