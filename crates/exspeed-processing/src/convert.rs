//! Conversions between broker records, Arrow batches and JSON.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    StringBuilder, TimestampMillisecondArray, UInt64Array, UnionArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion_functions_json::{JsonUnionEncoder, JsonUnionValue};
use exspeed_streams::StoredRecord;
use serde_json::{Map, Number, Value as Json};

use crate::error::ExqlError;

/// Field metadata key marking a Utf8 column that holds JSON text. Results
/// render such columns as parsed JSON instead of strings.
pub const JSON_META: &str = "exspeed.json";

pub const COL_OFFSET: &str = "offset";
pub const COL_TIMESTAMP: &str = "timestamp";
pub const COL_SUBJECT: &str = "subject";
pub const COL_KEY: &str = "key";
pub const COL_PAYLOAD: &str = "payload";
pub const COL_HEADERS: &str = "headers";

/// `Timestamp(Millisecond, "UTC")`, the type of every time value ExQL produces.
pub fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn json_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
        .with_metadata(HashMap::from([(JSON_META.to_string(), "true".to_string())]))
}

static STREAM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(COL_OFFSET, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, ts_type(), false),
        Field::new(COL_SUBJECT, DataType::Utf8, false),
        Field::new(COL_KEY, DataType::Utf8, true),
        json_field(COL_PAYLOAD, false),
        json_field(COL_HEADERS, false),
    ]))
});

/// The schema every stream exposes as a table.
pub fn stream_schema() -> SchemaRef {
    STREAM_SCHEMA.clone()
}

/// Whether a field holds JSON text.
pub fn is_json_field(f: &Field) -> bool {
    f.metadata().get(JSON_META).is_some_and(|v| v == "true")
}

fn headers_json(headers: &[(String, String)]) -> String {
    let mut m = Map::new();
    for (k, v) in headers {
        m.insert(k.clone(), Json::String(v.clone()));
    }
    Json::Object(m).to_string()
}

/// Convert stored records to a batch with the full stream schema.
pub fn records_to_batch(records: &[StoredRecord]) -> Result<RecordBatch, ExqlError> {
    records_to_batch_projected(records, None)
}

/// Convert stored records to a batch, materialising only the projected
/// columns (indices into [`stream_schema`]).
pub fn records_to_batch_projected(
    records: &[StoredRecord],
    projection: Option<&[usize]>,
) -> Result<RecordBatch, ExqlError> {
    let full = stream_schema();
    let all: Vec<usize> = (0..full.fields().len()).collect();
    let cols = projection.unwrap_or(&all);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for &c in cols {
        let array: ArrayRef = match c {
            0 => Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|r| r.offset.0),
            )),
            1 => Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    records.iter().map(|r| (r.timestamp / 1_000_000) as i64),
                )
                .with_timezone("UTC"),
            ),
            2 => Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.subject.as_str()),
            )),
            3 => Arc::new(StringArray::from_iter(records.iter().map(|r| {
                r.key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
            }))),
            4 => {
                let mut b = StringBuilder::with_capacity(
                    records.len(),
                    records.iter().map(|r| r.value.len()).sum(),
                );
                for r in records {
                    b.append_value(String::from_utf8_lossy(&r.value));
                }
                Arc::new(b.finish())
            }
            5 => Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| headers_json(&r.headers)),
            )),
            _ => return Err(ExqlError::Internal(format!("bad stream column {c}"))),
        };
        arrays.push(array);
    }
    let schema = match projection {
        Some(p) => Arc::new(full.project(p).map_err(ExqlError::from)?),
        None => full,
    };
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(records.len())),
    )
    .map_err(ExqlError::from)
}

fn f64_json(v: f64) -> Json {
    Number::from_f64(v).map(Json::Number).unwrap_or(Json::Null)
}

/// Format a millisecond timestamp as RFC 3339 (`2026-10-02T10:00:00.123Z`).
pub fn format_ts_millis(ms: i64) -> String {
    match chrono::DateTime::from_timestamp_millis(ms) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => ms.to_string(),
    }
}

fn union_to_json(arr: &UnionArray, row: usize) -> Json {
    let Some(enc) = JsonUnionEncoder::from_union(arr.clone()) else {
        return Json::Null;
    };
    match enc.get_value(row) {
        JsonUnionValue::JsonNull => Json::Null,
        JsonUnionValue::Bool(b) => Json::Bool(b),
        JsonUnionValue::Int(i) => Json::Number(i.into()),
        JsonUnionValue::Float(f) => f64_json(f),
        JsonUnionValue::Str(s) => Json::String(s.to_string()),
        JsonUnionValue::Array(s) | JsonUnionValue::Object(s) => {
            serde_json::from_str(s).unwrap_or_else(|_| Json::String(s.to_string()))
        }
    }
}

/// Render one cell as JSON.
pub fn cell_to_json(array: &ArrayRef, field: &Field, row: usize) -> Json {
    if array.is_null(row) {
        return Json::Null;
    }
    match array.data_type() {
        DataType::Utf8 => {
            let s = array.as_string::<i32>().value(row);
            if is_json_field(field) {
                serde_json::from_str(s).unwrap_or_else(|_| Json::String(s.to_string()))
            } else {
                Json::String(s.to_string())
            }
        }
        DataType::Int64 => Json::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row))
                .unwrap_or_default()
                .into(),
        ),
        DataType::UInt64 => Json::Number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|a| a.value(row))
                .unwrap_or_default()
                .into(),
        ),
        DataType::Float64 => f64_json(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row))
                .unwrap_or_default(),
        ),
        DataType::Boolean => Json::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| a.value(row))
                .unwrap_or_default(),
        ),
        DataType::Union(_, _) => match array.as_any().downcast_ref::<UnionArray>() {
            Some(u) => union_to_json(u, row),
            None => Json::Null,
        },
        _ => match ScalarValue::try_from_array(array, row) {
            Ok(v) => scalar_to_json(&v),
            Err(_) => Json::Null,
        },
    }
}

/// Render a scalar as JSON.
pub fn scalar_to_json(v: &ScalarValue) -> Json {
    if v.is_null() {
        return Json::Null;
    }
    match v {
        ScalarValue::Boolean(Some(b)) => Json::Bool(*b),
        ScalarValue::Int8(Some(i)) => Json::Number((*i).into()),
        ScalarValue::Int16(Some(i)) => Json::Number((*i).into()),
        ScalarValue::Int32(Some(i)) => Json::Number((*i).into()),
        ScalarValue::Int64(Some(i)) => Json::Number((*i).into()),
        ScalarValue::UInt8(Some(i)) => Json::Number((*i).into()),
        ScalarValue::UInt16(Some(i)) => Json::Number((*i).into()),
        ScalarValue::UInt32(Some(i)) => Json::Number((*i).into()),
        ScalarValue::UInt64(Some(i)) => Json::Number((*i).into()),
        ScalarValue::Float16(Some(f)) => f64_json(f.to_f64()),
        ScalarValue::Float32(Some(f)) => f64_json(*f as f64),
        ScalarValue::Float64(Some(f)) => f64_json(*f),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Json::String(s.clone()),
        ScalarValue::TimestampMillisecond(Some(ms), _) => Json::String(format_ts_millis(*ms)),
        ScalarValue::TimestampSecond(Some(s), _) => {
            Json::String(format_ts_millis(s.saturating_mul(1000)))
        }
        ScalarValue::TimestampMicrosecond(Some(us), _) => {
            Json::String(format_ts_millis(us.div_euclid(1000)))
        }
        ScalarValue::TimestampNanosecond(Some(ns), _) => {
            Json::String(format_ts_millis(ns.div_euclid(1_000_000)))
        }
        ScalarValue::Decimal128(Some(_), _, _) | ScalarValue::Decimal256(Some(_), _, _) => {
            let s = v.to_string();
            s.parse::<f64>().map(f64_json).unwrap_or(Json::String(s))
        }
        ScalarValue::List(arr) => {
            let values = arr.value(0);
            let field = Field::new("item", values.data_type().clone(), true);
            Json::Array(
                (0..values.len())
                    .map(|i| cell_to_json(&values, &field, i))
                    .collect(),
            )
        }
        ScalarValue::Struct(arr) => {
            let mut m = Map::new();
            for (i, f) in arr.fields().iter().enumerate() {
                m.insert(f.name().clone(), cell_to_json(arr.column(i), f, 0));
            }
            Json::Object(m)
        }
        other => Json::String(other.to_string()),
    }
}

/// Render a batch as rows of JSON values.
pub fn batch_rows_json(batch: &RecordBatch) -> Vec<Vec<Json>> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .zip(schema.fields().iter())
                .map(|(c, f)| cell_to_json(c, f, row))
                .collect()
        })
        .collect()
}

/// Render one row as a JSON object keyed by column name.
pub fn row_object(batch: &RecordBatch, row: usize) -> Map<String, Json> {
    let schema = batch.schema();
    let mut m = Map::with_capacity(batch.num_columns());
    for (c, f) in batch.columns().iter().zip(schema.fields().iter()) {
        m.insert(f.name().clone(), cell_to_json(c, f, row));
    }
    m
}

/// Plain-text rendering of a cell, used for record keys and table keys.
pub fn cell_to_text(array: &ArrayRef, field: &Field, row: usize) -> Option<String> {
    match cell_to_json(array, field, row) {
        Json::Null => None,
        Json::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

/// Plain-text rendering of a scalar.
pub fn scalar_to_text(v: &ScalarValue) -> Option<String> {
    match scalar_to_json(v) {
        Json::Null => None,
        Json::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

/// Convert a JSON value back to a scalar of `field`'s type (used to restore
/// table rows from changelog records). Values that don't fit become NULL.
pub fn json_to_scalar(v: &Json, field: &Field) -> ScalarValue {
    let ty = field.data_type();
    let null = || ScalarValue::try_from(ty).unwrap_or(ScalarValue::Null);
    if v.is_null() {
        return null();
    }
    let text = match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    };
    let direct = match (ty, v) {
        (DataType::Utf8, _) => Some(ScalarValue::Utf8(Some(text.clone()))),
        (DataType::LargeUtf8, _) => Some(ScalarValue::LargeUtf8(Some(text.clone()))),
        (DataType::Utf8View, _) => Some(ScalarValue::Utf8View(Some(text.clone()))),
        (DataType::Boolean, Json::Bool(b)) => Some(ScalarValue::Boolean(Some(*b))),
        (DataType::Int64, Json::Number(n)) => n.as_i64().map(|x| ScalarValue::Int64(Some(x))),
        (DataType::UInt64, Json::Number(n)) => n.as_u64().map(|x| ScalarValue::UInt64(Some(x))),
        (DataType::Float64, Json::Number(n)) => n.as_f64().map(|x| ScalarValue::Float64(Some(x))),
        (DataType::Timestamp(TimeUnit::Millisecond, tz), Json::String(s)) => {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|d| ScalarValue::TimestampMillisecond(Some(d.timestamp_millis()), tz.clone()))
        }
        _ => None,
    };
    if let Some(x) = direct {
        return x;
    }
    ScalarValue::Utf8(Some(text))
        .cast_to(ty)
        .unwrap_or_else(|_| null())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_common::Offset;

    #[test]
    fn records_round_trip_to_json() {
        let recs = vec![StoredRecord {
            offset: Offset(7),
            timestamp: 1_700_000_000_123_000_000,
            subject: "a.b".into(),
            key: Some(Bytes::from_static(b"k1")),
            value: Bytes::from_static(br#"{"x":1}"#),
            headers: vec![("h".into(), "v".into())],
        }];
        let batch = records_to_batch(&recs).unwrap();
        let rows = batch_rows_json(&batch);
        assert_eq!(rows[0][0], serde_json::json!(7));
        assert_eq!(rows[0][1], serde_json::json!("2023-11-14T22:13:20.123Z"));
        assert_eq!(rows[0][2], serde_json::json!("a.b"));
        assert_eq!(rows[0][3], serde_json::json!("k1"));
        assert_eq!(rows[0][4], serde_json::json!({"x": 1}));
        assert_eq!(rows[0][5], serde_json::json!({"h": "v"}));

        let projected = records_to_batch_projected(&recs, Some(&[4, 0])).unwrap();
        assert_eq!(projected.schema().field(0).name(), "payload");
        assert_eq!(projected.num_columns(), 2);
    }
}
