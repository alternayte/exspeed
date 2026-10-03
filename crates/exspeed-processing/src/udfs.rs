//! ExQL-specific scalar functions.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

use crate::convert::ts_type;

/// Internal marker for `window_start` in windowed continuous queries.
pub const WSTART: &str = "__exql_window_start";
/// Internal marker for `window_end` in windowed continuous queries.
pub const WEND: &str = "__exql_window_end";

fn to_array(v: &ColumnarValue, n: usize) -> datafusion::common::Result<ArrayRef> {
    v.clone().into_array(n)
}

fn rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|a| match a {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .unwrap_or(1)
}

fn all_scalar(args: &[ColumnarValue]) -> bool {
    args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)))
}

fn finish(out: ArrayRef, scalar: bool) -> datafusion::common::Result<ColumnarValue> {
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
    } else {
        Ok(ColumnarValue::Array(out))
    }
}

/// `subject_part(subject, n)`: the n-th (1-based) dot-delimited token, or
/// NULL. Negative `n` counts from the end.
pub fn subject_part() -> ScalarUDF {
    create_udf(
        "subject_part",
        vec![DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let n = rows(args);
            let s = to_array(&args[0], n)?;
            let i = to_array(&args[1], n)?;
            let s = s.as_string::<i32>();
            let i = i.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            let out: StringArray = (0..n)
                .map(|r| {
                    if s.is_null(r) || i.is_null(r) {
                        return None;
                    }
                    let toks: Vec<&str> = s.value(r).split('.').collect();
                    let k = i.value(r);
                    let idx = if k > 0 {
                        (k - 1) as usize
                    } else if k < 0 && (-k) as usize <= toks.len() {
                        toks.len() - (-k) as usize
                    } else {
                        return None;
                    };
                    toks.get(idx).map(|t| t.to_string())
                })
                .collect();
            finish(Arc::new(out), all_scalar(args))
        }),
    )
}

/// `subject_matches(subject, pattern)`: NATS-style wildcard match
/// (`*` one token, `>` one or more trailing tokens).
pub fn subject_matches() -> ScalarUDF {
    create_udf(
        "subject_matches",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let n = rows(args);
            let s = to_array(&args[0], n)?;
            let p = to_array(&args[1], n)?;
            let s = s.as_string::<i32>();
            let p = p.as_string::<i32>();
            let out: BooleanArray = (0..n)
                .map(|r| {
                    if s.is_null(r) || p.is_null(r) {
                        None
                    } else {
                        Some(exspeed_common::subject::subject_matches(
                            s.value(r),
                            p.value(r),
                        ))
                    }
                })
                .collect();
            finish(Arc::new(out), all_scalar(args))
        }),
    )
}

/// Name of the numeric sort key used for ORDER BY on JSON text.
pub const JSON_NUM: &str = "__exql_json_num";

/// `__exql_json_num(text)`: the text as a DOUBLE, or NULL if it isn't a
/// number. Used as the primary ORDER BY key for JSON text (a plain
/// `TRY_CAST` would be merged with the text key by the optimizer).
pub fn json_num() -> ScalarUDF {
    create_udf(
        JSON_NUM,
        vec![DataType::Utf8],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let n = rows(args);
            let s = to_array(&args[0], n)?;
            let s = s.as_string::<i32>();
            let out: datafusion::arrow::array::Float64Array = (0..n)
                .map(|r| {
                    if s.is_null(r) {
                        None
                    } else {
                        s.value(r)
                            .trim()
                            .parse::<f64>()
                            .ok()
                            .filter(|f| !f.is_nan())
                    }
                })
                .collect();
            finish(Arc::new(out), all_scalar(args))
        }),
    )
}

fn window_marker(name: &'static str) -> ScalarUDF {
    create_udf(
        name,
        vec![],
        ts_type(),
        Volatility::Volatile,
        Arc::new(move |_args: &[ColumnarValue]| {
            exec_err!("window_start / window_end are only available in windowed continuous queries")
        }),
    )
}

pub fn window_start_marker() -> ScalarUDF {
    window_marker(WSTART)
}

pub fn window_end_marker() -> ScalarUDF {
    window_marker(WEND)
}

/// Every ExQL function to register in a session.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        subject_part(),
        subject_matches(),
        json_num(),
        window_start_marker(),
        window_end_marker(),
    ]
}
