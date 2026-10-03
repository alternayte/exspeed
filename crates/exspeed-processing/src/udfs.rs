//! ExQL-specific scalar functions.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

use exspeed_common::SubjectFilter;

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
/// (`*` one token, `>` one or more trailing tokens), with exactly the
/// semantics of consumer and connector subject filters. The pattern is
/// parsed once per batch when it is a constant (and once per distinct value
/// otherwise); an invalid pattern (`a.>.c`, `a..b`, `a.b*`) is an error.
pub fn subject_matches() -> ScalarUDF {
    create_udf(
        "subject_matches",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(subject_matches_impl),
    )
}

fn subject_matches_impl(args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
    let n = rows(args);
    let s = to_array(&args[0], n)?;
    let s = s.as_string::<i32>();
    let constant = matches!(args[1], ColumnarValue::Scalar(_));
    let p = to_array(&args[1], if constant { 1 } else { n })?;
    let p = p.as_string::<i32>();
    let parse =
        |pat: &str| SubjectFilter::parse(pat).or_else(|e| exec_err!("subject_matches: {e}"));
    let mut cached: Option<(usize, SubjectFilter)> = None;
    let mut out = Vec::with_capacity(n);
    for r in 0..n {
        let pr = if constant { 0 } else { r };
        if s.is_null(r) || p.is_null(pr) {
            out.push(None);
            continue;
        }
        let same = cached
            .as_ref()
            .is_some_and(|(i, _)| *i == pr || p.value(*i) == p.value(pr));
        if !same {
            cached = Some((pr, parse(p.value(pr))?));
        }
        let f = &cached.as_ref().expect("cached above").1;
        out.push(Some(f.matches(s.value(r))));
    }
    finish(Arc::new(BooleanArray::from(out)), all_scalar(args))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v.to_string())))
    }

    fn col(v: &[&str]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(v.to_vec())))
    }

    fn bools(v: ColumnarValue) -> Vec<Option<bool>> {
        let a = v.into_array(1).unwrap();
        a.as_boolean().iter().collect()
    }

    /// Same semantics as `SubjectFilter`: no partial or non-final
    /// wildcards, no empty tokens.
    #[test]
    fn subject_matches_uses_subject_filter_semantics() {
        let subjects = col(&["a.b.c", "a.x", "a.x.y.z", "a.b"]);
        let out = subject_matches_impl(&[subjects.clone(), s("a.>")]).unwrap();
        assert_eq!(bools(out), vec![Some(true); 4]);
        let out = subject_matches_impl(&[subjects.clone(), s("a.*.c")]).unwrap();
        assert_eq!(
            bools(out),
            vec![Some(true), Some(false), Some(false), Some(false)]
        );
        // Per-row patterns (parsed once per distinct value).
        let per_row = col(&["a.*.c", "a.*.c", "a.>", "x"]);
        let out = subject_matches_impl(&[subjects.clone(), per_row]).unwrap();
        assert_eq!(
            bools(out),
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
        // The legacy matcher accepted these and treated `a.>.c` as `a.>`.
        for bad in ["a.>.c", "a..b", "a.b*"] {
            let err = subject_matches_impl(&[subjects.clone(), s(bad)]).unwrap_err();
            assert!(err.to_string().contains("subject_matches"), "{err}");
        }
        // Scalar in, scalar out; a NULL pattern gives NULL.
        let out = subject_matches_impl(&[s("a.b"), s("a.*")]).unwrap();
        assert!(matches!(
            out,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        let null = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let out = subject_matches_impl(&[subjects, null]).unwrap();
        assert_eq!(bools(out), vec![None; 4]);
    }
}
