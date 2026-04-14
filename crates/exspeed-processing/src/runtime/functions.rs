use crate::types::Value;

/// Dispatch a built-in scalar function by name.
///
/// All function names are matched case-insensitively (upper-cased internally).
pub fn call_function(name: &str, args: &[Value]) -> Value {
    match name.to_uppercase().as_str() {
        // ── NATS / subject helpers ──────────────────────────────────────
        "SUBJECT_PART" => fn_subject_part(args),
        "SUBJECT_MATCHES" => fn_subject_matches(args),

        // ── Temporal ────────────────────────────────────────────────────
        "NOW" => fn_now(),

        // ── String functions ────────────────────────────────────────────
        "UPPER" => fn_upper(args),
        "LOWER" => fn_lower(args),
        "LENGTH" | "LEN" | "CHAR_LENGTH" => fn_length(args),
        "CONCAT" => fn_concat(args),
        "SUBSTRING" | "SUBSTR" => fn_substring(args),
        "TRIM" => fn_trim(args),

        // ── Numeric functions ───────────────────────────────────────────
        "ABS" => fn_abs(args),
        "ROUND" => fn_round(args),
        "CEIL" | "CEILING" => fn_ceil(args),
        "FLOOR" => fn_floor(args),

        // ── Conditional / null helpers ──────────────────────────────────
        "COALESCE" => fn_coalesce(args),
        "NULLIF" => fn_nullif(args),

        // ── Header access (placeholder — headers resolved via row) ─────
        "HEADER" => Value::Null,

        _ => Value::Null,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Individual function implementations
// ═══════════════════════════════════════════════════════════════════════════

fn fn_subject_part(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let subject = match &args[0] {
        Value::Text(s) => s.as_str(),
        _ => return Value::Null,
    };
    let index = match args[1].to_i64() {
        Some(i) => i,
        None => return Value::Null,
    };
    // 1-based index
    let parts: Vec<&str> = subject.split('.').collect();
    if index < 1 || index as usize > parts.len() {
        return Value::Null;
    }
    Value::Text(parts[(index - 1) as usize].to_string())
}

fn fn_subject_matches(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let subject = match &args[0] {
        Value::Text(s) => s.as_str(),
        _ => return Value::Null,
    };
    let pattern = match &args[1] {
        Value::Text(s) => s.as_str(),
        _ => return Value::Null,
    };
    Value::Bool(exspeed_common::subject_matches(subject, pattern))
}

fn fn_now() -> Value {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    Value::Timestamp(nanos)
}

fn fn_upper(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.to_uppercase()),
        Some(v) => Value::Text(v.to_string().to_uppercase()),
        None => Value::Null,
    }
}

fn fn_lower(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.to_lowercase()),
        Some(v) => Value::Text(v.to_string().to_lowercase()),
        None => Value::Null,
    }
}

fn fn_length(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Int(s.len() as i64),
        Some(Value::Null) => Value::Null,
        Some(v) => Value::Int(v.to_string().len() as i64),
        None => Value::Null,
    }
}

fn fn_concat(args: &[Value]) -> Value {
    let mut buf = String::new();
    for arg in args {
        match arg {
            Value::Null => {}
            other => buf.push_str(&other.to_string()),
        }
    }
    Value::Text(buf)
}

fn fn_substring(args: &[Value]) -> Value {
    if args.is_empty() {
        return Value::Null;
    }
    let s = match &args[0] {
        Value::Text(s) => s.as_str(),
        Value::Null => return Value::Null,
        other => return Value::Text(substring_helper(&other.to_string(), args)),
    };
    Value::Text(substring_helper(s, args))
}

fn substring_helper(s: &str, args: &[Value]) -> String {
    // SQL SUBSTRING is 1-based.
    let start = args
        .get(1)
        .and_then(|v| v.to_i64())
        .unwrap_or(1)
        .max(1) as usize
        - 1;

    let len = args.get(2).and_then(|v| v.to_i64());

    let chars: Vec<char> = s.chars().collect();
    if start >= chars.len() {
        return String::new();
    }
    match len {
        Some(l) => chars[start..].iter().take(l.max(0) as usize).collect(),
        None => chars[start..].iter().collect(),
    }
}

fn fn_trim(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.trim().to_string()),
        Some(Value::Null) => Value::Null,
        Some(v) => Value::Text(v.to_string().trim().to_string()),
        None => Value::Null,
    }
}

fn fn_abs(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Int(v)) => Value::Int(v.abs()),
        Some(Value::Float(v)) => Value::Float(v.abs()),
        Some(other) => {
            if let Some(f) = other.to_f64() {
                Value::Float(f.abs())
            } else {
                Value::Null
            }
        }
        None => Value::Null,
    }
}

fn fn_round(args: &[Value]) -> Value {
    let val = match args.first() {
        Some(v) => v,
        None => return Value::Null,
    };
    let decimals = args.get(1).and_then(|v| v.to_i64()).unwrap_or(0);
    match val.to_f64() {
        Some(f) => {
            let factor = 10_f64.powi(decimals as i32);
            let rounded = (f * factor).round() / factor;
            if decimals <= 0 {
                Value::Int(rounded as i64)
            } else {
                Value::Float(rounded)
            }
        }
        None => Value::Null,
    }
}

fn fn_ceil(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.to_f64()) {
        Some(f) => Value::Int(f.ceil() as i64),
        None => Value::Null,
    }
}

fn fn_floor(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.to_f64()) {
        Some(f) => Value::Int(f.floor() as i64),
        None => Value::Null,
    }
}

fn fn_coalesce(args: &[Value]) -> Value {
    for arg in args {
        if !arg.is_null() {
            return arg.clone();
        }
    }
    Value::Null
}

fn fn_nullif(args: &[Value]) -> Value {
    if args.len() < 2 {
        return args.first().cloned().unwrap_or(Value::Null);
    }
    if args[0] == args[1] {
        Value::Null
    } else {
        args[0].clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper() {
        let result = call_function("upper", &[Value::Text("hello".into())]);
        assert_eq!(result, Value::Text("HELLO".into()));
    }

    #[test]
    fn test_lower() {
        let result = call_function("LOWER", &[Value::Text("HELLO".into())]);
        assert_eq!(result, Value::Text("hello".into()));
    }

    #[test]
    fn test_length() {
        let result = call_function("LENGTH", &[Value::Text("hello".into())]);
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_concat() {
        let result = call_function(
            "CONCAT",
            &[Value::Text("hello".into()), Value::Text(" world".into())],
        );
        assert_eq!(result, Value::Text("hello world".into()));
    }

    #[test]
    fn test_concat_with_null() {
        let result = call_function(
            "CONCAT",
            &[Value::Text("a".into()), Value::Null, Value::Text("b".into())],
        );
        assert_eq!(result, Value::Text("ab".into()));
    }

    #[test]
    fn test_substring() {
        let result = call_function(
            "SUBSTRING",
            &[Value::Text("hello world".into()), Value::Int(7), Value::Int(5)],
        );
        assert_eq!(result, Value::Text("world".into()));
    }

    #[test]
    fn test_trim() {
        let result = call_function("TRIM", &[Value::Text("  hello  ".into())]);
        assert_eq!(result, Value::Text("hello".into()));
    }

    #[test]
    fn test_abs() {
        assert_eq!(call_function("ABS", &[Value::Int(-5)]), Value::Int(5));
        assert_eq!(call_function("ABS", &[Value::Float(-3.14)]), Value::Float(3.14));
    }

    #[test]
    fn test_round() {
        assert_eq!(call_function("ROUND", &[Value::Float(3.7)]), Value::Int(4));
        assert_eq!(
            call_function("ROUND", &[Value::Float(3.456), Value::Int(2)]),
            Value::Float(3.46)
        );
    }

    #[test]
    fn test_ceil_floor() {
        assert_eq!(call_function("CEIL", &[Value::Float(3.2)]), Value::Int(4));
        assert_eq!(call_function("FLOOR", &[Value::Float(3.8)]), Value::Int(3));
    }

    #[test]
    fn test_coalesce() {
        let result = call_function("COALESCE", &[Value::Null, Value::Text("a".into())]);
        assert_eq!(result, Value::Text("a".into()));

        let result = call_function("COALESCE", &[Value::Int(1), Value::Int(2)]);
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            call_function("NULLIF", &[Value::Int(1), Value::Int(1)]),
            Value::Null
        );
        assert_eq!(
            call_function("NULLIF", &[Value::Int(1), Value::Int(2)]),
            Value::Int(1)
        );
    }

    #[test]
    fn test_subject_part() {
        let result = call_function(
            "SUBJECT_PART",
            &[Value::Text("order.eu.created".into()), Value::Int(2)],
        );
        assert_eq!(result, Value::Text("eu".into()));
    }

    #[test]
    fn test_subject_matches() {
        let result = call_function(
            "SUBJECT_MATCHES",
            &[
                Value::Text("order.eu.created".into()),
                Value::Text("order.*.created".into()),
            ],
        );
        assert_eq!(result, Value::Bool(true));
    }
}
