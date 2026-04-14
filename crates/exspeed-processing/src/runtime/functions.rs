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

        // ── Windowing ───────────────────────────────────────────────────
        "TUMBLING" => {
            let ts = args.first().and_then(|v| match v {
                Value::Timestamp(t) => Some(*t),
                Value::Int(n) => Some(*n as u64),
                _ => None,
            });
            let interval = args.get(1).and_then(|v| v.as_text());
            match (ts, interval) {
                (Some(ts), Some(interval)) => {
                    let window_nanos = parse_interval_nanos(interval);
                    if window_nanos == 0 {
                        Value::Null
                    } else {
                        let window_start = (ts / window_nanos) * window_nanos;
                        Value::Timestamp(window_start)
                    }
                }
                _ => Value::Null,
            }
        }

        _ => Value::Null,
    }
}

/// Parse an interval string like "1 hour", "30 minutes", "15 seconds" into nanoseconds.
///
/// Supports seconds, minutes, hours, days (singular and plural).
/// Returns 0 for unrecognised or malformed input.
pub fn parse_interval_nanos(interval: &str) -> u64 {
    let parts: Vec<&str> = interval.trim().splitn(2, ' ').collect();
    if parts.len() != 2 {
        return 0;
    }
    let amount: u64 = parts[0].parse().unwrap_or(0);
    let unit = parts[1].to_lowercase();
    let multiplier = match unit.trim_end_matches('s') {
        "second" => 1_000_000_000u64,
        "minute" => 60_000_000_000,
        "hour" => 3_600_000_000_000,
        "day" => 86_400_000_000_000,
        _ => 0,
    };
    amount * multiplier
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
    let start = args.get(1).and_then(|v| v.to_i64()).unwrap_or(1).max(1) as usize - 1;

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
            &[
                Value::Text("a".into()),
                Value::Null,
                Value::Text("b".into()),
            ],
        );
        assert_eq!(result, Value::Text("ab".into()));
    }

    #[test]
    fn test_substring() {
        let result = call_function(
            "SUBSTRING",
            &[
                Value::Text("hello world".into()),
                Value::Int(7),
                Value::Int(5),
            ],
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
        assert_eq!(
            call_function("ABS", &[Value::Float(-3.14)]),
            Value::Float(3.14)
        );
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

    // ── tumbling() tests ─────────────────────────────────────────────────

    /// A timestamp in the middle of an hour should floor to the start of that hour.
    #[test]
    fn tumbling_floors_to_hour() {
        // 2024-01-01 00:30:00 UTC in nanoseconds
        // 00:30:00 = 1800 seconds into the hour
        let hour_nanos: u64 = 3_600_000_000_000;
        let ts: u64 = 1_704_067_200_000_000_000 + 1_800_000_000_000; // + 30 min
        let result = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts), Value::Text("1 hour".into())],
        );
        let expected_start = (ts / hour_nanos) * hour_nanos;
        assert_eq!(result, Value::Timestamp(expected_start));
    }

    /// Two timestamps 30 minutes apart within the same hour should land in the same window.
    #[test]
    fn tumbling_same_window() {
        let hour_nanos: u64 = 3_600_000_000_000;
        let base: u64 = 1_704_067_200_000_000_000; // 2024-01-01 00:00:00 UTC
        let ts1 = base + 600_000_000_000; // + 10 min
        let ts2 = base + 1_800_000_000_000; // + 30 min

        let r1 = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts1), Value::Text("1 hour".into())],
        );
        let r2 = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts2), Value::Text("1 hour".into())],
        );

        assert_eq!(r1, r2);
        assert_eq!(r1, Value::Timestamp((ts1 / hour_nanos) * hour_nanos));
    }

    /// Timestamps in different hours should produce different window starts.
    #[test]
    fn tumbling_different_windows() {
        let base: u64 = 1_704_067_200_000_000_000; // 2024-01-01 00:00:00 UTC
        let ts1 = base + 600_000_000_000; // 00:10 — hour 0
        let ts2 = base + 3_660_000_000_000; // 01:01 — hour 1

        let r1 = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts1), Value::Text("1 hour".into())],
        );
        let r2 = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts2), Value::Text("1 hour".into())],
        );

        assert_ne!(r1, r2);
    }

    /// 5-minute tumbling windows should floor correctly.
    #[test]
    fn tumbling_five_minutes() {
        let five_min_nanos: u64 = 300_000_000_000;
        let base: u64 = 1_704_067_200_000_000_000; // 2024-01-01 00:00:00 UTC
        let ts = base + 7 * 60 * 1_000_000_000; // 00:07 → should land in [00:05, 00:10) window

        let result = call_function(
            "TUMBLING",
            &[Value::Timestamp(ts), Value::Text("5 minutes".into())],
        );
        let expected = (ts / five_min_nanos) * five_min_nanos;
        assert_eq!(result, Value::Timestamp(expected));
        // Sanity: window start is 00:05
        assert_eq!(expected, base + 5 * 60 * 1_000_000_000);
    }

    /// parse_interval_nanos handles a variety of unit spellings.
    #[test]
    fn parse_interval_various() {
        assert_eq!(parse_interval_nanos("1 hour"), 3_600_000_000_000);
        assert_eq!(parse_interval_nanos("30 minutes"), 30 * 60_000_000_000);
        assert_eq!(parse_interval_nanos("1 day"), 86_400_000_000_000);
        assert_eq!(parse_interval_nanos("15 seconds"), 15 * 1_000_000_000);
        assert_eq!(parse_interval_nanos("5 hours"), 5 * 3_600_000_000_000);
    }
}
