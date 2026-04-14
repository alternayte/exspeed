use crate::parser::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use crate::runtime::functions;
use crate::types::{Row, Value};

use std::cmp::Ordering;

// ═══════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════

/// Recursively evaluate an [`Expr`] against a [`Row`], producing a [`Value`].
pub fn eval_expr(expr: &Expr, row: &Row) -> Value {
    match expr {
        // ── Column reference ────────────────────────────────────────────
        Expr::Column { table, name } => {
            // Try qualified name first (table.column), then bare column name.
            if let Some(tbl) = table {
                let qualified = format!("{tbl}.{name}");
                if let Some(v) = row.get(&qualified) {
                    return v.clone();
                }
            }
            row.get(name).cloned().unwrap_or(Value::Null)
        }

        // ── Literal ─────────────────────────────────────────────────────
        Expr::Literal(lit) => match lit {
            LiteralValue::Int(v) => Value::Int(*v),
            LiteralValue::Float(v) => Value::Float(*v),
            LiteralValue::String(s) => Value::Text(s.clone()),
            LiteralValue::Bool(b) => Value::Bool(*b),
            LiteralValue::Null => Value::Null,
        },

        // ── Binary operator ─────────────────────────────────────────────
        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr(left, row);
            let rv = eval_expr(right, row);
            eval_binary_op(&lv, op, &rv)
        }

        // ── Unary operator ──────────────────────────────────────────────
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row);
            match op {
                UnaryOperator::Not => match v {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    _ => Value::Null,
                },
                UnaryOperator::Neg => match v {
                    Value::Int(i) => Value::Int(-i),
                    Value::Float(f) => Value::Float(-f),
                    Value::Null => Value::Null,
                    _ => Value::Null,
                },
            }
        }

        // ── JSON access (-> / ->>) ──────────────────────────────────────
        Expr::JsonAccess {
            expr,
            field,
            as_text,
        } => {
            let v = eval_expr(expr, row);
            eval_json_access(&v, field, *as_text)
        }

        // ── Cast ────────────────────────────────────────────────────────
        Expr::Cast { expr, to_type } => {
            let v = eval_expr(expr, row);
            eval_cast(&v, to_type)
        }

        // ── Function call ───────────────────────────────────────────────
        Expr::Function { name, args } => {
            let evaluated: Vec<Value> = args.iter().map(|a| eval_expr(a, row)).collect();
            functions::call_function(name, &evaluated)
        }

        // ── CASE / WHEN ─────────────────────────────────────────────────
        Expr::Case {
            conditions,
            else_val,
        } => {
            for (cond, result) in conditions {
                let cv = eval_expr(cond, row);
                if is_truthy(&cv) {
                    return eval_expr(result, row);
                }
            }
            match else_val {
                Some(e) => eval_expr(e, row),
                None => Value::Null,
            }
        }

        // ── IS NULL / IS NOT NULL ───────────────────────────────────────
        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, row);
            let is_null = v.is_null();
            Value::Bool(if *negated { !is_null } else { is_null })
        }

        // ── IN list ─────────────────────────────────────────────────────
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_expr(expr, row);
            let found = list.iter().any(|item| {
                let iv = eval_expr(item, row);
                compare_values(&v, &iv) == Some(Ordering::Equal)
            });
            Value::Bool(if *negated { !found } else { found })
        }

        // ── BETWEEN ────────────────────────────────────────────────────
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval_expr(expr, row);
            let lo = eval_expr(low, row);
            let hi = eval_expr(high, row);
            let in_range = matches!(compare_values(&v, &lo), Some(Ordering::Equal | Ordering::Greater))
                && matches!(compare_values(&v, &hi), Some(Ordering::Equal | Ordering::Less));
            Value::Bool(if *negated { !in_range } else { in_range })
        }

        // ── Interval literal ────────────────────────────────────────────
        Expr::Interval { value } => parse_interval(value),

        // ── Wildcard / Aggregate — resolved elsewhere ───────────────────
        Expr::Wildcard { .. } => Value::Null,
        Expr::Aggregate { .. } => Value::Null,

        // ── Subquery — not evaluated inline ─────────────────────────────
        Expr::Subquery(_) => Value::Null,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Evaluate a binary operator on two already-evaluated [`Value`]s.
fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Value {
    // NULL propagation for most operators (except And/Or which have
    // three-valued logic).
    match op {
        BinaryOperator::And => return eval_and(left, right),
        BinaryOperator::Or => return eval_or(left, right),
        _ => {}
    }

    if left.is_null() || right.is_null() {
        // Comparison with NULL yields NULL in standard SQL.
        return Value::Null;
    }

    match op {
        // ── Arithmetic ──────────────────────────────────────────────────
        BinaryOperator::Add => arith(left, right, |a, b| a + b),
        BinaryOperator::Sub => arith(left, right, |a, b| a - b),
        BinaryOperator::Mul => arith(left, right, |a, b| a * b),
        BinaryOperator::Div => {
            if let Some(b) = right.to_f64() {
                if b == 0.0 {
                    return Value::Null;
                }
            }
            arith(left, right, |a, b| a / b)
        }
        BinaryOperator::Mod => {
            if let Some(b) = right.to_f64() {
                if b == 0.0 {
                    return Value::Null;
                }
            }
            arith(left, right, |a, b| a % b)
        }

        // ── Comparison ──────────────────────────────────────────────────
        BinaryOperator::Eq => Value::Bool(compare_values(left, right) == Some(Ordering::Equal)),
        BinaryOperator::Neq => Value::Bool(compare_values(left, right) != Some(Ordering::Equal)),
        BinaryOperator::Lt => Value::Bool(compare_values(left, right) == Some(Ordering::Less)),
        BinaryOperator::Gt => Value::Bool(compare_values(left, right) == Some(Ordering::Greater)),
        BinaryOperator::Lte => {
            Value::Bool(matches!(
                compare_values(left, right),
                Some(Ordering::Less | Ordering::Equal)
            ))
        }
        BinaryOperator::Gte => {
            Value::Bool(matches!(
                compare_values(left, right),
                Some(Ordering::Greater | Ordering::Equal)
            ))
        }

        // ── LIKE ────────────────────────────────────────────────────────
        BinaryOperator::Like => {
            let lhs = value_to_string(left);
            let pattern = value_to_string(right);
            Value::Bool(like_match(&lhs, &pattern))
        }

        // And / Or already handled above.
        BinaryOperator::And | BinaryOperator::Or => unreachable!(),
    }
}

/// Three-valued AND: false AND x = false regardless of x.
fn eval_and(left: &Value, right: &Value) -> Value {
    let l = to_tribool(left);
    let r = to_tribool(right);
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

/// Three-valued OR: true OR x = true regardless of x.
fn eval_or(left: &Value, right: &Value) -> Value {
    let l = to_tribool(left);
    let r = to_tribool(right);
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

/// Convert a Value to an Option<bool> for three-valued logic.
fn to_tribool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Null => None,
        Value::Int(i) => Some(*i != 0),
        _ => None,
    }
}

/// Arithmetic helper: convert both sides to f64, apply `f`, and return
/// Int if both inputs were integral, Float otherwise.
fn arith(left: &Value, right: &Value, f: impl Fn(f64, f64) -> f64) -> Value {
    let a = match left.to_f64() {
        Some(v) => v,
        None => return Value::Null,
    };
    let b = match right.to_f64() {
        Some(v) => v,
        None => return Value::Null,
    };
    let result = f(a, b);

    // If both operands were integers and the result is representable as i64,
    // return an Int to preserve integer types.
    let both_int = matches!(left, Value::Int(_)) && matches!(right, Value::Int(_));
    if both_int && result.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&result) {
        Value::Int(result as i64)
    } else {
        Value::Float(result)
    }
}

/// Compare two non-null values, returning an ordering when comparable.
pub fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) | (_, Value::Null) => None,

        // Same-type comparisons
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),

        // Cross-numeric comparisons
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),

        // Fallback: compare Display representations
        _ => left.to_string().partial_cmp(&right.to_string()),
    }
}

/// Access a field on a JSON value (simulates `->` / `->>` operators).
fn eval_json_access(val: &Value, field: &str, as_text: bool) -> Value {
    let json_val = match val {
        Value::Json(j) => j,
        Value::Text(s) => {
            // Try parsing as JSON
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => {
                    return json_field_to_value(&j, field, as_text);
                }
                Err(_) => return Value::Null,
            }
        }
        _ => return Value::Null,
    };
    json_field_to_value(json_val, field, as_text)
}

fn json_field_to_value(json: &serde_json::Value, field: &str, as_text: bool) -> Value {
    let accessed = match json.get(field) {
        Some(v) => v,
        None => return Value::Null,
    };
    if as_text {
        // ->> returns the value as a plain string (unquoted for strings).
        match accessed {
            serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Null => Value::Null,
            other => Value::Text(other.to_string()),
        }
    } else {
        // -> returns a JSON value.
        Value::Json(accessed.clone())
    }
}

/// Evaluate a CAST expression.
fn eval_cast(val: &Value, to_type: &str) -> Value {
    match to_type.to_uppercase().as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => match val.to_i64() {
            Some(i) => Value::Int(i),
            None => Value::Null,
        },
        "FLOAT" | "DOUBLE" | "DECIMAL" | "REAL" | "NUMERIC" => match val.to_f64() {
            Some(f) => Value::Float(f),
            None => Value::Null,
        },
        "TEXT" | "VARCHAR" | "STRING" | "CHAR" => Value::Text(val.to_string()),
        "BOOL" | "BOOLEAN" => match val {
            Value::Bool(b) => Value::Bool(*b),
            Value::Int(i) => Value::Bool(*i != 0),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "t" => Value::Bool(true),
                "false" | "0" | "no" | "f" => Value::Bool(false),
                _ => Value::Null,
            },
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

/// Convert a Value to a string (for LIKE etc.).
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Simple LIKE pattern matching with `%` (any chars) and `_` (single char).
fn like_match(text: &str, pattern: &str) -> bool {
    like_match_recursive(text.as_bytes(), pattern.as_bytes())
}

fn like_match_recursive(text: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'%' => {
            // '%' matches zero or more characters.
            // Try matching the rest of the pattern at every position.
            for i in 0..=text.len() {
                if like_match_recursive(&text[i..], &pattern[1..]) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // '_' matches exactly one character.
            !text.is_empty() && like_match_recursive(&text[1..], &pattern[1..])
        }
        c => {
            !text.is_empty()
                && text[0].to_ascii_lowercase() == c.to_ascii_lowercase()
                && like_match_recursive(&text[1..], &pattern[1..])
        }
    }
}

/// Parse an interval string like "1 hour", "30 minutes", "7 days" into
/// nanoseconds, returned as `Value::Int`.
fn parse_interval(s: &str) -> Value {
    let s = s.trim().to_lowercase();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        // Try bare number (assume seconds)
        if let Ok(n) = s.parse::<i64>() {
            return Value::Int(n * 1_000_000_000);
        }
        return Value::Null;
    }

    let amount: f64 = match parts[0].parse() {
        Ok(n) => n,
        Err(_) => return Value::Null,
    };

    let unit = parts[1].trim_end_matches('s'); // normalize plural
    let nanos_per_unit: f64 = match unit {
        "nanosecond" | "nano" | "ns" => 1.0,
        "microsecond" | "micro" | "us" => 1_000.0,
        "millisecond" | "milli" | "ms" => 1_000_000.0,
        "second" | "sec" => 1_000_000_000.0,
        "minute" | "min" => 60.0 * 1_000_000_000.0,
        "hour" | "hr" => 3_600.0 * 1_000_000_000.0,
        "day" => 86_400.0 * 1_000_000_000.0,
        "week" => 7.0 * 86_400.0 * 1_000_000_000.0,
        _ => return Value::Null,
    };

    Value::Int((amount * nanos_per_unit) as i64)
}

/// Helper: is a value truthy?
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Null => false,
        _ => true,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{AggregateFunc, BinaryOperator, Expr, LiteralValue, UnaryOperator};
    use crate::types::{Row, Value};

    /// Helper to build a Row for testing.
    fn test_row() -> Row {
        Row {
            columns: vec![
                "id".into(),
                "name".into(),
                "score".into(),
                "payload".into(),
                "t.id".into(),
                "empty".into(),
            ],
            values: vec![
                Value::Int(1),
                Value::Text("Alice".into()),
                Value::Float(95.5),
                Value::Json(serde_json::json!({"name": "Bob", "age": 30})),
                Value::Int(42),
                Value::Null,
            ],
        }
    }

    #[test]
    fn eval_column_lookup() {
        let row = test_row();
        // Bare column
        let expr = Expr::Column {
            table: None,
            name: "name".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Text("Alice".into()));

        // Qualified column
        let expr = Expr::Column {
            table: Some("t".into()),
            name: "id".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(42));

        // Missing column
        let expr = Expr::Column {
            table: None,
            name: "missing".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Null);
    }

    #[test]
    fn eval_literal() {
        let row = test_row();

        assert_eq!(
            eval_expr(&Expr::Literal(LiteralValue::Int(7)), &row),
            Value::Int(7)
        );
        assert_eq!(
            eval_expr(&Expr::Literal(LiteralValue::String("hi".into())), &row),
            Value::Text("hi".into())
        );
        assert_eq!(
            eval_expr(&Expr::Literal(LiteralValue::Null), &row),
            Value::Null
        );
    }

    #[test]
    fn eval_binary_arithmetic() {
        let row = test_row();

        // 1 + 2 = 3
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(1))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int(2))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(3));

        // 10 - 3 = 7
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(10))),
            op: BinaryOperator::Sub,
            right: Box::new(Expr::Literal(LiteralValue::Int(3))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(7));

        // 4 * 5 = 20
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(4))),
            op: BinaryOperator::Mul,
            right: Box::new(Expr::Literal(LiteralValue::Int(5))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(20));
    }

    #[test]
    fn eval_binary_comparison() {
        let row = test_row();

        // 5 > 3 = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(5))),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(LiteralValue::Int(3))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        // "a" = "a" = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::String("a".into()))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(LiteralValue::String("a".into()))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        // 3 <= 3 = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(3))),
            op: BinaryOperator::Lte,
            right: Box::new(Expr::Literal(LiteralValue::Int(3))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));
    }

    #[test]
    fn eval_logical_and_or() {
        let row = test_row();

        // true AND false = false
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Bool(true))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(LiteralValue::Bool(false))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));

        // true OR false = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Bool(true))),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Literal(LiteralValue::Bool(false))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));
    }

    #[test]
    fn eval_json_access_test() {
        let row = test_row();

        // payload->>'name' on a Json value = "Bob" as Text
        let expr = Expr::JsonAccess {
            expr: Box::new(Expr::Column {
                table: None,
                name: "payload".into(),
            }),
            field: "name".into(),
            as_text: true,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Text("Bob".into()));

        // payload->'age' returns Json
        let expr = Expr::JsonAccess {
            expr: Box::new(Expr::Column {
                table: None,
                name: "payload".into(),
            }),
            field: "age".into(),
            as_text: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Json(serde_json::json!(30)));
    }

    #[test]
    fn eval_cast_text_to_int() {
        let row = test_row();

        // "42"::INTEGER = 42
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(LiteralValue::String("42".into()))),
            to_type: "INTEGER".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(42));
    }

    #[test]
    fn eval_case_when() {
        let row = test_row();

        // CASE WHEN true THEN 1 ELSE 2 END = 1
        let expr = Expr::Case {
            conditions: vec![(
                Expr::Literal(LiteralValue::Bool(true)),
                Expr::Literal(LiteralValue::Int(1)),
            )],
            else_val: Some(Box::new(Expr::Literal(LiteralValue::Int(2)))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(1));

        // CASE WHEN false THEN 1 ELSE 2 END = 2
        let expr = Expr::Case {
            conditions: vec![(
                Expr::Literal(LiteralValue::Bool(false)),
                Expr::Literal(LiteralValue::Int(1)),
            )],
            else_val: Some(Box::new(Expr::Literal(LiteralValue::Int(2)))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(2));
    }

    #[test]
    fn eval_is_null_test() {
        let row = test_row();

        // empty IS NULL = true
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "empty".into(),
            }),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        // name IS NULL = false
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "name".into(),
            }),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));

        // name IS NOT NULL = true
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "name".into(),
            }),
            negated: true,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));
    }

    #[test]
    fn eval_function_upper() {
        let row = test_row();

        let expr = Expr::Function {
            name: "UPPER".into(),
            args: vec![Expr::Literal(LiteralValue::String("hello".into()))],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Text("HELLO".into()));
    }

    #[test]
    fn eval_function_coalesce() {
        let row = test_row();

        let expr = Expr::Function {
            name: "COALESCE".into(),
            args: vec![
                Expr::Literal(LiteralValue::Null),
                Expr::Literal(LiteralValue::String("a".into())),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Text("a".into()));
    }

    #[test]
    fn eval_function_subject_part() {
        let row = test_row();

        let expr = Expr::Function {
            name: "subject_part".into(),
            args: vec![
                Expr::Literal(LiteralValue::String("order.eu.created".into())),
                Expr::Literal(LiteralValue::Int(2)),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Text("eu".into()));
    }

    #[test]
    fn eval_interval() {
        let row = test_row();

        let expr = Expr::Interval {
            value: "1 hour".into(),
        };
        assert_eq!(
            eval_expr(&expr, &row),
            Value::Int(3_600_000_000_000) // 1 hour in nanos
        );

        let expr = Expr::Interval {
            value: "30 minutes".into(),
        };
        assert_eq!(
            eval_expr(&expr, &row),
            Value::Int(30 * 60 * 1_000_000_000)
        );

        let expr = Expr::Interval {
            value: "7 days".into(),
        };
        assert_eq!(
            eval_expr(&expr, &row),
            Value::Int(7 * 86_400 * 1_000_000_000)
        );
    }

    #[test]
    fn eval_in_list() {
        let row = test_row();

        let expr = Expr::InList {
            expr: Box::new(Expr::Literal(LiteralValue::Int(2))),
            list: vec![
                Expr::Literal(LiteralValue::Int(1)),
                Expr::Literal(LiteralValue::Int(2)),
                Expr::Literal(LiteralValue::Int(3)),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        let expr = Expr::InList {
            expr: Box::new(Expr::Literal(LiteralValue::Int(5))),
            list: vec![
                Expr::Literal(LiteralValue::Int(1)),
                Expr::Literal(LiteralValue::Int(2)),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));
    }

    #[test]
    fn eval_between() {
        let row = test_row();

        let expr = Expr::Between {
            expr: Box::new(Expr::Literal(LiteralValue::Int(5))),
            low: Box::new(Expr::Literal(LiteralValue::Int(1))),
            high: Box::new(Expr::Literal(LiteralValue::Int(10))),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        let expr = Expr::Between {
            expr: Box::new(Expr::Literal(LiteralValue::Int(15))),
            low: Box::new(Expr::Literal(LiteralValue::Int(1))),
            high: Box::new(Expr::Literal(LiteralValue::Int(10))),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));
    }

    #[test]
    fn eval_unary_not() {
        let row = test_row();

        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Literal(LiteralValue::Bool(true))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));
    }

    #[test]
    fn eval_unary_neg() {
        let row = test_row();

        let expr = Expr::UnaryOp {
            op: UnaryOperator::Neg,
            expr: Box::new(Expr::Literal(LiteralValue::Int(42))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Int(-42));
    }

    #[test]
    fn eval_like() {
        let row = test_row();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::String("hello world".into()))),
            op: BinaryOperator::Like,
            right: Box::new(Expr::Literal(LiteralValue::String("hello%".into()))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(true));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::String("hello".into()))),
            op: BinaryOperator::Like,
            right: Box::new(Expr::Literal(LiteralValue::String("world%".into()))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Bool(false));
    }

    #[test]
    fn eval_wildcard_and_aggregate_return_null() {
        let row = test_row();

        assert_eq!(
            eval_expr(&Expr::Wildcard { table: None }, &row),
            Value::Null
        );
        assert_eq!(
            eval_expr(
                &Expr::Aggregate {
                    func: AggregateFunc::Count,
                    expr: Box::new(Expr::Wildcard { table: None }),
                    distinct: false,
                },
                &row,
            ),
            Value::Null
        );
    }

    #[test]
    fn eval_null_propagation() {
        let row = test_row();

        // NULL + 1 = NULL
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Null)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int(1))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Null);

        // NULL = NULL → NULL (SQL semantics)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Null)),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Null)),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Null);
    }

    #[test]
    fn eval_div_by_zero() {
        let row = test_row();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int(10))),
            op: BinaryOperator::Div,
            right: Box::new(Expr::Literal(LiteralValue::Int(0))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Null);
    }
}
