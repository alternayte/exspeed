use std::collections::HashMap;

use crate::parser::ast::{BinaryOperator, Expr};
use crate::runtime::eval::eval_expr;
use crate::types::{Row, Value};

/// Stateful processor for stream-stream WITHIN joins.
///
/// This is NOT a pull-based `Operator` — it is driven by the continuous
/// executor which pushes rows from two independent streams.  Each side
/// maintains a time-bounded buffer keyed by the join column value.  When a
/// new row arrives on one side we probe the opposite buffer for matches and
/// emit joined rows immediately.
pub struct StreamStreamJoinState {
    left_buffer: HashMap<String, Vec<(u64, Row)>>,
    right_buffer: HashMap<String, Vec<(u64, Row)>>,
    within_nanos: u64,
    left_key_expr: Expr,
    right_key_expr: Expr,
}

impl StreamStreamJoinState {
    /// Create a new join state.
    ///
    /// `within_nanos` is the maximum age (in nanoseconds) of buffered rows.
    /// `on_expr` must be `BinaryOp { left, op: Eq, right }` — the left
    /// sub-expression is used to extract the join key from left-side rows and
    /// the right sub-expression for right-side rows.
    pub fn new(within_nanos: u64, on_expr: &Expr) -> Self {
        let (left_key_expr, right_key_expr) = match on_expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => (*left.clone(), *right.clone()),
            other => (other.clone(), other.clone()),
        };

        Self {
            left_buffer: HashMap::new(),
            right_buffer: HashMap::new(),
            within_nanos,
            left_key_expr,
            right_key_expr,
        }
    }

    /// Process a row arriving on the **left** stream.
    ///
    /// 1. Evaluate `left_key_expr` against `row` to obtain the join key.
    /// 2. Probe `right_buffer` for matching entries and build joined rows.
    /// 3. Insert `(timestamp, row)` into `left_buffer`.
    /// 4. Return all joined rows (may be empty).
    pub fn process_left(&mut self, row: Row, timestamp: u64) -> Vec<Row> {
        let key = value_to_key(&eval_expr(&self.left_key_expr, &row));

        let mut results = Vec::new();
        if let Some(right_entries) = self.right_buffer.get(&key) {
            for (_ts, right_row) in right_entries {
                results.push(merge_rows(&row, right_row));
            }
        }

        self.left_buffer
            .entry(key)
            .or_default()
            .push((timestamp, row));

        results
    }

    /// Process a row arriving on the **right** stream.
    ///
    /// Mirror of [`process_left`] — probes the left buffer, inserts into the
    /// right buffer.
    pub fn process_right(&mut self, row: Row, timestamp: u64) -> Vec<Row> {
        let key = value_to_key(&eval_expr(&self.right_key_expr, &row));

        let mut results = Vec::new();
        if let Some(left_entries) = self.left_buffer.get(&key) {
            for (_ts, left_row) in left_entries {
                results.push(merge_rows(left_row, &row));
            }
        }

        self.right_buffer
            .entry(key)
            .or_default()
            .push((timestamp, row));

        results
    }

    /// Remove entries from both buffers whose timestamp is older than
    /// `now_nanos - within_nanos`.
    pub fn evict_expired(&mut self, now_nanos: u64) {
        let cutoff = now_nanos.saturating_sub(self.within_nanos);
        evict_buffer(&mut self.left_buffer, cutoff);
        evict_buffer(&mut self.right_buffer, cutoff);
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────

/// Convert a [`Value`] to a deterministic string for use as a `HashMap` key.
fn value_to_key(v: &Value) -> String {
    v.to_string()
}

/// Combine two rows (left + right) into one.
///
/// Column names from both sides are concatenated.  The downstream project
/// operator is responsible for aliasing / disambiguation.
fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.iter().cloned());
    let mut values = left.values.clone();
    values.extend(right.values.iter().cloned());
    Row { columns, values }
}

/// Remove entries whose timestamp is strictly less than `cutoff` from every
/// key in the buffer, dropping keys that become empty.
fn evict_buffer(buffer: &mut HashMap<String, Vec<(u64, Row)>>, cutoff: u64) {
    buffer.retain(|_key, entries| {
        entries.retain(|(ts, _)| *ts >= cutoff);
        !entries.is_empty()
    });
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr};
    use crate::types::{Row, Value};

    /// Build a simple `left_col = right_col` ON expression.
    fn eq_on_expr(left_col: &str, right_col: &str) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: left_col.into(),
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Column {
                table: None,
                name: right_col.into(),
            }),
        }
    }

    fn left_row(key: i64, order: &str) -> Row {
        Row {
            columns: vec!["user_id".into(), "order".into()],
            values: vec![Value::Int(key), Value::Text(order.into())],
        }
    }

    fn right_row(key: i64, name: &str) -> Row {
        Row {
            columns: vec!["id".into(), "name".into()],
            values: vec![Value::Int(key), Value::Text(name.into())],
        }
    }

    #[test]
    fn left_then_matching_right_emits_joined_row() {
        let on = eq_on_expr("user_id", "id");
        let mut state = StreamStreamJoinState::new(1_000_000_000, &on);

        // Left arrives first — no match yet.
        let results = state.process_left(left_row(1, "A"), 100);
        assert!(results.is_empty());

        // Right arrives with matching key — should emit joined row.
        let results = state.process_right(right_row(1, "Alice"), 200);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("user_id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("order"), Some(&Value::Text("A".into())));
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".into())));
    }

    #[test]
    fn left_with_no_match_returns_empty() {
        let on = eq_on_expr("user_id", "id");
        let mut state = StreamStreamJoinState::new(1_000_000_000, &on);

        // Insert a right row with key=2.
        let _ = state.process_right(right_row(2, "Bob"), 100);

        // Left row with key=1 — no match.
        let results = state.process_left(left_row(1, "A"), 200);
        assert!(results.is_empty());
    }

    #[test]
    fn right_arriving_later_matches_earlier_left() {
        let on = eq_on_expr("user_id", "id");
        let mut state = StreamStreamJoinState::new(1_000_000_000, &on);

        // Two left rows arrive with key=5.
        let _ = state.process_left(left_row(5, "X"), 100);
        let _ = state.process_left(left_row(5, "Y"), 200);

        // Right with key=5 should match both.
        let results = state.process_right(right_row(5, "Eve"), 300);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("order"), Some(&Value::Text("X".into())));
        assert_eq!(results[1].get("order"), Some(&Value::Text("Y".into())));
        // Both should have Eve's name.
        for r in &results {
            assert_eq!(r.get("name"), Some(&Value::Text("Eve".into())));
        }
    }

    #[test]
    fn eviction_removes_old_entries() {
        let on = eq_on_expr("user_id", "id");
        let within = 1_000; // 1000 ns window
        let mut state = StreamStreamJoinState::new(within, &on);

        // Insert left at t=100 and right at t=200.
        let _ = state.process_left(left_row(1, "A"), 100);
        let _ = state.process_right(right_row(1, "Alice"), 200);

        // Evict at t=1200 → cutoff = 1200 - 1000 = 200.
        // Left entry at t=100 should be evicted; right at t=200 survives (>= 200).
        state.evict_expired(1200);

        // New left with key=1 should still match the surviving right entry.
        let results = state.process_left(left_row(1, "B"), 1200);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".into())));

        // Evict at t=1201 → cutoff = 201 → right entry at 200 is also gone.
        state.evict_expired(1201);
        let results = state.process_left(left_row(1, "C"), 1201);
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_matches_both_buffers() {
        let on = eq_on_expr("user_id", "id");
        let mut state = StreamStreamJoinState::new(1_000_000_000, &on);

        // Two left rows with key=10.
        let _ = state.process_left(left_row(10, "P"), 100);
        let _ = state.process_left(left_row(10, "Q"), 200);

        // Two right rows with key=10.
        // First right matches the 2 existing lefts.
        let r1 = state.process_right(right_row(10, "Zara"), 300);
        assert_eq!(r1.len(), 2);

        // Second right also matches the 2 existing lefts.
        let r2 = state.process_right(right_row(10, "Yuki"), 400);
        assert_eq!(r2.len(), 2);

        // Now a new left should match both rights.
        let r3 = state.process_left(left_row(10, "R"), 500);
        assert_eq!(r3.len(), 2);
        let names: Vec<&str> = r3.iter().filter_map(|r| r.get("name")?.as_text()).collect();
        assert!(names.contains(&"Zara"));
        assert!(names.contains(&"Yuki"));
    }
}
