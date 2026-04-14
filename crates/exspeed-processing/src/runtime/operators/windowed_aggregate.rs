use std::collections::HashMap;

use crate::parser::ast::{AggregateFunc, EmitMode, Expr, SelectItem};
use crate::runtime::eval::{compare_values, eval_expr};
use crate::types::{Row, Value};

// ═══════════════════════════════════════════════════════════════════════════
// Public types
// ═══════════════════════════════════════════════════════════════════════════

/// Stateful processor for tumbling-window aggregations in the continuous
/// executor.  Records are pushed in via [`process_record`]; output rows are
/// either returned immediately (EMIT CHANGES) or on window close
/// (EMIT FINAL, via [`check_closed_windows`]).
pub struct WindowedAggregateState {
    window_size_nanos: u64,
    grace_period_nanos: u64,
    group_by_exprs: Vec<Expr>,
    aggregate_items: Vec<SelectItem>,
    emit_mode: EmitMode,
    windows: HashMap<u64, WindowState>,
}

struct WindowState {
    groups: HashMap<String, GroupAccumulator>,
    window_end: u64,
}

struct GroupAccumulator {
    group_values: Vec<Value>,
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Implementation
// ═══════════════════════════════════════════════════════════════════════════

impl WindowedAggregateState {
    pub fn new(
        window_size_nanos: u64,
        grace_period_nanos: u64,
        group_by_exprs: Vec<Expr>,
        aggregate_items: Vec<SelectItem>,
        emit_mode: EmitMode,
    ) -> Self {
        Self {
            window_size_nanos,
            grace_period_nanos,
            group_by_exprs,
            aggregate_items,
            emit_mode,
            windows: HashMap::new(),
        }
    }

    /// Push a record into the windowed aggregation state.
    ///
    /// Returns output rows immediately for EMIT CHANGES, or an empty vec
    /// for EMIT FINAL (rows are emitted via [`check_closed_windows`]).
    pub fn process_record(&mut self, row: &Row, timestamp: u64) -> Vec<Row> {
        let window_start = (timestamp / self.window_size_nanos) * self.window_size_nanos;
        let window_end = window_start + self.window_size_nanos;

        // Check if this record is a late arrival for an already-closed window.
        // A window is considered closed when the latest observed event time
        // has moved past the window's close deadline (window_end + grace).
        let max_observed_end = self.windows.values().map(|w| w.window_end).max().unwrap_or(0);
        if max_observed_end > 0
            && window_end + self.grace_period_nanos <= max_observed_end
        {
            return Vec::new();
        }

        // Compute the group key from group_by_exprs.
        let group_values: Vec<Value> = self
            .group_by_exprs
            .iter()
            .map(|expr| eval_expr(expr, row))
            .collect();
        let group_key = group_values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("|");

        // Pre-evaluate aggregate input values before borrowing windows mutably.
        let agg_inputs: Vec<Option<(AggregateFunc, Value)>> = self
            .aggregate_items
            .iter()
            .map(|item| {
                if let Expr::Aggregate { func, expr, .. } = &item.expr {
                    Some((func.clone(), eval_expr(expr, row)))
                } else {
                    None
                }
            })
            .collect();

        let has_count_agg = agg_inputs
            .iter()
            .any(|a| matches!(a, Some((AggregateFunc::Count, _))));

        // Get or create the WindowState and update the accumulator.
        {
            let ws = self.windows.entry(window_start).or_insert_with(|| WindowState {
                groups: HashMap::new(),
                window_end,
            });

            let acc = ws.groups.entry(group_key.clone()).or_insert_with(|| GroupAccumulator {
                group_values: group_values.clone(),
                count: 0,
                sum: 0.0,
                min: None,
                max: None,
            });

            for input in &agg_inputs {
                if let Some((func, val)) = input {
                    match func {
                        AggregateFunc::Count => {
                            acc.count += 1;
                        }
                        AggregateFunc::Sum | AggregateFunc::Avg => {
                            if let Some(f) = val.to_f64() {
                                acc.sum += f;
                            }
                        }
                        AggregateFunc::Min => {
                            if !val.is_null() {
                                match &acc.min {
                                    None => acc.min = Some(val.clone()),
                                    Some(current) => {
                                        if compare_values(val, current)
                                            == Some(std::cmp::Ordering::Less)
                                        {
                                            acc.min = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                        AggregateFunc::Max => {
                            if !val.is_null() {
                                match &acc.max {
                                    None => acc.max = Some(val.clone()),
                                    Some(current) => {
                                        if compare_values(val, current)
                                            == Some(std::cmp::Ordering::Greater)
                                        {
                                            acc.max = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !has_count_agg {
                acc.count += 1;
            }
        }
        // Mutable borrow of self.windows is now dropped.

        match self.emit_mode {
            EmitMode::Changes => {
                let ws = self.windows.get(&window_start).unwrap();
                let acc = ws.groups.get(&group_key).unwrap();
                let output = self.build_output_row(window_start, &group_key, acc);
                vec![output]
            }
            EmitMode::Final => Vec::new(),
        }
    }

    /// Check for windows that have closed (now_nanos > window_end + grace_period)
    /// and emit their final output rows.
    pub fn check_closed_windows(&mut self, now_nanos: u64) -> Vec<Row> {
        let mut output = Vec::new();

        // Find closed window starts.
        let closed: Vec<u64> = self
            .windows
            .iter()
            .filter(|(_, ws)| now_nanos > ws.window_end + self.grace_period_nanos)
            .map(|(start, _)| *start)
            .collect();

        for window_start in &closed {
            if let Some(ws) = self.windows.get(window_start) {
                // For EMIT FINAL, build output for all groups.
                // For EMIT CHANGES, groups were already emitted on each update,
                // but we still need to clean up.
                if self.emit_mode == EmitMode::Final {
                    for (group_key, acc) in &ws.groups {
                        output.push(self.build_output_row(*window_start, group_key, acc));
                    }
                }
            }
            self.windows.remove(window_start);
        }

        output
    }

    /// Build an output row from the window start, group key, and accumulator.
    fn build_output_row(
        &self,
        window_start: u64,
        _group_key: &str,
        acc: &GroupAccumulator,
    ) -> Row {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        // Add window_start column.
        columns.push("window_start".to_string());
        values.push(Value::Timestamp(window_start));

        // Add group columns.
        for (i, expr) in self.group_by_exprs.iter().enumerate() {
            let col_name = match expr {
                Expr::Column { name, .. } => name.clone(),
                _ => format!("group_{i}"),
            };
            columns.push(col_name);
            values.push(
                acc.group_values
                    .get(i)
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }

        // Add aggregate result columns.
        for item in &self.aggregate_items {
            let col_name = item
                .alias
                .clone()
                .unwrap_or_else(|| derive_column_name(&item.expr));

            let val = match &item.expr {
                Expr::Aggregate { func, .. } => match func {
                    AggregateFunc::Count => Value::Int(acc.count),
                    AggregateFunc::Sum => {
                        if acc.count == 0 {
                            Value::Null
                        } else if acc.sum.fract() == 0.0
                            && (i64::MIN as f64..=i64::MAX as f64).contains(&acc.sum)
                        {
                            Value::Int(acc.sum as i64)
                        } else {
                            Value::Float(acc.sum)
                        }
                    }
                    AggregateFunc::Avg => {
                        if acc.count == 0 {
                            Value::Null
                        } else {
                            Value::Float(acc.sum / acc.count as f64)
                        }
                    }
                    AggregateFunc::Min => acc.min.clone().unwrap_or(Value::Null),
                    AggregateFunc::Max => acc.max.clone().unwrap_or(Value::Null),
                },
                Expr::Column { .. } => {
                    // Look up from group_values via a temporary row.
                    let group_row = Row {
                        columns: self
                            .group_by_exprs
                            .iter()
                            .map(|e| match e {
                                Expr::Column { name, .. } => name.clone(),
                                _ => "?".into(),
                            })
                            .collect(),
                        values: acc.group_values.clone(),
                    };
                    eval_expr(&item.expr, &group_row)
                }
                _ => Value::Null,
            };

            columns.push(col_name);
            values.push(val);
        }

        Row { columns, values }
    }
}

/// Derive a column name for an expression.
fn derive_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate { func, .. } => match func {
            AggregateFunc::Count => "count".into(),
            AggregateFunc::Sum => "sum".into(),
            AggregateFunc::Avg => "avg".into(),
            AggregateFunc::Min => "min".into(),
            AggregateFunc::Max => "max".into(),
        },
        Expr::Column { name, .. } => name.clone(),
        _ => "?column?".into(),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{AggregateFunc, Expr, SelectItem};

    /// 1 hour in nanoseconds.
    const HOUR_NANOS: u64 = 3_600_000_000_000;
    /// 10 seconds in nanoseconds.
    const TEN_SEC_NANOS: u64 = 10_000_000_000;
    /// Grace period: 5 seconds.
    const GRACE_NANOS: u64 = 5_000_000_000;

    /// Build a simple row with (region: Text, amount: Int).
    fn make_row(region: &str, amount: i64) -> Row {
        Row {
            columns: vec!["region".into(), "amount".into()],
            values: vec![Value::Text(region.into()), Value::Int(amount)],
        }
    }

    /// Standard group-by: GROUP BY region.
    fn group_by_region() -> Vec<Expr> {
        vec![Expr::Column {
            table: None,
            name: "region".into(),
        }]
    }

    /// Standard SELECT items: COUNT(*), SUM(amount).
    fn count_sum_items() -> Vec<SelectItem> {
        vec![
            SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Count,
                    expr: Box::new(Expr::Wildcard { table: None }),
                    distinct: false,
                },
                alias: None,
            },
            SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Sum,
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "amount".into(),
                    }),
                    distinct: false,
                },
                alias: None,
            },
        ]
    }

    // ── Test 1: 10 records across 3 windows → 3 WindowStates created ────

    #[test]
    fn three_windows_from_ten_records() {
        let mut state = WindowedAggregateState::new(
            TEN_SEC_NANOS,
            GRACE_NANOS,
            group_by_region(),
            count_sum_items(),
            EmitMode::Final,
        );

        let base: u64 = 1_000_000_000_000; // arbitrary base

        // Window 0: [base, base+10s) — 4 records
        for i in 0..4 {
            let ts = base + i * 1_000_000_000; // +0s, +1s, +2s, +3s
            state.process_record(&make_row("us", 10), ts);
        }

        // Window 1: [base+10s, base+20s) — 3 records
        for i in 0..3 {
            let ts = base + TEN_SEC_NANOS + i * 1_000_000_000;
            state.process_record(&make_row("eu", 20), ts);
        }

        // Window 2: [base+20s, base+30s) — 3 records
        for i in 0..3 {
            let ts = base + 2 * TEN_SEC_NANOS + i * 1_000_000_000;
            state.process_record(&make_row("us", 30), ts);
        }

        assert_eq!(state.windows.len(), 3, "should have 3 distinct windows");
    }

    // ── Test 2: EMIT CHANGES returns updated row each time ──────────────

    #[test]
    fn emit_changes_returns_row_each_time() {
        let mut state = WindowedAggregateState::new(
            HOUR_NANOS,
            GRACE_NANOS,
            group_by_region(),
            count_sum_items(),
            EmitMode::Changes,
        );

        let base: u64 = 1_000_000_000_000;

        // First record.
        let result = state.process_record(&make_row("us", 10), base);
        assert_eq!(result.len(), 1);
        let r = &result[0];
        assert_eq!(r.get("count"), Some(&Value::Int(1)));
        assert_eq!(r.get("sum"), Some(&Value::Int(10)));

        // Second record — same group, same window.
        let result = state.process_record(&make_row("us", 20), base + 1_000_000_000);
        assert_eq!(result.len(), 1);
        let r = &result[0];
        assert_eq!(r.get("count"), Some(&Value::Int(2)));
        assert_eq!(r.get("sum"), Some(&Value::Int(30)));

        // Third record — different group, same window.
        let result = state.process_record(&make_row("eu", 50), base + 2_000_000_000);
        assert_eq!(result.len(), 1);
        let r = &result[0];
        assert_eq!(r.get("count"), Some(&Value::Int(1)));
        assert_eq!(r.get("sum"), Some(&Value::Int(50)));
        assert_eq!(r.get("region"), Some(&Value::Text("eu".into())));
    }

    // ── Test 3: EMIT FINAL — process returns empty, close emits ─────────

    #[test]
    fn emit_final_process_empty_close_emits() {
        let mut state = WindowedAggregateState::new(
            TEN_SEC_NANOS,
            GRACE_NANOS,
            group_by_region(),
            count_sum_items(),
            EmitMode::Final,
        );

        let base: u64 = 1_000_000_000_000;

        // Process records — should return empty for EMIT FINAL.
        let r1 = state.process_record(&make_row("us", 10), base);
        assert!(r1.is_empty(), "EMIT FINAL should not emit on process");

        let r2 = state.process_record(&make_row("us", 20), base + 1_000_000_000);
        assert!(r2.is_empty());

        // Not closed yet — within grace period.
        let not_yet = state.check_closed_windows(base + TEN_SEC_NANOS + GRACE_NANOS);
        assert!(not_yet.is_empty(), "window should not close at exactly end + grace");

        // Now close.
        let closed = state.check_closed_windows(base + TEN_SEC_NANOS + GRACE_NANOS + 1);
        assert_eq!(closed.len(), 1, "should emit 1 row (1 group)");
        assert_eq!(closed[0].get("count"), Some(&Value::Int(2)));
        assert_eq!(closed[0].get("sum"), Some(&Value::Int(30)));

        // Window should be removed.
        assert!(state.windows.is_empty());
    }

    // ── Test 4: Accumulator tracks count + sum correctly ────────────────

    #[test]
    fn accumulator_count_sum_correct() {
        let mut state = WindowedAggregateState::new(
            HOUR_NANOS,
            GRACE_NANOS,
            vec![], // no group by — single global group
            count_sum_items(),
            EmitMode::Changes,
        );

        let base: u64 = 1_000_000_000_000;

        let amounts = [10, 20, 30, 40, 50];
        for (i, &amt) in amounts.iter().enumerate() {
            let result = state.process_record(&make_row("any", amt), base + (i as u64) * 1_000_000_000);
            assert_eq!(result.len(), 1);

            let expected_count = (i + 1) as i64;
            let expected_sum: i64 = amounts[..=i].iter().sum();
            assert_eq!(
                result[0].get("count"),
                Some(&Value::Int(expected_count)),
                "count mismatch at step {i}"
            );
            assert_eq!(
                result[0].get("sum"),
                Some(&Value::Int(expected_sum)),
                "sum mismatch at step {i}"
            );
        }
    }

    // ── Test 5: Late arrival after grace period → no state created ──────

    #[test]
    fn late_arrival_rejected() {
        let mut state = WindowedAggregateState::new(
            TEN_SEC_NANOS,
            GRACE_NANOS,
            group_by_region(),
            count_sum_items(),
            EmitMode::Final,
        );

        let base: u64 = 1_000_000_000_000;

        // Insert into window 0.
        state.process_record(&make_row("us", 10), base);

        // Advance time: insert into a much later window so that window 0
        // is clearly past its grace period.
        // Window 0 ends at base + 10s.  Grace is 5s.
        // So we need a record whose window_end > base + 10s + 5s.
        // Window 2 starts at base + 20s, ends at base + 30s.
        state.process_record(&make_row("us", 20), base + 2 * TEN_SEC_NANOS);

        // Now attempt a late arrival for window 0.
        // Window 0: start=base, end=base+10s.
        // Current max window_end = base + 30s.
        // base+10s + 5s = base+15s  <=  base+30s → late.
        let late = state.process_record(&make_row("us", 999), base + 5_000_000_000);
        assert!(late.is_empty(), "late arrival should be rejected");

        // Verify no new window was created for window 0's timestamp.
        // Window 0 should still have only 1 record (count=1).
        let ws = state.windows.get(&base).unwrap();
        let acc = ws.groups.values().next().unwrap();
        assert_eq!(acc.count, 1, "late record should not have been accumulated");
    }
}
