use std::collections::HashMap;

use super::Operator;
use crate::parser::ast::{AggregateFunc, Expr, SelectItem};
use crate::runtime::eval::{compare_values, eval_expr};
use crate::types::{Row, Value};

/// Reads ALL input rows, groups by `group_by` expressions, accumulates
/// aggregates, then yields one row per group.
pub struct AggregateOperator {
    input: Box<dyn Operator>,
    group_by: Vec<Expr>,
    select_items: Vec<SelectItem>,
    result_rows: Vec<Row>,
    position: usize,
    computed: bool,
}

impl AggregateOperator {
    pub fn new(
        input: Box<dyn Operator>,
        group_by: Vec<Expr>,
        select_items: Vec<SelectItem>,
    ) -> Self {
        Self {
            input,
            group_by,
            select_items,
            result_rows: Vec::new(),
            position: 0,
            computed: false,
        }
    }

    /// Consume all input rows, group, aggregate, and store results.
    fn compute(&mut self) {
        // Collect aggregate descriptors from select items.
        let agg_descs: Vec<Option<AggDesc>> = self
            .select_items
            .iter()
            .map(|item| extract_agg_desc(&item.expr))
            .collect();

        // Track groups in insertion order.
        let mut group_keys: Vec<String> = Vec::new();
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        while let Some(row) = self.input.next() {
            // Compute the group key.
            let group_values: Vec<Value> = self
                .group_by
                .iter()
                .map(|expr| eval_expr(expr, &row))
                .collect();
            let key = group_values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\x00");

            let state = groups.entry(key.clone()).or_insert_with(|| {
                group_keys.push(key.clone());
                GroupState {
                    group_values: group_values.clone(),
                    accumulators: agg_descs.iter().map(|_| AggAccum::new()).collect(),
                }
            });

            // Feed each accumulator.
            for (i, desc) in agg_descs.iter().enumerate() {
                if let Some(d) = desc {
                    let val = eval_expr(&d.expr, &row);
                    state.accumulators[i].accumulate(&d.func, &val);
                }
            }
        }

        // Build result rows in insertion order.
        for key in &group_keys {
            let state = &groups[key];
            let mut columns = Vec::new();
            let mut values = Vec::new();

            for (i, item) in self.select_items.iter().enumerate() {
                let col_name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| derive_agg_column_name(&item.expr));
                columns.push(col_name);

                if let Some(desc) = &agg_descs[i] {
                    values.push(state.accumulators[i].result(&desc.func));
                } else {
                    // Non-aggregate expression — evaluate using group values.
                    // Build a temporary row from the group values.
                    let group_row = Row {
                        columns: self
                            .group_by
                            .iter()
                            .map(|e| match e {
                                Expr::Column { name, .. } => name.clone(),
                                _ => "?".into(),
                            })
                            .collect(),
                        values: state.group_values.clone(),
                    };
                    values.push(eval_expr(&item.expr, &group_row));
                }
            }

            self.result_rows.push(Row { columns, values });
        }

        // Handle the case where there are no groups but there are aggregate
        // functions (e.g., `SELECT COUNT(*) FROM empty_table` should return
        // a single row with count = 0).
        if self.result_rows.is_empty()
            && self.group_by.is_empty()
            && has_any_aggregate(&self.select_items)
        {
            let mut columns = Vec::new();
            let mut values = Vec::new();
            for (i, item) in self.select_items.iter().enumerate() {
                let col_name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| derive_agg_column_name(&item.expr));
                columns.push(col_name);
                if let Some(desc) = &agg_descs[i] {
                    let acc = AggAccum::new();
                    values.push(acc.result(&desc.func));
                } else {
                    values.push(Value::Null);
                }
            }
            self.result_rows.push(Row { columns, values });
        }

        self.computed = true;
    }
}

impl Operator for AggregateOperator {
    fn next(&mut self) -> Option<Row> {
        if !self.computed {
            self.compute();
        }
        if self.position < self.result_rows.len() {
            let row = self.result_rows[self.position].clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        self.select_items
            .iter()
            .map(|item| {
                item.alias
                    .clone()
                    .unwrap_or_else(|| derive_agg_column_name(&item.expr))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct GroupState {
    group_values: Vec<Value>,
    accumulators: Vec<AggAccum>,
}

struct AggDesc {
    func: AggregateFunc,
    expr: Expr,
}

struct AggAccum {
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggAccum {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn accumulate(&mut self, func: &AggregateFunc, val: &Value) {
        match func {
            AggregateFunc::Count => {
                // COUNT(*) counts all rows; COUNT(expr) counts non-null.
                // We always increment — the caller passes Wildcard for COUNT(*).
                if !val.is_null() || matches!(val, Value::Null) {
                    // For COUNT(*) the expr evaluates to Null (since Wildcard
                    // returns Null), but we still count.
                    self.count += 1;
                }
            }
            AggregateFunc::Sum => {
                if let Some(f) = val.to_f64() {
                    self.sum += f;
                    self.count += 1;
                }
            }
            AggregateFunc::Avg => {
                if let Some(f) = val.to_f64() {
                    self.sum += f;
                    self.count += 1;
                }
            }
            AggregateFunc::Min => {
                if !val.is_null() {
                    match &self.min {
                        None => self.min = Some(val.clone()),
                        Some(current) => {
                            if compare_values(val, current) == Some(std::cmp::Ordering::Less) {
                                self.min = Some(val.clone());
                            }
                        }
                    }
                    self.count += 1;
                }
            }
            AggregateFunc::Max => {
                if !val.is_null() {
                    match &self.max {
                        None => self.max = Some(val.clone()),
                        Some(current) => {
                            if compare_values(val, current) == Some(std::cmp::Ordering::Greater) {
                                self.max = Some(val.clone());
                            }
                        }
                    }
                    self.count += 1;
                }
            }
        }
    }

    fn result(&self, func: &AggregateFunc) -> Value {
        match func {
            AggregateFunc::Count => Value::Int(self.count),
            AggregateFunc::Sum => {
                if self.count == 0 {
                    Value::Null
                } else {
                    // Return Int if the sum is a whole number.
                    if self.sum.fract() == 0.0
                        && (i64::MIN as f64..=i64::MAX as f64).contains(&self.sum)
                    {
                        Value::Int(self.sum as i64)
                    } else {
                        Value::Float(self.sum)
                    }
                }
            }
            AggregateFunc::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float(self.sum / self.count as f64)
                }
            }
            AggregateFunc::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateFunc::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

/// Extract an aggregate descriptor from an expression, if it is an aggregate.
fn extract_agg_desc(expr: &Expr) -> Option<AggDesc> {
    match expr {
        Expr::Aggregate { func, expr, .. } => Some(AggDesc {
            func: func.clone(),
            expr: *expr.clone(),
        }),
        _ => None,
    }
}

/// Check if any select item contains an aggregate.
fn has_any_aggregate(items: &[SelectItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item.expr, Expr::Aggregate { .. }))
}

/// Derive a column name for an aggregate or other expression.
fn derive_agg_column_name(expr: &Expr) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::SelectItem;
    use crate::runtime::operators::scan::ScanOperator;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["dept".into(), "salary".into()],
                values: vec![Value::Text("eng".into()), Value::Int(100)],
            },
            Row {
                columns: vec!["dept".into(), "salary".into()],
                values: vec![Value::Text("eng".into()), Value::Int(120)],
            },
            Row {
                columns: vec!["dept".into(), "salary".into()],
                values: vec![Value::Text("sales".into()), Value::Int(80)],
            },
            Row {
                columns: vec!["dept".into(), "salary".into()],
                values: vec![Value::Text("sales".into()), Value::Int(90)],
            },
            Row {
                columns: vec!["dept".into(), "salary".into()],
                values: vec![Value::Text("eng".into()), Value::Int(110)],
            },
        ]
    }

    #[test]
    fn aggregate_count_sum_group_by() {
        let group_by = vec![Expr::Column {
            table: None,
            name: "dept".into(),
        }];

        let select_items = vec![
            SelectItem {
                expr: Expr::Column {
                    table: None,
                    name: "dept".into(),
                },
                alias: None,
            },
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
                        name: "salary".into(),
                    }),
                    distinct: false,
                },
                alias: None,
            },
        ];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut agg = AggregateOperator::new(scan, group_by, select_items);

        assert_eq!(agg.columns(), vec!["dept", "count", "sum"]);

        let r1 = agg.next().unwrap();
        assert_eq!(r1.get("dept"), Some(&Value::Text("eng".into())));
        assert_eq!(r1.get("count"), Some(&Value::Int(3)));
        assert_eq!(r1.get("sum"), Some(&Value::Int(330)));

        let r2 = agg.next().unwrap();
        assert_eq!(r2.get("dept"), Some(&Value::Text("sales".into())));
        assert_eq!(r2.get("count"), Some(&Value::Int(2)));
        assert_eq!(r2.get("sum"), Some(&Value::Int(170)));

        assert!(agg.next().is_none());
    }

    #[test]
    fn aggregate_min_max_avg() {
        let group_by = vec![Expr::Column {
            table: None,
            name: "dept".into(),
        }];

        let select_items = vec![
            SelectItem {
                expr: Expr::Column {
                    table: None,
                    name: "dept".into(),
                },
                alias: None,
            },
            SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Min,
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "salary".into(),
                    }),
                    distinct: false,
                },
                alias: None,
            },
            SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Max,
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "salary".into(),
                    }),
                    distinct: false,
                },
                alias: None,
            },
            SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Avg,
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "salary".into(),
                    }),
                    distinct: false,
                },
                alias: None,
            },
        ];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut agg = AggregateOperator::new(scan, group_by, select_items);

        let r1 = agg.next().unwrap();
        assert_eq!(r1.get("dept"), Some(&Value::Text("eng".into())));
        assert_eq!(r1.get("min"), Some(&Value::Int(100)));
        assert_eq!(r1.get("max"), Some(&Value::Int(120)));
        // avg(100, 120, 110) = 110.0
        assert_eq!(r1.get("avg"), Some(&Value::Float(110.0)));

        let r2 = agg.next().unwrap();
        assert_eq!(r2.get("dept"), Some(&Value::Text("sales".into())));
        assert_eq!(r2.get("min"), Some(&Value::Int(80)));
        assert_eq!(r2.get("max"), Some(&Value::Int(90)));
        assert_eq!(r2.get("avg"), Some(&Value::Float(85.0)));

        assert!(agg.next().is_none());
    }

    #[test]
    fn aggregate_no_groups_count_star() {
        // SELECT COUNT(*) FROM (empty)
        let select_items = vec![SelectItem {
            expr: Expr::Aggregate {
                func: AggregateFunc::Count,
                expr: Box::new(Expr::Wildcard { table: None }),
                distinct: false,
            },
            alias: None,
        }];

        let scan = Box::new(ScanOperator::new(vec![]));
        let mut agg = AggregateOperator::new(scan, vec![], select_items);

        let r1 = agg.next().unwrap();
        assert_eq!(r1.get("count"), Some(&Value::Int(0)));

        assert!(agg.next().is_none());
    }
}
