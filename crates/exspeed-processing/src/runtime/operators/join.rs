use std::collections::HashMap;

use super::Operator;
use crate::parser::ast::{Expr, JoinType};
use crate::runtime::eval::eval_expr;
use crate::types::{Row, Value};

/// Hash-join operator.
///
/// Build phase: reads ALL rows from the right input and builds a
/// `HashMap<String, Vec<Row>>` keyed by the right-side join column value.
///
/// Probe phase: for each left row, evaluates the left side of the ON
/// expression, looks up matches in the hash map, and emits combined rows.
pub struct HashJoinOperator {
    left: Box<dyn Operator>,
    right_lookup: HashMap<String, Vec<Row>>,
    join_type: JoinType,
    left_key_expr: Expr,
    // Iteration state
    current_left: Option<Row>,
    current_matches: Vec<Row>,
    match_idx: usize,
    left_columns: Vec<String>,
    right_columns: Vec<String>,
    exhausted: bool,
    left_had_match: bool,
}

impl HashJoinOperator {
    /// Create a new hash-join operator.
    ///
    /// `on_expr` must be a `BinaryOp { left, op: Eq, right }` where `left`
    /// references a column from the left input and `right` references a
    /// column from the right input.
    pub fn new(
        left: Box<dyn Operator>,
        mut right: Box<dyn Operator>,
        join_type: JoinType,
        on_expr: Expr,
    ) -> Self {
        let left_columns = left.columns();
        let right_columns = right.columns();

        // Decompose ON expression to get left-key and right-key expressions.
        let (left_key_expr, right_key_expr) = decompose_on_expr(&on_expr);

        // Build phase: consume entire right side into a hash map.
        let mut right_lookup: HashMap<String, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next() {
            let key = eval_expr(&right_key_expr, &row).to_string();
            right_lookup.entry(key).or_default().push(row);
        }

        Self {
            left,
            right_lookup,
            join_type,
            left_key_expr,
            current_left: None,
            current_matches: Vec::new(),
            match_idx: 0,
            left_columns,
            right_columns,
            exhausted: false,
            left_had_match: false,
        }
    }
}

impl Operator for HashJoinOperator {
    fn next(&mut self) -> Option<Row> {
        if self.exhausted {
            return None;
        }

        loop {
            // If we have pending matches from the current left row, yield them.
            if self.match_idx < self.current_matches.len() {
                let left_row = self.current_left.as_ref().unwrap();
                let right_row = &self.current_matches[self.match_idx];
                self.match_idx += 1;
                self.left_had_match = true;
                return Some(combine_rows(left_row, right_row));
            }

            // If this was a LEFT JOIN and the previous left row had no match,
            // emit left + NULLs.
            if let Some(left_row) = self.current_left.take() {
                if !self.left_had_match && matches!(self.join_type, JoinType::Left) {
                    let null_right = Row {
                        columns: self.right_columns.clone(),
                        values: vec![Value::Null; self.right_columns.len()],
                    };
                    // Reset state for next iteration.
                    self.current_matches.clear();
                    self.match_idx = 0;
                    return Some(combine_rows(&left_row, &null_right));
                }
            }

            // Advance to the next left row.
            match self.left.next() {
                Some(left_row) => {
                    let key = eval_expr(&self.left_key_expr, &left_row).to_string();
                    self.current_matches = self
                        .right_lookup
                        .get(&key)
                        .cloned()
                        .unwrap_or_default();
                    self.match_idx = 0;
                    self.current_left = Some(left_row);
                    self.left_had_match = false;
                }
                None => {
                    self.exhausted = true;
                    return None;
                }
            }
        }
    }

    fn columns(&self) -> Vec<String> {
        let mut cols = self.left_columns.clone();
        cols.extend(self.right_columns.clone());
        cols
    }
}

/// Decompose `a = b` into `(a, b)`. Falls back to treating the whole
/// expression as both the left and right key if it is not a simple equality.
fn decompose_on_expr(expr: &Expr) -> (Expr, Expr) {
    match expr {
        Expr::BinaryOp { left, op: crate::parser::ast::BinaryOperator::Eq, right } => {
            (*left.clone(), *right.clone())
        }
        _ => (expr.clone(), expr.clone()),
    }
}

/// Combine a left row and a right row into a single joined row.
fn combine_rows(left: &Row, right: &Row) -> Row {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    Row { columns, values }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::BinaryOperator;
    use crate::runtime::operators::scan::ScanOperator;

    fn left_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["user_id".into(), "order".into()],
                values: vec![Value::Int(1), Value::Text("A".into())],
            },
            Row {
                columns: vec!["user_id".into(), "order".into()],
                values: vec![Value::Int(2), Value::Text("B".into())],
            },
            Row {
                columns: vec!["user_id".into(), "order".into()],
                values: vec![Value::Int(3), Value::Text("C".into())],
            },
        ]
    }

    fn right_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["id".into(), "name".into()],
                values: vec![Value::Int(1), Value::Text("Alice".into())],
            },
            Row {
                columns: vec!["id".into(), "name".into()],
                values: vec![Value::Int(2), Value::Text("Bob".into())],
            },
        ]
    }

    fn on_expr() -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "user_id".into(),
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Column {
                table: None,
                name: "id".into(),
            }),
        }
    }

    #[test]
    fn inner_join() {
        let left = Box::new(ScanOperator::new(left_rows()));
        let right = Box::new(ScanOperator::new(right_rows()));
        let mut join = HashJoinOperator::new(left, right, JoinType::Inner, on_expr());

        assert_eq!(
            join.columns(),
            vec!["user_id", "order", "id", "name"]
        );

        let r1 = join.next().unwrap();
        assert_eq!(r1.get("user_id"), Some(&Value::Int(1)));
        assert_eq!(r1.get("name"), Some(&Value::Text("Alice".into())));

        let r2 = join.next().unwrap();
        assert_eq!(r2.get("user_id"), Some(&Value::Int(2)));
        assert_eq!(r2.get("name"), Some(&Value::Text("Bob".into())));

        // user_id=3 has no match in right, so inner join skips it
        assert!(join.next().is_none());
    }

    #[test]
    fn left_join() {
        let left = Box::new(ScanOperator::new(left_rows()));
        let right = Box::new(ScanOperator::new(right_rows()));
        let mut join = HashJoinOperator::new(left, right, JoinType::Left, on_expr());

        let r1 = join.next().unwrap();
        assert_eq!(r1.get("user_id"), Some(&Value::Int(1)));
        assert_eq!(r1.get("name"), Some(&Value::Text("Alice".into())));

        let r2 = join.next().unwrap();
        assert_eq!(r2.get("user_id"), Some(&Value::Int(2)));
        assert_eq!(r2.get("name"), Some(&Value::Text("Bob".into())));

        // user_id=3 has no match: left join emits NULLs for right columns
        let r3 = join.next().unwrap();
        assert_eq!(r3.get("user_id"), Some(&Value::Int(3)));
        assert_eq!(r3.get("name"), Some(&Value::Null));

        assert!(join.next().is_none());
    }
}
