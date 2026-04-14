use std::cmp::Ordering;

use super::Operator;
use crate::parser::ast::OrderByItem;
use crate::runtime::eval::eval_expr;
use crate::types::Row;

/// Reads ALL input rows, sorts in memory, then yields them in sorted order.
pub struct SortOperator {
    input: Box<dyn Operator>,
    order_by: Vec<OrderByItem>,
    sorted_rows: Vec<Row>,
    position: usize,
    computed: bool,
}

impl SortOperator {
    pub fn new(input: Box<dyn Operator>, order_by: Vec<OrderByItem>) -> Self {
        Self {
            input,
            order_by,
            sorted_rows: Vec::new(),
            position: 0,
            computed: false,
        }
    }

    fn compute(&mut self) {
        // Consume all input.
        while let Some(row) = self.input.next() {
            self.sorted_rows.push(row);
        }

        // Sort using the ORDER BY items.
        let order_by = self.order_by.clone();
        self.sorted_rows.sort_by(|a, b| {
            for item in &order_by {
                let va = eval_expr(&item.expr, a);
                let vb = eval_expr(&item.expr, b);
                let ka = va.to_sort_key();
                let kb = vb.to_sort_key();
                let cmp = ka.cmp(&kb);
                if cmp != Ordering::Equal {
                    return if item.descending { cmp.reverse() } else { cmp };
                }
            }
            Ordering::Equal
        });

        self.computed = true;
    }
}

impl Operator for SortOperator {
    fn next(&mut self) -> Option<Row> {
        if !self.computed {
            self.compute();
        }
        if self.position < self.sorted_rows.len() {
            let row = self.sorted_rows[self.position].clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        // Before computation, delegate to input. After, use stored rows.
        if self.computed {
            self.sorted_rows
                .first()
                .map(|r| r.columns.clone())
                .unwrap_or_default()
        } else {
            self.input.columns()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::Expr;
    use crate::runtime::operators::scan::ScanOperator;
    use crate::types::Value;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["name".into(), "score".into()],
                values: vec![Value::Text("Charlie".into()), Value::Int(70)],
            },
            Row {
                columns: vec!["name".into(), "score".into()],
                values: vec![Value::Text("Alice".into()), Value::Int(95)],
            },
            Row {
                columns: vec!["name".into(), "score".into()],
                values: vec![Value::Text("Bob".into()), Value::Int(80)],
            },
        ]
    }

    #[test]
    fn sort_ascending() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: false,
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut sort = SortOperator::new(scan, order_by);

        let r1 = sort.next().unwrap();
        assert_eq!(r1.get("score"), Some(&Value::Int(70)));

        let r2 = sort.next().unwrap();
        assert_eq!(r2.get("score"), Some(&Value::Int(80)));

        let r3 = sort.next().unwrap();
        assert_eq!(r3.get("score"), Some(&Value::Int(95)));

        assert!(sort.next().is_none());
    }

    #[test]
    fn sort_descending() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: true,
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut sort = SortOperator::new(scan, order_by);

        let r1 = sort.next().unwrap();
        assert_eq!(r1.get("score"), Some(&Value::Int(95)));

        let r2 = sort.next().unwrap();
        assert_eq!(r2.get("score"), Some(&Value::Int(80)));

        let r3 = sort.next().unwrap();
        assert_eq!(r3.get("score"), Some(&Value::Int(70)));

        assert!(sort.next().is_none());
    }

    #[test]
    fn sort_by_text() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "name".into(),
            },
            descending: false,
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut sort = SortOperator::new(scan, order_by);

        let r1 = sort.next().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::Text("Alice".into())));

        let r2 = sort.next().unwrap();
        assert_eq!(r2.get("name"), Some(&Value::Text("Bob".into())));

        let r3 = sort.next().unwrap();
        assert_eq!(r3.get("name"), Some(&Value::Text("Charlie".into())));

        assert!(sort.next().is_none());
    }
}
