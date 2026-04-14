use super::Operator;
use crate::parser::ast::Expr;
use crate::runtime::eval::eval_expr;
use crate::types::{Row, Value};

/// Wraps an input operator, yielding only rows that match the predicate.
pub struct FilterOperator {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl FilterOperator {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for FilterOperator {
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.input.next()?;
            if eval_expr(&self.predicate, &row) == Value::Bool(true) {
                return Some(row);
            }
        }
    }

    fn columns(&self) -> Vec<String> {
        self.input.columns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, LiteralValue};
    use crate::runtime::operators::scan::ScanOperator;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["id".into(), "score".into()],
                values: vec![Value::Int(1), Value::Int(10)],
            },
            Row {
                columns: vec!["id".into(), "score".into()],
                values: vec![Value::Int(2), Value::Int(50)],
            },
            Row {
                columns: vec!["id".into(), "score".into()],
                values: vec![Value::Int(3), Value::Int(30)],
            },
            Row {
                columns: vec!["id".into(), "score".into()],
                values: vec![Value::Int(4), Value::Int(80)],
            },
        ]
    }

    #[test]
    fn filter_keeps_matching_rows() {
        // score > 25
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "score".into(),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(LiteralValue::Int(25))),
        };

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut filter = FilterOperator::new(scan, predicate);

        let r1 = filter.next().unwrap();
        assert_eq!(r1.values[0], Value::Int(2)); // score=50

        let r2 = filter.next().unwrap();
        assert_eq!(r2.values[0], Value::Int(3)); // score=30

        let r3 = filter.next().unwrap();
        assert_eq!(r3.values[0], Value::Int(4)); // score=80

        assert!(filter.next().is_none());
    }

    #[test]
    fn filter_no_match() {
        // score > 100
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "score".into(),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(LiteralValue::Int(100))),
        };

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut filter = FilterOperator::new(scan, predicate);

        assert!(filter.next().is_none());
    }
}
