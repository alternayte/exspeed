use super::Operator;
use crate::types::Row;

/// Yields pre-loaded rows one at a time.
pub struct ScanOperator {
    rows: Vec<Row>,
    position: usize,
    column_names: Vec<String>,
}

impl ScanOperator {
    pub fn new(rows: Vec<Row>) -> Self {
        let column_names = rows
            .first()
            .map(|r| r.columns.clone())
            .unwrap_or_default();
        Self {
            rows,
            position: 0,
            column_names,
        }
    }
}

impl Operator for ScanOperator {
    fn next(&mut self) -> Option<Row> {
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        self.column_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["id".into(), "name".into()],
                values: vec![Value::Int(1), Value::Text("Alice".into())],
            },
            Row {
                columns: vec!["id".into(), "name".into()],
                values: vec![Value::Int(2), Value::Text("Bob".into())],
            },
            Row {
                columns: vec!["id".into(), "name".into()],
                values: vec![Value::Int(3), Value::Text("Carol".into())],
            },
        ]
    }

    #[test]
    fn scan_yields_all_rows_then_none() {
        let mut scan = ScanOperator::new(sample_rows());
        assert_eq!(scan.columns(), vec!["id", "name"]);

        let r1 = scan.next().unwrap();
        assert_eq!(r1.values[0], Value::Int(1));

        let r2 = scan.next().unwrap();
        assert_eq!(r2.values[0], Value::Int(2));

        let r3 = scan.next().unwrap();
        assert_eq!(r3.values[0], Value::Int(3));

        assert!(scan.next().is_none());
        assert!(scan.next().is_none()); // idempotent
    }

    #[test]
    fn scan_empty() {
        let mut scan = ScanOperator::new(vec![]);
        assert!(scan.columns().is_empty());
        assert!(scan.next().is_none());
    }
}
