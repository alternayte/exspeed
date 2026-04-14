use super::Operator;
use crate::types::Row;

/// Wraps an input operator, skips `offset` rows, then yields at most
/// `limit` rows before stopping.
pub struct LimitOperator {
    input: Box<dyn Operator>,
    limit: Option<u64>,
    offset: u64,
    emitted: u64,
    skipped: u64,
}

impl LimitOperator {
    pub fn new(input: Box<dyn Operator>, limit: Option<u64>, offset: u64) -> Self {
        Self {
            input,
            limit,
            offset,
            emitted: 0,
            skipped: 0,
        }
    }
}

impl Operator for LimitOperator {
    fn next(&mut self) -> Option<Row> {
        // Skip offset rows first.
        while self.skipped < self.offset {
            self.input.next()?;
            self.skipped += 1;
        }

        // Check limit.
        if let Some(lim) = self.limit {
            if self.emitted >= lim {
                return None;
            }
        }

        let row = self.input.next()?;
        self.emitted += 1;
        Some(row)
    }

    fn columns(&self) -> Vec<String> {
        self.input.columns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::operators::scan::ScanOperator;
    use crate::types::Value;

    fn sample_rows() -> Vec<Row> {
        (1..=10)
            .map(|i| Row {
                columns: vec!["id".into()],
                values: vec![Value::Int(i)],
            })
            .collect()
    }

    #[test]
    fn limit_only() {
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut limit = LimitOperator::new(scan, Some(3), 0);

        assert_eq!(limit.next().unwrap().values[0], Value::Int(1));
        assert_eq!(limit.next().unwrap().values[0], Value::Int(2));
        assert_eq!(limit.next().unwrap().values[0], Value::Int(3));
        assert!(limit.next().is_none());
    }

    #[test]
    fn offset_only() {
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut limit = LimitOperator::new(scan, None, 7);

        assert_eq!(limit.next().unwrap().values[0], Value::Int(8));
        assert_eq!(limit.next().unwrap().values[0], Value::Int(9));
        assert_eq!(limit.next().unwrap().values[0], Value::Int(10));
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_and_offset() {
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut limit = LimitOperator::new(scan, Some(2), 3);

        assert_eq!(limit.next().unwrap().values[0], Value::Int(4));
        assert_eq!(limit.next().unwrap().values[0], Value::Int(5));
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_zero() {
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut limit = LimitOperator::new(scan, Some(0), 0);
        assert!(limit.next().is_none());
    }

    #[test]
    fn offset_exceeds_rows() {
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut limit = LimitOperator::new(scan, None, 100);
        assert!(limit.next().is_none());
    }
}
