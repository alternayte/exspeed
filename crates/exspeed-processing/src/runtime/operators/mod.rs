pub mod scan;
pub mod filter;
pub mod project;
pub mod join;
pub mod aggregate;
pub mod sort;
pub mod limit;

use crate::types::Row;

/// Pull-based operator: call `next()` to get the next row.
pub trait Operator {
    /// Return the next row, or `None` when exhausted.
    fn next(&mut self) -> Option<Row>;

    /// Return the output column names for this operator.
    fn columns(&self) -> Vec<String>;
}
