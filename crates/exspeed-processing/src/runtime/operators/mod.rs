pub mod aggregate;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;
pub mod stream_join;
pub mod windowed_aggregate;

use crate::types::Row;

/// Pull-based operator: call `next()` to get the next row.
pub trait Operator: Send {
    /// Return the next row, or `None` when exhausted.
    fn next(&mut self) -> Option<Row>;

    /// Return the output column names for this operator.
    fn columns(&self) -> Vec<String>;
}
