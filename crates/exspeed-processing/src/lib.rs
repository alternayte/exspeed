//! ExQL: SQL over streams, on Apache DataFusion.

pub mod ast;
pub mod bounded;
pub mod catalog;
pub mod convert;
pub mod error;
pub mod external;
pub mod json_rule;
pub mod session;
pub mod sql;
pub mod stream_table;
pub mod tables;
pub mod udfs;

#[cfg(test)]
mod bounded_tests;
