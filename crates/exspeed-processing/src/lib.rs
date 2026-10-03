//! ExQL: SQL over streams, on Apache DataFusion.

pub mod ast;
pub mod bounded;
pub mod catalog;
pub mod continuous;
pub mod convert;
pub mod engine;
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
#[cfg(test)]
mod continuous_tests;
#[cfg(test)]
mod differential_tests;
#[cfg(test)]
mod test_util;

pub use bounded::QueryResult;
pub use engine::{
    DesiredState, ExqlEngine, QueryDef, QueryInfo, QueryKind, StatementResult, TableInfo,
};
pub use error::ExqlError;
pub use session::ExqlConfig;
