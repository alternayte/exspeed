//! Continuous queries: a micro-batch dataflow over streams.
//!
//! - [`plan`] compiles a `CREATE STREAM/TABLE … AS SELECT` into a
//!   [`plan::Dataflow`] of stateless steps (DataFusion physical expressions)
//!   and stateful operators ([`ops`]).
//! - [`runner`] drives it: reads sources, tracks event time and watermarks,
//!   writes output through the broker `Log` and checkpoints state
//!   ([`checkpoint`]).

pub mod checkpoint;
pub mod ops;
pub mod plan;
pub mod rows;
pub mod runner;
