//! Per-record SQL transforms (used by connectors): `SELECT … [WHERE …]`
//! without a FROM clause, evaluated on one record at a time with the same
//! functions and JSON semantics as ExQL queries.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{SelectItem, SetExpr, Statement as SQLStatement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use exspeed_common::Offset;
use exspeed_streams::StoredRecord;
use serde_json::Value as Json;

use crate::continuous::plan::{compile_chain, split_chain};
use crate::continuous::rows::{Chain, RowId, Rows};
use crate::convert::{records_to_batch, row_object, stream_schema};
use crate::error::ExqlError;

const TABLE: &str = "__transform__";

/// A compiled record transform.
pub struct RecordTransform {
    chain: Chain,
    /// `SELECT *`: matching records pass unchanged.
    passthrough: bool,
}

impl std::fmt::Debug for RecordTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordTransform")
            .field("passthrough", &self.passthrough)
            .finish()
    }
}

/// Drive a future that never actually waits (planning against in-memory
/// tables) to completion without a runtime.
fn run_ready<F: Future>(fut: F) -> Result<F::Output, ExqlError> {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Ok(v),
        Poll::Pending => Err(ExqlError::Internal(
            "transform planning did not complete".into(),
        )),
    }
}

/// Insert `FROM __transform__` before a top-level WHERE (or at the end).
fn with_from(fragment: &str) -> Result<String, ExqlError> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, fragment)
        .tokenize()
        .map_err(|e| ExqlError::parse(e.to_string()))?;
    let mut depth = 0i64;
    let mut out = String::new();
    let mut inserted = false;
    for t in &tokens {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(w)
                if depth == 0
                    && !inserted
                    && w.quote_style.is_none()
                    && w.value.eq_ignore_ascii_case("WHERE") =>
            {
                out.push_str(&format!(" FROM {TABLE} "));
                inserted = true;
            }
            Token::Word(w)
                if depth == 0
                    && w.quote_style.is_none()
                    && w.value.eq_ignore_ascii_case("FROM") =>
            {
                return Err(ExqlError::parse(
                    "a transform has no FROM clause: SELECT … [WHERE …]",
                ));
            }
            _ => {}
        }
        out.push_str(&crate::sql::token_sql(t));
    }
    if !inserted {
        out.push_str(&format!(" FROM {TABLE}"));
    }
    crate::sql::normalize_sql(&out)
}

impl RecordTransform {
    /// Compile `SELECT <items> [WHERE <predicate>]`. Columns: `offset`,
    /// `timestamp`, `subject`, `key`, `payload`, `headers`.
    pub fn compile(fragment: &str) -> Result<Self, ExqlError> {
        let sql = with_from(fragment)?;
        let mut stmt = crate::ast::parse_one(&sql)?;
        let passthrough = match &stmt {
            DFStatement::Statement(s) => match s.as_ref() {
                SQLStatement::Query(q) => match q.body.as_ref() {
                    SetExpr::Select(sel) => {
                        sel.projection.len() == 1
                            && matches!(sel.projection[0], SelectItem::Wildcard(_))
                    }
                    _ => false,
                },
                _ => return Err(ExqlError::parse("a transform must be a SELECT")),
            },
            _ => return Err(ExqlError::parse("a transform must be a SELECT")),
        };
        crate::ast::rewrite_json_numeric_args(&mut stmt)?;
        let state = crate::session::standalone_state()?;
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            TABLE,
            Arc::new(MemTable::try_new(stream_schema(), vec![vec![]])?),
        )?;
        let state = ctx.state();
        let plan = run_ready(state.statement_to_plan(stmt))??;
        let plan = crate::ast::analyze(&state, plan)?;
        let (nodes, rest) = split_chain(&plan);
        if !matches!(rest, LogicalPlan::TableScan(_)) {
            return Err(ExqlError::unsupported(
                "aggregates, joins, ORDER BY or LIMIT in a transform",
                "a transform maps one record at a time: SELECT <expressions> [WHERE <predicate>]",
            ));
        }
        let chain = compile_chain(&state, &nodes)?;
        Ok(Self { chain, passthrough })
    }

    /// Apply to one record: `None` if the WHERE clause rejects it,
    /// otherwise the new payload (the original one for `SELECT *`).
    pub fn apply(
        &self,
        key: Option<&Bytes>,
        subject: &str,
        value: &Bytes,
        headers: &[(String, String)],
    ) -> Result<Option<Bytes>, ExqlError> {
        let rec = StoredRecord {
            offset: Offset(0),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default(),
            subject: subject.to_string(),
            key: key.cloned(),
            value: value.clone(),
            headers: headers.to_vec(),
        };
        let batch = records_to_batch(std::slice::from_ref(&rec))?;
        let rows = self.chain.apply(Rows {
            batch,
            et: vec![0],
            ids: vec![RowId::Src { src: 0, off: 0 }],
        })?;
        if rows.is_empty() {
            return Ok(None);
        }
        if self.passthrough {
            return Ok(Some(value.clone()));
        }
        let obj = row_object(&rows.batch, 0);
        Ok(Some(Bytes::from(Json::Object(obj).to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_and_project() {
        let t = RecordTransform::compile(
            "SELECT payload->>'name' AS n, payload->>'age' + 1 AS next WHERE payload->>'age' > 18",
        )
        .unwrap();
        let v = Bytes::from_static(br#"{"name":"Al","age":30}"#);
        let out = t.apply(None, "s", &v, &[]).unwrap().unwrap();
        let j: Json = serde_json::from_slice(&out).unwrap();
        assert_eq!(j, serde_json::json!({"n": "Al", "next": 31.0}));
        let young = Bytes::from_static(br#"{"name":"Bo","age":9}"#);
        assert!(t.apply(None, "s", &young, &[]).unwrap().is_none());
        let star = RecordTransform::compile("SELECT * WHERE subject = 'a'").unwrap();
        assert_eq!(star.apply(None, "a", &v, &[]).unwrap(), Some(v.clone()));
        assert!(star.apply(None, "b", &v, &[]).unwrap().is_none());
        assert!(RecordTransform::compile("SELECT COUNT(*)").is_err());
    }
}
