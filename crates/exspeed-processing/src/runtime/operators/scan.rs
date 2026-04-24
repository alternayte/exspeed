use std::collections::VecDeque;
use std::sync::Arc;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::StorageEngine;

use crate::parser::ast::Expr;
use crate::planner::column_set::ColumnSet;
use crate::runtime::eval::eval_expr;
use crate::runtime::row_builder::stored_record_to_row;
use crate::runtime::operators::Operator;
use crate::types::Row;

const BATCH_SIZE: usize = 1024;

/// ScanOperator produces rows either:
/// (a) from a pre-materialised `Vec<Row>` (used by the materialised-view path), or
/// (b) by pulling 1024-record batches from storage on demand.
pub struct ScanOperator {
    mode: Mode,
    column_names: Vec<String>,
}

enum Mode {
    Rows { rows: Vec<Row>, position: usize },
    Streaming(StreamingState),
    ReverseTail(ReverseTailState),
}

struct StreamingState {
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    alias: Option<String>,
    required: ColumnSet,
    predicate: Option<Expr>,
    cursor: Offset,
    buf: VecDeque<Row>,
    exhausted: bool,
}

struct ReverseTailState {
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    alias: Option<String>,
    required: ColumnSet,
    predicate: Option<Expr>,
    limit: u64,
    rows: Vec<Row>,
    position: usize,
    computed: bool,
}

impl ScanOperator {
    /// Construct from a pre-materialised row list (legacy/MV path).
    pub fn from_rows(rows: Vec<Row>) -> Self {
        let column_names = rows.first().map(|r| r.columns.clone()).unwrap_or_default();
        Self {
            mode: Mode::Rows { rows, position: 0 },
            column_names,
        }
    }

    /// Backward-compatible alias so existing callers compile without changes.
    pub fn new(rows: Vec<Row>) -> Self {
        Self::from_rows(rows)
    }

    /// Construct a streaming scan that pulls batches from storage on demand,
    /// optionally applying a pushed-down predicate during batch conversion.
    pub fn streaming_with_predicate(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        alias: Option<String>,
        required: ColumnSet,
        predicate: Option<Expr>,
    ) -> Self {
        // Compute the column names by building a dummy row from an empty
        // record — cheap and keeps `.columns()` consistent with what scan
        // will actually emit. For empty streams this still produces the
        // expected column schema.
        let column_names = {
            use exspeed_streams::StoredRecord;
            let dummy = StoredRecord {
                offset: Offset(0),
                timestamp: 0,
                key: None,
                subject: String::new(),
                value: bytes::Bytes::from_static(b"{}"),
                headers: vec![],
            };
            stored_record_to_row(&dummy, alias.as_deref(), &required).columns
        };
        Self {
            mode: Mode::Streaming(StreamingState {
                storage,
                stream,
                alias,
                required,
                predicate,
                cursor: Offset(0),
                buf: VecDeque::new(),
                exhausted: false,
            }),
            column_names,
        }
    }

    /// Construct a streaming scan that pulls batches from storage on demand.
    pub fn streaming(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        alias: Option<String>,
        required: ColumnSet,
    ) -> Self {
        Self::streaming_with_predicate(storage, stream, alias, required, None)
    }

    /// Construct a reverse-tail scan that reads the last `limit` records
    /// from the stream and yields them in descending offset order.
    pub fn reverse_tail(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        alias: Option<String>,
        required: ColumnSet,
        predicate: Option<Expr>,
        limit: u64,
    ) -> Self {
        let column_names = {
            use exspeed_streams::StoredRecord;
            let dummy = StoredRecord {
                offset: Offset(0),
                timestamp: 0,
                key: None,
                subject: String::new(),
                value: bytes::Bytes::from_static(b"{}"),
                headers: vec![],
            };
            stored_record_to_row(&dummy, alias.as_deref(), &required).columns
        };
        Self {
            mode: Mode::ReverseTail(ReverseTailState {
                storage,
                stream,
                alias,
                required,
                predicate,
                limit,
                rows: Vec::new(),
                position: 0,
                computed: false,
            }),
            column_names,
        }
    }
}

impl Operator for ScanOperator {
    fn next(&mut self) -> Option<Row> {
        match &mut self.mode {
            Mode::Rows { rows, position } => {
                if *position < rows.len() {
                    let row = rows[*position].clone();
                    *position += 1;
                    Some(row)
                } else {
                    None
                }
            }
            Mode::ReverseTail(s) => {
                if !s.computed {
                    let bounds = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current()
                            .block_on(s.storage.stream_bounds(&s.stream))
                    });
                    if let Ok((earliest, next)) = bounds {
                        if next.0 > earliest.0 {
                            let overfetch = if s.predicate.is_some() { s.limit * 4 } else { s.limit };
                            let start = next.0.saturating_sub(overfetch).max(earliest.0);
                            let count = (next.0 - start) as usize;
                            let batch = tokio::task::block_in_place(|| {
                                tokio::runtime::Handle::current()
                                    .block_on(s.storage.read(&s.stream, Offset(start), count))
                            })
                            .unwrap_or_default();

                            let mut rows: Vec<Row> = batch
                                .iter()
                                .map(|r| stored_record_to_row(r, s.alias.as_deref(), &s.required))
                                .filter(|row| {
                                    s.predicate.as_ref().map_or(true, |pred| {
                                        eval_expr(pred, row) == crate::types::Value::Bool(true)
                                    })
                                })
                                .collect();

                            let take = s.limit as usize;
                            if rows.len() > take {
                                rows = rows.split_off(rows.len() - take);
                            }
                            rows.reverse();
                            s.rows = rows;
                        }
                    }
                    s.computed = true;
                }

                if s.position < s.rows.len() {
                    let row = s.rows[s.position].clone();
                    s.position += 1;
                    Some(row)
                } else {
                    None
                }
            }
            Mode::Streaming(s) => loop {
                if let Some(row) = s.buf.pop_front() {
                    return Some(row);
                }
                if s.exhausted {
                    return None;
                }
                let batch = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(s.storage.read(&s.stream, s.cursor, BATCH_SIZE))
                });
                let batch = match batch {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(stream = %s.stream, error = %e, "scan: storage.read failed, treating as end-of-stream");
                        s.exhausted = true;
                        return None;
                    }
                };
                if batch.is_empty() {
                    s.exhausted = true;
                    return None;
                }
                s.cursor = Offset(batch.last().unwrap().offset.0 + 1);
                for r in &batch {
                    let row = stored_record_to_row(r, s.alias.as_deref(), &s.required);
                    if let Some(ref pred) = s.predicate {
                        if eval_expr(pred, &row) != crate::types::Value::Bool(true) {
                            continue;
                        }
                    }
                    s.buf.push_back(row);
                }
            },
        }
    }

    fn columns(&self) -> Vec<String> {
        self.column_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::column_set::ColumnSet;
    use crate::types::Value;
    use exspeed_common::StreamName;
    use exspeed_storage::memory::MemoryStorage;
    use exspeed_streams::{Record, StorageEngine};
    use std::sync::Arc;

    fn sample_rows() -> Vec<Row> {
        (1..=3)
            .map(|i| Row {
                columns: vec!["id".into()],
                values: vec![Value::Int(i)],
            })
            .collect()
    }

    // ─── from_rows constructor (legacy) ──────────────────────────────────

    #[test]
    fn from_rows_yields_all_then_none() {
        let mut scan = ScanOperator::from_rows(sample_rows());
        assert_eq!(scan.next().unwrap().values[0], Value::Int(1));
        assert_eq!(scan.next().unwrap().values[0], Value::Int(2));
        assert_eq!(scan.next().unwrap().values[0], Value::Int(3));
        assert!(scan.next().is_none());
    }

    #[test]
    fn from_rows_empty() {
        let mut scan = ScanOperator::from_rows(vec![]);
        assert!(scan.next().is_none());
    }

    // ─── streaming constructor ───────────────────────────────────────────

    async fn seed(storage: &Arc<dyn StorageEngine>, stream: &StreamName, n: usize) {
        storage.create_stream(stream, 0, 0).await.unwrap();
        for i in 0..n {
            let rec = Record {
                key: Some(format!("k{i}").into_bytes().into()),
                subject: "s.a".into(),
                value: format!(r#"{{"i":{i}}}"#).into_bytes().into(),
                headers: vec![],
                timestamp_ns: None,
            };
            storage.append(stream, &rec).await.unwrap();
        }
    }

    fn multi_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn streaming_empty_stream() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t1").unwrap();
            storage.create_stream(&stream, 0, 0).await.unwrap();
            let cs = ColumnSet::needs_everything();
            let mut scan = ScanOperator::streaming(storage, stream, None, cs);
            assert!(tokio::task::block_in_place(|| scan.next()).is_none());
        });
    }

    #[test]
    fn streaming_yields_every_record_across_batches() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t2").unwrap();
            seed(&storage, &stream, 2500).await; // > 2 batches of 1024
            let cs = ColumnSet::needs_everything();
            let mut scan = ScanOperator::streaming(storage, stream, None, cs);
            let mut count = 0;
            while tokio::task::block_in_place(|| scan.next()).is_some() {
                count += 1;
            }
            assert_eq!(count, 2500);
        });
    }

    #[test]
    fn streaming_payload_not_referenced_omits_payload_column() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t3").unwrap();
            seed(&storage, &stream, 5).await;
            let mut cs = ColumnSet::default();
            cs.virtual_cols.insert("offset".into());
            let mut scan = ScanOperator::streaming(storage, stream, None, cs);
            let row = tokio::task::block_in_place(|| scan.next()).unwrap();
            assert_eq!(row.columns, vec!["offset"]);
            assert!(!row.columns.contains(&"payload".to_string()));
        });
    }

    #[test]
    fn streaming_with_predicate_filters_rows() {
        use crate::parser::ast::{BinaryOperator, Expr, LiteralValue};

        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t_filter").unwrap();
            seed(&storage, &stream, 100).await;

            let predicate = Expr::BinaryOp {
                left: Box::new(Expr::Column { table: None, name: "offset".into() }),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Literal(LiteralValue::Int(5))),
            };

            let cs = ColumnSet::needs_everything();
            let mut scan = ScanOperator::streaming_with_predicate(
                storage, stream, None, cs, Some(predicate),
            );

            let mut count = 0;
            while tokio::task::block_in_place(|| scan.next()).is_some() {
                count += 1;
            }
            assert_eq!(count, 5);
        });
    }

    #[test]
    fn reverse_tail_returns_last_n_in_desc_order() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t_rev").unwrap();
            seed(&storage, &stream, 100).await;

            let cs = ColumnSet::needs_everything();
            let mut scan = ScanOperator::reverse_tail(storage, stream, None, cs, None, 5);

            let mut offsets = Vec::new();
            while let Some(row) = tokio::task::block_in_place(|| scan.next()) {
                if let Some(Value::Int(o)) = row.get("offset") {
                    offsets.push(*o);
                }
            }
            assert_eq!(offsets, vec![99, 98, 97, 96, 95]);
        });
    }

    #[test]
    fn streaming_exhaustion_is_idempotent() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let stream = StreamName::try_from("t4").unwrap();
            seed(&storage, &stream, 1).await;
            let cs = ColumnSet::needs_everything();
            let mut scan = ScanOperator::streaming(storage, stream, None, cs);
            assert!(tokio::task::block_in_place(|| scan.next()).is_some());
            assert!(tokio::task::block_in_place(|| scan.next()).is_none());
            assert!(tokio::task::block_in_place(|| scan.next()).is_none());
        });
    }
}
