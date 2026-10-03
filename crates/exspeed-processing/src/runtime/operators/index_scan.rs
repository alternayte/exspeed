use std::path::PathBuf;
use std::sync::Arc;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::StorageEngine;

use crate::parser::ast::Expr;
use crate::planner::column_set::ColumnSet;
use crate::runtime::eval::eval_expr;
use crate::runtime::operators::Operator;
use crate::runtime::row_builder::stored_record_to_row;
use crate::types::{Row, Value};

pub struct IndexScanOperator {
    rows: Vec<Row>,
    position: usize,
    computed: bool,
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    alias: Option<String>,
    required: ColumnSet,
    partition_dir: PathBuf,
    index_name: String,
    lookup_value: String,
    predicate: Option<Expr>,
}

impl IndexScanOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        alias: Option<String>,
        required: ColumnSet,
        partition_dir: PathBuf,
        index_name: String,
        lookup_value: String,
        predicate: Option<Expr>,
    ) -> Self {
        Self {
            rows: Vec::new(),
            position: 0,
            computed: false,
            storage,
            stream,
            alias,
            required,
            partition_dir,
            index_name,
            lookup_value,
            predicate,
        }
    }

    fn compute(&mut self) {
        // The storage engine no longer builds secondary-index (`.sidx`)
        // files, so this is a predicate-filtered scan of the whole stream.
        let _ = (&self.partition_dir, &self.index_name, &self.lookup_value);
        let active_start = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.storage.stream_bounds(&self.stream))
        })
        .map_or(0, |(earliest, _)| earliest.0);

        let batch_size = 1024usize;
        let mut cursor = Offset(active_start);
        loop {
            let batch = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.storage.read(
                    &self.stream,
                    cursor,
                    batch_size,
                ))
            });

            let records = match batch {
                Ok(r) => r,
                Err(_) => break,
            };

            if records.is_empty() {
                break;
            }

            cursor = Offset(records.last().unwrap().offset.0 + 1);

            for record in &records {
                let row = stored_record_to_row(record, self.alias.as_deref(), &self.required);
                if let Some(ref pred) = self.predicate {
                    if eval_expr(pred, &row) != Value::Bool(true) {
                        continue;
                    }
                }
                self.rows.push(row);
            }
        }

        self.computed = true;
    }
}

impl Operator for IndexScanOperator {
    fn next(&mut self) -> Option<Row> {
        if !self.computed {
            self.compute();
        }
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        use exspeed_streams::StoredRecord;
        let dummy = StoredRecord {
            offset: Offset(0),
            timestamp: 0,
            key: None,
            subject: String::new(),
            value: bytes::Bytes::from_static(b"{}"),
            headers: vec![],
        };
        stored_record_to_row(&dummy, self.alias.as_deref(), &self.required).columns
    }
}
