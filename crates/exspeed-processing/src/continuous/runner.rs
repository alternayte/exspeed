//! The micro-batch loop of a continuous query.
//!
//! Each micro-batch reads new records from every source (woken by
//! `watch_appends`, falling back to polling), runs them through the
//! dataflow, writes the output through the [`Log`] and advances the source
//! positions. Every output record carries
//!
//! - `x-idempotency-key: <query_id>:<row tag>` — deterministic, so the Log
//!   drops replays after a restore;
//! - `x-exql-query: <query_id>`;
//! - `x-exql-pos: <next offset per source>` — the micro-batch boundary.
//!
//! After a restore the runner scans the outputs written since the
//! checkpoint: it skips records it already wrote and replays the original
//! micro-batch boundaries, so the output after a crash is identical to the
//! output without one.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use exspeed_broker::broker_append::IDEMPOTENCY_HEADER;
use exspeed_broker::log::{Log, LogError};
use exspeed_common::metrics::Metrics;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageEngine, StorageError, StoredRecord};
use serde_json::{Map, Value as Json};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::checkpoint::{self, Checkpoint, CheckpointMeta, OutputPos, SourceState};
use super::plan::{Dataflow, InputOp};
use super::rows::{batch_from_rows, eval, RowId, Rows};
use crate::convert::{cell_to_text, records_to_batch, row_object};
use crate::error::ExqlError;
use crate::session::ExqlConfig;
use crate::sql::Emit;
use crate::tables::{key_string, MaterializedTable};

pub const H_QUERY: &str = "x-exql-query";
pub const H_POS: &str = "x-exql-pos";
pub const H_OP: &str = "x-exql-op";

const READ_BYTES: usize = 8 * 1024 * 1024;

/// Live counters of a running query.
#[derive(Debug)]
pub struct QueryStats {
    pub records_in: AtomicU64,
    pub records_out: AtomicU64,
    pub late_dropped: AtomicU64,
    pub checkpoints: AtomicU64,
    /// `i64::MIN` when there is no watermark yet.
    pub watermark: AtomicI64,
    /// Epoch ms of the last checkpoint, 0 if none.
    pub last_checkpoint_ms: AtomicI64,
}

impl Default for QueryStats {
    fn default() -> Self {
        Self {
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            late_dropped: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            watermark: AtomicI64::new(i64::MIN),
            last_checkpoint_ms: AtomicI64::new(0),
        }
    }
}

impl QueryStats {
    pub fn to_json(&self) -> Json {
        let wm = self.watermark.load(Ordering::Relaxed);
        let ck = self.last_checkpoint_ms.load(Ordering::Relaxed);
        serde_json::json!({
            "records_in": self.records_in.load(Ordering::Relaxed),
            "records_out": self.records_out.load(Ordering::Relaxed),
            "late_records_dropped": self.late_dropped.load(Ordering::Relaxed),
            "checkpoints": self.checkpoints.load(Ordering::Relaxed),
            "watermark": if wm == i64::MIN { Json::Null } else { Json::String(crate::convert::format_ts_millis(wm)) },
            "last_checkpoint": if ck == 0 { Json::Null } else { Json::String(crate::convert::format_ts_millis(ck)) },
        })
    }
}

/// Where a query's rows go.
pub enum Sink {
    Stream(StreamName),
    Table {
        table: Arc<MaterializedTable>,
        changelog: StreamName,
    },
}

impl Sink {
    fn stream(&self) -> &StreamName {
        match self {
            Sink::Stream(s) => s,
            Sink::Table { changelog, .. } => changelog,
        }
    }
}

pub struct RunCtx {
    pub query_id: String,
    pub log: Arc<Log>,
    pub cfg: ExqlConfig,
    pub stats: Arc<QueryStats>,
    pub metrics: Option<Arc<Metrics>>,
}

/// One output row (or table delete) ready to be written.
struct OutItem {
    idem: String,
    record_key: Option<String>,
    subject: String,
    /// `None` = delete (tables only).
    payload: Option<Map<String, Json>>,
    /// Table key and row.
    table: Option<(Vec<ScalarValue>, Option<Vec<ScalarValue>>)>,
}

pub struct Runner {
    ctx: RunCtx,
    df: Dataflow,
    sink: Sink,
    storage: Arc<dyn StorageEngine>,
    pos: Vec<u64>,
    max_et: Vec<Option<i64>>,
    /// Idempotency keys already present in the output after the checkpoint.
    written: HashSet<String>,
    /// Micro-batch boundaries to replay after a restore.
    forced: VecDeque<Vec<u64>>,
    seq: u64,
    dirty: bool,
    batches_since_ckpt: u64,
    last_ckpt: Instant,
    ckpt_stream: StreamName,
    key_col: Option<usize>,
    subject_col: Option<usize>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// Short, stable form of a key for idempotency keys.
fn key_tag(s: &str) -> String {
    if s.len() <= 96 {
        return s.to_string();
    }
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("#{h:016x}{:08x}", crc32c::crc32c(s.as_bytes()))
}

fn valid_subject(s: &str) -> bool {
    s.len() <= 1024
        && !s.is_empty()
        && s.split('.').all(|t| {
            !t.is_empty() && !t.contains('*') && !t.contains('>') && !t.chars().any(char::is_whitespace)
        })
}

fn parse_ts_text(s: &str) -> Option<i64> {
    let t = s.trim();
    if let Ok(v) = t.parse::<i64>() {
        return Some(v);
    }
    if let Ok(v) = t.parse::<f64>() {
        if v.is_finite() {
            return Some(v.round() as i64);
        }
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
        return Some(dt.timestamp_millis());
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, fmt) {
            return Some(dt.and_utc().timestamp_millis());
        }
    }
    None
}

/// Convert a `TIMESTAMP BY` result to epoch ms; NULL / unparseable values
/// fall back to the record timestamp.
fn event_times(arr: &ArrayRef, fallback: &[i64]) -> Result<Vec<i64>, ExqlError> {
    let n = arr.len();
    let pick = |vals: Vec<Option<i64>>| -> Vec<i64> {
        vals.into_iter()
            .zip(fallback)
            .map(|(v, f)| v.unwrap_or(*f))
            .collect()
    };
    match arr.data_type() {
        DataType::Timestamp(_, _) => {
            let c = cast(arr, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            let c = cast(&c, &DataType::Int64)?;
            let a = c.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            Ok(pick((0..n).map(|i| a.is_valid(i).then(|| a.value(i))).collect()))
        }
        DataType::Date32 | DataType::Date64 => {
            let c = cast(arr, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            event_times(&c, fallback)
        }
        t if t.is_integer() => {
            let c = cast(arr, &DataType::Int64)?;
            let a = c.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            Ok(pick((0..n).map(|i| a.is_valid(i).then(|| a.value(i))).collect()))
        }
        t if t.is_floating() => {
            let c = cast(arr, &DataType::Float64)?;
            let a = c.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            Ok(pick(
                (0..n)
                    .map(|i| (a.is_valid(i) && a.value(i).is_finite()).then(|| a.value(i).round() as i64))
                    .collect(),
            ))
        }
        _ => {
            let field = datafusion::arrow::datatypes::Field::new("t", arr.data_type().clone(), true);
            Ok(pick(
                (0..n)
                    .map(|i| cell_to_text(arr, &field, i).and_then(|s| parse_ts_text(&s)))
                    .collect(),
            ))
        }
    }
}

impl Runner {
    pub fn new(ctx: RunCtx, df: Dataflow, sink: Sink) -> Result<Self, ExqlError> {
        let storage = ctx.log.storage().clone();
        let n = df.sources.len();
        let ckpt_stream = checkpoint::ckpt_stream(&ctx.query_id)?;
        let key_col = df.out_schema.index_of("key").ok();
        let subject_col = df.out_schema.index_of("subject").ok();
        Ok(Self {
            ctx,
            df,
            sink,
            storage,
            pos: vec![0; n],
            max_et: vec![None; n],
            written: HashSet::new(),
            forced: VecDeque::new(),
            seq: 0,
            dirty: false,
            batches_since_ckpt: 0,
            last_ckpt: Instant::now(),
            ckpt_stream,
            key_col,
            subject_col,
        })
    }

    fn watermark(&self) -> Option<i64> {
        let seen: Vec<i64> = self.max_et.iter().flatten().copied().collect();
        if seen.is_empty() {
            return None;
        }
        seen.into_iter().min().map(|m| m.saturating_sub(self.df.grace_ms))
    }

    fn qid(&self) -> &str {
        &self.ctx.query_id
    }

    fn add_late(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.ctx.stats.late_dropped.fetch_add(n, Ordering::Relaxed);
        if let Some(m) = &self.ctx.metrics {
            m.record_exql_late(&self.ctx.query_id, n);
        }
    }

    // -- restore ------------------------------------------------------------

    async fn restore(&mut self) -> Result<(), ExqlError> {
        checkpoint::ensure_stream(&self.ctx.log, &self.ckpt_stream).await?;
        let ck = checkpoint::load(self.storage.as_ref(), &self.ckpt_stream).await?;
        let mut out_from = 0u64;
        match ck {
            Some(ck) => {
                let names: Vec<String> = self.df.sources.iter().map(|s| s.stream.to_string()).collect();
                let ck_names: Vec<String> = ck.meta.sources.iter().map(|s| s.stream.clone()).collect();
                if names != ck_names {
                    return Err(ExqlError::Internal(format!(
                        "checkpoint is for sources {ck_names:?}, query reads {names:?}"
                    )));
                }
                for (i, s) in ck.meta.sources.iter().enumerate() {
                    self.pos[i] = s.next_offset;
                    self.max_et[i] = s.max_et;
                }
                if let Some(agg) = self.df.agg.as_mut() {
                    if let Some(b) = ck.section("agg") {
                        agg.restore(b)?;
                    }
                }
                if let InputOp::StreamJoin(j) = &mut self.df.input {
                    if let (Some(l), Some(r)) = (ck.section("join_left"), ck.section("join_right")) {
                        j.restore(l, r)?;
                    }
                }
                let stats = &self.ctx.stats;
                stats.records_in.store(ck.meta.records_in, Ordering::Relaxed);
                stats.records_out.store(ck.meta.records_out, Ordering::Relaxed);
                stats.late_dropped.store(ck.meta.late_dropped, Ordering::Relaxed);
                self.seq = ck.meta.seq;
                let out = self.sink.stream().to_string();
                out_from = ck
                    .meta
                    .outputs
                    .iter()
                    .find(|o| o.stream == out)
                    .map(|o| o.offset)
                    .unwrap_or(0);
                debug!(query = %self.qid(), seq = self.seq, "restored checkpoint");
            }
            None => {
                for (i, s) in self.df.sources.iter().enumerate() {
                    self.pos[i] = self.storage.stream_bounds(&s.stream).await?.0 .0;
                }
                if let Some(agg) = self.df.agg.as_mut() {
                    agg.init_global()?;
                }
            }
        }
        if let Some(w) = self.watermark() {
            self.ctx.stats.watermark.store(w, Ordering::Relaxed);
        }
        if matches!(self.sink, Sink::Table { .. }) {
            self.rebuild_table()?;
        }
        self.scan_outputs(out_from).await
    }

    /// Collect what this query already wrote after the checkpoint.
    async fn scan_outputs(&mut self, from: u64) -> Result<(), ExqlError> {
        let stream = self.sink.stream().clone();
        let (earliest, end) = match self.storage.stream_bounds(&stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut p = from.max(earliest.0);
        let mut last: Vec<u64> = self.pos.clone();
        while p < end.0 {
            let b = self
                .storage
                .read_batch(
                    &stream,
                    Offset(p),
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: READ_BYTES,
                    },
                )
                .await?;
            if b.records.is_empty() {
                break;
            }
            for r in &b.records {
                let mine = r
                    .headers
                    .iter()
                    .any(|(k, v)| k == H_QUERY && v == &self.ctx.query_id);
                if !mine {
                    continue;
                }
                for (k, v) in &r.headers {
                    if k == IDEMPOTENCY_HEADER {
                        self.written.insert(v.clone());
                    } else if k == H_POS {
                        let parsed: Option<Vec<u64>> = v.split(',').map(|x| x.parse().ok()).collect();
                        if let Some(pv) = parsed {
                            let ahead = pv.len() == last.len()
                                && pv.iter().zip(&last).all(|(a, b)| a >= b)
                                && pv != last;
                            if ahead {
                                last = pv.clone();
                                self.forced.push_back(pv);
                            }
                        }
                    }
                }
            }
            p = b.next_offset.0;
        }
        if !self.forced.is_empty() {
            debug!(query = %self.qid(), batches = self.forced.len(), "replaying micro-batches written after the checkpoint");
        }
        Ok(())
    }

    fn rebuild_table(&mut self) -> Result<(), ExqlError> {
        let Sink::Table { table, .. } = &self.sink else {
            return Ok(());
        };
        let table = table.clone();
        let Some(agg) = self.df.agg.as_ref() else {
            return Ok(());
        };
        let keys = agg.all_keys();

        let outs = self.group_outputs(&keys)?;
        let rows = outs
            .into_iter()
            .filter_map(|(kv, row)| row.map(|(values, _, _)| (kv, values)))
            .collect();
        table.replace(rows);
        Ok(())
    }

    // -- checkpoint ---------------------------------------------------------

    async fn checkpoint(&mut self) -> Result<(), ExqlError> {
        let mut sections = vec![];
        if let Some(agg) = self.df.agg.as_mut() {
            sections.push(("agg".to_string(), agg.snapshot()?));
        }
        if let InputOp::StreamJoin(j) = &self.df.input {
            let (l, r) = j.snapshot()?;
            sections.push(("join_left".to_string(), l));
            sections.push(("join_right".to_string(), r));
        }
        let out = self.sink.stream().clone();
        let out_end = match self.storage.stream_bounds(&out).await {
            Ok((_, e)) => e.0,
            Err(StorageError::StreamNotFound(_)) => 0,
            Err(e) => return Err(e.into()),
        };
        let stats = &self.ctx.stats;
        let meta = CheckpointMeta {
            version: 1,
            query_id: self.ctx.query_id.clone(),
            seq: self.seq + 1,
            sources: self
                .df
                .sources
                .iter()
                .enumerate()
                .map(|(i, s)| SourceState {
                    stream: s.stream.to_string(),
                    next_offset: self.pos[i],
                    max_et: self.max_et[i],
                })
                .collect(),
            outputs: vec![OutputPos {
                stream: out.to_string(),
                offset: out_end,
            }],
            records_in: stats.records_in.load(Ordering::Relaxed),
            records_out: stats.records_out.load(Ordering::Relaxed),
            late_dropped: stats.late_dropped.load(Ordering::Relaxed),
            sections: vec![],
        };
        let ck = Checkpoint { meta, sections };
        checkpoint::save(&self.ctx.log, &self.ckpt_stream, &ck).await?;
        self.seq += 1;
        self.dirty = false;
        self.batches_since_ckpt = 0;
        self.last_ckpt = Instant::now();
        if self.forced.is_empty() {
            self.written.clear();
        }
        stats.checkpoints.fetch_add(1, Ordering::Relaxed);
        stats.last_checkpoint_ms.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }

    // -- reading ------------------------------------------------------------

    async fn read_sources(
        &self,
        target: Option<&Vec<u64>>,
    ) -> Result<(Vec<Vec<StoredRecord>>, Vec<u64>), ExqlError> {
        let mut all = Vec::with_capacity(self.df.sources.len());
        let mut new_pos = self.pos.clone();
        let batch = self.ctx.cfg.micro_batch_records.max(1);
        for (i, s) in self.df.sources.iter().enumerate() {
            let end = target.and_then(|t| t.get(i).copied());
            let mut recs: Vec<StoredRecord> = vec![];
            let mut p = self.pos[i];
            loop {
                let want = match end {
                    Some(e) if p >= e => break,
                    Some(e) => ((e - p) as usize).min(batch),
                    None => batch,
                };
                let b = self
                    .storage
                    .read_batch(
                        &s.stream,
                        Offset(p),
                        ReadLimits {
                            max_records: want,
                            max_bytes: READ_BYTES,
                        },
                    )
                    .await?;
                if b.records.is_empty() {
                    p = p.max(b.next_offset.0);
                    break;
                }
                for r in b.records {
                    if end.is_some_and(|e| r.offset.0 >= e) {
                        break;
                    }
                    p = r.offset.0 + 1;
                    recs.push(r);
                }
                if end.is_none() {
                    break;
                }
            }
            new_pos[i] = p;
            all.push(recs);
        }
        Ok((all, new_pos))
    }

    fn source_rows(&self, i: usize, recs: &[StoredRecord]) -> Result<Rows, ExqlError> {
        let def = &self.df.sources[i];
        let mut batch = records_to_batch(recs)?;
        if let Some((idx, schema)) = &def.scan_cols {
            let cols = idx.iter().map(|&c| batch.column(c).clone()).collect();
            batch = RecordBatch::try_new_with_options(
                schema.clone(),
                cols,
                &RecordBatchOptions::new().with_row_count(Some(recs.len())),
            )?;
        }
        let rec_ts: Vec<i64> = recs.iter().map(|r| (r.timestamp / 1_000_000) as i64).collect();
        let et = match &def.ts {
            Some(e) if !recs.is_empty() => event_times(&eval(e, &batch)?, &rec_ts)?,
            _ => rec_ts,
        };
        Ok(Rows {
            batch,
            et,
            ids: recs
                .iter()
                .map(|r| RowId::Src {
                    src: i as u8,
                    off: r.offset.0,
                })
                .collect(),
        })
    }

    // -- processing ---------------------------------------------------------

    /// Group rows through the top steps: `(group values, Some((row, payload,
    /// record key)))`, or `None` when HAVING removed the group.
    #[allow(clippy::type_complexity)]
    fn group_outputs(
        &mut self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<(Vec<ScalarValue>, Option<(Vec<ScalarValue>, Map<String, Json>, Option<String>)>)>, ExqlError> {
        let agg = self.df.agg.as_mut().expect("aggregate");
        let mut rows = vec![];
        let mut kvs = vec![];
        for k in keys {
            if let Some(r) = agg.row(k)? {
                rows.push(r);
                kvs.push(agg.key_values(k).unwrap_or_default());
            }
        }
        let agg_schema = agg.out_schema.clone();
        let n = rows.len();
        let batch = batch_from_rows(&agg_schema, &rows)?;
        let out = self.df.top.apply(Rows {
            batch,
            et: vec![0; n],
            ids: (0..n).map(|i| RowId::Group(i as u32)).collect(),
        })?;
        let mut produced: Vec<Option<usize>> = vec![None; n];
        for (j, id) in out.ids.iter().enumerate() {
            if let RowId::Group(g) = id {
                produced[*g as usize] = Some(j);
            }
        }
        let window_keys = if self.df.window.is_some() { 2 } else { 0 };
        let mut res = Vec::with_capacity(n);
        for (g, kv) in kvs.into_iter().enumerate() {
            let item = match produced[g] {
                Some(j) => {
                    let values = out.row_values(j)?;
                    let payload = row_object(&out.batch, j);
                    let rk = match self.key_col {
                        Some(c) => cell_to_text(out.batch.column(c), out.batch.schema().field(c), j),
                        None if kv.len() > window_keys => Some(key_string(&kv[window_keys..])),
                        None => None,
                    };
                    Some((values, payload, rk))
                }
                None => None,
            };
            res.push((kv, item));
        }
        Ok(res)
    }

    fn subject_of(&self, out: &Rows, j: usize, meta: &HashMap<(u8, u64), (String, Option<Bytes>)>) -> String {
        if let Some(c) = self.subject_col {
            if let Some(s) = cell_to_text(out.batch.column(c), out.batch.schema().field(c), j) {
                if valid_subject(&s) {
                    return s;
                }
            }
            return String::new();
        }
        if let RowId::Src { src, off } = &out.ids[j] {
            if let Some((s, _)) = meta.get(&(*src, *off)) {
                return s.clone();
            }
        }
        String::new()
    }

    fn process(&mut self, recs: Vec<Vec<StoredRecord>>, sig: &str) -> Result<Vec<OutItem>, ExqlError> {
        let wm_prev = self.watermark();
        let delay = self.df.delay_ms;
        let mut src_rows = Vec::with_capacity(recs.len());
        let single_stateless = matches!(self.df.input, InputOp::Single { .. }) && self.df.agg.is_none();
        let mut meta: HashMap<(u8, u64), (String, Option<Bytes>)> = HashMap::new();
        for (i, rs) in recs.iter().enumerate() {
            let rows = self.source_rows(i, rs)?;
            if let Some(m) = rows.et.iter().max() {
                self.max_et[i] = Some(self.max_et[i].map_or(*m, |x| x.max(*m)));
            }
            if single_stateless {
                for r in rs {
                    meta.insert((i as u8, r.offset.0), (r.subject.clone(), r.key.clone()));
                }
            }
            src_rows.push(Some(rows));
        }
        let wm_new = self.watermark();
        if let Some(w) = wm_new {
            self.ctx.stats.watermark.store(w, Ordering::Relaxed);
        }
        let mut late = 0u64;
        let input = match &mut self.df.input {
            InputOp::Single { src } => src_rows[*src].take().expect("source rows"),
            InputOp::StreamJoin(j) => {
                let l = j.left.chain.apply(src_rows[j.left.src].take().expect("left rows"))?;
                let r = j.right.chain.apply(src_rows[j.right.src].take().expect("right rows"))?;
                let out = j.process(l, r, wm_prev)?;
                late += out.late;
                let unmatched = j.advance(wm_new)?;
                Rows::concat(j.out_schema.clone(), vec![out.rows, unmatched])?
            }
            InputOp::TableJoin(t) => {
                let s = t.stream.chain.apply(src_rows[t.stream.src].take().expect("stream rows"))?;
                t.process(s)?
            }
        };
        let rows = self.df.mid.apply(input)?;
        let qid = self.ctx.query_id.clone();
        let mut items = vec![];

        if self.df.agg.is_none() {
            let out = self.df.top.apply(rows)?;
            let mut seen: HashMap<String, u32> = HashMap::new();
            for j in 0..out.len() {
                let tag = out.ids[j].tag();
                let n = seen.entry(tag.clone()).or_insert(0);
                let idem = format!("{qid}:{tag}:{n}");
                *n += 1;
                let record_key = match self.key_col {
                    Some(c) => cell_to_text(out.batch.column(c), out.batch.schema().field(c), j),
                    None => match &out.ids[j] {
                        RowId::Src { src, off } => meta
                            .get(&(*src, *off))
                            .and_then(|(_, k)| k.as_ref())
                            .map(|k| String::from_utf8_lossy(k).into_owned()),
                        _ => None,
                    },
                };
                items.push(OutItem {
                    idem,
                    record_key,
                    subject: self.subject_of(&out, j, &meta),
                    payload: Some(row_object(&out.batch, j)),
                    table: None,
                });
            }
            self.add_late(late);
            return Ok(items);
        }

        let close_prev = wm_prev.map(|w| w.saturating_sub(delay));
        let close_new = wm_new.map(|w| w.saturating_sub(delay));
        let upd = self.df.agg.as_mut().expect("aggregate").update(&rows, close_prev)?;
        late += upd.late;
        self.add_late(late);
        let emit = self.df.emit;
        if emit == Emit::Changes && !upd.changed.is_empty() {
            for (kv, row) in self.group_outputs(&upd.changed)? {
                let gk = key_string(&kv);
                let idem = format!("{qid}:c{sig}:{}", key_tag(&gk));
                items.push(self.group_item(idem, kv, row));
            }
        }
        let closed = self.df.agg.as_mut().expect("aggregate").close(close_new);
        if emit == Emit::Final && !closed.is_empty() {
            for (kv, row) in self.group_outputs(&closed)? {
                let gk = key_string(&kv);
                let idem = format!("{qid}:f:{}", key_tag(&gk));
                items.push(self.group_item(idem, kv, row));
            }
        }
        let agg = self.df.agg.as_mut().expect("aggregate");
        for k in &closed {
            agg.remove(k);
        }
        Ok(items)
    }

    fn group_item(
        &self,
        idem: String,
        kv: Vec<ScalarValue>,
        row: Option<(Vec<ScalarValue>, Map<String, Json>, Option<String>)>,
    ) -> OutItem {
        match row {
            Some((values, payload, rk)) => {
                let subject = self
                    .subject_col
                    .and_then(|c| payload.get(self.df.out_schema.field(c).name()))
                    .and_then(|v| v.as_str())
                    .filter(|s| valid_subject(s))
                    .unwrap_or_default()
                    .to_string();
                OutItem {
                    idem,
                    record_key: rk,
                    subject,
                    payload: Some(payload),
                    table: Some((kv, Some(values))),
                }
            }
            None => OutItem {
                idem,
                record_key: None,
                subject: String::new(),
                payload: None,
                table: Some((kv, None)),
            },
        }
    }

    // -- writing ------------------------------------------------------------

    /// Append with retries on transient errors.
    async fn append_retry(&self, stream: &StreamName, records: Vec<Record>) -> Result<(), ExqlError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut attempt = 0u32;
        loop {
            match self.ctx.log.append_batch(stream, records.clone()).await {
                Ok(_) => return Ok(()),
                Err(LogError::Storage(StorageError::KeyCollision { .. })) => {
                    // A replayed key with a different body: keep what is
                    // already there, write the rest one by one.
                    for r in records {
                        match self.ctx.log.append(stream, r).await {
                            Ok(_) => {}
                            Err(LogError::Storage(StorageError::KeyCollision { stored_offset })) => {
                                warn!(query = %self.qid(), stream = %stream, stored_offset, "output differs from the copy written before the restore; keeping the stored record");
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }
                    return Ok(());
                }
                Err(e) if e.is_retryable() && attempt < 50 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(100 * attempt.min(10) as u64)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    async fn write(&mut self, items: Vec<OutItem>, sig: &str) -> Result<(), ExqlError> {
        if items.is_empty() {
            return Ok(());
        }
        let stream = self.sink.stream().clone();
        let is_table = matches!(self.sink, Sink::Table { .. });
        let mut records = Vec::with_capacity(items.len());
        for it in &items {
            if !is_table && it.payload.is_none() {
                continue;
            }
            if self.written.remove(&it.idem) {
                continue;
            }
            let mut headers = vec![
                (IDEMPOTENCY_HEADER.to_string(), it.idem.clone()),
                (H_QUERY.to_string(), self.ctx.query_id.clone()),
                (H_POS.to_string(), sig.to_string()),
            ];
            let (key, value) = if is_table {
                let tk = it
                    .table
                    .as_ref()
                    .map(|(k, _)| key_string(k))
                    .unwrap_or_default();
                match &it.payload {
                    Some(p) => {
                        headers.push((H_OP.to_string(), "upsert".to_string()));
                        (Some(tk), Json::Object(p.clone()).to_string())
                    }
                    None => {
                        headers.push((H_OP.to_string(), "delete".to_string()));
                        (Some(tk), String::new())
                    }
                }
            } else {
                (
                    it.record_key.clone(),
                    Json::Object(it.payload.clone().unwrap_or_default()).to_string(),
                )
            };
            records.push(Record {
                key: key.filter(|k| !k.is_empty()).map(Bytes::from),
                value: Bytes::from(value),
                subject: it.subject.clone(),
                headers,
                timestamp_ns: None,
            });
        }
        let n = records.len() as u64;
        self.append_retry(&stream, records).await?;
        self.ctx.stats.records_out.fetch_add(n, Ordering::Relaxed);
        if let Sink::Table { table, .. } = &self.sink {
            for it in items {
                if let Some((k, row)) = it.table {
                    match row {
                        Some(r) => table.upsert(k, r),
                        None => table.delete(&k),
                    }
                }
            }
        }
        Ok(())
    }

    // -- main loop ----------------------------------------------------------

    /// Run until `cancel` fires. Writes a final checkpoint on the way out.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<(), ExqlError> {
        self.restore().await?;
        let mut watchers: Vec<tokio::sync::watch::Receiver<u64>> = self
            .df
            .sources
            .iter()
            .filter_map(|s| self.storage.watch_appends(&s.stream))
            .collect();
        let all_watched = watchers.len() == self.df.sources.len();
        let interval = self.ctx.cfg.checkpoint_interval;
        let result = async {
            loop {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                for w in watchers.iter_mut() {
                    w.borrow_and_update();
                }
                let target = self.forced.front().cloned();
                let (recs, new_pos) = self.read_sources(target.as_ref()).await?;
                if target.is_some() {
                    self.forced.pop_front();
                }
                let total: usize = recs.iter().map(|r| r.len()).sum();
                if total == 0 {
                    if new_pos != self.pos {
                        self.pos = new_pos;
                        self.dirty = true;
                    }
                    if target.is_some() {
                        continue;
                    }
                    if self.dirty && self.last_ckpt.elapsed() >= interval {
                        self.checkpoint().await?;
                    }
                    let poll = self.ctx.cfg.poll_interval;
                    let wait = if all_watched { poll * 5 } else { poll };
                    wait_for_data(&mut watchers, wait, &cancel).await;
                    continue;
                }
                let sig = new_pos.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",");
                let items = self.process(recs, &sig)?;
                self.write(items, &sig).await?;
                self.ctx.stats.records_in.fetch_add(total as u64, Ordering::Relaxed);
                self.pos = new_pos;
                self.dirty = true;
                self.batches_since_ckpt += 1;
                let every = self.ctx.cfg.checkpoint_every_batches;
                if self.last_ckpt.elapsed() >= interval || (every > 0 && self.batches_since_ckpt >= every) {
                    self.checkpoint().await?;
                }
            }
        }
        .await;
        if self.dirty && self.ctx.log.can_write() {
            if let Err(e) = self.checkpoint().await {
                warn!(query = %self.ctx.query_id, "final checkpoint failed: {e}");
            }
        }
        result
    }
}

async fn wait_for_data(
    watchers: &mut [tokio::sync::watch::Receiver<u64>],
    timeout: Duration,
    cancel: &CancellationToken,
) {
    let changed = async {
        if watchers.is_empty() {
            std::future::pending::<()>().await;
        }
        let futs = watchers.iter_mut().map(|w| Box::pin(w.changed()));
        let (res, _, _) = futures_util::future::select_all(futs).await;
        if res.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = changed => {}
        _ = tokio::time::sleep(timeout) => {}
        _ = cancel.cancelled() => {}
    }
}
