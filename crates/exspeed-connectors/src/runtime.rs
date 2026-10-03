//! The connector runtime: one supervisor task per connector, running the
//! source or sink loop that implements the checkpoint protocol.
//!
//! ```text
//!            ┌──────────── restart (stop → backoff → start) ◄─────────┐
//!            ▼                                                        │
//! Starting ─► Running ── Transient (retries exhausted) / Connection / panic ─► Backoff
//!                 │
//!                 ├── Fatal (config, auth) / retries exhausted with `fail` ─► Failed
//!                 └── cancelled (stop, delete, demotion, shutdown) ────────► Stopped
//! ```
//!
//! Every run ends with an awaited `stop()` on the plugin before the next
//! `start()`, so a restarted instance never races the previous one (for
//! example for a Postgres replication slot).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use exspeed_broker::broker_append::{AppendResult, IDEMPOTENCY_HEADER};
use exspeed_broker::log::{Log, LogError};
use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageEngine, StorageError};
use futures_util::FutureExt;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{ConnectorConfig, ConnectorType, OnTransientExhausted, Settings};
use crate::dlq::{DlqEntry, DlqWriter};
use crate::offset_store::OffsetStore;
use crate::registry::{PluginInit, Registry};
use crate::retry::RetryPolicy;
use crate::status::{ConnectorState, Status};
use crate::traits::{
    ConnectorError, ErrorKind, Lag, LagUnit, PoisonReason, SinkConnector, SinkRecord,
    SourceConnector, SourceRecord, WriteResult,
};
use crate::transform::Transform;

/// How long a plugin's `stop()` may take before it is abandoned.
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// Everything one connector run needs.
pub struct RunContext {
    pub config: ConnectorConfig,
    /// Resolved settings (`${VAR}` substituted where allowed).
    pub settings: Settings,
    pub log: Arc<Log>,
    pub storage: Arc<dyn StorageEngine>,
    pub offsets: Arc<dyn OffsetStore>,
    pub metrics: Arc<Metrics>,
    pub state: Arc<ConnectorState>,
    pub registry: Registry,
}

impl RunContext {
    fn init(&self) -> PluginInit {
        PluginInit {
            config: self.config.clone(),
            settings: self.settings.clone(),
            metrics: self.metrics.clone(),
        }
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

/// Handle to a running supervisor.
pub struct RunHandle {
    cancel: CancellationToken,
    done: watch::Receiver<bool>,
    abort: AbortHandle,
}

impl RunHandle {
    /// Ask the connector to stop and wait (up to `timeout`) until its
    /// supervisor has exited — i.e. the plugin's `stop()` has returned and,
    /// for sinks, the final flush + commit is done. Returns `false` if the
    /// task had to be aborted.
    pub async fn stop(&self, timeout: Duration) -> bool {
        self.cancel.cancel();
        let mut done = self.done.clone();
        let finished = tokio::time::timeout(timeout, done.wait_for(|d| *d)).await;
        match finished {
            Ok(_) => true,
            Err(_) => {
                self.abort.abort();
                false
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        *self.done.borrow()
    }
}

struct DoneGuard(watch::Sender<bool>);
impl Drop for DoneGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Spawn a supervisor for `ctx` under a child of `parent`.
pub fn spawn(ctx: Arc<RunContext>, parent: &CancellationToken) -> RunHandle {
    let cancel = parent.child_token();
    let (tx, rx) = watch::channel(false);
    let task_cancel = cancel.clone();
    let join = tokio::spawn(async move {
        let _done = DoneGuard(tx);
        supervise(ctx, task_cancel).await;
    });
    RunHandle {
        cancel,
        done: rx,
        abort: join.abort_handle(),
    }
}

/// How one run ended.
#[derive(Debug)]
enum RunEnd {
    Cancelled,
    /// Restart after backoff.
    Restart(String),
    /// Give up: `failed`.
    Fatal(String),
}

fn end_for(e: &ConnectorError, during: &str) -> RunEnd {
    let msg = format!("{during}: {e}");
    match e.kind() {
        ErrorKind::Fatal => RunEnd::Fatal(msg),
        _ => RunEnd::Restart(msg),
    }
}

async fn supervise(ctx: Arc<RunContext>, cancel: CancellationToken) {
    let mut consecutive: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        ctx.state.set_status(Status::Starting);
        let before = ctx.state.last_success();
        let end = run_once(&ctx, &cancel).await;
        if ctx.state.last_success() != before {
            consecutive = 0;
        }
        match end {
            RunEnd::Cancelled => break,
            RunEnd::Fatal(msg) => {
                error!(connector = ctx.name(), error = %msg, "connector failed");
                ctx.state.set_error(Status::Failed, msg);
                return;
            }
            RunEnd::Restart(msg) => {
                consecutive = consecutive.saturating_add(1);
                let max = ctx.config.restart.max_restarts;
                if max > 0 && consecutive > max {
                    let msg = format!("gave up after {max} consecutive restarts: {msg}");
                    error!(connector = ctx.name(), error = %msg, "connector failed");
                    ctx.state.set_error(Status::Failed, msg);
                    return;
                }
                let delay = ctx.config.restart.delay_for(consecutive - 1);
                warn!(
                    connector = ctx.name(),
                    error = %msg,
                    backoff_ms = delay.as_millis() as u64,
                    "connector run failed; restarting after backoff"
                );
                ctx.state.set_error(Status::Backoff, msg);
                ctx.state.record_restart();
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = cancel.cancelled() => break,
                }
            }
        }
    }
    ctx.state.set_status(Status::Stopped);
    info!(connector = ctx.name(), "connector stopped");
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".into()
    }
}

async fn run_once(ctx: &Arc<RunContext>, cancel: &CancellationToken) -> RunEnd {
    let init = ctx.init();
    match ctx.config.connector_type {
        ConnectorType::Source => {
            let mut source = match ctx.registry.create_source(&init) {
                Ok(s) => s,
                Err(e) => return end_for(&e, "create"),
            };
            let end = AssertUnwindSafe(run_source(ctx, source.as_mut(), cancel))
                .catch_unwind()
                .await
                .unwrap_or_else(|p| RunEnd::Restart(format!("panic: {}", panic_message(&*p))));
            stop_plugin(ctx.name(), AssertUnwindSafe(source.stop())).await;
            end
        }
        ConnectorType::Sink => {
            let mut sink = match ctx.registry.create_sink(&init) {
                Ok(s) => s,
                Err(e) => return end_for(&e, "create"),
            };
            let end = AssertUnwindSafe(run_sink(ctx, sink.as_mut(), cancel))
                .catch_unwind()
                .await
                .unwrap_or_else(|p| RunEnd::Restart(format!("panic: {}", panic_message(&*p))));
            stop_plugin(ctx.name(), AssertUnwindSafe(sink.stop())).await;
            end
        }
    }
}

async fn stop_plugin<F>(name: &str, fut: AssertUnwindSafe<F>)
where
    F: std::future::Future<Output = Result<(), ConnectorError>>,
{
    match tokio::time::timeout(STOP_TIMEOUT, fut.catch_unwind()).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => warn!(connector = name, error = %e, "plugin stop failed"),
        Ok(Err(p)) => warn!(connector = name, panic = %panic_message(&*p), "plugin stop panicked"),
        Err(_) => warn!(connector = name, "plugin stop timed out"),
    }
}

/// Sleep unless cancelled. `Err(())` = cancelled.
async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> Result<(), ()> {
    tokio::select! {
        _ = tokio::time::sleep(d) => Ok(()),
        _ = cancel.cancelled() => Err(()),
    }
}

/// In-place retries for one operation.
struct Retrier<'a> {
    policy: &'a RetryPolicy,
    attempt: u32,
}

impl<'a> Retrier<'a> {
    fn new(policy: &'a RetryPolicy) -> Self {
        Self { policy, attempt: 0 }
    }

    /// Next delay, honouring a server-supplied minimum; `None` = exhausted.
    fn next(&mut self, retry_after: Option<Duration>) -> Option<Duration> {
        let d = self.policy.delay_for(self.attempt)?;
        self.attempt += 1;
        Some(match retry_after {
            Some(min) => d.max(min).min(Duration::from_secs(300)),
            None => d,
        })
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Retry an operation in place on transient errors; connection/fatal
/// errors and exhaustion end the run. `$op` is re-evaluated per attempt.
macro_rules! retry_in_place {
    ($ctx:expr, $cancel:expr, $what:expr, $op:expr) => {{
        let mut retry = Retrier::new(&$ctx.config.retry);
        loop {
            match $op {
                Ok(v) => break Ok(v),
                Err(e) if e.kind() == ErrorKind::Transient => match retry.next(e.retry_after()) {
                    Some(d) => {
                        warn!(connector = $ctx.name(), error = %e, retry_in_ms = d.as_millis() as u64,
                              "{} failed; retrying", $what);
                        $ctx.state.note_retry(format!("{}: {e}", $what));
                        if sleep_or_cancel(d, $cancel).await.is_err() {
                            break Err(RunEnd::Cancelled);
                        }
                    }
                    None => break Err(exhausted($ctx, &format!("{}: {e}", $what))),
                },
                Err(e) => break Err(end_for(&e, $what)),
            }
        }
    }};
}

fn exhausted(ctx: &RunContext, msg: &str) -> RunEnd {
    let action = match ctx.config.on_transient_exhausted {
        OnTransientExhausted::Fail => "fail",
        _ => "restart",
    };
    ctx.metrics.connector_retry_attempts_total.add(
        1,
        &[
            opentelemetry::KeyValue::new("connector", ctx.name().to_string()),
            opentelemetry::KeyValue::new("outcome", "exhausted"),
        ],
    );
    ctx.metrics.connector_transient_exhausted_total.add(
        1,
        &[
            opentelemetry::KeyValue::new("connector", ctx.name().to_string()),
            opentelemetry::KeyValue::new("action", action),
        ],
    );
    let msg = format!("retries exhausted: {msg}");
    match ctx.config.on_transient_exhausted {
        OnTransientExhausted::Fail => RunEnd::Fatal(msg),
        _ => RunEnd::Restart(msg),
    }
}

fn dlq_writer(ctx: &RunContext) -> Result<DlqWriter, RunEnd> {
    let stream = match &ctx.config.dlq_stream {
        Some(s) => Some(
            StreamName::try_from(s.as_str())
                .map_err(|e| RunEnd::Fatal(format!("invalid dlq_stream '{s}': {e}")))?,
        ),
        None => None,
    };
    Ok(DlqWriter::new(
        ctx.log.clone(),
        stream,
        ctx.config.name.clone(),
        ctx.config.stream.clone(),
        ctx.metrics.clone(),
    ))
}

fn log_err_to_end(e: LogError, what: &str) -> RunEnd {
    if e.is_retryable() {
        RunEnd::Restart(format!("{what}: {e}"))
    } else {
        RunEnd::Fatal(format!("{what}: {e}"))
    }
}

async fn ensure_streams(
    ctx: &RunContext,
    stream: &StreamName,
    dlq: &DlqWriter,
) -> Result<(), RunEnd> {
    ctx.log
        .ensure_stream(stream)
        .await
        .map_err(|e| log_err_to_end(e, "create stream"))?;
    dlq.ensure_stream()
        .await
        .map_err(|e| log_err_to_end(e, "create dlq stream"))
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

fn is_poison(e: &LogError) -> bool {
    matches!(
        e,
        LogError::InvalidRecord(_) | LogError::Storage(StorageError::KeyCollision { .. })
    )
}

/// Turn plugin records into log records: transform, then `key_field`.
fn prepare(
    ctx: &RunContext,
    transform: Option<&Transform>,
    records: Vec<SourceRecord>,
) -> Vec<Record> {
    let key_field = &ctx.config.key_field;
    records
        .into_iter()
        .filter_map(|r| match transform {
            Some(t) => t.apply(&r),
            None => Some(r),
        })
        .map(|r| {
            let key = r.key.clone().or_else(|| {
                if key_field.is_empty() {
                    return None;
                }
                let json: serde_json::Value = serde_json::from_slice(&r.value).ok()?;
                crate::subject::lookup(&json, key_field).map(|v| match v {
                    serde_json::Value::String(s) => bytes::Bytes::from(s.clone().into_bytes()),
                    other => bytes::Bytes::from(other.to_string().into_bytes()),
                })
            });
            Record {
                key,
                value: r.value,
                subject: r.subject,
                headers: r.headers,
                timestamp_ns: None,
            }
        })
        .collect()
}

/// Append `records`, retrying transient failures. Poison records are
/// isolated (by appending one at a time) and routed to the DLQ.
async fn append_all(
    ctx: &RunContext,
    stream: &StreamName,
    records: Vec<Record>,
    dlq: &DlqWriter,
    cancel: &CancellationToken,
) -> Result<u64, RunEnd> {
    if records.is_empty() {
        return Ok(0);
    }
    let mut retry = Retrier::new(&ctx.config.retry);
    loop {
        match ctx.log.append_batch(stream, records.clone()).await {
            Ok(results) => {
                return Ok(results
                    .iter()
                    .filter(|r| matches!(r, AppendResult::Written(..)))
                    .count() as u64)
            }
            Err(e) if is_poison(&e) => break,
            Err(LogError::Storage(StorageError::StreamNotFound(_))) => {
                ctx.log
                    .ensure_stream(stream)
                    .await
                    .map_err(|e| log_err_to_end(e, "re-create stream"))?;
            }
            Err(e) if e.is_retryable() => match retry.next(None) {
                Some(d) => {
                    warn!(connector = ctx.name(), error = %e, "append failed; retrying");
                    ctx.state.note_retry(format!("append: {e}"));
                    sleep_or_cancel(d, cancel)
                        .await
                        .map_err(|_| RunEnd::Cancelled)?;
                }
                None => return Err(exhausted(ctx, &format!("append: {e}"))),
            },
            Err(e) => return Err(RunEnd::Fatal(format!("append: {e}"))),
        }
    }

    // One bad record poisons a whole batch append: isolate it.
    let mut written = 0u64;
    for record in records {
        retry.reset();
        loop {
            match ctx.log.append(stream, record.clone()).await {
                Ok(AppendResult::Written(..)) => {
                    written += 1;
                    break;
                }
                Ok(AppendResult::Duplicate(_)) => break,
                Err(e) if is_poison(&e) => {
                    let identity = record
                        .headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(IDEMPOTENCY_HEADER))
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| format!("{:016x}", stable_hash(&record.value)));
                    let reason = PoisonReason::InvalidRecord {
                        detail: e.to_string(),
                    };
                    let entry = DlqEntry {
                        subject: record.subject.clone(),
                        key: record.key.clone(),
                        value: record.value.clone(),
                        headers: record.headers.clone(),
                        original_offset: None,
                        timestamp: None,
                        identity,
                    };
                    dlq_with_retry(ctx, dlq, entry, &reason, cancel).await?;
                    break;
                }
                Err(e) if e.is_retryable() => match retry.next(None) {
                    Some(d) => {
                        ctx.state.note_retry(format!("append: {e}"));
                        sleep_or_cancel(d, cancel)
                            .await
                            .map_err(|_| RunEnd::Cancelled)?;
                    }
                    None => return Err(exhausted(ctx, &format!("append: {e}"))),
                },
                Err(e) => return Err(RunEnd::Fatal(format!("append: {e}"))),
            }
        }
    }
    Ok(written)
}

async fn dlq_with_retry(
    ctx: &RunContext,
    dlq: &DlqWriter,
    entry: DlqEntry,
    reason: &PoisonReason,
    cancel: &CancellationToken,
) -> Result<(), RunEnd> {
    let mut retry = Retrier::new(&ctx.config.retry);
    loop {
        match dlq.handle(entry.clone(), reason).await {
            Ok(()) => return Ok(()),
            Err(e) => match retry.next(None) {
                Some(d) => sleep_or_cancel(d, cancel)
                    .await
                    .map_err(|_| RunEnd::Cancelled)?,
                None => return Err(exhausted(ctx, &format!("dlq append: {e}"))),
            },
        }
    }
}

async fn run_source(
    ctx: &RunContext,
    src: &mut dyn SourceConnector,
    cancel: &CancellationToken,
) -> RunEnd {
    match source_loop(ctx, src, cancel).await {
        Ok(()) => RunEnd::Cancelled,
        Err(end) => end,
    }
}

async fn source_loop(
    ctx: &RunContext,
    src: &mut dyn SourceConnector,
    cancel: &CancellationToken,
) -> Result<(), RunEnd> {
    let name = ctx.name().to_string();
    let stream = StreamName::try_from(ctx.config.stream.as_str())
        .map_err(|e| RunEnd::Fatal(format!("invalid stream: {e}")))?;
    let transform = if ctx.config.transform_sql.is_empty() {
        None
    } else {
        Some(
            Transform::compile(&ctx.config.transform_sql)
                .map_err(|e| RunEnd::Fatal(format!("transform: {e}")))?,
        )
    };
    let dlq = dlq_writer(ctx)?;

    // A load error must not silently restart from scratch.
    let checkpoint = ctx
        .offsets
        .load_source(&name)
        .await
        .map_err(|e| RunEnd::Fatal(format!("failed to load offset: {e}")))?;
    ctx.state.set_checkpoint(checkpoint.clone());

    ensure_streams(ctx, &stream, &dlq).await?;

    tokio::select! {
        r = src.start(checkpoint.clone()) => r.map_err(|e| end_for(&e, "start"))?,
        _ = cancel.cancelled() => return Ok(()),
    }
    info!(connector = %name, checkpoint = ?checkpoint, "source started");
    ctx.state.set_status(Status::Running);

    let batch_size = ctx.config.batch_size as usize;
    let idle = Duration::from_millis(ctx.config.poll_interval_ms);
    let mut saved = checkpoint;

    loop {
        // 1. Poll (cancellable; nothing has been acknowledged yet).
        let batch = {
            let mut retry = Retrier::new(&ctx.config.retry);
            loop {
                let r = tokio::select! {
                    r = src.poll(batch_size) => r,
                    _ = cancel.cancelled() => return Ok(()),
                };
                match r {
                    Ok(b) => break b,
                    Err(e) if e.kind() == ErrorKind::Transient => match retry.next(e.retry_after())
                    {
                        Some(d) => {
                            warn!(connector = %name, error = %e, "poll failed; retrying");
                            ctx.state.note_retry(format!("poll: {e}"));
                            if sleep_or_cancel(d, cancel).await.is_err() {
                                return Ok(());
                            }
                        }
                        None => return Err(exhausted(ctx, &format!("poll: {e}"))),
                    },
                    Err(e) => return Err(end_for(&e, "poll")),
                }
            }
        };
        ctx.state.set_lag(src.lag());

        let new_checkpoint = batch.checkpoint.filter(|c| Some(c) != saved.as_ref());
        if batch.records.is_empty() && new_checkpoint.is_none() {
            if sleep_or_cancel(idle, cancel).await.is_err() {
                return Ok(());
            }
            continue;
        }

        // 2. Append. Cancellation only between attempts (the batch is then
        //    simply replayed from the last checkpoint).
        let records = prepare(ctx, transform.as_ref(), batch.records);
        let written = append_all(ctx, &stream, records, &dlq, cancel).await?;

        // 3. Persist the checkpoint.
        if let Some(cp) = &new_checkpoint {
            retry_in_place!(
                ctx,
                cancel,
                "save offset",
                ctx.offsets
                    .save_source(&name, cp)
                    .await
                    .map_err(|e| ConnectorError::transient(e.to_string()))
            )?;
            saved = Some(cp.clone());
        }

        // 4. Only now may the source acknowledge externally.
        retry_in_place!(ctx, cancel, "ack", src.ack(saved.as_deref()).await)?;

        ctx.state.record_success(written, "in", new_checkpoint);
    }
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

async fn run_sink(
    ctx: &RunContext,
    sink: &mut dyn SinkConnector,
    cancel: &CancellationToken,
) -> RunEnd {
    let mut st = SinkLoop::default();
    let result = sink_loop(ctx, sink, cancel, &mut st).await;
    match result {
        Ok(()) => {
            // Graceful stop: final flush + commit. On failure the
            // uncommitted records are simply re-delivered next time.
            if st.pending != st.committed {
                match flush_and_commit(ctx, sink, &mut st, &CancellationToken::new()).await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(connector = ctx.name(), error = ?e, "final flush failed; records will be re-delivered")
                    }
                }
            }
            RunEnd::Cancelled
        }
        Err(end) => end,
    }
}

#[derive(Default)]
struct SinkLoop {
    /// Last committed position (next offset to read after a restart).
    committed: u64,
    /// Position covering everything accepted by the sink so far.
    pending: u64,
    accepted_since_commit: u64,
    last_flush: Option<Instant>,
}

async fn flush_and_commit(
    ctx: &RunContext,
    sink: &mut dyn SinkConnector,
    st: &mut SinkLoop,
    cancel: &CancellationToken,
) -> Result<(), RunEnd> {
    retry_in_place!(ctx, cancel, "flush", sink.flush().await)?;
    st.last_flush = Some(Instant::now());
    if st.pending == st.committed {
        return Ok(());
    }
    let name = ctx.name().to_string();
    let pos = st.pending;
    retry_in_place!(
        ctx,
        cancel,
        "commit offset",
        ctx.offsets
            .save_sink(&name, pos)
            .await
            .map_err(|e| ConnectorError::transient(e.to_string()))
    )?;
    st.committed = pos;
    ctx.state
        .record_success(st.accepted_since_commit, "out", Some(pos.to_string()));
    st.accepted_since_commit = 0;
    Ok(())
}

async fn sink_loop(
    ctx: &RunContext,
    sink: &mut dyn SinkConnector,
    cancel: &CancellationToken,
    st: &mut SinkLoop,
) -> Result<(), RunEnd> {
    let name = ctx.name().to_string();
    let stream = StreamName::try_from(ctx.config.stream.as_str())
        .map_err(|e| RunEnd::Fatal(format!("invalid stream: {e}")))?;
    let filter = exspeed_common::SubjectFilter::parse(&ctx.config.subject_filter)
        .map_err(|e| RunEnd::Fatal(format!("invalid subject_filter: {e}")))?;
    let dlq = dlq_writer(ctx)?;

    let start = ctx
        .offsets
        .load_sink(&name)
        .await
        .map_err(|e| RunEnd::Fatal(format!("failed to load offset: {e}")))?
        .unwrap_or(0);
    st.committed = start;
    st.pending = start;
    ctx.state.set_checkpoint(Some(start.to_string()));

    ensure_streams(ctx, &stream, &dlq).await?;

    tokio::select! {
        r = sink.start() => r.map_err(|e| end_for(&e, "start"))?,
        _ = cancel.cancelled() => return Ok(()),
    }
    info!(connector = %name, offset = start, "sink started");
    ctx.state.set_status(Status::Running);

    let flush_every = ctx
        .config
        .flush_interval_ms
        .map(Duration::from_millis)
        .unwrap_or_else(|| sink.default_flush_interval());
    let idle = Duration::from_millis(ctx.config.poll_interval_ms.max(1));
    let limits = ReadLimits {
        max_records: ctx.config.batch_size as usize,
        max_bytes: 4 * 1024 * 1024,
    };
    let mut read_pos = start;
    let mut appends = ctx.storage.watch_appends(&stream);
    st.last_flush = Some(Instant::now());

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let flush_due = st.last_flush.is_none_or(|t| t.elapsed() >= flush_every);
        if sink.wants_flush() || (flush_due && st.pending != st.committed) {
            flush_and_commit(ctx, sink, st, cancel).await?;
        }

        let batch = match ctx
            .storage
            .read_batch(&stream, Offset(read_pos), limits)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(connector = %name, error = %e, "sink read failed");
                ctx.state.note_retry(format!("read: {e}"));
                if sleep_or_cancel(idle.max(Duration::from_millis(500)), cancel)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                continue;
            }
        };
        ctx.state.set_lag(Some(Lag {
            value: batch.high_watermark.0.saturating_sub(st.committed),
            unit: LagUnit::Records,
        }));

        if batch.records.is_empty() {
            // Caught up: wait for new data, the flush deadline or cancel.
            let wait = if st.pending != st.committed {
                flush_every
                    .saturating_sub(st.last_flush.map(|t| t.elapsed()).unwrap_or_default())
                    .min(idle.max(Duration::from_millis(250)))
            } else {
                idle.max(Duration::from_millis(250))
            };
            let notified = async {
                match appends.as_mut() {
                    Some(rx) => {
                        let _ = rx.changed().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(wait) => {}
                _ = cancel.cancelled() => return Ok(()),
            }
            continue;
        }

        let next = batch.next_offset.0;
        let records: Vec<SinkRecord> = batch
            .records
            .into_iter()
            .filter(|r| filter.matches(&r.subject))
            .map(|r| SinkRecord {
                offset: r.offset.0,
                timestamp: r.timestamp,
                subject: r.subject,
                key: r.key,
                value: r.value,
                headers: r.headers,
            })
            .collect();

        write_all(ctx, sink, &records, &dlq, cancel).await?;
        st.accepted_since_commit += records.len() as u64;
        read_pos = next;
        st.pending = next;

        if flush_every.is_zero() {
            flush_and_commit(ctx, sink, st, cancel).await?;
        }
    }
}

/// Write `records`, routing poison to the DLQ and retrying transient
/// failures. Returns once every record is accepted or handled.
async fn write_all(
    ctx: &RunContext,
    sink: &mut dyn SinkConnector,
    records: &[SinkRecord],
    dlq: &DlqWriter,
    cancel: &CancellationToken,
) -> Result<(), RunEnd> {
    let mut rest = records;
    let mut retry = Retrier::new(&ctx.config.retry);
    while !rest.is_empty() {
        let result = match sink.write(rest).await {
            Ok(r) => r,
            Err(e) => WriteResult::Failed {
                accepted: 0,
                error: e,
            },
        };
        match result {
            WriteResult::Accepted => return Ok(()),
            WriteResult::Poison { index, reason } => {
                let idx = index.min(rest.len() - 1);
                dlq_sink_record(ctx, dlq, &rest[idx], &reason, cancel).await?;
                rest = &rest[idx + 1..];
                retry.reset();
            }
            WriteResult::Failed { accepted, error } => {
                let accepted = accepted.min(rest.len());
                if accepted > 0 {
                    retry.reset();
                }
                rest = &rest[accepted..];
                if rest.is_empty() {
                    return Ok(());
                }
                match error {
                    ConnectorError::Poison(reason) => {
                        dlq_sink_record(ctx, dlq, &rest[0], &reason, cancel).await?;
                        rest = &rest[1..];
                    }
                    e @ ConnectorError::Transient { .. } => match retry.next(e.retry_after()) {
                        Some(d) => {
                            warn!(connector = ctx.name(), error = %e, retry_in_ms = d.as_millis() as u64,
                                  "sink write failed; retrying");
                            ctx.state.note_retry(format!("write: {e}"));
                            sleep_or_cancel(d, cancel)
                                .await
                                .map_err(|_| RunEnd::Cancelled)?;
                        }
                        None => {
                            if ctx.config.on_transient_exhausted == OnTransientExhausted::DlqBatch
                                && dlq.stream().is_some()
                            {
                                ctx.metrics.connector_transient_exhausted_total.add(
                                    1,
                                    &[
                                        opentelemetry::KeyValue::new(
                                            "connector",
                                            ctx.name().to_string(),
                                        ),
                                        opentelemetry::KeyValue::new("action", "dlq_batch"),
                                    ],
                                );
                                let reason = PoisonReason::RetriesExhausted {
                                    detail: e.to_string(),
                                };
                                for r in rest {
                                    dlq_sink_record(ctx, dlq, r, &reason, cancel).await?;
                                }
                                return Ok(());
                            }
                            // `loop_forever`: one record the sink keeps
                            // rejecting with an unclassified error would
                            // otherwise stall the sink forever. After
                            // repeated exhaustions at the same offset,
                            // isolate it and dead-letter it.
                            let strikes = ctx.state.note_stuck(rest[0].offset);
                            if ctx.config.on_transient_exhausted
                                == OnTransientExhausted::LoopForever
                                && dlq.stream().is_some()
                                && strikes >= STUCK_EXHAUSTIONS_BEFORE_DLQ
                            {
                                let done = isolate_stuck(ctx, sink, rest, dlq, cancel, &e).await?;
                                rest = &rest[done..];
                                retry.reset();
                                continue;
                            }
                            return Err(exhausted(ctx, &format!("write: {e}")));
                        }
                    },
                    e => return Err(end_for(&e, "write")),
                }
            }
        }
    }
    Ok(())
}

/// Consecutive retry exhaustions of a sink write starting at the same
/// offset (one per run, i.e. per supervisor restart) before the runtime
/// assumes a poison record behind an unclassified error and isolates it.
pub const STUCK_EXHAUSTIONS_BEFORE_DLQ: u32 = 3;

/// Write `rest` one record at a time, one attempt each. The first record that
/// still fails with a retryable error is dead-lettered (`retries_exhausted`):
/// the batch starting here has failed every retry across several restarts.
/// At most one record is dead-lettered this way per call, so a real outage
/// that begins mid-isolation costs at most one record. Returns how many
/// records of `rest` are done.
async fn isolate_stuck(
    ctx: &RunContext,
    sink: &mut dyn SinkConnector,
    rest: &[SinkRecord],
    dlq: &DlqWriter,
    cancel: &CancellationToken,
    batch_error: &ConnectorError,
) -> Result<usize, RunEnd> {
    warn!(connector = ctx.name(), offset = rest[0].offset, error = %batch_error,
          "sink write keeps failing at the same offset; isolating the record");
    for (i, r) in rest.iter().enumerate() {
        let error = match sink.write(std::slice::from_ref(r)).await {
            Ok(WriteResult::Accepted) => continue,
            Ok(WriteResult::Failed { accepted, .. }) if accepted >= 1 => continue,
            Ok(WriteResult::Poison { reason, .. }) => {
                dlq_sink_record(ctx, dlq, r, &reason, cancel).await?;
                continue;
            }
            Ok(WriteResult::Failed { error, .. }) | Err(error) => error,
        };
        match error {
            ConnectorError::Poison(reason) => {
                dlq_sink_record(ctx, dlq, r, &reason, cancel).await?;
            }
            e @ ConnectorError::Transient { .. } => {
                ctx.metrics.connector_transient_exhausted_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new("connector", ctx.name().to_string()),
                        opentelemetry::KeyValue::new("action", "dlq_record"),
                    ],
                );
                let reason = PoisonReason::RetriesExhausted {
                    detail: format!(
                        "failed every retry across {STUCK_EXHAUSTIONS_BEFORE_DLQ} restarts: {e}"
                    ),
                };
                dlq_sink_record(ctx, dlq, r, &reason, cancel).await?;
                ctx.state.clear_stuck();
                return Ok(i + 1);
            }
            e => return Err(end_for(&e, "write")),
        }
    }
    // Every record went through one at a time: whatever failed has cleared.
    ctx.state.clear_stuck();
    Ok(rest.len())
}

async fn dlq_sink_record(
    ctx: &RunContext,
    dlq: &DlqWriter,
    r: &SinkRecord,
    reason: &PoisonReason,
    cancel: &CancellationToken,
) -> Result<(), RunEnd> {
    warn!(connector = ctx.name(), offset = r.offset, reason = reason.label(), detail = %reason.detail(), "poison record");
    let entry = DlqEntry {
        subject: r.subject.clone(),
        key: r.key.clone(),
        value: r.value.clone(),
        headers: r.headers.clone(),
        original_offset: Some(r.offset),
        timestamp: Some(r.timestamp),
        identity: format!("{}:{}", ctx.config.stream, r.offset),
    };
    dlq_with_retry(ctx, dlq, entry, reason, cancel).await
}
