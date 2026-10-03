//! `GET /api/v1/backup`: stream an online backup as a tar archive. The
//! snapshot and archive logic live in `exspeed_storage::file::backup`.

use std::io::{self, Write};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Extension, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use exspeed_common::auth::Identity;
use exspeed_storage::file::backup::prepare_backup;
use serde_json::json;
use tokio::sync::mpsc;

use crate::state::AppState;

/// Chunk size handed to the response body.
const CHUNK: usize = 256 * 1024;

/// `Write` adapter that forwards chunks to the response body. Fails with
/// `BrokenPipe` once the client has gone away, which aborts the backup and
/// releases its snapshot.
struct BodyWriter {
    tx: mpsc::Sender<Result<Bytes, io::Error>>,
    buf: Vec<u8>,
}

impl BodyWriter {
    fn send(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK)));
        self.tx
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected"))
    }
}

impl Write for BodyWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK {
            self.send()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send()
    }
}

/// `GET /api/v1/backup` — online backup of the whole data directory.
///
/// Every stream is snapshotted at its high watermark before the first byte
/// is sent; the response then streams a tar archive (`exspeed-backup.json`
/// manifest first) while writes continue. Requires global admin. Restore
/// it offline with `exspeed restore`.
#[utoipa::path(
    get,
    path = "/api/v1/backup",
    tag = "operations",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Tar archive: the `exspeed-backup.json` manifest, then `streams/…` and the config directories", content_type = "application/x-tar", body = Vec<u8>),
        (status = 403, description = "Not a global admin", body = crate::openapi::ErrorBody),
        (status = 500, description = "A snapshot could not be taken", body = crate::openapi::ErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ErrorBody),
    )
)]
pub async fn backup(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(r) = super::require_global_admin(&id) {
            return r;
        }
    }
    let storage = state.storage.clone();
    let prepared =
        tokio::task::spawn_blocking(move || prepare_backup(&storage, env!("CARGO_PKG_VERSION")))
            .await;
    let prepared = match prepared {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("backup failed: {e}")})),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("backup task failed: {e}")})),
            )
                .into_response()
        }
    };
    let streams = prepared.manifest().streams.len();
    let filename = format!(
        "exspeed-backup-{}.tar",
        prepared
            .manifest()
            .created_at
            .replace([':', '.'], "-")
            .trim_end_matches('Z')
    );

    let (tx, mut rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let mut w = BodyWriter {
            tx: tx.clone(),
            buf: Vec::with_capacity(CHUNK),
        };
        match prepared.write_to(&mut w) {
            Ok(m) => {
                let records: u64 = m.streams.iter().map(|s| s.records).sum();
                tracing::info!(streams, records, "backup streamed");
            }
            Err(e) => {
                tracing::error!(error = %e, "backup aborted");
                // Abort the response so the client sees an error instead of
                // a silently truncated archive.
                let _ = tx.blocking_send(Err(e));
            }
        }
    });
    let body = Body::from_stream(futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx)));
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-tar".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        body,
    )
        .into_response()
}
