use std::path::{Path, PathBuf};
use std::time::Duration;

use s3::Bucket;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::file::partition::SealedSegmentInfo;
use crate::s3::manifest::{Manifest, SegmentEntry};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Spawn a background task that receives sealed-segment notifications, uploads
/// the segment files to S3, updates the stream's manifest, and forwards the
/// notification to `uploaded_tx` so the cache tracker can record the upload.
pub fn spawn_uploader(
    mut rx: mpsc::UnboundedReceiver<SealedSegmentInfo>,
    bucket: Box<Bucket>,
    prefix: String,
    uploaded_tx: mpsc::UnboundedSender<SealedSegmentInfo>,
) {
    tokio::spawn(async move {
        while let Some(info) = rx.recv().await {
            let stream = &info.stream_name;
            info!(stream, base_offset = info.base_offset, "uploading sealed segment to S3");

            // Upload .seg / .idx / .tix with retry.
            if let Err(e) = upload_with_retry(&bucket, &prefix, &info).await {
                error!(stream, base_offset = info.base_offset, error = %e, "S3 upload failed after retries");
                continue;
            }

            info!(stream, base_offset = info.base_offset, "S3 upload complete, updating manifest");

            // Load the existing manifest, add the new entry, and PUT it back.
            let mut manifest = load_manifest(&bucket, &prefix, stream).await;
            manifest.add_segment(SegmentEntry {
                base_offset: info.base_offset,
                end_offset: info.end_offset,
                size_bytes: info.size_bytes,
                record_count: info.record_count,
                first_timestamp: info.first_timestamp,
                last_timestamp: info.last_timestamp,
                uploaded_at: chrono::Utc::now().to_rfc3339(),
            });

            if let Err(e) = save_manifest(&bucket, &prefix, &manifest).await {
                error!(stream, base_offset = info.base_offset, error = %e, "failed to save S3 manifest");
                // We still forward the notification — the files are uploaded.
            }

            // Notify the cache tracker that this segment is now in S3.
            let _ = uploaded_tx.send(info);
        }

        info!("S3 uploader task exiting — channel closed");
    });
}

/// Download `.seg`, `.idx`, and `.tix` files for a segment from S3 into
/// `local_dir`.  The `.seg` file is required; the index files are optional.
///
/// Creates `local_dir` if it does not exist.
pub async fn download_segment(
    bucket: &Bucket,
    prefix: &str,
    stream: &str,
    partition_id: u32,
    base_offset: u64,
    local_dir: &Path,
) -> Result<(), String> {
    tokio::fs::create_dir_all(local_dir)
        .await
        .map_err(|e| format!("failed to create local dir {}: {e}", local_dir.display()))?;

    let base_name = format!("{base_offset:020}");

    // .seg is required.
    download_file(bucket, prefix, stream, partition_id, &format!("{base_name}.seg"), local_dir, true).await?;

    // .idx and .tix are optional.
    download_file(bucket, prefix, stream, partition_id, &format!("{base_name}.idx"), local_dir, false).await?;
    download_file(bucket, prefix, stream, partition_id, &format!("{base_name}.tix"), local_dir, false).await?;

    Ok(())
}

/// List all stream manifests stored under `prefix`.
///
/// Uses the `delimiter="/"` trick to enumerate stream-level prefixes, then
/// attempts to load `manifest.json` for each.  Returns only manifests that
/// contain at least one segment entry.
pub async fn load_all_manifests(bucket: &Bucket, prefix: &str) -> Vec<Manifest> {
    let results = match bucket.list(prefix.to_string(), Some("/".to_string())).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to list S3 prefixes for manifests");
            return Vec::new();
        }
    };

    let mut manifests = Vec::new();

    for page in &results {
        let Some(ref common_prefixes) = page.common_prefixes else {
            continue;
        };
        for cp in common_prefixes {
            // Each common prefix looks like "{prefix}{stream_name}/".
            // Extract the stream name by stripping the outer prefix and trailing '/'.
            let stream_prefix = &cp.prefix;
            let stream_name = stream_prefix
                .strip_prefix(prefix)
                .unwrap_or(stream_prefix)
                .trim_end_matches('/');

            if stream_name.is_empty() {
                continue;
            }

            let manifest = load_manifest(bucket, prefix, stream_name).await;
            if !manifest.segments.is_empty() {
                manifests.push(manifest);
            }
        }
    }

    manifests
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Upload the three segment files for `info` to S3.  Retries up to 5 times
/// with exponential back-off starting at 2 seconds.
async fn upload_with_retry(
    bucket: &Bucket,
    prefix: &str,
    info: &SealedSegmentInfo,
) -> Result<(), String> {
    const MAX_ATTEMPTS: u32 = 5;
    let mut delay = Duration::from_secs(2);

    for attempt in 1..=MAX_ATTEMPTS {
        match upload_segment_files(bucket, prefix, info).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt == MAX_ATTEMPTS {
                    return Err(e);
                }
                warn!(
                    stream = %info.stream_name,
                    base_offset = info.base_offset,
                    attempt,
                    error = %e,
                    "S3 upload attempt failed, retrying in {}s",
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }

    unreachable!()
}

/// Upload `.seg`, `.idx`, `.tix`, and `.bloom` companion files for the given segment.
async fn upload_segment_files(
    bucket: &Bucket,
    prefix: &str,
    info: &SealedSegmentInfo,
) -> Result<(), String> {
    let seg_path = &info.seg_path;
    let idx_path = seg_path.with_extension("idx");
    let tix_path = seg_path.with_extension("tix");
    let bloom_path = seg_path.with_extension("bloom");

    let stream = &info.stream_name;
    let partition_id = info.partition_id;

    // .seg is always required.
    upload_file(bucket, prefix, stream, partition_id, seg_path, true).await?;

    // .idx, .tix, and .bloom may not exist yet (e.g. empty segment), treat as optional.
    upload_file(bucket, prefix, stream, partition_id, &idx_path, false).await?;
    upload_file(bucket, prefix, stream, partition_id, &tix_path, false).await?;
    upload_file(bucket, prefix, stream, partition_id, &bloom_path, false).await?;

    Ok(())
}

/// Upload a single local file to S3.
///
/// When `required` is `true` and the file cannot be read, the error is
/// propagated.  When `required` is `false` a missing/unreadable file is
/// silently skipped.
async fn upload_file(
    bucket: &Bucket,
    prefix: &str,
    stream: &str,
    partition_id: u32,
    local_path: &Path,
    required: bool,
) -> Result<(), String> {
    let filename = local_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let s3_key = s3_key(prefix, stream, partition_id, filename);

    let data = match tokio::fs::read(local_path).await {
        Ok(d) => d,
        Err(e) => {
            if required {
                return Err(format!(
                    "failed to read {}: {e}",
                    local_path.display()
                ));
            } else {
                // Optional file doesn't exist — skip silently.
                return Ok(());
            }
        }
    };

    bucket
        .put_object(&s3_key, &data)
        .await
        .map_err(|e| format!("S3 put_object({s3_key}) failed: {e}"))?;

    info!(s3_key, bytes = data.len(), "uploaded file to S3");
    Ok(())
}

/// Download a single file from S3 and write it to `local_dir`.
///
/// When `required` is `true` and the object is missing (404), an error is
/// returned.  When `required` is `false` a 404 is silently ignored.
async fn download_file(
    bucket: &Bucket,
    prefix: &str,
    stream: &str,
    partition_id: u32,
    filename: &str,
    local_dir: &Path,
    required: bool,
) -> Result<(), String> {
    let s3_key = s3_key(prefix, stream, partition_id, filename);

    let response = bucket
        .get_object(&s3_key)
        .await
        .map_err(|e| format!("S3 get_object({s3_key}) failed: {e}"))?;

    if response.status_code() == 404 {
        if required {
            return Err(format!("required S3 object not found: {s3_key}"));
        }
        return Ok(());
    }

    if response.status_code() != 200 {
        let msg = format!(
            "S3 get_object({s3_key}) returned status {}",
            response.status_code()
        );
        if required {
            return Err(msg);
        }
        warn!("{msg}");
        return Ok(());
    }

    let local_path: PathBuf = local_dir.join(filename);
    tokio::fs::write(&local_path, response.as_slice())
        .await
        .map_err(|e| format!("failed to write {}: {e}", local_path.display()))?;

    info!(s3_key, local = %local_path.display(), bytes = response.as_slice().len(), "downloaded file from S3");
    Ok(())
}

/// Load the manifest for `stream` from S3.  Returns an empty manifest if the
/// object is absent or cannot be parsed.
async fn load_manifest(bucket: &Bucket, prefix: &str, stream: &str) -> Manifest {
    let key = manifest_key(prefix, stream);

    match bucket.get_object(&key).await {
        Err(e) => {
            warn!(key, error = %e, "could not load S3 manifest, using empty");
            Manifest::new(stream)
        }
        Ok(resp) => {
            if resp.status_code() == 404 {
                Manifest::new(stream)
            } else {
                match Manifest::from_json(resp.as_slice()) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(key, error = %e, "failed to parse S3 manifest, using empty");
                        Manifest::new(stream)
                    }
                }
            }
        }
    }
}

/// Serialize and PUT the manifest to S3.
async fn save_manifest(
    bucket: &Bucket,
    prefix: &str,
    manifest: &Manifest,
) -> Result<(), String> {
    let key = manifest_key(prefix, &manifest.stream);
    let data = manifest
        .to_json()
        .map_err(|e| format!("failed to serialize manifest for {}: {e}", manifest.stream))?;

    bucket
        .put_object(&key, &data)
        .await
        .map_err(|e| format!("S3 put_object({key}) failed: {e}"))?;

    info!(key, "saved S3 manifest");
    Ok(())
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Build the S3 key for a segment file.
///
/// Format: `{prefix}{stream}/partitions/{partition_id}/{filename}`
fn s3_key(prefix: &str, stream: &str, partition_id: u32, filename: &str) -> String {
    format!("{prefix}{stream}/partitions/{partition_id}/{filename}")
}

/// Build the S3 key for a stream's manifest.
///
/// Format: `{prefix}{stream}/manifest.json`
fn manifest_key(prefix: &str, stream: &str) -> String {
    format!("{prefix}{stream}/manifest.json")
}
