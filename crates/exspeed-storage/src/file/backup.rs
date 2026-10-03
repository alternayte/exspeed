//! Online backup and offline restore.
//!
//! [`FileStorage::snapshot_stream`] pins a point-in-time view of one stream
//! without stopping its writer: it reads the high watermark `H`, then the
//! segment list, and keeps an `Arc` to every segment so that retention or
//! compaction running meanwhile can't pull the files away (their open file
//! handles stay readable after an unlink or rename). The snapshot holds:
//!
//! * every sealed segment that ends at or below `H`, copied whole, with its
//!   `.idx` and `.meta` regenerated from the in-memory segment (so they
//!   describe exactly the bytes copied, even if compaction replaced the
//!   files on disk meanwhile);
//! * the segment that contains `H` (normally the active one), cut after its
//!   last complete record below `H`, with an index and metadata built by
//!   scanning the bytes that are included;
//! * an empty, header-only segment with base `H`, which becomes the active
//!   segment on restore, so the restored stream's next offset is exactly
//!   `H` and every other segment opens from its `.meta` without a scan.
//!
//! Segment files are append-only, except for `truncate_from` (follower
//! divergence repair), which shrinks a file in place. A snapshot records
//! the partition's truncation epoch and [`StreamSnapshot::verify`] fails if
//! a truncation started while the bytes were being copied.
//!
//! [`write_backup`] streams a tar archive of every stream's snapshot plus
//! the configuration directories in [`CONFIG_DIRS`], led by a manifest
//! (`exspeed-backup.json`). [`restore_backup`] unpacks such an archive into
//! a data directory that no server is using, validating it on the way.
//!
//! Consistency is per stream: each stream is a prefix of its log as of the
//! moment its snapshot was taken. All snapshots are taken before any byte
//! is written, so they are close together in time, but not atomic across
//! streams. Internal streams (consumer state, connector offsets, query
//! checkpoints) are snapshotted before the streams whose progress they
//! record, so after a restore that progress can lag the data (records are
//! redelivered) but never run ahead of it.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use exspeed_common::{StreamName, INTERNAL_STREAM_PREFIX};
use exspeed_streams::StorageError;
use serde::{Deserialize, Serialize};

use crate::file::fsutil::{fsync_dir, read_at};
use crate::file::partition::PartitionShared;
use crate::file::segment::{
    encode_index, header_bytes, FrameError, FrameIter, IndexBuilder, IndexEntry, Segment,
    SegmentMeta, SEGMENT_HEADER_LEN,
};
use crate::file::{FileStorage, StorageOptions};

/// Name of the manifest, always the first entry of a backup archive.
pub const MANIFEST_NAME: &str = "exspeed-backup.json";
/// Value of [`BackupManifest::format`].
pub const BACKUP_FORMAT: &str = "exspeed-backup";
/// Archive layout version written by this build. [`restore_backup`]
/// accepts this version only.
pub const BACKUP_VERSION: u32 = 1;

/// Directories of the data dir (besides `streams/`) that a backup copies
/// as they are when the backup reaches them: connector configs and offsets,
/// external connections and ExQL query definitions. The credentials file,
/// the dedup snapshots (rebuilt from the log) and replication state are not
/// included.
pub const CONFIG_DIRS: &[&str] = &[
    "connectors",
    "connectors.d",
    "connector-offsets",
    "connections",
    "connections.d",
    "exql",
];

/// What a backup contains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Always [`BACKUP_FORMAT`].
    pub format: String,
    /// Archive layout version ([`BACKUP_VERSION`]).
    pub version: u32,
    /// Version of the server that wrote the backup.
    pub server_version: String,
    /// RFC 3339 UTC time at which the snapshots were taken.
    pub created_at: String,
    /// Every stream, in snapshot order (internal streams first).
    pub streams: Vec<StreamManifest>,
    /// The entries of [`CONFIG_DIRS`] that were present and copied.
    #[serde(default)]
    pub config_dirs: Vec<String>,
}

/// One stream in a backup: every retained record with an offset in
/// `[earliest_offset, next_offset)` is included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamManifest {
    pub name: String,
    /// Offset of the first included record (equal to `next_offset` when
    /// the stream is empty).
    pub earliest_offset: u64,
    /// The stream's high watermark at snapshot time: the offset the next
    /// record gets after a restore.
    pub next_offset: u64,
    /// Number of records included (smaller than the offset range when the
    /// stream is compacted).
    pub records: u64,
    /// Segment bytes included, headers included.
    pub bytes: u64,
}

/// Where the bytes of a [`SnapshotFile`] come from.
pub enum SnapshotData {
    /// The first `len` bytes of a segment file.
    Segment { segment: Arc<Segment>, len: u64 },
    /// Generated in memory (configs, indexes, metadata, empty segments).
    Bytes(Vec<u8>),
}

/// One file of a stream snapshot.
pub struct SnapshotFile {
    /// Path relative to the stream directory (`stream.json`,
    /// `partitions/0/<base>.seg`, ...).
    pub path: String,
    pub data: SnapshotData,
}

impl SnapshotFile {
    pub fn len(&self) -> u64 {
        match &self.data {
            SnapshotData::Segment { len, .. } => *len,
            SnapshotData::Bytes(b) => b.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A reader over the file's bytes. A segment that turns out shorter
    /// than recorded (truncated underneath) fails with `UnexpectedEof`
    /// instead of producing a short copy.
    pub fn reader(&self) -> Box<dyn Read + Send + '_> {
        match &self.data {
            SnapshotData::Segment { segment, len } => Box::new(SegmentReader {
                segment,
                pos: 0,
                end: *len,
            }),
            SnapshotData::Bytes(b) => Box::new(io::Cursor::new(b.as_slice())),
        }
    }
}

struct SegmentReader<'a> {
    segment: &'a Segment,
    pos: u64,
    end: u64,
}

impl Read for SegmentReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let want = buf.len().min((self.end - self.pos) as usize);
        if want == 0 {
            return Ok(0);
        }
        let n = read_at(self.segment.file(), &mut buf[..want], self.pos)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{}: file ends at byte {} but the snapshot includes {} bytes",
                    self.segment.path.display(),
                    self.pos,
                    self.end
                ),
            ));
        }
        self.pos += n as u64;
        Ok(n)
    }
}

/// A point-in-time view of one stream. Holding it keeps the segments it
/// references open (and their disk space allocated, even if retention
/// deletes them meanwhile) until it is dropped.
pub struct StreamSnapshot {
    pub name: String,
    pub earliest_offset: u64,
    pub next_offset: u64,
    pub records: u64,
    /// Files relative to the stream directory, in archive order.
    pub files: Vec<SnapshotFile>,
    shared: Arc<PartitionShared>,
    truncation_epoch: u64,
}

impl StreamSnapshot {
    /// Segment bytes included, headers included.
    pub fn segment_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|f| f.path.ends_with(".seg"))
            .map(SnapshotFile::len)
            .sum()
    }

    /// Fail if the partition was truncated since the snapshot was taken:
    /// bytes copied from it may then not be the bytes the snapshot
    /// describes. Call after copying.
    pub fn verify(&self) -> Result<(), StorageError> {
        if self.shared.truncation_epoch() != self.truncation_epoch {
            return Err(StorageError::Io(io::Error::other(format!(
                "stream {} was truncated while it was being backed up; retry the backup",
                self.name
            ))));
        }
        Ok(())
    }

    pub fn manifest(&self) -> StreamManifest {
        StreamManifest {
            name: self.name.clone(),
            earliest_offset: self.earliest_offset,
            next_offset: self.next_offset,
            records: self.records,
            bytes: self.segment_bytes(),
        }
    }
}

fn seg_name(base: u64) -> String {
    format!("partitions/0/{base:020}.seg")
}
fn idx_name(base: u64) -> String {
    format!("partitions/0/{base:020}.idx")
}
fn meta_name(base: u64) -> String {
    format!("partitions/0/{base:020}.meta")
}

fn corrupt(seg: &Segment, e: FrameError) -> StorageError {
    StorageError::Io(e.into_io(&seg.path))
}

/// Scan `seg` from its first record and stop before the first record at or
/// above `hwm` (or at `len`). Returns the metadata (its `len` is the cut
/// position) and index entries of the included prefix.
fn cut_segment(
    seg: &Segment,
    len: u64,
    hwm: u64,
) -> Result<(SegmentMeta, Vec<IndexEntry>), StorageError> {
    let mut it = FrameIter::new(seg.file(), SEGMENT_HEADER_LEN, len, 1 << 20);
    let mut ib = IndexBuilder::default();
    let mut entries: Vec<IndexEntry> = Vec::new();
    let mut meta = SegmentMeta {
        base_offset: seg.base_offset,
        len: SEGMENT_HEADER_LEN,
        end_offset: seg.base_offset,
        first_ts: None,
        max_ts: None,
        records: 0,
        index_entries: 0,
    };
    loop {
        let f = match it.next_frame() {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return Err(corrupt(seg, e)),
        };
        if f.offset >= hwm {
            break;
        }
        if let Some(e) = ib.observe(f.offset, f.pos, f.timestamp) {
            entries.push(e);
        }
        meta.records += 1;
        meta.first_ts.get_or_insert(f.timestamp);
        meta.max_ts = Some(ib.max_ts);
        meta.end_offset = f.offset + 1;
        meta.len = f.pos + f.size;
    }
    meta.index_entries = entries.len() as u64;
    Ok((meta, entries))
}

fn meta_json(meta: &SegmentMeta) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec_pretty(meta).map_err(|e| StorageError::Io(io::Error::other(e)))
}

impl FileStorage {
    /// Take a point-in-time snapshot of `stream` (see the module docs).
    /// Cheap apart from scanning the segment that holds the high watermark
    /// (at most one segment, usually the active one). Blocking.
    pub fn snapshot_stream(&self, stream: &str) -> Result<StreamSnapshot, StorageError> {
        let h = self.handle_by_name(stream).ok_or_else(|| {
            StorageError::StreamNotFound(
                StreamName::try_from(stream).unwrap_or_else(|_| StreamName::try_from("_").unwrap()),
            )
        })?;
        let shared = h.shared.clone();
        if let Some(e) = shared.failed_error() {
            return Err(e);
        }
        // Order matters: epoch, then high watermark, then segment list. The
        // writer publishes a record's bytes and the segment length before
        // the high watermark, so every record below `hwm` is inside the
        // lengths loaded afterwards.
        let truncation_epoch = shared.truncation_epoch();
        let hwm = shared.high_watermark();
        let list = shared.segments();

        let mut files = Vec::new();
        let config = match shared.config() {
            Some(c) => Some(serde_json::to_vec_pretty(&c).map_err(|e| StorageError::Io(e.into()))?),
            // Unparseable in memory: copy whatever is on disk.
            None => shared
                .dir
                .parent()
                .and_then(Path::parent)
                .and_then(|d| fs::read(d.join("stream.json")).ok()),
        };
        if let Some(c) = config {
            files.push(SnapshotFile {
                path: "stream.json".into(),
                data: SnapshotData::Bytes(c),
            });
        }

        let mut earliest: Option<u64> = None;
        let mut records = 0u64;
        for seg in list.iter() {
            if seg.base_offset >= hwm {
                break;
            }
            // Load the length before the end offset: the writer stores the
            // end offset first, so `end <= hwm` then proves that every
            // record inside `len` is below the high watermark.
            let len = seg.len();
            let end = seg.end_offset();
            let (meta, entries) = if seg.is_sealed() && end <= hwm {
                let mut meta = seg.meta();
                meta.len = len;
                let entries = seg.index_entries().map_err(StorageError::Io)?;
                meta.index_entries = entries.len() as u64;
                (meta, entries)
            } else {
                cut_segment(seg, len, hwm)?
            };
            if meta.records == 0 {
                continue; // nothing below the high watermark (or compacted away)
            }
            earliest.get_or_insert(seg.base_offset);
            records += meta.records;
            let base = seg.base_offset;
            files.push(SnapshotFile {
                path: seg_name(base),
                data: SnapshotData::Segment {
                    segment: seg.clone(),
                    len: meta.len,
                },
            });
            files.push(SnapshotFile {
                path: idx_name(base),
                data: SnapshotData::Bytes(encode_index(&entries)),
            });
            files.push(SnapshotFile {
                path: meta_name(base),
                data: SnapshotData::Bytes(meta_json(&meta)?),
            });
        }
        // The empty active segment that pins the next offset to `hwm`.
        files.push(SnapshotFile {
            path: seg_name(hwm),
            data: SnapshotData::Bytes(header_bytes(hwm).to_vec()),
        });

        Ok(StreamSnapshot {
            name: stream.to_string(),
            earliest_offset: earliest.unwrap_or(hwm),
            next_offset: hwm,
            records,
            files,
            shared,
            truncation_epoch,
        })
    }

    /// Snapshot every stream: internal streams (`__*`: consumer state,
    /// connector offsets, query checkpoints) first, then the others, each
    /// group in name order. Taking the progress records before the data
    /// they describe means a restore can only replay (at-least-once), never
    /// skip records.
    pub fn snapshot_all(&self) -> Result<Vec<StreamSnapshot>, StorageError> {
        let mut names = self.list_streams();
        names.sort_by_key(|n| (!n.starts_with(INTERNAL_STREAM_PREFIX), n.clone()));
        names.iter().map(|s| self.snapshot_stream(s)).collect()
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn tar_header(len: u64, mode: u32) -> tar::Header {
    let mut h = tar::Header::new_gnu();
    h.set_size(len);
    h.set_mode(mode);
    h.set_entry_type(tar::EntryType::Regular);
    h.set_mtime(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    );
    h
}

/// Every regular file under `dir`, as (path relative to `root`, contents).
/// Files are read whole (config files are small and written atomically),
/// so the tar header size always matches the data. Symlinks and `*.tmp`
/// leftovers are skipped.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> io::Result<()> {
    let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let ft = e.file_type()?;
        let path = e.path();
        if ft.is_dir() {
            collect_files(root, &path, out)?;
        } else if ft.is_file() {
            if path.extension().is_some_and(|x| x == "tmp") {
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_string_lossy()
                .replace('\\', "/");
            match fs::read(&path) {
                Ok(data) => out.push((rel, data)),
                // Deleted between listing and reading.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

/// A backup whose stream snapshots have been taken but whose archive has
/// not been written yet. Holding it pins the snapshotted segments.
pub struct PreparedBackup {
    manifest: BackupManifest,
    snapshots: Vec<StreamSnapshot>,
    data_dir: PathBuf,
}

/// Take a snapshot of every stream and build the manifest. Blocking.
pub fn prepare_backup(
    storage: &FileStorage,
    server_version: &str,
) -> Result<PreparedBackup, StorageError> {
    let created_at = now_rfc3339();
    let snapshots = storage.snapshot_all()?;
    let data_dir = storage.data_dir().to_path_buf();
    let config_dirs: Vec<String> = CONFIG_DIRS
        .iter()
        .filter(|d| data_dir.join(d).is_dir())
        .map(|d| d.to_string())
        .collect();
    let manifest = BackupManifest {
        format: BACKUP_FORMAT.into(),
        version: BACKUP_VERSION,
        server_version: server_version.into(),
        created_at,
        streams: snapshots.iter().map(StreamSnapshot::manifest).collect(),
        config_dirs,
    };
    Ok(PreparedBackup {
        manifest,
        snapshots,
        data_dir,
    })
}

impl PreparedBackup {
    pub fn manifest(&self) -> &BackupManifest {
        &self.manifest
    }

    /// Write the archive: the manifest, then every stream's files, then the
    /// configuration directories (copied as they are now). Appends continue
    /// meanwhile. Blocking. On error the archive is incomplete and must be
    /// discarded.
    pub fn write_to<W: Write>(self, out: W) -> io::Result<BackupManifest> {
        let PreparedBackup {
            manifest,
            snapshots,
            data_dir,
        } = self;
        let mut tar = tar::Builder::new(out);
        let json = serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?;
        tar.append_data(
            &mut tar_header(json.len() as u64, 0o644),
            MANIFEST_NAME,
            &json[..],
        )?;

        for snap in &snapshots {
            for f in &snap.files {
                let path = format!("streams/{}/{}", snap.name, f.path);
                tar.append_data(&mut tar_header(f.len(), 0o644), &path, f.reader())?;
            }
            snap.verify().map_err(storage_io)?;
        }
        drop(snapshots);

        for dir in &manifest.config_dirs {
            let mut files = Vec::new();
            match collect_files(&data_dir, &data_dir.join(dir), &mut files) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
            for (rel, data) in files {
                tar.append_data(&mut tar_header(data.len() as u64, 0o600), &rel, &data[..])?;
            }
        }
        tar.into_inner()?.flush()?;
        Ok(manifest)
    }
}

/// [`prepare_backup`] then [`PreparedBackup::write_to`].
pub fn write_backup<W: Write>(
    storage: &FileStorage,
    server_version: &str,
    out: W,
) -> io::Result<BackupManifest> {
    prepare_backup(storage, server_version)
        .map_err(storage_io)?
        .write_to(out)
}

fn storage_io(e: StorageError) -> io::Error {
    match e {
        StorageError::Io(e) => e,
        other => io::Error::other(other.to_string()),
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Check a manifest's format and version.
pub fn validate_manifest(m: &BackupManifest) -> io::Result<()> {
    if m.format != BACKUP_FORMAT {
        return Err(invalid(format!(
            "{MANIFEST_NAME}: format is {:?}, expected {BACKUP_FORMAT:?}",
            m.format
        )));
    }
    if m.version != BACKUP_VERSION {
        return Err(invalid(format!(
            "{MANIFEST_NAME}: backup version {} is not supported by this build (expected {BACKUP_VERSION})",
            m.version
        )));
    }
    for s in &m.streams {
        StreamName::try_from(s.name.as_str()).map_err(|e| {
            invalid(format!(
                "{MANIFEST_NAME}: bad stream name {:?}: {e}",
                s.name
            ))
        })?;
        if s.earliest_offset > s.next_offset {
            return Err(invalid(format!(
                "{MANIFEST_NAME}: stream {} has earliest_offset {} above next_offset {}",
                s.name, s.earliest_offset, s.next_offset
            )));
        }
    }
    Ok(())
}

/// Options for [`restore_backup`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RestoreOptions {
    /// Restore into a data directory that already has content. Its
    /// `streams/`, [`CONFIG_DIRS`], replication state and leftovers of an
    /// interrupted restore are deleted first; other files (credentials,
    /// `exspeed.toml`) are kept.
    pub force: bool,
}

/// Names that don't count as content when checking that a data dir is
/// empty (the lock file the restoring process itself holds).
const IGNORED_IN_EMPTY_CHECK: &[&str] = &[".exspeed.lock"];

const STAGING_PREFIX: &str = ".restore-";

fn is_allowed_path(rel: &Path) -> bool {
    let mut comps = rel.components();
    let Some(Component::Normal(first)) = comps.next() else {
        return false;
    };
    let first = first.to_string_lossy();
    if !(first == "streams" || CONFIG_DIRS.contains(&first.as_ref())) {
        return false;
    }
    comps.all(|c| matches!(c, Component::Normal(_)))
}

/// Restore a backup archive into `data_dir`, which must not be in use by a
/// running server (callers should hold the data-dir lock).
///
/// The archive is unpacked into a staging directory inside `data_dir`,
/// checked (manifest first, only `streams/` and config directories, every
/// stream opens and has exactly the offsets the manifest lists), and only
/// then moved into place.
pub fn restore_backup<R: Read>(
    input: R,
    data_dir: &Path,
    opts: RestoreOptions,
) -> io::Result<BackupManifest> {
    fs::create_dir_all(data_dir)?;
    let mut existing = Vec::new();
    for e in fs::read_dir(data_dir)? {
        let name = e?.file_name().to_string_lossy().into_owned();
        if !IGNORED_IN_EMPTY_CHECK.contains(&name.as_str()) {
            existing.push(name);
        }
    }
    if !existing.is_empty() && !opts.force {
        existing.sort();
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "data directory {} is not empty ({}); restore into an empty directory or pass --force",
                data_dir.display(),
                existing.join(", ")
            ),
        ));
    }

    let staging = data_dir.join(format!(
        "{STAGING_PREFIX}{}",
        crate::file::writer::now_nanos()
    ));
    fs::create_dir(&staging)?;
    let result = unpack_and_check(input, &staging);
    let manifest = match result {
        Ok(m) => m,
        Err(e) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(e);
        }
    };

    if opts.force {
        for name in existing {
            let doomed = name == "streams"
                || name == ".trash"
                || name == "replication"
                || name.starts_with(STAGING_PREFIX)
                || CONFIG_DIRS.contains(&name.as_str());
            if doomed {
                let p = data_dir.join(&name);
                if p.is_dir() {
                    fs::remove_dir_all(&p)?;
                } else {
                    fs::remove_file(&p)?;
                }
            }
        }
    }
    for e in fs::read_dir(&staging)? {
        let e = e?;
        fs::rename(e.path(), data_dir.join(e.file_name()))?;
    }
    fs::remove_dir_all(&staging)?;
    fsync_dir(data_dir)?;
    Ok(manifest)
}

fn unpack_and_check<R: Read>(input: R, staging: &Path) -> io::Result<BackupManifest> {
    let mut archive = tar::Archive::new(input);
    let mut manifest: Option<BackupManifest> = None;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path: PathBuf = entry.path()?.into_owned();
        let Some(m) = manifest.as_ref() else {
            if path != Path::new(MANIFEST_NAME) {
                return Err(invalid(format!(
                    "not an exspeed backup: the first entry is {}, expected {MANIFEST_NAME}",
                    path.display()
                )));
            }
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            let m: BackupManifest = serde_json::from_slice(&buf)
                .map_err(|e| invalid(format!("{MANIFEST_NAME}: {e}")))?;
            validate_manifest(&m)?;
            manifest = Some(m);
            continue;
        };
        let _ = m;
        let et = entry.header().entry_type();
        if !(et.is_file() || et.is_dir()) {
            return Err(invalid(format!(
                "{}: unsupported archive entry type {:?}",
                path.display(),
                et
            )));
        }
        if !is_allowed_path(&path) {
            return Err(invalid(format!(
                "{}: unexpected path in backup archive",
                path.display()
            )));
        }
        if !entry.unpack_in(staging)? {
            return Err(invalid(format!(
                "{}: refusing to unpack outside the data directory",
                path.display()
            )));
        }
    }
    let manifest = manifest.ok_or_else(|| invalid("empty archive: no manifest"))?;

    // Every stream opens and has exactly the offsets the manifest lists.
    let streams_dir = staging.join("streams");
    fs::create_dir_all(&streams_dir)?;
    let storage = FileStorage::open_with_options(
        staging,
        StorageOptions {
            compaction_interval: std::time::Duration::ZERO,
            ..StorageOptions::default()
        },
    )?;
    let check = (|| {
        let expected: BTreeMap<&str, &StreamManifest> = manifest
            .streams
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();
        let found = storage.list_streams();
        let found_set: Vec<&str> = found.iter().map(String::as_str).collect();
        let expected_set: Vec<&str> = expected.keys().copied().collect();
        if found_set != expected_set {
            return Err(invalid(format!(
                "archive streams {found_set:?} do not match the manifest {expected_set:?}"
            )));
        }
        for (name, s) in &expected {
            let h = storage
                .handle_by_name(name)
                .ok_or_else(|| invalid(format!("stream {name} missing")))?;
            let (earliest, next) = (h.shared.earliest(), h.shared.high_watermark());
            if (earliest, next) != (s.earliest_offset, s.next_offset) {
                return Err(invalid(format!(
                    "stream {name}: restored offsets [{earliest}, {next}) do not match the manifest [{}, {})",
                    s.earliest_offset, s.next_offset
                )));
            }
        }
        Ok(())
    })();
    storage.close();
    drop(storage);
    check?;
    Ok(manifest)
}

/// Read only the manifest of a backup archive (its first entry).
pub fn read_manifest<R: Read>(input: R) -> io::Result<BackupManifest> {
    let mut archive = tar::Archive::new(input);
    let mut entries = archive.entries()?;
    let mut entry = entries
        .next()
        .ok_or_else(|| invalid("empty archive: no manifest"))??;
    if entry.path()?.as_ref() != Path::new(MANIFEST_NAME) {
        return Err(invalid(format!(
            "not an exspeed backup: the first entry is not {MANIFEST_NAME}"
        )));
    }
    let mut buf = Vec::new();
    entry.read_to_end(&mut buf)?;
    let m: BackupManifest =
        serde_json::from_slice(&buf).map_err(|e| invalid(format!("{MANIFEST_NAME}: {e}")))?;
    validate_manifest(&m)?;
    Ok(m)
}
