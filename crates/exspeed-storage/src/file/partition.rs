// Built in Task 8, updated in Task 4 (Phase 3)

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use exspeed_common::Offset;
use exspeed_streams::record::{Record, StoredRecord};
use tracing::info;

use crate::file::offset_index::OffsetIndex;
use crate::file::segment_reader::SegmentReader;
use crate::file::segment_writer::SegmentWriter;
use crate::file::time_index::{self, TimeIndex};
use crate::file::wal::{replay_wal, WalWriter};

/// Default maximum segment size before rolling: 256 MiB.
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Manages a single partition's on-disk state: active segment writer,
/// sealed segment readers, and the write-ahead log.
pub struct Partition {
    dir: PathBuf,
    active_writer: SegmentWriter,
    sealed_readers: Vec<SegmentReader>,
    wal: WalWriter,
    next_offset: u64,
    stream_name: String,
    partition_id: u32,
    segment_max_bytes: u64,
}

impl Partition {
    /// Create a brand-new partition directory with its first segment and WAL.
    pub fn create(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let writer = SegmentWriter::create(dir, 0)?;
        let wal = WalWriter::open(&dir.join("wal.log"))?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer: writer,
            sealed_readers: Vec::new(),
            wal,
            next_offset: 0,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
        })
    }

    /// Open an existing partition directory, recovering state from segment
    /// files and replaying the WAL.
    pub fn open(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        // Find all .seg files, sorted by name (which sorts by base_offset
        // due to zero-padded filenames).
        let mut seg_paths: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("seg") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        seg_paths.sort();

        if seg_paths.is_empty() {
            // No segments exist — treat as a fresh partition.
            return Self::create(dir, stream_name, partition_id);
        }

        // Open all but the last as sealed readers.
        let mut sealed_readers = Vec::new();
        for path in &seg_paths[..seg_paths.len() - 1] {
            sealed_readers.push(SegmentReader::open(path)?);
        }

        // Read metadata from the last segment to derive base_offset and next_offset.
        let last_seg_path = &seg_paths[seg_paths.len() - 1];
        let last_reader = SegmentReader::open(last_seg_path)?;
        let base_offset = last_reader.base_offset();

        // Derive next_offset from the last offset across all segments.
        let mut next_offset: u64 = 0;
        for reader in &sealed_readers {
            if let Some(last) = reader.last_offset()? {
                next_offset = next_offset.max(last + 1);
            }
        }
        if let Some(last) = last_reader.last_offset()? {
            next_offset = next_offset.max(last + 1);
        }

        // Open the WAL and replay any records that weren't flushed to segments.
        let wal_path = dir.join("wal.log");
        let wal_records = replay_wal(&wal_path)?;

        let mut wal = WalWriter::open(&wal_path)?;

        // Open the last segment as the active writer and replay WAL records
        // that are not yet in any segment.
        let replay_base = next_offset;
        let current_size = fs::metadata(last_seg_path)?.len();
        let mut active_writer =
            SegmentWriter::open_append(last_seg_path, base_offset, current_size)?;

        let mut replayed = 0usize;
        for wal_rec in &wal_records {
            if wal_rec.offset.0 >= replay_base {
                active_writer.append(wal_rec.offset, wal_rec.timestamp, &wal_rec.record)?;
                next_offset = wal_rec.offset.0 + 1;
                replayed += 1;
            }
        }

        if replayed > 0 {
            active_writer.sync()?;
            info!(
                stream = stream_name,
                partition = partition_id,
                replayed,
                "WAL replay complete"
            );
        }

        // Truncate the WAL now that all records are in segments.
        wal.truncate()?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer,
            sealed_readers,
            wal,
            next_offset,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
        })
    }

    /// Append a record to this partition.
    ///
    /// The record is first written to the WAL (and synced), then to the
    /// active segment. If the segment exceeds `segment_max_bytes` after the
    /// write, a new segment is rolled.
    pub fn append(&mut self, record: &Record) -> io::Result<Offset> {
        let offset = Offset(self.next_offset);
        let timestamp = now_nanos();

        // WAL first — durable after sync_data inside WalWriter::append.
        self.wal.append(
            &self.stream_name,
            self.partition_id,
            offset,
            timestamp,
            record,
        )?;

        // Then segment.
        self.active_writer.append(offset, timestamp, record)?;
        self.next_offset += 1;

        // Check if we need to roll the segment.
        if self.active_writer.bytes_written() >= self.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(offset)
    }

    /// Read records from this partition starting at `from_offset`.
    ///
    /// Reads from sealed segments first, then from the active segment.
    /// The active writer is synced before reading so that recently appended
    /// data is visible.
    pub fn read(&self, from: Offset, max_records: usize) -> io::Result<Vec<StoredRecord>> {
        let mut result = Vec::new();
        let mut remaining = max_records;

        // Read from sealed segments (they are ordered by base_offset).
        for reader in &self.sealed_readers {
            if remaining == 0 {
                break;
            }
            let records = reader.read_from(from.0, remaining)?;
            remaining -= records.len();
            result.extend(records);
        }

        // Read from the active segment.
        if remaining > 0 {
            // Sync so that buffered writes are visible to a new reader.
            self.active_writer.sync()?;

            let active_reader = SegmentReader::open(self.active_writer.path())?;
            let records = active_reader.read_from(from.0, remaining)?;
            result.extend(records);
        }

        Ok(result)
    }

    /// Roll the active segment: seal it, build indexes, open a reader for it,
    /// and create a new active segment starting at `next_offset`.
    fn roll_segment(&mut self) -> io::Result<()> {
        // Sync the current active writer.
        self.active_writer.sync()?;

        // Build indexes for the sealed segment.
        let seg_path = self.active_writer.path().to_path_buf();
        self.build_indexes(&seg_path)?;

        // Open the current segment as a sealed reader (will load the new index files).
        let sealed = SegmentReader::open(&seg_path)?;
        self.sealed_readers.push(sealed);

        // Create a new segment.
        self.active_writer = SegmentWriter::create(&self.dir, self.next_offset)?;

        // Truncate the WAL — all records prior to this point are in sealed segments.
        self.wal.truncate()?;

        Ok(())
    }

    /// Scan the segment at `seg_path` and build companion `.idx` and `.tix`
    /// index files alongside it.
    fn build_indexes(&self, seg_path: &Path) -> io::Result<()> {
        // Open a temporary reader to scan the data. Indexes don't exist yet,
        // so offset_index and time_index will be None — that's fine, we only
        // need sequential scanning here.
        let reader = SegmentReader::open(seg_path)?;
        let index_data = reader.scan_for_index_data()?;

        if index_data.is_empty() {
            return Ok(());
        }

        // Build offset index (.idx).
        let offset_entries: Vec<(u64, u32)> = index_data
            .iter()
            .map(|&(offset, file_pos, _)| (offset, file_pos))
            .collect();
        let idx_path = seg_path.with_extension("idx");
        OffsetIndex::build(&idx_path, &offset_entries)?;

        // Build timestamp index (.tix).
        let time_entries: Vec<(u64, u64)> = index_data
            .iter()
            .map(|&(offset, _, timestamp)| (timestamp, offset))
            .collect();
        let tix_path = seg_path.with_extension("tix");
        TimeIndex::build(&tix_path, &time_entries, time_index::DEFAULT_INTERVAL)?;

        Ok(())
    }
}
