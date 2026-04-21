// Built in Task 5

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use exspeed_common::Offset;
use exspeed_streams::record::Record;

use crate::encoding::{encode_record, wrap_with_crc};

/// Four-byte magic number at the start of every segment file.
pub const SEGMENT_MAGIC: &[u8; 4] = b"EXSG";

/// Segment file format version.
pub const SEGMENT_VERSION: u8 = 0x01;

/// Size of the fixed segment file header in bytes.
pub const SEGMENT_HEADER_SIZE: usize = 16;

/// Append-only writer for a single segment file.
///
/// Segment file header layout (16 bytes, all integers little-endian):
///   magic        [u8; 4]   "EXSG"
///   version      u8        0x01
///   base_offset  u64 LE
///   reserved     [u8; 3]   zero bytes
pub struct SegmentWriter {
    file: File,
    path: PathBuf,
    base_offset: u64,
    bytes_written: u64,
    record_count: u64,
}

impl SegmentWriter {
    /// Create a new segment file in `dir` with the given `base_offset`.
    ///
    /// The filename is `{base_offset:020}.seg`. The 16-byte header is written
    /// immediately so `bytes_written` starts at `SEGMENT_HEADER_SIZE`.
    pub fn create(dir: &Path, base_offset: u64) -> io::Result<Self> {
        let filename = format!("{:020}.seg", base_offset);
        let path = dir.join(&filename);

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let header = Self::build_header(base_offset);
        file.write_all(&header)?;

        Ok(Self {
            file,
            path,
            base_offset,
            bytes_written: SEGMENT_HEADER_SIZE as u64,
            record_count: 0,
        })
    }

    /// Open an existing segment file for appending.
    ///
    /// `current_size` must be the current byte length of the file so that
    /// `bytes_written` is initialised correctly without re-scanning the file.
    pub fn open_append(path: &Path, base_offset: u64, current_size: u64) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).read(true).open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            base_offset,
            bytes_written: current_size,
            record_count: 0,
        })
    }

    /// Encode `record`, wrap with a CRC32C frame, and append to the file.
    ///
    /// Returns the number of bytes written for this record (the framed size).
    pub fn append(&mut self, offset: Offset, timestamp: u64, record: &Record) -> io::Result<u64> {
        let mut encoded = Vec::new();
        encode_record(offset, timestamp, record, &mut encoded);

        let framed = wrap_with_crc(&encoded);
        let n = framed.len() as u64;

        self.file.write_all(&framed)?;
        self.bytes_written += n;
        self.record_count += 1;

        Ok(n)
    }

    /// Flush OS-level buffers for data (metadata not guaranteed).
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Append N records in one `write_all`. Performs `sync_data` once at the
    /// end IFF `sync_now` is true. Used by:
    /// - `Partition::append_batch` in sync mode (`sync_now = true`).
    /// - `SegmentAppender`'s async mode (`sync_now = false`; `SegmentSyncer`
    ///   handles the periodic fsync on a cloned file handle).
    pub fn append_batch(
        &mut self,
        records: &[(Offset, u64, Record)],
        sync_now: bool,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        // Best-effort pre-size: ~64 bytes per record including framing.
        let mut combined: Vec<u8> = Vec::with_capacity(records.len() * 64);
        let mut payload: Vec<u8> = Vec::new();

        for (offset, timestamp, record) in records {
            payload.clear();
            encode_record(*offset, *timestamp, record, &mut payload);
            let framed = wrap_with_crc(&payload);
            combined.extend_from_slice(&framed);
        }

        let n = combined.len() as u64;
        self.file.write_all(&combined)?;
        self.bytes_written += n;
        self.record_count += records.len() as u64;
        if sync_now {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// Clone the underlying `File` handle so a separate task can issue
    /// `sync_data` on the same kernel fd without holding the `Partition`
    /// mutex. Used by `SegmentSyncer` in async storage mode.
    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    /// Force a `sync_data` on the segment file. Used by `SegmentSyncer` (via
    /// its cloned handle) or directly by `Partition` when it needs to commit
    /// the current batch in sync mode.
    pub fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Total bytes written to the file so far (including the 16-byte header).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// The base offset this segment was created with.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// The filesystem path of the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Scan an existing segment file from `SEGMENT_HEADER_SIZE` forward,
    /// validating each CRC-framed record. Stops at the first torn or corrupt
    /// frame and truncates the file to the last known-good boundary.
    ///
    /// Returns `(max_offset_seen, max_timestamp_seen, final_file_size)`. If
    /// the segment contains no records (empty past the header), both max
    /// values are `None` and `final_file_size` is `SEGMENT_HEADER_SIZE`.
    ///
    /// Used by `Partition::open` as the unified recovery primitive. Replaces
    /// the separate WAL replay path.
    pub fn recover_tail(path: &Path) -> io::Result<(Option<u64>, Option<u64>, u64)> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let total_size = file.metadata()?.len();

        if total_size <= SEGMENT_HEADER_SIZE as u64 {
            return Ok((None, None, SEGMENT_HEADER_SIZE as u64));
        }

        file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

        let mut max_offset: Option<u64> = None;
        let mut max_timestamp: Option<u64> = None;
        let mut last_good_pos: u64 = SEGMENT_HEADER_SIZE as u64;

        loop {
            let pos = file.stream_position()?;
            if pos >= total_size {
                break;
            }

            // Read length prefix (u32 LE).
            let remaining = total_size - pos;
            if remaining < 4 {
                // Torn: partial length prefix.
                break;
            }
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let frame_len = u32::from_le_bytes(len_buf) as u64;

            // frame_len includes the 4-byte CRC + payload. We already read the length prefix.
            let after_len = pos + 4;
            if total_size - after_len < frame_len {
                // Torn: payload truncated.
                break;
            }

            // Read CRC (u32 LE) + payload.
            let payload_len = frame_len - 4;
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);

            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)?;

            let computed_crc = crc32c::crc32c(&payload);
            if computed_crc != stored_crc {
                // Corrupt: CRC mismatch. Treat as end of valid data.
                break;
            }

            // Payload is: offset(u64 LE) + timestamp(u64 LE) + encoded_record_bytes
            if payload_len < 16 {
                // Malformed: too short to contain offset + timestamp.
                break;
            }
            let offset = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let timestamp = u64::from_le_bytes(payload[8..16].try_into().unwrap());

            max_offset = Some(max_offset.map_or(offset, |m| m.max(offset)));
            max_timestamp = Some(max_timestamp.map_or(timestamp, |m| m.max(timestamp)));
            last_good_pos = pos + 4 + frame_len; // advance past this whole frame
        }

        // Truncate any torn/corrupt tail past the last good frame.
        if last_good_pos < total_size {
            file.set_len(last_good_pos)?;
        }
        Ok((max_offset, max_timestamp, last_good_pos))
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    fn build_header(base_offset: u64) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        // bytes 0-3: magic
        header[0..4].copy_from_slice(SEGMENT_MAGIC);
        // byte 4: version
        header[4] = SEGMENT_VERSION;
        // bytes 5-12: base_offset (u64 LE)
        header[5..13].copy_from_slice(&base_offset.to_le_bytes());
        // bytes 13-15: reserved (already zero)
        header
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        // Best-effort: ignore errors during drop.
        let _ = self.file.sync_all();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::segment_reader::SegmentReader;
    use bytes::Bytes;
    use exspeed_streams::record::Record;
    use tempfile::TempDir;

    fn make_record() -> Record {
        Record {
            key: None,
            value: Bytes::from_static(b"hello segment"),
            subject: "test.subject".to_string(),
            headers: vec![],
            timestamp_ns: None,
        }
    }

    #[test]
    fn create_segment_writes_header() {
        let dir = TempDir::new().unwrap();
        let base_offset: u64 = 0;

        let writer = SegmentWriter::create(dir.path(), base_offset).unwrap();

        // File must exist.
        assert!(writer.path().exists(), "segment file should exist on disk");

        // bytes_written must equal the header size.
        assert_eq!(
            writer.bytes_written(),
            SEGMENT_HEADER_SIZE as u64,
            "bytes_written should equal SEGMENT_HEADER_SIZE after create"
        );

        // Filename must embed the base offset.
        let filename = writer
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(
            filename.contains(&format!("{:020}", base_offset)),
            "filename '{}' should contain zero-padded base offset",
            filename
        );
    }

    #[test]
    fn append_record_increases_size() {
        let dir = TempDir::new().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), 0).unwrap();

        let before = writer.bytes_written();
        let written = writer
            .append(Offset(0), 1_700_000_000, &make_record())
            .unwrap();
        let after = writer.bytes_written();

        assert!(written > 0, "append should report non-zero bytes written");
        assert_eq!(
            after,
            before + written,
            "bytes_written should increase by the framed record size"
        );
        assert!(
            after > before,
            "total bytes_written should be larger after append"
        );
    }

    #[test]
    fn segment_filename_includes_base_offset() {
        let dir = TempDir::new().unwrap();
        let base_offset: u64 = 100_000;

        let writer = SegmentWriter::create(dir.path(), base_offset).unwrap();

        let filename = writer
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        // The zero-padded representation of 100_000 is "00000000000000100000".
        let expected_part = format!("{:020}", base_offset);
        assert!(
            filename.contains(&expected_part),
            "filename '{}' should contain '{}'",
            filename,
            expected_part
        );
        assert!(
            filename.ends_with(".seg"),
            "filename '{}' should end with .seg",
            filename
        );
    }

    #[test]
    fn append_batch_writes_all_records_with_one_sync() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();

        let rec = Record {
            subject: "s".into(),
            key: None,
            value: Bytes::from_static(b"v"),
            headers: vec![],
            timestamp_ns: None,
        };
        let entries: Vec<(Offset, u64, Record)> = (0..5)
            .map(|i| (Offset(i), 1000 + i, rec.clone()))
            .collect();

        writer.append_batch(&entries, /*sync_now=*/ true).unwrap();
        assert!(writer.bytes_written() > 5 * 8); // at least 5 framed records
        drop(writer);

        // Verify round-trip via SegmentReader.
        let seg_path = tmp.path().join("00000000000000000000.seg");
        let reader = SegmentReader::open(&seg_path).unwrap();
        let records = reader.read_from(0, 100).unwrap();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.offset.0, i as u64);
            assert_eq!(r.timestamp, 1000 + i as u64);
        }
    }

    #[test]
    fn append_batch_empty_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let before = writer.bytes_written();
        writer.append_batch(&[], false).unwrap();
        assert_eq!(writer.bytes_written(), before);
    }

    #[test]
    fn try_clone_file_returns_independent_handle() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;

        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let cloned = writer.try_clone_file().unwrap();

        // Both handles refer to the same kernel fd. Writing through the writer
        // and sync'ing through the clone should flush the write.
        let rec = Record {
            subject: "s".into(), key: None, value: Bytes::from_static(b"v"),
            headers: vec![], timestamp_ns: None,
        };
        writer.append(Offset(0), 1, &rec).unwrap();
        cloned.sync_data().unwrap();

        // Prove the clone actually points at the same inode AND that sync through
        // the clone flushed the writer's buffered bytes: on-disk size must exceed
        // just the header.
        let on_disk_len = std::fs::metadata(writer.path()).unwrap().len();
        assert!(
            on_disk_len > SEGMENT_HEADER_SIZE as u64,
            "on-disk length {} should exceed SEGMENT_HEADER_SIZE ({}) after sync via clone",
            on_disk_len,
            SEGMENT_HEADER_SIZE,
        );
    }

    #[test]
    fn sync_data_method_flushes_pending_write() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;

        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let rec = Record {
            subject: "s".into(), key: None, value: Bytes::from_static(b"hello"),
            headers: vec![], timestamp_ns: None,
        };
        writer.append(Offset(0), 100, &rec).unwrap();
        writer.sync_data().unwrap();

        // After sync_data the bytes written through `writer` must be visible on
        // disk — the file's metadata length should be at least bytes_written().
        let on_disk_len = std::fs::metadata(writer.path()).unwrap().len();
        assert!(
            on_disk_len >= writer.bytes_written(),
            "on-disk length {} should be >= bytes_written {} after sync_data",
            on_disk_len,
            writer.bytes_written(),
        );
    }

    #[test]
    fn recover_tail_on_empty_segment_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let _writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let seg_path = tmp.path().join("00000000000000000000.seg");
        let (max_offset, max_ts, _file_size) = SegmentWriter::recover_tail(&seg_path).unwrap();
        assert_eq!(max_offset, None);
        assert_eq!(max_ts, None);
    }

    #[test]
    fn recover_tail_walks_valid_records() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let rec = Record {
            subject: "s".into(), key: None, value: Bytes::from_static(b"v"),
            headers: vec![], timestamp_ns: None,
        };
        for i in 0..10 {
            writer.append(Offset(i), 2000 + i, &rec).unwrap();
        }
        writer.sync_data().unwrap();
        drop(writer);

        let seg_path = tmp.path().join("00000000000000000000.seg");
        let (max_offset, max_ts, _size) = SegmentWriter::recover_tail(&seg_path).unwrap();
        assert_eq!(max_offset, Some(9));
        assert_eq!(max_ts, Some(2009));
    }

    #[test]
    fn recover_tail_truncates_trailing_partial_length() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;
        use std::fs::OpenOptions;
        use std::io::Write;
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let rec = Record {
            subject: "s".into(), key: None, value: Bytes::from_static(b"v"),
            headers: vec![], timestamp_ns: None,
        };
        for i in 0..5 {
            writer.append(Offset(i), 3000 + i, &rec).unwrap();
        }
        writer.sync_data().unwrap();
        drop(writer);

        // Append 2 garbage bytes (incomplete length prefix).
        let seg_path = tmp.path().join("00000000000000000000.seg");
        let size_before = std::fs::metadata(&seg_path).unwrap().len();
        let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
        f.write_all(&[0xFF, 0xFF]).unwrap();
        drop(f);

        let (max_offset, _ts, size_after) = SegmentWriter::recover_tail(&seg_path).unwrap();
        assert_eq!(max_offset, Some(4));
        assert_eq!(size_after, size_before, "file truncated back to last valid record");
        assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), size_before);
    }

    #[test]
    fn recover_tail_truncates_bad_crc_mid_record() {
        use bytes::Bytes;
        use exspeed_streams::record::Record;
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write, Read};
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();
        let rec = Record {
            subject: "s".into(), key: None, value: Bytes::from_static(b"v"),
            headers: vec![], timestamp_ns: None,
        };
        for i in 0..3 {
            writer.append(Offset(i), 4000 + i, &rec).unwrap();
        }
        writer.sync_data().unwrap();
        let good_size = writer.bytes_written();
        drop(writer);

        // Append a record whose body we then corrupt.
        let mut writer = SegmentWriter::open_append(
            &tmp.path().join("00000000000000000000.seg"),
            0,
            good_size,
        ).unwrap();
        writer.append(Offset(3), 4003, &rec).unwrap();
        writer.sync_data().unwrap();
        drop(writer);

        // Flip the last payload byte to break the CRC.
        let seg_path = tmp.path().join("00000000000000000000.seg");
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&seg_path).unwrap();
            f.seek(SeekFrom::End(-1)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            f.seek(SeekFrom::End(-1)).unwrap();
            f.write_all(&[byte[0] ^ 0x01]).unwrap();
        }

        let (max_offset, _ts, size_after) = SegmentWriter::recover_tail(&seg_path).unwrap();
        assert_eq!(max_offset, Some(2), "CRC-failed record discarded, only 0..3 valid");
        assert_eq!(size_after, good_size as u64, "file truncated to pre-corruption length");
    }

    #[test]
    fn append_batch_without_sync_is_persisted_after_drop() {
        use bytes::Bytes;
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let mut writer = SegmentWriter::create(tmp.path(), 0).unwrap();

        let rec = Record {
            subject: "s".into(),
            key: None,
            value: Bytes::from_static(b"v"),
            headers: vec![],
            timestamp_ns: None,
        };
        let entries: Vec<(Offset, u64, Record)> = (0..5)
            .map(|i| (Offset(i), 1000 + i, rec.clone()))
            .collect();

        // sync_now = false: async-syncer path. Durability should come from the
        // Drop impl's best-effort sync_all.
        writer.append_batch(&entries, /*sync_now=*/ false).unwrap();
        drop(writer);

        // Round-trip via SegmentReader to confirm all records reached disk.
        let seg_path = tmp.path().join("00000000000000000000.seg");
        let reader = SegmentReader::open(&seg_path).unwrap();
        let records = reader.read_from(0, 100).unwrap();
        assert_eq!(records.len(), 5, "all 5 records should be durable after drop");
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.offset.0, i as u64);
            assert_eq!(r.timestamp, 1000 + i as u64);
        }
    }
}
