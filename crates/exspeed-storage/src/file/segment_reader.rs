// Built in Task 6, updated in Task 4 (Phase 3)

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use exspeed_streams::record::StoredRecord;

use crate::encoding::{decode_record, unwrap_crc};
use crate::file::offset_index::OffsetIndex;
use crate::file::segment_writer::{SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
use crate::file::time_index::TimeIndex;

/// Read-only accessor for a sealed segment file.
///
/// A new `File` handle is opened for each read operation so the reader
/// can be used safely across threads without holding a persistent handle.
///
/// When companion index files (`.idx`, `.tix`) exist alongside the segment,
/// they are loaded automatically and used to accelerate lookups. Segments
/// without index files fall back to sequential scanning.
#[derive(Debug, Clone)]
pub struct SegmentReader {
    path: PathBuf,
    base_offset: u64,
    file_size: u64,
    offset_index: Option<OffsetIndex>,
    time_index: Option<TimeIndex>,
}

impl SegmentReader {
    /// Open an existing segment file, validate the header, and return a reader.
    ///
    /// Returns an error if the file cannot be opened, if the magic bytes do not
    /// match `SEGMENT_MAGIC`, or if the version byte is not `SEGMENT_VERSION`.
    ///
    /// Companion index files are loaded on a best-effort basis:
    /// - `.idx` (offset index) — enables O(1) offset lookups
    /// - `.tix` (time index)   — enables seek-by-timestamp
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        // Read the 16-byte header.
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut header)?;

        // Validate magic bytes (bytes 0-3).
        if &header[0..4] != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid segment magic: expected {:?}, got {:?}",
                    SEGMENT_MAGIC,
                    &header[0..4]
                ),
            ));
        }

        // Validate version (byte 4).
        if header[4] != SEGMENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported segment version: expected {:#04x}, got {:#04x}",
                    SEGMENT_VERSION, header[4]
                ),
            ));
        }

        // Read base_offset from bytes 5-12 (u64 LE).
        let base_offset = u64::from_le_bytes(header[5..13].try_into().unwrap());

        // Try to load companion index files (graceful fallback to None).
        let idx_path = path.with_extension("idx");
        let offset_index = OffsetIndex::load(&idx_path).ok();

        let tix_path = path.with_extension("tix");
        let time_index = TimeIndex::load(&tix_path).ok();

        Ok(Self {
            path: path.to_path_buf(),
            base_offset,
            file_size,
            offset_index,
            time_index,
        })
    }

    /// The base offset this segment was created with.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Get the file size of this segment.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Seek by timestamp using the time index. Returns approximate offset.
    pub fn seek_by_time(&self, timestamp: u64) -> Option<u64> {
        self.time_index.as_ref()?.seek_by_time(timestamp)
    }

    /// Get the first timestamp from the time index.
    pub fn first_timestamp(&self) -> Option<u64> {
        self.time_index.as_ref()?.first_timestamp()
    }

    /// Get the last timestamp from the time index.
    pub fn last_timestamp(&self) -> Option<u64> {
        self.time_index.as_ref()?.last_timestamp()
    }

    /// The filesystem path of this segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read all records from the segment file sequentially.
    ///
    /// Stops cleanly on EOF. Returns an error on CRC mismatch or decode
    /// failure.
    pub fn read_all(&self) -> io::Result<Vec<StoredRecord>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

        let mut records = Vec::new();
        let mut pos = SEGMENT_HEADER_SIZE as u64;

        loop {
            match self.read_one_record(&mut file, pos) {
                Ok((record, consumed)) => {
                    pos += consumed as u64;
                    records.push(record);
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Read records from `from_offset` (inclusive), returning at most
    /// `max_records` results.
    ///
    /// When an offset index is available, seeks directly to the target offset's
    /// file position instead of scanning the entire segment.
    pub fn read_from(&self, from_offset: u64, max_records: usize) -> io::Result<Vec<StoredRecord>> {
        if let Some(ref idx) = self.offset_index {
            if let Some(file_pos) = idx.lookup(from_offset) {
                // Fast path: jump directly to the record's file position.
                return self.read_from_file_position(file_pos as u64, from_offset, max_records);
            }
        }

        // Fallback: sequential read from the start, stopping after max_records.
        self.read_from_file_position(SEGMENT_HEADER_SIZE as u64, from_offset, max_records)
    }

    /// Return the offset of the last record in the segment, or `None` if the
    /// segment contains no records.
    pub fn last_offset(&self) -> io::Result<Option<u64>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

        let mut last: Option<u64> = None;
        let mut pos = SEGMENT_HEADER_SIZE as u64;

        loop {
            match self.read_one_record(&mut file, pos) {
                Ok((record, consumed)) => {
                    last = Some(record.offset.0);
                    pos += consumed as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(last)
    }

    /// Scan a segment sequentially, collecting data needed for index building.
    /// Returns Vec of (offset, file_position, timestamp) for each record.
    pub fn scan_for_index_data(&self) -> io::Result<Vec<(u64, u32, u64)>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

        let mut entries = Vec::new();
        let mut pos = SEGMENT_HEADER_SIZE as u64;

        loop {
            let file_pos = pos as u32;
            match self.read_one_record(&mut file, pos) {
                Ok((record, consumed)) => {
                    entries.push((record.offset.0, file_pos, record.timestamp));
                    pos += consumed as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Read records starting from `file_pos` in the segment file, collecting
    /// only those with offset >= `from_offset`, up to `max_records`.
    fn read_from_file_position(
        &self,
        file_pos: u64,
        from_offset: u64,
        max_records: usize,
    ) -> io::Result<Vec<StoredRecord>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(file_pos))?;

        let mut records = Vec::new();
        let mut pos = file_pos;

        loop {
            if records.len() >= max_records {
                break;
            }
            match self.read_one_record(&mut file, pos) {
                Ok((record, consumed)) => {
                    pos += consumed as u64;
                    if record.offset.0 >= from_offset {
                        records.push(record);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Read a single framed record from `file` starting at byte position `pos`.
    ///
    /// Frame layout on disk:
    ///   length     u32 LE   (= 4 [CRC] + record_bytes.len())
    ///   crc        u32 LE
    ///   record_bytes
    ///
    /// Returns `(record, total_bytes_consumed)` where `total_bytes_consumed`
    /// includes the 4-byte length prefix.
    ///
    /// Returns `UnexpectedEof` when the length field cannot be read (signals
    /// end of segment to the caller).
    fn read_one_record(&self, file: &mut File, _pos: u64) -> io::Result<(StoredRecord, usize)> {
        // Read the 4-byte length field.
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read the payload (CRC + record bytes).
        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)?;

        // Validate CRC and strip the 4-byte CRC prefix.
        let record_bytes = unwrap_crc(&payload).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC validation failed: {}", e),
            )
        })?;

        // Decode the record.
        let (record, _) = decode_record(record_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("record decode failed: {}", e),
            )
        })?;

        // Total bytes consumed = 4 (length field) + payload_len.
        let total = 4 + payload_len;
        Ok((record, total))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_common::Offset;
    use exspeed_streams::record::Record;
    use tempfile::TempDir;

    use crate::file::segment_writer::SegmentWriter;

    fn make_record(value: &'static [u8]) -> Record {
        Record {
            key: None,
            value: Bytes::from_static(value),
            subject: "test.subject".to_string(),
            headers: vec![],
            timestamp_ns: None,
        }
    }

    #[test]
    fn write_then_read_back() {
        let dir = TempDir::new().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), 0).unwrap();

        writer
            .append(Offset(0), 1_000, &make_record(b"value-0"))
            .unwrap();
        writer
            .append(Offset(1), 2_000, &make_record(b"value-1"))
            .unwrap();
        writer
            .append(Offset(2), 3_000, &make_record(b"value-2"))
            .unwrap();

        let path = writer.path().to_path_buf();
        drop(writer);

        let reader = SegmentReader::open(&path).unwrap();
        let records = reader.read_all().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset.0, 0);
        assert_eq!(records[0].value, Bytes::from_static(b"value-0"));
        assert_eq!(records[1].offset.0, 1);
        assert_eq!(records[1].value, Bytes::from_static(b"value-1"));
        assert_eq!(records[2].offset.0, 2);
        assert_eq!(records[2].value, Bytes::from_static(b"value-2"));
    }

    #[test]
    fn read_from_offset() {
        let dir = TempDir::new().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), 0).unwrap();

        for i in 0u64..5 {
            writer
                .append(Offset(i), i * 1000, &make_record(b"data"))
                .unwrap();
        }

        let path = writer.path().to_path_buf();
        drop(writer);

        let reader = SegmentReader::open(&path).unwrap();
        let records = reader.read_from(2, 2).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset.0, 2);
        assert_eq!(records[1].offset.0, 3);
    }

    #[test]
    fn last_offset_returns_highest() {
        let dir = TempDir::new().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), 0).unwrap();

        writer
            .append(Offset(0), 1_000, &make_record(b"first"))
            .unwrap();
        writer
            .append(Offset(1), 2_000, &make_record(b"second"))
            .unwrap();

        let path = writer.path().to_path_buf();
        drop(writer);

        let reader = SegmentReader::open(&path).unwrap();
        let last = reader.last_offset().unwrap();

        assert_eq!(last, Some(1));
    }

    #[test]
    fn empty_segment_has_no_last_offset() {
        let dir = TempDir::new().unwrap();
        let writer = SegmentWriter::create(dir.path(), 0).unwrap();
        let path = writer.path().to_path_buf();
        drop(writer);

        let reader = SegmentReader::open(&path).unwrap();
        let last = reader.last_offset().unwrap();

        assert_eq!(last, None);
    }

    #[test]
    fn crc_corruption_detected() {
        let dir = TempDir::new().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), 0).unwrap();
        writer
            .append(Offset(0), 1_000, &make_record(b"integrity"))
            .unwrap();
        let path = writer.path().to_path_buf();
        drop(writer);

        // Read file bytes, flip a byte in the record area (after the 16-byte header).
        let mut bytes = std::fs::read(&path).unwrap();
        let corrupt_idx = bytes.len() - 1;
        bytes[corrupt_idx] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let err = reader.read_all().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("CRC"),
            "expected error message to contain 'CRC', got: {}",
            msg
        );
    }

    #[test]
    fn invalid_magic_rejected() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("bad.seg");
        std::fs::write(
            &file_path,
            b"BADMAGIC this is not a valid segment file at all",
        )
        .unwrap();

        let err = SegmentReader::open(&file_path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("magic"),
            "expected error message to contain 'magic', got: {}",
            msg
        );
    }
}
