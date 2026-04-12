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
    use bytes::Bytes;
    use exspeed_streams::record::Record;
    use tempfile::TempDir;

    fn make_record() -> Record {
        Record {
            key: None,
            value: Bytes::from_static(b"hello segment"),
            subject: "test.subject".to_string(),
            headers: vec![],
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
}
