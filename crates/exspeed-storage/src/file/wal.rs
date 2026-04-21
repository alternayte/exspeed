// Built in Task 7

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::BufMut;
use exspeed_common::Offset;
use exspeed_streams::record::{Record, StoredRecord};
use tracing::warn;

use crate::encoding::{decode_record, encode_record, unwrap_crc, wrap_with_crc};

/// A single record recovered from the WAL during replay.
pub struct WalRecord {
    pub stream: String,
    pub partition: u32,
    pub offset: Offset,
    pub timestamp: u64,
    pub record: Record,
}

/// Append-only writer for the write-ahead log.
///
/// WAL record format on disk (all integers little-endian):
///   length       u32 LE   (= 4 [CRC] + payload.len())
///   crc          u32 LE
///   payload:
///     stream_len u16 LE
///     stream     [u8; stream_len]  (UTF-8)
///     partition  u32 LE
///     record_bytes  (same encoding as segment records)
pub struct WalWriter {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl WalWriter {
    /// Open or create the WAL file at `path` in append mode.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Encode and append a WAL entry, then call `sync_data`.
    pub fn append(
        &mut self,
        stream: &str,
        partition: u32,
        offset: Offset,
        timestamp: u64,
        record: &Record,
    ) -> io::Result<()> {
        // Build payload: stream (u16 len + UTF-8) + partition (u32) + encoded record bytes.
        let stream_bytes = stream.as_bytes();
        let mut payload = Vec::new();
        payload.put_u16_le(stream_bytes.len() as u16);
        payload.put_slice(stream_bytes);
        payload.put_u32_le(partition);
        encode_record(offset, timestamp, record, &mut payload);

        // Wrap with CRC frame (length + CRC + payload) and write atomically.
        let framed = wrap_with_crc(&payload);
        self.file.write_all(&framed)?;
        self.file.sync_data()?;

        Ok(())
    }

    /// Append N records in a single `write_all`. Performs `sync_data` once at
    /// the end IFF `sync_now` is true. Used by:
    /// - The `WalAppender` group-commit path (sync_now = true) — durable mode.
    /// - The async-sync path (sync_now = false) — fsync handled by `WalSyncer`.
    ///
    /// On error, no caller's record is guaranteed persisted; the file may
    /// contain a partial batch. Replay tolerates partial WAL trailers (existing
    /// behavior of `replay_wal`).
    pub fn append_batch(
        &mut self,
        records: &[(String, u32, Offset, u64, Record)],
        sync_now: bool,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Build one big buffer with all framed records, then write once.
        // Best-effort lower bound; eliminates most reallocations for typical
        // payload sizes without precise pre-encoding.
        let mut combined: Vec<u8> = Vec::with_capacity(records.len() * 64);
        let mut payload: Vec<u8> = Vec::new();
        for (stream, partition, offset, timestamp, record) in records {
            let stream_bytes = stream.as_bytes();
            payload.clear();
            payload.put_u16_le(stream_bytes.len() as u16);
            payload.put_slice(stream_bytes);
            payload.put_u32_le(*partition);
            encode_record(*offset, *timestamp, record, &mut payload);
            let framed = wrap_with_crc(&payload);
            combined.extend_from_slice(&framed);
        }
        self.file.write_all(&combined)?;
        if sync_now {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// Truncate the WAL to zero bytes and seek back to the start.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Best-effort: ignore errors during drop.
        let _ = self.file.sync_all();
    }
}

/// Read all valid WAL records from `path`.
///
/// - Empty file → returns `Ok(vec![])`.
/// - `UnexpectedEof` while reading the length field → clean EOF, stop.
/// - `UnexpectedEof` while reading data → partial write, warn and stop.
/// - CRC mismatch → warn and skip that record.
/// - Decode errors → warn and skip that record.
pub fn replay_wal(path: &Path) -> io::Result<Vec<WalRecord>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };

    // Return early for empty files.
    if file.metadata()?.len() == 0 {
        return Ok(vec![]);
    }

    let mut records = Vec::new();

    loop {
        // Read the 4-byte length field.
        let mut len_buf = [0u8; 4];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read payload (CRC + data).
        let mut payload = vec![0u8; payload_len];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                warn!("WAL replay: partial write detected, stopping replay");
                break;
            }
            Err(e) => return Err(e),
        }

        // Validate CRC.
        let data = match unwrap_crc(&payload) {
            Ok(d) => d,
            Err(e) => {
                warn!("WAL replay: CRC mismatch, skipping record: {}", e);
                continue;
            }
        };

        // Decode the payload: stream (u16 + UTF-8) + partition (u32) + record bytes.
        let wal_record = match decode_wal_payload(data) {
            Ok(r) => r,
            Err(e) => {
                warn!("WAL replay: failed to decode record, skipping: {}", e);
                continue;
            }
        };

        records.push(wal_record);
    }

    Ok(records)
}

/// Decode the inner WAL payload (after CRC stripping) into a `WalRecord`.
fn decode_wal_payload(data: &[u8]) -> Result<WalRecord, String> {
    if data.len() < 2 {
        return Err(format!(
            "WAL payload too short for stream length: need 2, have {}",
            data.len()
        ));
    }
    let stream_len = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
    let mut pos = 2;

    if data.len() < pos + stream_len {
        return Err(format!(
            "WAL payload truncated: need {} bytes for stream name, have {}",
            stream_len,
            data.len() - pos
        ));
    }
    let stream = std::str::from_utf8(&data[pos..pos + stream_len])
        .map_err(|e| format!("invalid stream name UTF-8: {}", e))?
        .to_string();
    pos += stream_len;

    if data.len() < pos + 4 {
        return Err(format!(
            "WAL payload truncated: need 4 bytes for partition, have {}",
            data.len() - pos
        ));
    }
    let partition = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let (stored, _) =
        decode_record(&data[pos..]).map_err(|e| format!("record decode failed: {}", e))?;

    let record = stored_to_record(&stored);
    Ok(WalRecord {
        stream,
        partition,
        offset: stored.offset,
        timestamp: stored.timestamp,
        record,
    })
}

/// Convert a `StoredRecord` into a `Record` by discarding offset/timestamp.
fn stored_to_record(stored: &StoredRecord) -> Record {
    Record {
        key: stored.key.clone(),
        value: stored.value.clone(),
        subject: stored.subject.clone(),
        headers: stored.headers.clone(),
        timestamp_ns: None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_streams::record::Record;
    use tempfile::TempDir;

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
    fn wal_write_and_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer
                .append("orders", 0, Offset(0), 1_000, &make_record(b"record-0"))
                .unwrap();
            writer
                .append("orders", 0, Offset(1), 2_000, &make_record(b"record-1"))
                .unwrap();
            writer
                .append("orders", 1, Offset(0), 3_000, &make_record(b"record-2"))
                .unwrap();
        }

        let records = replay_wal(&wal_path).unwrap();
        assert_eq!(records.len(), 3);

        assert_eq!(records[0].stream, "orders");
        assert_eq!(records[0].partition, 0);
        assert_eq!(records[0].offset, Offset(0));
        assert_eq!(records[0].record.value, Bytes::from_static(b"record-0"));

        assert_eq!(records[1].stream, "orders");
        assert_eq!(records[1].partition, 0);
        assert_eq!(records[1].offset, Offset(1));
        assert_eq!(records[1].record.value, Bytes::from_static(b"record-1"));

        assert_eq!(records[2].stream, "orders");
        assert_eq!(records[2].partition, 1);
        assert_eq!(records[2].offset, Offset(0));
        assert_eq!(records[2].record.value, Bytes::from_static(b"record-2"));
    }

    #[test]
    fn wal_truncate() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer
            .append("stream", 0, Offset(0), 1_000, &make_record(b"data"))
            .unwrap();
        writer.truncate().unwrap();
        drop(writer);

        let records = replay_wal(&wal_path).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn wal_empty_file_replays_to_empty() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("wal.log");

        // Open and immediately drop — creates an empty file.
        let writer = WalWriter::open(&wal_path).unwrap();
        drop(writer);

        let records = replay_wal(&wal_path).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn wal_partial_write_skipped() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("wal.log");

        // Write one complete good record.
        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer
                .append("stream", 0, Offset(0), 1_000, &make_record(b"good"))
                .unwrap();
        }

        // Manually append a partial record: length field says 100 bytes but
        // we only write 5 bytes of actual data.
        {
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            let length: u32 = 100;
            file.write_all(&length.to_le_bytes()).unwrap();
            file.write_all(&[0u8; 5]).unwrap();
        }

        let records = replay_wal(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.value, Bytes::from_static(b"good"));
    }

    #[test]
    fn append_batch_writes_all_records_with_one_sync() {
        use tempfile::tempdir;
        let tmp = tempdir().unwrap();
        let mut wal = WalWriter::open(&tmp.path().join("wal.log")).unwrap();
        let rec = Record {
            subject: "s".into(),
            key: None,
            value: Bytes::from_static(b"v"),
            headers: vec![],
            timestamp_ns: None,
        };
        let entries: Vec<_> = (0..5)
            .map(|i| ("stream".to_string(), 0u32, Offset(i), 1000 + i, rec.clone()))
            .collect();
        wal.append_batch(&entries, /*sync_now=*/ true).unwrap();
        drop(wal);

        // Replay must yield 5 records in order.
        let replayed = replay_wal(&tmp.path().join("wal.log")).unwrap();
        assert_eq!(replayed.len(), 5);
        for (i, r) in replayed.iter().enumerate() {
            assert_eq!(r.offset, Offset(i as u64));
            assert_eq!(r.timestamp, 1000 + i as u64);
            assert_eq!(r.stream, "stream");
            assert_eq!(r.partition, 0);
        }
    }

    #[test]
    fn append_batch_without_sync_is_persisted_after_drop() {
        use tempfile::tempdir;
        let tmp = tempdir().unwrap();
        let mut wal = WalWriter::open(&tmp.path().join("wal.log")).unwrap();
        let rec = Record {
            subject: "s".into(),
            key: None,
            value: Bytes::from_static(b"v"),
            headers: vec![],
            timestamp_ns: None,
        };
        let entries: Vec<_> = (0..3)
            .map(|i| ("stream".to_string(), 0u32, Offset(i), 1000 + i, rec.clone()))
            .collect();
        // sync_now = false: caller is responsible for calling sync_data later
        // (e.g. via WalSyncer task in Wave 2). Drop calls sync_all, so post-drop
        // replay should still see all records.
        wal.append_batch(&entries, /*sync_now=*/ false).unwrap();
        drop(wal); // Drop's sync_all flushes
        let replayed = replay_wal(&tmp.path().join("wal.log")).unwrap();
        assert_eq!(replayed.len(), 3);
    }
}
