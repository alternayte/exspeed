use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Magic bytes at the start of every `.tix` file.
pub const TIX_MAGIC: &[u8; 4] = b"EXTX";

/// Size of the file header in bytes: magic(4) + entry_count(4) + interval(4) + reserved(4).
pub const TIX_HEADER_SIZE: usize = 16;

/// Size of a single entry in bytes: timestamp(8) + offset(8).
pub const TIX_ENTRY_SIZE: usize = 16;

/// Default sampling interval: one index entry per this many records.
pub const DEFAULT_INTERVAL: u32 = 256;

/// A single entry in the timestamp index.
#[derive(Debug, Clone, Copy)]
pub struct TimeIndexEntry {
    pub timestamp: u64,
    pub offset: u64,
}

/// Sparse sorted timestamp index stored on disk.
///
/// Covers one segment file. Every `interval`-th record is sampled, plus the
/// first and last records are always included. Entries are stored in ascending
/// timestamp order, enabling binary search for seek-by-time.
pub struct TimeIndex {
    path: PathBuf,
    entries: Vec<TimeIndexEntry>,
    interval: u32,
}

impl TimeIndex {
    /// Build a new index from `all_records` and write it to `tix_path`.
    ///
    /// `all_records` is a slice of `(timestamp, offset)` pairs for every record
    /// in the segment, in order. Every `interval`-th record is sampled; the
    /// first and last records are always included regardless of interval.
    pub fn build(
        tix_path: &Path,
        all_records: &[(u64, u64)],
        interval: u32,
    ) -> io::Result<Self> {
        let mut sampled: Vec<TimeIndexEntry> = Vec::new();

        if !all_records.is_empty() {
            let interval_usize = interval as usize;
            for (i, &(timestamp, offset)) in all_records.iter().enumerate() {
                let is_first = i == 0;
                let is_last = i == all_records.len() - 1;
                let is_sampled = i % interval_usize == 0;
                if is_first || is_last || is_sampled {
                    // Avoid duplicates when first/last overlap with sampled
                    if sampled.last().map_or(true, |e: &TimeIndexEntry| e.offset != offset) {
                        sampled.push(TimeIndexEntry { timestamp, offset });
                    }
                }
            }
        }

        let entry_count = sampled.len() as u32;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tix_path)?;

        // Header: magic(4) + entry_count(4) + interval(4) + reserved(4)
        let mut header = [0u8; TIX_HEADER_SIZE];
        header[0..4].copy_from_slice(TIX_MAGIC);
        header[4..8].copy_from_slice(&entry_count.to_le_bytes());
        header[8..12].copy_from_slice(&interval.to_le_bytes());
        // bytes 12..16 remain zero (reserved)
        file.write_all(&header)?;

        // Entries
        for entry in &sampled {
            let mut buf = [0u8; TIX_ENTRY_SIZE];
            buf[0..8].copy_from_slice(&entry.timestamp.to_le_bytes());
            buf[8..16].copy_from_slice(&entry.offset.to_le_bytes());
            file.write_all(&buf)?;
        }

        file.flush()?;
        file.sync_all()?;

        Ok(Self {
            path: tix_path.to_path_buf(),
            entries: sampled,
            interval,
        })
    }

    /// Load an existing index from `tix_path`.
    ///
    /// Returns an error if the file is missing, too short, or has an
    /// unexpected magic value.
    pub fn load(tix_path: &Path) -> io::Result<Self> {
        let mut file = File::open(tix_path)?;

        let mut header = [0u8; TIX_HEADER_SIZE];
        file.read_exact(&mut header)?;

        // Validate magic.
        if &header[0..4] != TIX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bad tix magic: expected {:?}, got {:?}",
                    TIX_MAGIC,
                    &header[0..4]
                ),
            ));
        }

        let entry_count = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let interval = u32::from_le_bytes(header[8..12].try_into().unwrap());

        // Read all entries.
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let mut buf = [0u8; TIX_ENTRY_SIZE];
            file.read_exact(&mut buf)?;
            let timestamp = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            entries.push(TimeIndexEntry { timestamp, offset });
        }

        Ok(Self {
            path: tix_path.to_path_buf(),
            entries,
            interval,
        })
    }

    /// Find the file offset for the entry closest to (but not exceeding)
    /// `target_timestamp`.
    ///
    /// Returns `None` if the index is empty.
    pub fn seek_by_time(&self, target_timestamp: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        match self
            .entries
            .binary_search_by(|e| e.timestamp.cmp(&target_timestamp))
        {
            Ok(idx) => Some(self.entries[idx].offset),
            Err(0) => Some(self.entries[0].offset),
            Err(idx) => Some(self.entries[idx - 1].offset),
        }
    }

    /// Return the timestamp of the last entry, or `None` if the index is empty.
    pub fn last_timestamp(&self) -> Option<u64> {
        self.entries.last().map(|e| e.timestamp)
    }

    /// Return the timestamp of the first entry, or `None` if the index is empty.
    pub fn first_timestamp(&self) -> Option<u64> {
        self.entries.first().map(|e| e.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Generate `count` `(timestamp, offset)` tuples.
    /// timestamp = 1_000_000_000 + i * 1_000_000
    /// offset    = i
    fn sample_records(count: usize) -> Vec<(u64, u64)> {
        (0..count)
            .map(|i| (1_000_000_000u64 + i as u64 * 1_000_000, i as u64))
            .collect()
    }

    // -----------------------------------------------------------------------
    // 1. build_and_seek_exact — 1000 records, seek to exact timestamp of #500
    // -----------------------------------------------------------------------
    #[test]
    fn build_and_seek_exact() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let records = sample_records(1000);
        let index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();

        // Record 512 is sampled (512 % 256 == 0); record 500 is not sampled
        // directly so use a record that IS sampled: record 512.
        let (ts_512, off_512) = records[512];
        let result = index.seek_by_time(ts_512);
        assert_eq!(result, Some(off_512));
    }

    // -----------------------------------------------------------------------
    // 2. seek_between_entries — seek to timestamp between entries, get closest earlier
    // -----------------------------------------------------------------------
    #[test]
    fn seek_between_entries() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let records = sample_records(1000);
        let index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();

        // Timestamp between record 256 and 512 (both sampled).
        // Entry at 256: ts = 1_000_000_000 + 256 * 1_000_000 = 1_256_000_000
        // Entry at 512: ts = 1_000_000_000 + 512 * 1_000_000 = 1_512_000_000
        // Pick a timestamp in the middle.
        let between_ts = 1_000_000_000u64 + 400 * 1_000_000;
        let result = index.seek_by_time(between_ts);
        // Should return offset of record 256 (the last sampled entry <= between_ts)
        assert_eq!(result, Some(256u64));
    }

    // -----------------------------------------------------------------------
    // 3. seek_before_first — seek to 0, get first offset
    // -----------------------------------------------------------------------
    #[test]
    fn seek_before_first() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let records = sample_records(100);
        let index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();

        let result = index.seek_by_time(0);
        assert_eq!(result, Some(0u64)); // first record has offset 0
    }

    // -----------------------------------------------------------------------
    // 4. seek_after_last — seek to u64::MAX, get last offset
    // -----------------------------------------------------------------------
    #[test]
    fn seek_after_last() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let records = sample_records(100);
        let index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();

        let result = index.seek_by_time(u64::MAX);
        // Last record is always included; offset = 99
        assert_eq!(result, Some(99u64));
    }

    // -----------------------------------------------------------------------
    // 5. save_and_reload — build, load, seek works
    // -----------------------------------------------------------------------
    #[test]
    fn save_and_reload() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let records = sample_records(1000);

        {
            let _index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();
            // Drop here — ensures data was flushed to disk.
        }

        let reloaded = TimeIndex::load(&tix_path).unwrap();

        // Seek to exact timestamp of sampled record 256
        let (ts_256, off_256) = records[256];
        let result = reloaded.seek_by_time(ts_256);
        assert_eq!(result, Some(off_256));
    }

    // -----------------------------------------------------------------------
    // 6. empty_index — 0 records, seek returns None
    // -----------------------------------------------------------------------
    #[test]
    fn empty_index() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        let index = TimeIndex::build(&tix_path, &[], DEFAULT_INTERVAL).unwrap();

        assert_eq!(index.seek_by_time(0), None);
        assert_eq!(index.seek_by_time(u64::MAX), None);
        assert_eq!(index.first_timestamp(), None);
        assert_eq!(index.last_timestamp(), None);
    }

    // -----------------------------------------------------------------------
    // 7. first_and_last_always_included — 10 records (< interval), both endpoints captured
    // -----------------------------------------------------------------------
    #[test]
    fn first_and_last_always_included() {
        let dir = tempdir().unwrap();
        let tix_path = dir.path().join("test.tix");

        // 10 records is less than DEFAULT_INTERVAL (256), so only first and
        // last would be sampled by the interval rule — but we always include both.
        let records = sample_records(10);
        let index = TimeIndex::build(&tix_path, &records, DEFAULT_INTERVAL).unwrap();

        let (first_ts, first_off) = records[0];
        let (last_ts, last_off) = records[9];

        assert_eq!(index.first_timestamp(), Some(first_ts));
        assert_eq!(index.last_timestamp(), Some(last_ts));

        // Seek to exact first timestamp
        assert_eq!(index.seek_by_time(first_ts), Some(first_off));
        // Seek to exact last timestamp
        assert_eq!(index.seek_by_time(last_ts), Some(last_off));
    }
}
