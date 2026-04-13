use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Magic bytes at the start of every `.idx` file.
pub const IDX_MAGIC: &[u8; 4] = b"EXIX";

/// Size of the file header in bytes: magic(4) + slot_count(4) + reserved(8).
pub const IDX_HEADER_SIZE: usize = 16;

/// Size of a single slot in bytes: offset(8) + file_pos(4).
pub const SLOT_SIZE: usize = 12;

/// Sentinel value used to mark an empty (unoccupied) hash slot.
pub const EMPTY_OFFSET: u64 = u64::MAX;

/// A hash-based offset index stored on disk.
///
/// Maps logical stream offset (`u64`) to the byte position of that record
/// within a segment file (`u32`). Uses open addressing with linear probing
/// at a load factor of ~0.77 (slot_count = ceil(entries * 1.3)).
pub struct OffsetIndex {
    #[allow(dead_code)]
    path: PathBuf,
    slot_count: u32,
    slots: Vec<(u64, u32)>,
}

impl OffsetIndex {
    /// Build a new index from `entries` and write it to `idx_path`.
    ///
    /// `entries` is a slice of `(offset, file_pos)` pairs. Duplicate offsets
    /// are not checked; callers must ensure uniqueness.
    pub fn build(idx_path: &Path, entries: &[(u64, u32)]) -> io::Result<Self> {
        // Slot count: ceil(n * 1.3), minimum 1.
        let slot_count = {
            let n = entries.len();
            let raw = (n as f64 * 1.3).ceil() as u32;
            raw.max(1)
        };

        // Initialise all slots to EMPTY.
        let mut slots: Vec<(u64, u32)> = vec![(EMPTY_OFFSET, 0u32); slot_count as usize];

        // Insert entries via linear probing.
        for &(offset, file_pos) in entries {
            let mut idx = (offset % slot_count as u64) as usize;
            loop {
                if slots[idx].0 == EMPTY_OFFSET {
                    slots[idx] = (offset, file_pos);
                    break;
                }
                idx = (idx + 1) % slot_count as usize;
            }
        }

        // Serialise to disk.
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(idx_path)?;

        // Header: magic(4) + slot_count(4) + reserved(8)
        let mut header = [0u8; IDX_HEADER_SIZE];
        header[0..4].copy_from_slice(IDX_MAGIC);
        header[4..8].copy_from_slice(&slot_count.to_le_bytes());
        // bytes 8..16 remain zero (reserved)
        file.write_all(&header)?;

        // Slots
        for &(offset, file_pos) in &slots {
            let mut slot_buf = [0u8; SLOT_SIZE];
            slot_buf[0..8].copy_from_slice(&offset.to_le_bytes());
            slot_buf[8..12].copy_from_slice(&file_pos.to_le_bytes());
            file.write_all(&slot_buf)?;
        }

        file.flush()?;
        file.sync_all()?;

        Ok(Self {
            path: idx_path.to_path_buf(),
            slot_count,
            slots,
        })
    }

    /// Load an existing index from `idx_path`.
    ///
    /// Returns an error if the file is missing, too short, or has an
    /// unexpected magic value.
    pub fn load(idx_path: &Path) -> io::Result<Self> {
        let mut file = File::open(idx_path)?;

        let mut header = [0u8; IDX_HEADER_SIZE];
        file.read_exact(&mut header)?;

        // Validate magic.
        if &header[0..4] != IDX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bad idx magic: expected {:?}, got {:?}",
                    IDX_MAGIC,
                    &header[0..4]
                ),
            ));
        }

        let slot_count = u32::from_le_bytes(header[4..8].try_into().unwrap());

        // Read all slots.
        let mut slots = Vec::with_capacity(slot_count as usize);
        for _ in 0..slot_count {
            let mut buf = [0u8; SLOT_SIZE];
            file.read_exact(&mut buf)?;
            let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let file_pos = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            slots.push((offset, file_pos));
        }

        Ok(Self {
            path: idx_path.to_path_buf(),
            slot_count,
            slots,
        })
    }

    /// Look up the file position for `target_offset`.
    ///
    /// Returns `None` if the offset is not present in the index.
    pub fn lookup(&self, target_offset: u64) -> Option<u32> {
        if self.slot_count == 0 {
            return None;
        }

        let mut idx = (target_offset % self.slot_count as u64) as usize;
        loop {
            let (slot_offset, file_pos) = self.slots[idx];
            if slot_offset == EMPTY_OFFSET {
                return None;
            }
            if slot_offset == target_offset {
                return Some(file_pos);
            }
            idx = (idx + 1) % self.slot_count as usize;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // -----------------------------------------------------------------------
    // 1. build_and_lookup — 100 entries, look up each
    // -----------------------------------------------------------------------
    #[test]
    fn build_and_lookup() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        let entries: Vec<(u64, u32)> = (0u64..100).map(|i| (i, (i * 10) as u32)).collect();
        let index = OffsetIndex::build(&idx_path, &entries).unwrap();

        for (offset, expected_pos) in &entries {
            let result = index.lookup(*offset);
            assert_eq!(
                result,
                Some(*expected_pos),
                "lookup failed for offset {offset}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 2. lookup_nonexistent_returns_none — 3 entries, look up 999
    // -----------------------------------------------------------------------
    #[test]
    fn lookup_nonexistent_returns_none() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        let entries = vec![(1u64, 10u32), (2u64, 20u32), (3u64, 30u32)];
        let index = OffsetIndex::build(&idx_path, &entries).unwrap();

        assert_eq!(index.lookup(999), None);
    }

    // -----------------------------------------------------------------------
    // 3. save_and_reload — build, drop, load from disk, look up
    // -----------------------------------------------------------------------
    #[test]
    fn save_and_reload() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        let entries: Vec<(u64, u32)> = (0u64..50).map(|i| (i * 2, i as u32 * 4)).collect();

        {
            let _index = OffsetIndex::build(&idx_path, &entries).unwrap();
            // Drop here — ensures data was flushed to disk.
        }

        let reloaded = OffsetIndex::load(&idx_path).unwrap();

        for (offset, expected_pos) in &entries {
            assert_eq!(
                reloaded.lookup(*offset),
                Some(*expected_pos),
                "reloaded lookup failed for offset {offset}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 4. empty_index — 0 entries, lookup returns None
    // -----------------------------------------------------------------------
    #[test]
    fn empty_index() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        let index = OffsetIndex::build(&idx_path, &[]).unwrap();

        assert_eq!(index.lookup(0), None);
        assert_eq!(index.lookup(42), None);
    }

    // -----------------------------------------------------------------------
    // 5. hash_collisions_resolved — entries that collide in a small table
    //    With slot_count = ceil(4 * 1.3) = 6:
    //      0 % 6 = 0, 6 % 6 = 0 (collision), 12 % 6 = 0, 18 % 6 = 0
    //    All four entries hash to slot 0 and are resolved by linear probing.
    // -----------------------------------------------------------------------
    #[test]
    fn hash_collisions_resolved() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        // These four offsets are all multiples of 6, so they all hash to
        // slot 0 in a 6-slot table (ceil(4 * 1.3) = 6).
        let entries: Vec<(u64, u32)> = vec![
            (0u64, 100u32),
            (6u64, 200u32),
            (12u64, 300u32),
            (18u64, 400u32),
        ];

        let index = OffsetIndex::build(&idx_path, &entries).unwrap();

        assert_eq!(index.lookup(0), Some(100));
        assert_eq!(index.lookup(6), Some(200));
        assert_eq!(index.lookup(12), Some(300));
        assert_eq!(index.lookup(18), Some(400));
        assert_eq!(index.lookup(1), None);
    }
}
