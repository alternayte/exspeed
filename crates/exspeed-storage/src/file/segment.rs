//! Segment files, their sparse indexes and metadata sidecars.
//!
//! A partition directory holds, per segment `B` (its base offset, zero
//! padded to 20 digits):
//!
//! * `B.seg` — a 16-byte header (`"EXSG"`, version, base offset) followed by
//!   records in their wire encoding, each with its own length and CRC (see
//!   [`crate::encoding`]). Offsets inside a segment
//!   are strictly increasing but may have gaps (compaction, `append_at`).
//! * `B.idx` — the sparse offset + time index: one 24-byte entry
//!   `(offset u64, file position u64, running max timestamp u64)` for the
//!   first record of the segment and then for the first record after every
//!   ~4 KiB of data. The running max makes the time column monotonic.
//! * `B.meta` — JSON metadata written when the segment is sealed (or
//!   rewritten by compaction): length, last offset, timestamp range. Startup
//!   reads it instead of scanning the segment. The active (last) segment
//!   never has one; its index is rebuilt from the recovery tail scan.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use exspeed_common::record_format;

use crate::encoding::{check_crc, frame_offset_ts, frame_size, LEN_FIELD, MAX_FRAME_LEN};
use crate::file::fsutil::{atomic_write, fsync_dir, read_at};

pub const SEGMENT_MAGIC: &[u8; 4] = b"EXSG";
/// Version 3: records are stored in the client protocol's `WireRecord`
/// encoding (length, CRC, delivery count, then the fields), so reads can
/// copy them into response frames as they are. Version 2 had an 8-byte
/// `len` + `crc` frame header and no delivery count; version 1 had dense
/// indexes. Older segments are refused.
pub const SEGMENT_VERSION: u8 = 3;
pub const SEGMENT_HEADER_LEN: u64 = 16;
/// Approximate number of data bytes between two index entries.
pub const INDEX_INTERVAL_BYTES: u64 = 4096;
pub const INDEX_ENTRY_LEN: usize = 24;

pub fn seg_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.seg"))
}
pub fn idx_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.idx"))
}
pub fn meta_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.meta"))
}

/// Parse `00000000000000000042.seg` into `42`.
pub fn parse_seg_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".seg")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

pub fn header_bytes(base: u64) -> [u8; SEGMENT_HEADER_LEN as usize] {
    let mut h = [0u8; SEGMENT_HEADER_LEN as usize];
    h[0..4].copy_from_slice(SEGMENT_MAGIC);
    h[4] = SEGMENT_VERSION;
    h[5..13].copy_from_slice(&base.to_le_bytes());
    h
}

/// Create a new, empty segment file (header only), fsync it and its
/// directory. Returns an append handle.
pub fn create_segment_file(dir: &Path, base: u64) -> io::Result<File> {
    let path = seg_path(dir, base);
    let mut f = OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&path)?;
    f.write_all(&header_bytes(base))?;
    f.sync_all()?;
    fsync_dir(dir)?;
    Ok(f)
}

/// Validate a segment header and return its base offset.
pub fn read_header(file: &File, path: &Path) -> io::Result<u64> {
    let mut h = [0u8; SEGMENT_HEADER_LEN as usize];
    let n = read_at(file, &mut h, 0)?;
    if n < h.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: segment header truncated", path.display()),
        ));
    }
    if &h[0..4] != SEGMENT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: not a segment file (bad magic)", path.display()),
        ));
    }
    if h[4] != SEGMENT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: segment format version {} is not supported (expected {}); \
                 this data directory was written by an older exspeed — start \
                 with a fresh data directory",
                path.display(),
                h[4],
                SEGMENT_VERSION
            ),
        ));
    }
    Ok(u64::from_le_bytes(h[5..13].try_into().unwrap()))
}

/// One sparse index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: u64,
    pub pos: u64,
    /// Running max timestamp of the segment up to and including this record.
    pub max_ts: u64,
}

impl IndexEntry {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.offset.to_le_bytes());
        dst.extend_from_slice(&self.pos.to_le_bytes());
        dst.extend_from_slice(&self.max_ts.to_le_bytes());
    }
    pub fn decode(b: &[u8]) -> Self {
        Self {
            offset: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            pos: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            max_ts: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        }
    }
}

/// Decides where index entries go while records are appended or scanned.
#[derive(Debug, Clone, Default)]
pub struct IndexBuilder {
    last_pos: Option<u64>,
    pub max_ts: u64,
}

impl IndexBuilder {
    /// Continue after the entries already in a segment.
    pub fn resume(last_entry_pos: Option<u64>, max_ts: u64) -> Self {
        Self {
            last_pos: last_entry_pos,
            max_ts,
        }
    }

    pub fn observe(&mut self, offset: u64, pos: u64, ts: u64) -> Option<IndexEntry> {
        self.max_ts = self.max_ts.max(ts);
        match self.last_pos {
            Some(last) if pos - last < INDEX_INTERVAL_BYTES => None,
            _ => {
                self.last_pos = Some(pos);
                Some(IndexEntry {
                    offset,
                    pos,
                    max_ts: self.max_ts,
                })
            }
        }
    }
}

pub fn encode_index(entries: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * INDEX_ENTRY_LEN);
    for e in entries {
        e.encode(&mut buf);
    }
    buf
}

/// Metadata sidecar for a sealed segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub base_offset: u64,
    /// File length in bytes, header included.
    pub len: u64,
    /// One past the last record's offset (`base_offset` when empty).
    pub end_offset: u64,
    pub first_ts: Option<u64>,
    pub max_ts: Option<u64>,
    pub records: u64,
    pub index_entries: u64,
}

pub fn load_meta(path: &Path) -> Option<SegmentMeta> {
    let data = std::fs::read(path).ok()?;
    serde_json::from_slice(&data).ok()
}

pub fn save_meta(path: &Path, meta: &SegmentMeta) -> io::Result<()> {
    let json = serde_json::to_vec_pretty(meta).map_err(io::Error::other)?;
    atomic_write(path, &json)
}

enum SegIndex {
    /// In-memory, appended to by the writer.
    Active(RwLock<Vec<IndexEntry>>),
    /// On disk; looked up with a binary search over `pread`s, so sealed
    /// segments cost no memory.
    Sealed { file: File, entries: u64 },
}

/// A segment as seen by readers. Shared via `Arc`; the writer publishes
/// progress on the active segment through the atomics.
pub struct Segment {
    pub base_offset: u64,
    pub path: PathBuf,
    file: File,
    index: SegIndex,
    /// Bytes readers may read (header included). Always a frame boundary.
    len: AtomicU64,
    /// One past the last visible record (`base_offset` when empty).
    end_offset: AtomicU64,
    first_ts: AtomicU64,
    max_ts: AtomicU64,
    records: AtomicU64,
}

/// Snapshot of a segment's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentStats {
    pub len: u64,
    pub end_offset: u64,
    pub first_ts: Option<u64>,
    pub max_ts: Option<u64>,
    pub records: u64,
}

impl Segment {
    /// A segment that is being appended to.
    pub fn new_active(
        dir: &Path,
        base_offset: u64,
        stats: SegmentStats,
        entries: Vec<IndexEntry>,
    ) -> io::Result<Self> {
        let path = seg_path(dir, base_offset);
        let file = File::open(&path)?;
        Ok(Self::build(
            base_offset,
            path,
            file,
            SegIndex::Active(RwLock::new(entries)),
            stats,
        ))
    }

    /// A sealed segment; its `.idx` file must be complete.
    pub fn new_sealed(dir: &Path, meta: &SegmentMeta) -> io::Result<Self> {
        let path = seg_path(dir, meta.base_offset);
        let file = File::open(&path)?;
        let idx = File::open(idx_path(dir, meta.base_offset))?;
        let stats = SegmentStats {
            len: meta.len,
            end_offset: meta.end_offset,
            first_ts: meta.first_ts,
            max_ts: meta.max_ts,
            records: meta.records,
        };
        Ok(Self::build(
            meta.base_offset,
            path,
            file,
            SegIndex::Sealed {
                file: idx,
                entries: meta.index_entries,
            },
            stats,
        ))
    }

    fn build(
        base_offset: u64,
        path: PathBuf,
        file: File,
        index: SegIndex,
        s: SegmentStats,
    ) -> Self {
        Self {
            base_offset,
            path,
            file,
            index,
            len: AtomicU64::new(s.len),
            end_offset: AtomicU64::new(s.end_offset),
            first_ts: AtomicU64::new(s.first_ts.unwrap_or(u64::MAX)),
            max_ts: AtomicU64::new(s.max_ts.unwrap_or(0)),
            records: AtomicU64::new(s.records),
        }
    }

    pub fn is_sealed(&self) -> bool {
        matches!(self.index, SegIndex::Sealed { .. })
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    /// Committed length in bytes, including the header.
    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    /// True when the segment holds no records (only its header).
    pub fn is_empty(&self) -> bool {
        self.len() <= SEGMENT_HEADER_LEN
    }

    pub fn end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> SegmentStats {
        let records = self.records.load(Ordering::Acquire);
        SegmentStats {
            len: self.len(),
            end_offset: self.end_offset(),
            first_ts: (records > 0).then(|| self.first_ts.load(Ordering::Acquire)),
            max_ts: (records > 0).then(|| self.max_ts.load(Ordering::Acquire)),
            records,
        }
    }

    pub fn meta(&self) -> SegmentMeta {
        let s = self.stats();
        SegmentMeta {
            base_offset: self.base_offset,
            len: s.len,
            end_offset: s.end_offset,
            first_ts: s.first_ts,
            max_ts: s.max_ts,
            records: s.records,
            index_entries: match &self.index {
                SegIndex::Active(v) => v.read().unwrap().len() as u64,
                SegIndex::Sealed { entries, .. } => *entries,
            },
        }
    }

    /// Writer only: publish newly committed records on the active segment.
    /// Index entries are pushed before the length is advanced, so a reader
    /// that sees the new length also finds the entries.
    pub fn publish(&self, new_entries: &[IndexEntry], s: SegmentStats) {
        if let SegIndex::Active(v) = &self.index {
            if !new_entries.is_empty() {
                v.write().unwrap().extend_from_slice(new_entries);
            }
        }
        if let Some(ts) = s.first_ts {
            self.first_ts.store(ts, Ordering::Release);
        }
        if let Some(ts) = s.max_ts {
            self.max_ts.store(ts, Ordering::Release);
        }
        self.records.store(s.records, Ordering::Release);
        self.end_offset.store(s.end_offset, Ordering::Release);
        self.len.store(s.len, Ordering::Release);
    }

    /// Writer only: shrink the visible region (truncation).
    pub fn retract(&self, s: SegmentStats, keep_entries_below: u64) {
        self.len.store(s.len, Ordering::Release);
        self.end_offset.store(s.end_offset, Ordering::Release);
        self.records.store(s.records, Ordering::Release);
        self.first_ts
            .store(s.first_ts.unwrap_or(u64::MAX), Ordering::Release);
        self.max_ts.store(s.max_ts.unwrap_or(0), Ordering::Release);
        if let SegIndex::Active(v) = &self.index {
            v.write().unwrap().retain(|e| e.offset < keep_entries_below);
        }
    }

    /// All index entries (active: from memory, sealed: from disk).
    pub fn index_entries(&self) -> io::Result<Vec<IndexEntry>> {
        match &self.index {
            SegIndex::Active(v) => Ok(v.read().unwrap().clone()),
            SegIndex::Sealed { file, entries } => {
                let mut buf = vec![0u8; *entries as usize * INDEX_ENTRY_LEN];
                let n = read_at(file, &mut buf, 0)?;
                buf.truncate(n - n % INDEX_ENTRY_LEN);
                Ok(buf
                    .as_chunks::<INDEX_ENTRY_LEN>()
                    .0
                    .iter()
                    .map(|c| IndexEntry::decode(c))
                    .collect())
            }
        }
    }

    /// The last index entry for which `pred` holds, where `pred` is true for
    /// a prefix of the entries (binary search).
    fn last_where(&self, pred: impl Fn(&IndexEntry) -> bool) -> io::Result<Option<IndexEntry>> {
        match &self.index {
            SegIndex::Active(v) => {
                let v = v.read().unwrap();
                let n = v.partition_point(&pred);
                Ok(n.checked_sub(1).map(|i| v[i]))
            }
            SegIndex::Sealed { file, entries } => {
                let (mut lo, mut hi) = (0u64, *entries);
                let mut found = None;
                let mut buf = [0u8; INDEX_ENTRY_LEN];
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let n = read_at(file, &mut buf, mid * INDEX_ENTRY_LEN as u64)?;
                    if n < INDEX_ENTRY_LEN {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "index file shorter than its metadata says",
                        ));
                    }
                    let e = IndexEntry::decode(&buf);
                    if pred(&e) {
                        found = Some(e);
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                Ok(found)
            }
        }
    }

    /// File position to start scanning from to find `offset`: the last
    /// indexed record at or before it (or the first record).
    pub fn position_for_offset(&self, offset: u64) -> io::Result<u64> {
        Ok(self
            .last_where(|e| e.offset <= offset)?
            .map_or(SEGMENT_HEADER_LEN, |e| e.pos))
    }

    /// File position to start scanning from to find the first record with
    /// timestamp `>= ts`: every record before it has a smaller timestamp.
    pub fn position_for_time(&self, ts: u64) -> io::Result<u64> {
        Ok(self
            .last_where(|e| e.max_ts < ts)?
            .map_or(SEGMENT_HEADER_LEN, |e| e.pos))
    }
}

/// One record read from a segment.
#[derive(Debug, Clone)]
pub struct Frame {
    pub pos: u64,
    /// Total bytes of the record.
    pub size: u64,
    pub offset: u64,
    pub timestamp: u64,
    /// The whole record (zero-copy slice of the read buffer).
    pub raw: Bytes,
}

#[derive(Debug)]
pub enum FrameError {
    /// The frame extends past `end`.
    Torn {
        pos: u64,
    },
    /// The file is shorter than `end` (it was truncated underneath us).
    Short {
        pos: u64,
    },
    /// Bad length field or CRC.
    Corrupt {
        pos: u64,
        reason: String,
    },
    Io(io::Error),
}

impl FrameError {
    pub fn into_io(self, path: &Path) -> io::Error {
        match self {
            FrameError::Io(e) => e,
            FrameError::Torn { pos } => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: torn frame at byte {pos}", path.display()),
            ),
            FrameError::Short { pos } => io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{}: file ends early at byte {pos}", path.display()),
            ),
            FrameError::Corrupt { pos, reason } => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: corrupt frame at byte {pos}: {reason}", path.display()),
            ),
        }
    }
}

const MAX_CHUNK: usize = 4 * 1024 * 1024;

/// Sequential frame reader over `[pos, end)` of a segment file, reading in
/// chunks with `pread`. The chunk size starts at the caller's hint and
/// doubles on every refill up to 4 MiB.
pub struct FrameIter<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
    buf: Bytes,
    buf_start: u64,
    chunk: usize,
    verify_crc: bool,
}

impl<'a> FrameIter<'a> {
    pub fn new(file: &'a File, pos: u64, end: u64, chunk: usize) -> Self {
        Self {
            file,
            pos,
            end,
            buf: Bytes::new(),
            buf_start: pos,
            chunk: chunk.clamp(4096, MAX_CHUNK),
            verify_crc: true,
        }
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Make `[self.pos, self.pos + need)` available in `self.buf`.
    fn ensure(&mut self, need: usize) -> Result<(), FrameError> {
        let have_end = self.buf_start + self.buf.len() as u64;
        if self.pos >= self.buf_start && self.pos + need as u64 <= have_end {
            return Ok(());
        }
        if self.pos + need as u64 > self.end {
            return Err(FrameError::Torn { pos: self.pos });
        }
        let want = need.max(self.chunk).min((self.end - self.pos) as usize);
        // Grow the read size for long sequential scans.
        self.chunk = (self.chunk * 2).min(MAX_CHUNK);
        let mut b = BytesMut::zeroed(want);
        let n = read_at(self.file, &mut b, self.pos).map_err(FrameError::Io)?;
        if n < need {
            return Err(FrameError::Short {
                pos: self.pos + n as u64,
            });
        }
        b.truncate(n);
        self.buf = b.freeze();
        self.buf_start = self.pos;
        Ok(())
    }

    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.pos >= self.end {
            return Ok(None);
        }
        self.ensure(LEN_FIELD)?;
        let at = (self.pos - self.buf_start) as usize;
        let size =
            frame_size(&self.buf[at..at + LEN_FIELD]).map_err(|reason| FrameError::Corrupt {
                pos: self.pos,
                reason,
            })?;
        debug_assert!(size <= MAX_FRAME_LEN);
        self.ensure(size)?;
        let at = (self.pos - self.buf_start) as usize;
        let raw = self.buf.slice(at..at + size);
        if self.verify_crc {
            check_crc(&raw).map_err(|reason| FrameError::Corrupt {
                pos: self.pos,
                reason,
            })?;
        }
        let (offset, timestamp) = frame_offset_ts(&raw);
        let frame = Frame {
            pos: self.pos,
            size: size as u64,
            offset,
            timestamp,
            raw,
        };
        self.pos += size as u64;
        Ok(Some(frame))
    }
}

/// Why a recovery scan stopped before the end of the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanStop {
    pub pos: u64,
    pub reason: String,
}

/// Result of scanning a segment from its header.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub entries: Vec<IndexEntry>,
    pub stats: SegmentStats,
    /// Set when the scan stopped at a bad frame instead of the end of file.
    pub stop: Option<ScanStop>,
}

/// Scan a whole segment, validating CRCs and that offsets are `>= base` and
/// strictly increasing. Stops at the first bad frame.
pub fn scan_segment(file: &File, base: u64, file_len: u64) -> io::Result<ScanResult> {
    let mut it = FrameIter::new(file, SEGMENT_HEADER_LEN, file_len, 1024 * 1024);
    let mut ib = IndexBuilder::default();
    let mut entries = Vec::new();
    let mut stats = SegmentStats {
        len: SEGMENT_HEADER_LEN,
        end_offset: base,
        first_ts: None,
        max_ts: None,
        records: 0,
    };
    let mut prev: Option<u64> = None;
    let stop = loop {
        match it.next_frame() {
            Ok(None) => break None,
            Ok(Some(f)) => {
                let min = prev.map_or(base, |p| p + 1);
                if f.offset < min {
                    break Some(ScanStop {
                        pos: f.pos,
                        reason: format!(
                            "offset {} is not increasing (expected >= {min})",
                            f.offset
                        ),
                    });
                }
                prev = Some(f.offset);
                if let Some(e) = ib.observe(f.offset, f.pos, f.timestamp) {
                    entries.push(e);
                }
                stats.records += 1;
                stats.first_ts.get_or_insert(f.timestamp);
                stats.max_ts = Some(ib.max_ts);
                stats.end_offset = f.offset + 1;
                stats.len = f.pos + f.size;
            }
            Err(FrameError::Io(e)) => return Err(e),
            Err(FrameError::Torn { pos }) | Err(FrameError::Short { pos }) => {
                break Some(ScanStop {
                    pos,
                    reason: "torn frame at end of file".into(),
                })
            }
            Err(FrameError::Corrupt { pos, reason }) => break Some(ScanStop { pos, reason }),
        }
    };
    Ok(ScanResult {
        entries,
        stats,
        stop,
    })
}

/// Window [`valid_frame_after`] reads the file through.
const PROBE_WINDOW: usize = 1 << 20;
/// CRC verifications [`valid_frame_after`] may spend before giving up.
const PROBE_CRC_BUDGET: usize = 4096;

/// Whether any valid frame with offset `> after_offset` starts anywhere in
/// `(from, file_len)`. Used to tell a torn tail (nothing valid after the bad
/// spot) from corruption in the middle of the file.
///
/// Bounded: the file is scanned through a 1 MiB window (never loaded
/// whole), a candidate position must have a plausible length and an offset
/// `> after_offset` before its CRC is checked, and at most
/// [`PROBE_CRC_BUDGET`] CRCs are verified. When that budget runs out the
/// answer is `true` ("valid data may follow"), the conservative choice:
/// recovery then refuses to truncate instead of dropping data.
pub fn valid_frame_after(
    file: &File,
    from: u64,
    file_len: u64,
    after_offset: Option<u64>,
) -> io::Result<bool> {
    if file_len <= from + 1 {
        return Ok(false);
    }
    let min_offset = after_offset.map_or(0, |o| o + 1);
    // Bytes a candidate needs in the window for the cheap checks: the
    // length field through the end of the offset field.
    const PREFIX: usize = record_format::OFFSET_AT + 8;
    let mut win = vec![0u8; PROBE_WINDOW];
    let mut win_start = u64::MAX; // file position of win[0]
    let mut win_len = 0usize;
    let mut crcs = 0usize;
    let mut frame = Vec::new();
    let mut pos = from + 1;
    while pos + LEN_FIELD as u64 <= file_len {
        if win_start == u64::MAX || pos + PREFIX as u64 > win_start + win_len as u64 {
            // Slide the window to start at `pos`.
            let want = ((file_len - pos) as usize).min(PROBE_WINDOW);
            win_len = read_at(file, &mut win[..want], pos)?;
            win_start = pos;
            if win_len < LEN_FIELD {
                break;
            }
        }
        let i = (pos - win_start) as usize;
        let head = &win[i..win_len];
        pos += 1;
        let Ok(size) = frame_size(&head[..LEN_FIELD.min(head.len())]) else {
            continue;
        };
        // `size` is at least MIN_RECORD_LEN, which covers PREFIX.
        if pos - 1 + size as u64 > file_len || head.len() < PREFIX {
            continue;
        }
        if record_format::offset(&head[..PREFIX]) < min_offset {
            continue;
        }
        if crcs >= PROBE_CRC_BUDGET {
            return Ok(true);
        }
        crcs += 1;
        let raw: &[u8] = if head.len() >= size {
            &head[..size]
        } else {
            frame.resize(size, 0);
            let n = read_at(file, &mut frame, pos - 1)?;
            if n < size {
                continue;
            }
            &frame
        };
        if check_crc(raw).is_ok() && frame_offset_ts(raw).0 >= min_offset {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_names_roundtrip() {
        let p = seg_path(Path::new("/x"), 42);
        let name = p.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "00000000000000000042.seg");
        assert_eq!(parse_seg_name(name), Some(42));
        assert_eq!(parse_seg_name("42.seg"), None);
        assert_eq!(parse_seg_name("00000000000000000042.idx"), None);
    }

    #[test]
    fn index_builder_spacing() {
        let mut ib = IndexBuilder::default();
        assert!(ib.observe(0, 16, 5).is_some(), "first record is indexed");
        assert!(ib.observe(1, 100, 3).is_none());
        let e = ib.observe(2, 16 + INDEX_INTERVAL_BYTES, 4).unwrap();
        assert_eq!(e.max_ts, 5, "time column is the running max");
    }

    fn rec(offset: u64, value: &[u8]) -> Vec<u8> {
        let mut b = BytesMut::new();
        record_format::encode(
            &mut b,
            &record_format::Fields {
                offset,
                timestamp_ns: 1,
                delivery_count: 0,
                subject: "s",
                key: None,
                value,
                headers: &[],
            },
        )
        .unwrap();
        b.to_vec()
    }

    fn file_of(bytes: &[u8]) -> (tempfile::NamedTempFile, File) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        let file = File::open(f.path()).unwrap();
        (f, file)
    }

    /// `valid_frame_after` streams the file through a bounded window, still
    /// finds valid records far past the bad spot (and records larger than
    /// the window), and tells a torn tail from mid-file corruption.
    #[test]
    fn valid_frame_after_scans_in_bounded_chunks() {
        // Mid-file corruption: a valid record 3 MiB after the bad byte.
        let mut bytes = rec(0, b"a");
        let bad = bytes.len() as u64;
        bytes.extend(vec![0u8; 3 << 20]);
        bytes.extend(rec(1, b"b"));
        let (_t, f) = file_of(&bytes);
        assert!(valid_frame_after(&f, bad, bytes.len() as u64, Some(0)).unwrap());
        // ... but not when it isn't newer than the last good offset.
        assert!(!valid_frame_after(&f, bad, bytes.len() as u64, Some(1)).unwrap());

        // A torn tail: only garbage after the bad spot.
        let mut torn = rec(0, b"a");
        let bad = torn.len() as u64;
        torn.extend((0..10_000u32).map(|i| (i * 7 + 3) as u8));
        let (_t, f) = file_of(&torn);
        assert!(!valid_frame_after(&f, bad, torn.len() as u64, Some(0)).unwrap());

        // A valid record bigger than the window.
        let mut big = rec(0, b"a");
        let bad = big.len() as u64;
        big.extend([0xFFu8; 100]);
        big.extend(rec(1, &vec![b'x'; 2 * PROBE_WINDOW]));
        let (_t, f) = file_of(&big);
        assert!(valid_frame_after(&f, bad, big.len() as u64, Some(0)).unwrap());
    }

    /// Garbage full of plausible-looking candidates can't make the probe
    /// spend unbounded CRC work: past the budget it answers "valid data may
    /// follow", the conservative choice.
    #[test]
    fn valid_frame_after_has_a_crc_budget() {
        let mut bytes = rec(0, b"a");
        let bad = bytes.len() as u64;
        for i in 0..(PROBE_CRC_BUDGET + 100) {
            let mut r = rec(10 + i as u64, b"zz");
            r[record_format::CRC_AT] ^= 0xFF; // bad CRC
            bytes.extend(r);
        }
        let (_t, f) = file_of(&bytes);
        assert!(valid_frame_after(&f, bad, bytes.len() as u64, Some(0)).unwrap());
    }
}
