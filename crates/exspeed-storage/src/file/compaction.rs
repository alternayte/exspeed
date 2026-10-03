//! Log compaction: keep only the latest record per key in sealed segments.
//!
//! * Records without a key are always kept.
//! * A record with a key survives only if it is the newest record for that
//!   key anywhere in the visible log (the active segment included).
//! * A tombstone (key + empty value) deletes its key: older records for the
//!   key are removed, and the tombstone itself is removed once it is older
//!   than the stream's `tombstone_retention_secs`.
//! * Offsets are preserved, so compacted segments have offset gaps.
//! * The active segment is never rewritten.
//!
//! A rewrite is crash-safe: the kept frames are copied into
//! `B.seg.compacting` / `B.idx.compacting` and fsynced, then the writer
//! thread renames them over the originals, writes the new `.meta` and swaps
//! the segment list. Leftover `.compacting` files are deleted by recovery;
//! a crash between the renames leaves a `.meta` that doesn't match the
//! segment, which makes recovery rebuild the index from the (complete,
//! fsynced) compacted file.
//!
//! The key map holds every distinct key in memory. That is fine for the
//! metadata streams this is built for; very large keyspaces would need a
//! bounded, multi-pass map.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use crate::encoding::payload_key_value_len;
use crate::file::partition::PartitionShared;
use crate::file::segment::{
    encode_index, header_bytes, FrameError, FrameIter, IndexBuilder, Segment, SegmentMeta,
    SEGMENT_HEADER_LEN,
};
use crate::file::writer::CompactionJob;

/// What one compaction pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    pub segments_rewritten: u32,
    pub records_removed: u64,
    pub bytes_reclaimed: u64,
}

impl std::ops::AddAssign for CompactionStats {
    fn add_assign(&mut self, o: Self) {
        self.segments_rewritten += o.segments_rewritten;
        self.records_removed += o.records_removed;
        self.bytes_reclaimed += o.bytes_reclaimed;
    }
}

fn tmp_paths(dir: &Path, base: u64) -> (PathBuf, PathBuf) {
    (
        dir.join(format!("{base:020}.seg.compacting")),
        dir.join(format!("{base:020}.idx.compacting")),
    )
}

/// Visit every visible frame of `seg` with `f(offset, ts, key, is_tombstone, raw)`.
fn for_each_frame(
    seg: &Segment,
    hwm: u64,
    mut f: impl FnMut(u64, u64, Option<&[u8]>, bool, &Bytes) -> io::Result<()>,
) -> io::Result<()> {
    let mut it = FrameIter::new(seg.file(), SEGMENT_HEADER_LEN, seg.len(), 1 << 20);
    loop {
        let frame = match it.next_frame() {
            Ok(Some(fr)) => fr,
            Ok(None) | Err(FrameError::Short { .. }) => return Ok(()),
            Err(e) => return Err(e.into_io(&seg.path)),
        };
        if frame.offset >= hwm {
            return Ok(());
        }
        let (key, value_len) = payload_key_value_len(&frame.payload)
            .map_err(|r| io::Error::new(io::ErrorKind::InvalidData, r))?;
        f(
            frame.offset,
            frame.timestamp,
            key,
            key.is_some() && value_len == 0,
            &frame.raw,
        )?;
    }
}

/// Plan and write compacted copies of every sealed segment that has
/// something to remove. Nothing is installed yet.
pub fn prepare(
    shared: &PartitionShared,
    tombstone_retention_secs: u64,
    now_nanos: u64,
) -> io::Result<Vec<(CompactionJob, CompactionStats)>> {
    let hwm = shared.high_watermark();
    let list = shared.segments();
    if list.len() < 2 {
        return Ok(Vec::new());
    }

    // Pass 1: newest offset per key across the whole visible log.
    let mut latest: HashMap<Bytes, u64> = HashMap::new();
    for seg in list.iter() {
        for_each_frame(seg, hwm, |offset, _, key, _, _| {
            if let Some(k) = key {
                match latest.get_mut(k) {
                    Some(o) => *o = offset,
                    None => {
                        latest.insert(Bytes::copy_from_slice(k), offset);
                    }
                }
            }
            Ok(())
        })?;
    }

    let tombstone_cutoff =
        now_nanos.saturating_sub(tombstone_retention_secs.saturating_mul(1_000_000_000));
    let mut jobs = Vec::new();
    let keep = |offset: u64, ts: u64, key: Option<&[u8]>, tombstone: bool| match key {
        None => true,
        Some(k) => latest.get(k) == Some(&offset) && !(tombstone && ts < tombstone_cutoff),
    };
    // Pass 2: count what each sealed segment would lose; pass 3: rewrite
    // the ones that lose something, streaming the kept frames.
    for seg in &list[..list.len() - 1] {
        let mut removed = 0u64;
        for_each_frame(seg, hwm, |offset, ts, key, tombstone, _| {
            if !keep(offset, ts, key, tombstone) {
                removed += 1;
            }
            Ok(())
        })?;
        if removed == 0 {
            continue;
        }
        let job = write_compacted(&shared.dir, seg, hwm, &keep)?;
        let stats = CompactionStats {
            segments_rewritten: 1,
            records_removed: removed,
            bytes_reclaimed: seg.len().saturating_sub(job.meta.len),
        };
        jobs.push((job, stats));
    }
    Ok(jobs)
}

/// Decides whether a frame survives: `(offset, timestamp_ns, key, is_tombstone)`.
type KeepFn<'a> = dyn Fn(u64, u64, Option<&[u8]>, bool) -> bool + 'a;

fn write_compacted(
    dir: &Path,
    seg: &Arc<Segment>,
    hwm: u64,
    keep: &KeepFn<'_>,
) -> io::Result<CompactionJob> {
    let base = seg.base_offset;
    let (tmp_seg, tmp_idx) = tmp_paths(dir, base);
    let mut w = BufWriter::with_capacity(1 << 20, File::create(&tmp_seg)?);
    w.write_all(&header_bytes(base))?;
    let mut pos = SEGMENT_HEADER_LEN;
    let mut ib = IndexBuilder::default();
    let mut entries = Vec::new();
    let mut first_ts = None;
    let mut last_offset = None;
    let mut records = 0u64;
    for_each_frame(seg, hwm, |offset, ts, key, tombstone, raw| {
        if !keep(offset, ts, key, tombstone) {
            return Ok(());
        }
        if let Some(e) = ib.observe(offset, pos, ts) {
            entries.push(e);
        }
        first_ts.get_or_insert(ts);
        last_offset = Some(offset);
        records += 1;
        w.write_all(raw)?;
        pos += raw.len() as u64;
        Ok(())
    })?;
    w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    {
        let mut f = File::create(&tmp_idx)?;
        f.write_all(&encode_index(&entries))?;
        f.sync_all()?;
    }
    let meta = SegmentMeta {
        base_offset: base,
        len: pos,
        end_offset: last_offset.map_or(base, |o| o + 1),
        first_ts,
        max_ts: (records > 0).then_some(ib.max_ts),
        records,
        index_entries: entries.len() as u64,
    };
    Ok(CompactionJob {
        old: seg.clone(),
        tmp_seg,
        tmp_idx,
        meta,
    })
}
