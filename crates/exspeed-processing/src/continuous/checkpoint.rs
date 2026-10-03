//! Durable checkpoints of continuous queries.
//!
//! A checkpoint holds the source positions, per-source max event time,
//! output positions, counters and operator state (Arrow IPC). It is written
//! through the [`Log`] to the internal stream `__exql_ckpt_<query_id>`, split
//! into parts of at most [`PART_BYTES`] written in one `append_batch`. The
//! newest complete checkpoint wins; restore reads the tail of the stream.

use std::io::Cursor;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use exspeed_broker::log::Log;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageEngine, StorageError, StreamConfig};
use serde::{Deserialize, Serialize};

use crate::error::ExqlError;

const MAGIC: &[u8; 8] = b"EXQLCK01";
pub const PART_BYTES: usize = 4 * 1024 * 1024;
const H_SEQ: &str = "x-exql-ckpt-seq";
const H_PART: &str = "x-exql-ckpt-part";
const H_PARTS: &str = "x-exql-ckpt-parts";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceState {
    pub stream: String,
    pub next_offset: u64,
    pub max_et: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputPos {
    pub stream: String,
    /// End of the output stream when the checkpoint was taken.
    pub offset: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointMeta {
    pub version: u32,
    pub query_id: String,
    pub seq: u64,
    pub sources: Vec<SourceState>,
    pub outputs: Vec<OutputPos>,
    pub records_in: u64,
    pub records_out: u64,
    pub late_dropped: u64,
    pub sections: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Checkpoint {
    pub meta: CheckpointMeta,
    pub sections: Vec<(String, RecordBatch)>,
}

impl Checkpoint {
    pub fn section(&self, name: &str) -> Option<&RecordBatch> {
        self.sections
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| b)
    }
}

/// Name of a query's checkpoint stream.
pub fn ckpt_stream(query_id: &str) -> Result<StreamName, ExqlError> {
    StreamName::try_from(format!("__exql_ckpt_{query_id}"))
        .map_err(|e| ExqlError::Internal(e.to_string()))
}

fn put_u32(out: &mut Vec<u8>, v: usize) -> Result<(), ExqlError> {
    let v =
        u32::try_from(v).map_err(|_| ExqlError::Internal("checkpoint section too large".into()))?;
    out.extend_from_slice(&v.to_le_bytes());
    Ok(())
}

fn get_u32(b: &[u8], pos: &mut usize) -> Result<usize, ExqlError> {
    let bad = || ExqlError::Storage("truncated checkpoint".into());
    let s = b.get(*pos..*pos + 4).ok_or_else(bad)?;
    *pos += 4;
    Ok(u32::from_le_bytes(s.try_into().unwrap()) as usize)
}

pub fn encode(ck: &Checkpoint) -> Result<Vec<u8>, ExqlError> {
    let mut meta = ck.meta.clone();
    meta.sections = ck.sections.iter().map(|(n, _)| n.clone()).collect();
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    let m = serde_json::to_vec(&meta).map_err(|e| ExqlError::Internal(e.to_string()))?;
    put_u32(&mut out, m.len())?;
    out.extend_from_slice(&m);
    for (_, batch) in &ck.sections {
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &batch.schema())?;
            w.write(batch)?;
            w.finish()?;
        }
        put_u32(&mut out, buf.len())?;
        out.extend_from_slice(&buf);
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<Checkpoint, ExqlError> {
    let bad = |m: &str| ExqlError::Storage(format!("invalid checkpoint: {m}"));
    if bytes.len() < MAGIC.len() + 8 || &bytes[..8] != MAGIC {
        return Err(bad("bad magic"));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad("checksum mismatch"));
    }
    let mut pos = 8;
    let mlen = get_u32(body, &mut pos)?;
    let meta: CheckpointMeta =
        serde_json::from_slice(body.get(pos..pos + mlen).ok_or_else(|| bad("meta"))?)
            .map_err(|e| bad(&e.to_string()))?;
    pos += mlen;
    let mut sections = Vec::new();
    for name in &meta.sections {
        let len = get_u32(body, &mut pos)?;
        let chunk = body.get(pos..pos + len).ok_or_else(|| bad("section"))?;
        pos += len;
        let mut r = StreamReader::try_new(Cursor::new(chunk), None)?;
        let batch = match r.next() {
            Some(b) => b?,
            None => RecordBatch::new_empty(r.schema()),
        };
        sections.push((name.clone(), batch));
    }
    Ok(Checkpoint { meta, sections })
}

/// Settings for internal streams: long retention, bounded size.
pub fn internal_stream_config() -> StreamConfig {
    StreamConfig {
        max_age_secs: 10 * 365 * 86_400,
        max_bytes: 1024 * 1024 * 1024,
        ..StreamConfig::default()
    }
}

/// Create the checkpoint stream if needed.
pub async fn ensure_stream(log: &Log, stream: &StreamName) -> Result<(), ExqlError> {
    match log.storage().stream_bounds(stream).await {
        Ok(_) => Ok(()),
        Err(StorageError::StreamNotFound(_)) => {
            match log.create_stream(stream, &internal_stream_config()).await {
                Ok(()) => Ok(()),
                Err(exspeed_broker::log::LogError::Storage(StorageError::StreamAlreadyExists(
                    _,
                ))) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// Write a checkpoint.
pub async fn save(log: &Log, stream: &StreamName, ck: &Checkpoint) -> Result<(), ExqlError> {
    let bytes = encode(ck)?;
    let parts: Vec<&[u8]> = bytes.chunks(PART_BYTES).collect();
    let n = parts.len();
    let records = parts
        .into_iter()
        .enumerate()
        .map(|(i, p)| Record {
            key: None,
            value: Bytes::copy_from_slice(p),
            subject: String::new(),
            headers: vec![
                (H_SEQ.into(), ck.meta.seq.to_string()),
                (H_PART.into(), i.to_string()),
                (H_PARTS.into(), n.to_string()),
            ],
            timestamp_ns: None,
        })
        .collect();
    log.append_batch(stream, records).await?;
    Ok(())
}

fn header<'a>(r: &'a exspeed_streams::StoredRecord, k: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(h, _)| h == k)
        .map(|(_, v)| v.as_str())
}

async fn read_one(
    storage: &dyn StorageEngine,
    stream: &StreamName,
    off: u64,
) -> Result<Option<exspeed_streams::StoredRecord>, ExqlError> {
    let b = storage
        .read_batch(
            stream,
            Offset(off),
            ReadLimits {
                max_records: 1,
                max_bytes: PART_BYTES * 2,
            },
        )
        .await?;
    Ok(b.records.into_iter().find(|r| r.offset.0 == off))
}

/// Load the newest complete checkpoint, if any.
pub async fn load(
    storage: &dyn StorageEngine,
    stream: &StreamName,
) -> Result<Option<Checkpoint>, ExqlError> {
    let (earliest, next) = match storage.stream_bounds(stream).await {
        Ok(b) => b,
        Err(StorageError::StreamNotFound(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut end = next.0;
    let mut attempts = 0;
    while end > earliest.0 && attempts < 16 {
        attempts += 1;
        let Some(last) = read_one(storage, stream, end - 1).await? else {
            end -= 1;
            continue;
        };
        let parse = |r: &exspeed_streams::StoredRecord, k: &str| -> Option<u64> {
            header(r, k)?.parse().ok()
        };
        let (Some(seq), Some(part), Some(parts)) = (
            parse(&last, H_SEQ),
            parse(&last, H_PART),
            parse(&last, H_PARTS),
        ) else {
            end -= 1;
            continue;
        };
        if part + 1 != parts || parts == 0 || parts > end - earliest.0 {
            end -= 1;
            continue;
        }
        let first = end - parts;
        let mut bytes = Vec::new();
        let mut ok = true;
        for (i, off) in (first..end).enumerate() {
            let Some(r) = read_one(storage, stream, off).await? else {
                ok = false;
                break;
            };
            if parse(&r, H_SEQ) != Some(seq) || parse(&r, H_PART) != Some(i as u64) {
                ok = false;
                break;
            }
            bytes.extend_from_slice(&r.value);
        }
        if ok {
            return decode(&bytes).map(Some);
        }
        end -= 1;
    }
    if end > earliest.0 {
        return Err(ExqlError::Storage(format!(
            "no readable checkpoint at the end of '{stream}'"
        )));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn round_trip() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let ck = Checkpoint {
            meta: CheckpointMeta {
                version: 1,
                query_id: "q".into(),
                seq: 7,
                sources: vec![SourceState {
                    stream: "s".into(),
                    next_offset: 10,
                    max_et: Some(5),
                }],
                ..Default::default()
            },
            sections: vec![("agg".into(), batch.clone())],
        };
        let bytes = encode(&ck).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.meta.seq, 7);
        assert_eq!(back.meta.sources, ck.meta.sources);
        assert_eq!(back.section("agg").unwrap(), &batch);
        let mut bad = bytes.clone();
        bad[20] ^= 1;
        assert!(decode(&bad).is_err());
    }
}
