//! The record encoding shared by segment files and the client protocol.
//!
//! A record is stored on disk in exactly the bytes a client receives for it
//! in a `Deliver`, `Messages` or `ReadResult` frame (`WireRecord` in
//! `docs/protocol.md`), so the server answers reads by copying byte ranges
//! out of segment files instead of decoding and re-encoding records.
//!
//! All integers are little-endian:
//!
//! | Bytes | Field | Notes |
//! |-------|-------|-------|
//! | 0–3   | `len` `u32` | bytes after this field (record size − 4) |
//! | 4–7   | `crc` `u32` | CRC32C of bytes 10..end (everything after `delivery_count`) |
//! | 8–9   | `delivery_count` `u16` | 0 on disk; the server patches it in place on consumer delivery, which is why the CRC skips it |
//! | 10–17 | `offset` `u64` | |
//! | 18–25 | `timestamp_ns` `u64` | append time, nanoseconds since the Unix epoch |
//! | 26–   | `subject` | `u16` length + UTF-8 |
//! |       | `key` | `u8` flag (0 absent, 1 present) [+ `u32` length + bytes] |
//! |       | `value` | `u32` length + bytes |
//! |       | `headers` | `u16` count + (`u16` len + UTF-8 key, `u16` len + UTF-8 value) pairs |
//!
//! Encoders validate every length against its field width and fail instead
//! of truncating. Parsers bounds-check every length against the bytes
//! present, so a corrupt length never causes a large allocation or a panic.

use std::ops::Range;

/// Size of the fixed prefix: `len`, `crc`, `delivery_count`.
pub const HEADER_LEN: usize = 10;
/// Byte position of the `crc` field.
pub const CRC_AT: usize = 4;
/// Byte position of the `delivery_count` field.
pub const DELIVERY_COUNT_AT: usize = 8;
/// Byte position of the `offset` field (also where CRC coverage starts).
pub const OFFSET_AT: usize = 10;
/// Byte position of the `timestamp_ns` field.
pub const TIMESTAMP_AT: usize = 18;
/// Byte position of the subject length.
pub const SUBJECT_AT: usize = 26;
/// Smallest possible record: header + offset + timestamp + empty subject +
/// absent key + empty value + no headers.
pub const MIN_RECORD_LEN: usize = HEADER_LEN + 8 + 8 + 2 + 1 + 4 + 2;
/// Largest record (total size) the server writes or accepts. Values are
/// limited to 8 MiB by the write path, so anything above this is corruption.
pub const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

/// A record that can't be encoded, or bytes that aren't a valid record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatError(pub String);

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FormatError {}

fn err<T>(msg: impl Into<String>) -> Result<T, FormatError> {
    Err(FormatError(msg.into()))
}

/// The fields of one record, borrowed.
#[derive(Debug, Clone, Copy)]
pub struct Fields<'a> {
    pub offset: u64,
    pub timestamp_ns: u64,
    pub delivery_count: u16,
    pub subject: &'a str,
    pub key: Option<&'a [u8]>,
    pub value: &'a [u8],
    pub headers: &'a [(String, String)],
}

fn check_len(what: &str, len: usize, max: usize) -> Result<(), FormatError> {
    if len > max {
        err(format!("{what} is {len} bytes; the limit is {max}"))
    } else {
        Ok(())
    }
}

/// Total encoded size of a record, validating every field width.
pub fn encoded_len(f: &Fields<'_>) -> Result<usize, FormatError> {
    check_len("subject", f.subject.len(), u16::MAX as usize)?;
    if let Some(k) = f.key {
        check_len("key", k.len(), u32::MAX as usize)?;
    }
    check_len("value", f.value.len(), u32::MAX as usize)?;
    if f.headers.len() > u16::MAX as usize {
        return err(format!(
            "{} headers; the limit is {}",
            f.headers.len(),
            u16::MAX
        ));
    }
    let mut len = MIN_RECORD_LEN + f.subject.len() + f.value.len();
    if let Some(k) = f.key {
        len += 4 + k.len();
    }
    for (k, v) in f.headers {
        check_len("header key", k.len(), u16::MAX as usize)?;
        check_len("header value", v.len(), u16::MAX as usize)?;
        len += 4 + k.len() + v.len();
    }
    check_len("encoded record", len, MAX_RECORD_LEN)?;
    Ok(len)
}

/// Append one encoded record to `dst` and return its size. On error `dst`
/// is left untouched.
pub fn encode<B>(dst: &mut B, f: &Fields<'_>) -> Result<usize, FormatError>
where
    B: bytes::BufMut + AsMut<[u8]>,
{
    let size = encoded_len(f)?;
    let start = dst.as_mut().len();
    dst.put_u32_le((size - 4) as u32);
    dst.put_u32_le(0); // CRC, filled in below
    dst.put_u16_le(f.delivery_count);
    dst.put_u64_le(f.offset);
    dst.put_u64_le(f.timestamp_ns);
    dst.put_u16_le(f.subject.len() as u16);
    dst.put_slice(f.subject.as_bytes());
    match f.key {
        None => dst.put_u8(0),
        Some(k) => {
            dst.put_u8(1);
            dst.put_u32_le(k.len() as u32);
            dst.put_slice(k);
        }
    }
    dst.put_u32_le(f.value.len() as u32);
    dst.put_slice(f.value);
    dst.put_u16_le(f.headers.len() as u16);
    for (k, v) in f.headers {
        dst.put_u16_le(k.len() as u16);
        dst.put_slice(k.as_bytes());
        dst.put_u16_le(v.len() as u16);
        dst.put_slice(v.as_bytes());
    }
    let rec = &mut dst.as_mut()[start..];
    debug_assert_eq!(rec.len(), size);
    let crc = crc32c::crc32c(&rec[OFFSET_AT..]);
    rec[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(size)
}

/// Total size of the record starting at `buf[0]`, read from its `len`
/// field (`buf` needs at least 4 bytes). Fails when the length is out of
/// bounds.
pub fn record_size(buf: &[u8]) -> Result<usize, FormatError> {
    if buf.len() < 4 {
        return err(format!("truncated record: {} bytes", buf.len()));
    }
    let size = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize + 4;
    if !(MIN_RECORD_LEN..=MAX_RECORD_LEN).contains(&size) {
        return err(format!("record length {size} out of bounds"));
    }
    Ok(size)
}

/// Verify the CRC of one complete record.
pub fn check_crc(rec: &[u8]) -> Result<(), FormatError> {
    if rec.len() < MIN_RECORD_LEN {
        return err(format!("truncated record: {} bytes", rec.len()));
    }
    let stored = u32::from_le_bytes(rec[CRC_AT..CRC_AT + 4].try_into().unwrap());
    let computed = crc32c::crc32c(&rec[OFFSET_AT..]);
    if stored != computed {
        return err(format!(
            "CRC mismatch: stored {stored:#010x}, computed {computed:#010x}"
        ));
    }
    Ok(())
}

/// The `offset` of a record (`rec` must hold at least [`MIN_RECORD_LEN`]
/// bytes, which [`record_size`] guarantees for a complete record).
pub fn offset(rec: &[u8]) -> u64 {
    u64::from_le_bytes(rec[OFFSET_AT..OFFSET_AT + 8].try_into().unwrap())
}

/// The `timestamp_ns` of a record.
pub fn timestamp_ns(rec: &[u8]) -> u64 {
    u64::from_le_bytes(rec[TIMESTAMP_AT..TIMESTAMP_AT + 8].try_into().unwrap())
}

/// The `delivery_count` of a record.
pub fn delivery_count(rec: &[u8]) -> u16 {
    u16::from_le_bytes(
        rec[DELIVERY_COUNT_AT..DELIVERY_COUNT_AT + 2]
            .try_into()
            .unwrap(),
    )
}

/// Overwrite the `delivery_count` of a record in place. The CRC stays
/// valid because it does not cover this field.
pub fn set_delivery_count(rec: &mut [u8], count: u16) {
    rec[DELIVERY_COUNT_AT..DELIVERY_COUNT_AT + 2].copy_from_slice(&count.to_le_bytes());
}

/// The subject of a record, parsed in place without allocating.
pub fn subject(rec: &[u8]) -> Result<&str, FormatError> {
    let mut c = Cursor::new(rec, SUBJECT_AT);
    let n = c.u16()?;
    let r = c.take(n)?;
    std::str::from_utf8(&rec[r]).map_err(|e| FormatError(format!("invalid subject UTF-8: {e}")))
}

/// Where each variable-length field of a record lives (byte ranges into
/// the record), after full structural validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    pub offset: u64,
    pub timestamp_ns: u64,
    pub delivery_count: u16,
    pub subject: Range<usize>,
    pub key: Option<Range<usize>>,
    pub value: Range<usize>,
    /// `(key, value)` byte ranges of each header.
    pub headers: Vec<(Range<usize>, Range<usize>)>,
}

impl Layout {
    /// The subject (validated as UTF-8 by [`layout`]).
    pub fn subject<'a>(&self, rec: &'a [u8]) -> &'a str {
        std::str::from_utf8(&rec[self.subject.clone()]).unwrap_or_default()
    }

    /// The headers as owned strings (validated as UTF-8 by [`layout`]).
    pub fn headers(&self, rec: &[u8]) -> Vec<(String, String)> {
        let s = |r: &Range<usize>| {
            std::str::from_utf8(&rec[r.clone()])
                .unwrap_or_default()
                .to_owned()
        };
        self.headers.iter().map(|(k, v)| (s(k), s(v))).collect()
    }
}

/// Validate one complete record (`rec.len()` must equal its `len` field +
/// 4) and locate its fields. Does not check the CRC.
pub fn layout(rec: &[u8]) -> Result<Layout, FormatError> {
    let size = record_size(rec)?;
    if size != rec.len() {
        return err(format!(
            "record length field says {size} bytes, have {}",
            rec.len()
        ));
    }
    let mut c = Cursor::new(rec, SUBJECT_AT);
    let n = c.u16()?;
    let subject = c.take(n)?;
    std::str::from_utf8(&rec[subject.clone()])
        .map_err(|e| FormatError(format!("invalid subject UTF-8: {e}")))?;
    let key = match c.u8()? {
        0 => None,
        1 => {
            let n = c.u32()?;
            Some(c.take(n)?)
        }
        f => return err(format!("invalid key flag {f}")),
    };
    let n = c.u32()?;
    let value = c.take(n)?;
    let count = c.u16()?;
    // Each header needs at least 4 bytes; never pre-allocate more than the
    // remaining bytes could hold.
    let mut headers = Vec::with_capacity(count.min((rec.len() - c.pos) / 4));
    for _ in 0..count {
        let n = c.u16()?;
        let k = c.take(n)?;
        let n = c.u16()?;
        let v = c.take(n)?;
        for r in [&k, &v] {
            std::str::from_utf8(&rec[r.clone()])
                .map_err(|e| FormatError(format!("invalid header UTF-8: {e}")))?;
        }
        headers.push((k, v));
    }
    if c.pos != rec.len() {
        return err(format!("{} trailing bytes after record", rec.len() - c.pos));
    }
    Ok(Layout {
        offset: offset(rec),
        timestamp_ns: timestamp_ns(rec),
        delivery_count: delivery_count(rec),
        subject,
        key,
        value,
        headers,
    })
}

/// The key and the value length of a record, without decoding the rest.
pub fn key_and_value_len(rec: &[u8]) -> Result<(Option<&[u8]>, usize), FormatError> {
    let mut c = Cursor::new(rec, SUBJECT_AT);
    let n = c.u16()?;
    c.take(n)?;
    let key = match c.u8()? {
        0 => None,
        1 => {
            let n = c.u32()?;
            Some(&rec[c.take(n)?])
        }
        f => return err(format!("invalid key flag {f}")),
    };
    Ok((key, c.u32()?))
}

/// The value of the first header named `name`, parsed in place without
/// allocating. `None` when the record has no such header or is malformed.
pub fn header<'a>(rec: &'a [u8], name: &str) -> Option<&'a str> {
    let mut c = Cursor::new(rec, SUBJECT_AT);
    let n = c.u16().ok()?;
    c.take(n).ok()?;
    match c.u8().ok()? {
        0 => {}
        1 => {
            let n = c.u32().ok()?;
            c.take(n).ok()?;
        }
        _ => return None,
    }
    let n = c.u32().ok()?;
    c.take(n).ok()?;
    let count = c.u16().ok()?;
    for _ in 0..count {
        let n = c.u16().ok()?;
        let k = c.take(n).ok()?;
        let n = c.u16().ok()?;
        let v = c.take(n).ok()?;
        if &rec[k] == name.as_bytes() {
            return std::str::from_utf8(&rec[v]).ok();
        }
    }
    None
}

/// One record inside a buffer of back-to-back records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPos {
    /// Byte position of the record in the buffer.
    pub start: usize,
    /// Total size of the record.
    pub size: usize,
    pub offset: u64,
}

impl RecordPos {
    pub fn range(&self) -> Range<usize> {
        self.start..self.start + self.size
    }
    pub fn end(&self) -> usize {
        self.start + self.size
    }
}

/// Iterate the records of a buffer holding back-to-back encoded records.
/// Yields an error (and then stops) at a record whose length is out of
/// bounds or runs past the end of the buffer.
pub fn iter(buf: &[u8]) -> Iter<'_> {
    Iter { buf, pos: 0 }
}

pub struct Iter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Iterator for Iter<'_> {
    type Item = Result<RecordPos, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let rest = &self.buf[self.pos..];
        let r = record_size(rest).and_then(|size| {
            if size > rest.len() {
                err(format!(
                    "record of {size} bytes at {} runs past the end of the buffer",
                    self.pos
                ))
            } else {
                Ok(RecordPos {
                    start: self.pos,
                    size,
                    offset: offset(rest),
                })
            }
        });
        match &r {
            Ok(p) => self.pos += p.size,
            Err(_) => self.pos = self.buf.len(),
        }
        Some(r)
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }
    fn take(&mut self, n: usize) -> Result<Range<usize>, FormatError> {
        let have = self.buf.len().saturating_sub(self.pos);
        if have < n {
            return err(format!(
                "truncated record: need {n} bytes at {}, have {have}",
                self.pos
            ));
        }
        let r = self.pos..self.pos + n;
        self.pos += n;
        Ok(r)
    }
    fn u8(&mut self) -> Result<u8, FormatError> {
        let r = self.take(1)?;
        Ok(self.buf[r.start])
    }
    fn u16(&mut self) -> Result<usize, FormatError> {
        let r = self.take(2)?;
        Ok(u16::from_le_bytes(self.buf[r].try_into().unwrap()) as usize)
    }
    fn u32(&mut self) -> Result<usize, FormatError> {
        let r = self.take(4)?;
        Ok(u32::from_le_bytes(self.buf[r].try_into().unwrap()) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields<'a>(headers: &'a [(String, String)], key: Option<&'a [u8]>) -> Fields<'a> {
        Fields {
            offset: 42,
            timestamp_ns: 1_700_000_000_123_456_789,
            delivery_count: 0,
            subject: "orders.created",
            key,
            value: b"hello world",
            headers,
        }
    }

    #[test]
    fn layout_matches_the_documented_positions() {
        let mut buf = Vec::new();
        let n = encode(&mut buf, &fields(&[], None)).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(n, MIN_RECORD_LEN + "orders.created".len() + 11);
        assert_eq!(
            u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize,
            n - 4
        );
        assert_eq!(delivery_count(&buf), 0);
        assert_eq!(offset(&buf), 42);
        assert_eq!(timestamp_ns(&buf), 1_700_000_000_123_456_789);
        assert_eq!(subject(&buf).unwrap(), "orders.created");
        assert_eq!(buf[SUBJECT_AT + 2 + 14], 0, "key flag follows the subject");
    }

    #[test]
    fn roundtrip_with_key_and_headers() {
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("trace-id".to_string(), "abc123".to_string()),
        ];
        let mut buf = Vec::new();
        encode(&mut buf, &fields(&headers, Some(b"my-key"))).unwrap();
        check_crc(&buf).unwrap();
        let l = layout(&buf).unwrap();
        assert_eq!(l.offset, 42);
        assert_eq!(l.subject(&buf), "orders.created");
        assert_eq!(&buf[l.key.clone().unwrap()], b"my-key");
        assert_eq!(&buf[l.value.clone()], b"hello world");
        assert_eq!(l.headers(&buf), headers);
        assert_eq!(key_and_value_len(&buf).unwrap(), (Some(&b"my-key"[..]), 11));
    }

    #[test]
    fn delivery_count_is_outside_the_crc() {
        let mut buf = Vec::new();
        encode(&mut buf, &fields(&[], None)).unwrap();
        set_delivery_count(&mut buf, 7);
        assert_eq!(delivery_count(&buf), 7);
        check_crc(&buf).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(check_crc(&buf).is_err());
    }

    #[test]
    fn oversized_fields_are_rejected_not_truncated() {
        let mut buf = vec![1, 2, 3];
        let subject = "a".repeat(70_000);
        let f = Fields {
            subject: &subject,
            ..fields(&[], None)
        };
        assert!(encode(&mut buf, &f).is_err());
        assert_eq!(buf, vec![1, 2, 3], "dst must be untouched on error");
        let headers = vec![("k".to_string(), "v".repeat(70_000))];
        assert!(encode(&mut buf, &fields(&headers, None)).is_err());
        let many: Vec<(String, String)> = (0..70_000)
            .map(|_| (String::new(), String::new()))
            .collect();
        assert!(encode(&mut buf, &fields(&many, None)).is_err());
    }

    #[test]
    fn corrupt_lengths_never_overallocate_or_panic() {
        let mut buf = Vec::new();
        encode(&mut buf, &fields(&[], Some(b"k"))).unwrap();
        let key_len_at = SUBJECT_AT + 2 + "orders.created".len() + 1;
        let mut bad = buf.clone();
        bad[key_len_at..key_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(layout(&bad).is_err());
        assert!(key_and_value_len(&bad).is_err());
        assert!(record_size(&u32::MAX.to_le_bytes()).is_err());
        assert!(record_size(&[0u8; 4]).is_err());
        let mut short = buf.clone();
        short.truncate(buf.len() - 1);
        assert!(layout(&short).is_err());
        let mut flag = buf.clone();
        flag[key_len_at - 1] = 2;
        assert!(layout(&flag).is_err());
    }

    #[test]
    fn iter_walks_back_to_back_records() {
        let mut buf = Vec::new();
        for i in 0..3 {
            encode(
                &mut buf,
                &Fields {
                    offset: 10 + i,
                    ..fields(&[], None)
                },
            )
            .unwrap();
        }
        let pos: Vec<RecordPos> = iter(&buf).map(Result::unwrap).collect();
        assert_eq!(pos.len(), 3);
        assert_eq!(pos[2].offset, 12);
        assert_eq!(pos[2].end(), buf.len());
        let torn = &buf[..buf.len() - 1];
        let r: Vec<_> = iter(torn).collect();
        assert_eq!(r.len(), 3);
        assert!(r[2].is_err());
    }
}
