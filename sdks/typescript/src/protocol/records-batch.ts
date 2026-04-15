import { writeString, readString, writeU32, readU32, writeU64, readU64, writeBytes, readBytes, writeHeaders, readHeaders, stringByteLength, headersSize } from "./primitives.js";
import { FLAG_HAS_KEY, type BatchRecord } from "./types.js";

export function encodeRecordsBatch(records: BatchRecord[]): Buffer {
  let size = 4;
  for (const r of records) {
    size += 8 + 8 + stringByteLength(r.subject) + 1;
    if (r.key !== undefined) size += 4 + r.key.length;
    size += 4 + r.value.length;
    size += headersSize(r.headers);
  }
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeU32(buf, pos, records.length);
  for (const r of records) {
    pos += writeU64(buf, pos, r.offset);
    pos += writeU64(buf, pos, r.timestamp);
    pos += writeString(buf, pos, r.subject);
    const hasKey = r.key !== undefined;
    buf[pos] = hasKey ? FLAG_HAS_KEY : 0; pos += 1;
    if (hasKey) pos += writeBytes(buf, pos, r.key!);
    pos += writeBytes(buf, pos, r.value);
    pos += writeHeaders(buf, pos, r.headers);
  }
  return buf;
}

export function decodeRecordsBatch(buf: Buffer): BatchRecord[] {
  let pos = 0;
  const [count] = readU32(buf, pos); pos += 4;
  const records: BatchRecord[] = [];
  for (let i = 0; i < count; i++) {
    const [offset] = readU64(buf, pos); pos += 8;
    const [timestamp] = readU64(buf, pos); pos += 8;
    const [subject, subjectLen] = readString(buf, pos); pos += subjectLen;
    const flags = buf[pos]; pos += 1;
    let key: Buffer | undefined;
    if (flags & FLAG_HAS_KEY) { const [k, kLen] = readBytes(buf, pos); key = k; pos += kLen; }
    const [value, valueLen] = readBytes(buf, pos); pos += valueLen;
    const [headers, headersLen] = readHeaders(buf, pos); pos += headersLen;
    records.push({ offset, timestamp, subject, key, value, headers });
  }
  return records;
}
