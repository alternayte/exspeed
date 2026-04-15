import { writeString, readString, writeU16, readU16, writeU64, readU64, writeBytes, readBytes, writeHeaders, readHeaders, stringByteLength, headersSize } from "./primitives.js";
import { FLAG_HAS_KEY, type RecordDelivery } from "./types.js";

export function encodeRecord(record: RecordDelivery): Buffer {
  const hasKey = record.key !== undefined;
  const headers = record.headers ?? [];
  let size = stringByteLength(record.consumerName) + 8 + 8 + stringByteLength(record.subject) + 2 + 1;
  if (hasKey) size += 4 + record.key!.length;
  size += 4 + record.value.length;
  size += headersSize(headers);
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, record.consumerName);
  pos += writeU64(buf, pos, record.offset);
  pos += writeU64(buf, pos, record.timestamp);
  pos += writeString(buf, pos, record.subject);
  pos += writeU16(buf, pos, record.deliveryAttempt);
  buf[pos] = hasKey ? FLAG_HAS_KEY : 0; pos += 1;
  if (hasKey) pos += writeBytes(buf, pos, record.key!);
  pos += writeBytes(buf, pos, record.value);
  writeHeaders(buf, pos, headers);
  return buf;
}

export function decodeRecord(buf: Buffer): RecordDelivery {
  let pos = 0;
  const [consumerName, nameLen] = readString(buf, pos); pos += nameLen;
  const [offset] = readU64(buf, pos); pos += 8;
  const [timestamp] = readU64(buf, pos); pos += 8;
  const [subject, subjectLen] = readString(buf, pos); pos += subjectLen;
  const [deliveryAttempt] = readU16(buf, pos); pos += 2;
  const flags = buf[pos]; pos += 1;
  let key: Buffer | undefined;
  if (flags & FLAG_HAS_KEY) { const [k, kLen] = readBytes(buf, pos); key = k; pos += kLen; }
  const [value, valueLen] = readBytes(buf, pos); pos += valueLen;
  const [headers] = readHeaders(buf, pos);
  return { consumerName, offset, timestamp, subject, deliveryAttempt, key, value, headers };
}
