import { writeString, readString, writeBytes, readBytes, writeHeaders, readHeaders, stringByteLength, headersSize } from "./primitives.js";
import { FLAG_HAS_KEY, type PublishRequest } from "./types.js";

export function encodePublish(req: PublishRequest): Buffer {
  const hasKey = req.key !== undefined;
  const headers = req.headers ?? [];
  let size = stringByteLength(req.stream) + stringByteLength(req.subject) + 1;
  if (hasKey) size += 4 + req.key!.length;
  size += 4 + req.value.length;
  size += headersSize(headers);
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.stream);
  pos += writeString(buf, pos, req.subject);
  buf[pos] = hasKey ? FLAG_HAS_KEY : 0;
  pos += 1;
  if (hasKey) pos += writeBytes(buf, pos, req.key!);
  pos += writeBytes(buf, pos, req.value);
  writeHeaders(buf, pos, headers);
  return buf;
}

export function decodePublish(buf: Buffer): PublishRequest {
  let pos = 0;
  const [stream, streamLen] = readString(buf, pos); pos += streamLen;
  const [subject, subjectLen] = readString(buf, pos); pos += subjectLen;
  const flags = buf[pos]; pos += 1;
  let key: Buffer | undefined;
  if (flags & FLAG_HAS_KEY) { const [k, kLen] = readBytes(buf, pos); key = k; pos += kLen; }
  const [value, valueLen] = readBytes(buf, pos); pos += valueLen;
  const [headers] = readHeaders(buf, pos);
  return { stream, subject, value, key, headers };
}

export function decodePublishOk(buf: Buffer): { offset: bigint } {
  return { offset: buf.readBigUInt64LE(0) };
}
