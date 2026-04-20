import { writeString, readString, writeBytes, readBytes, writeHeaders, readHeaders, stringByteLength, headersSize } from "./primitives.js";
import { FLAG_HAS_KEY, FLAG_HAS_MSG_ID, MAX_MSG_ID_BYTES, type PublishRequest } from "./types.js";

export function encodePublish(req: PublishRequest): Buffer {
  const hasKey = req.key !== undefined;
  const hasMsgId = req.msgId !== undefined;
  const headers = req.headers ?? [];
  const value = req.value instanceof Buffer ? req.value : Buffer.from(req.value);

  let msgIdBytes: Buffer | undefined;
  if (hasMsgId) {
    msgIdBytes = Buffer.from(req.msgId!, "utf8");
    if (msgIdBytes.length > MAX_MSG_ID_BYTES) {
      throw new RangeError(`msgId exceeds maximum of ${MAX_MSG_ID_BYTES} bytes`);
    }
  }

  let size = stringByteLength(req.stream) + stringByteLength(req.subject) + 1;
  if (hasKey) size += 4 + req.key!.length;
  if (hasMsgId) size += 2 + msgIdBytes!.length;
  size += 4 + value.length;
  size += headersSize(headers);

  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.stream);
  pos += writeString(buf, pos, req.subject);

  let flags = 0;
  if (hasKey) flags |= FLAG_HAS_KEY;
  if (hasMsgId) flags |= FLAG_HAS_MSG_ID;
  buf[pos] = flags;
  pos += 1;

  if (hasKey) pos += writeBytes(buf, pos, req.key!);
  if (hasMsgId) {
    buf.writeUInt16LE(msgIdBytes!.length, pos); pos += 2;
    msgIdBytes!.copy(buf, pos); pos += msgIdBytes!.length;
  }
  pos += writeBytes(buf, pos, value);
  writeHeaders(buf, pos, headers);
  return buf;
}

/** Alias for encodePublish — preferred name for new code. */
export const encodePublishRequest = encodePublish;

export function decodePublish(buf: Buffer): PublishRequest {
  let pos = 0;
  const [stream, streamLen] = readString(buf, pos); pos += streamLen;
  const [subject, subjectLen] = readString(buf, pos); pos += subjectLen;
  const flags = buf[pos]; pos += 1;
  let key: Buffer | undefined;
  if (flags & FLAG_HAS_KEY) { const [k, kLen] = readBytes(buf, pos); key = k; pos += kLen; }
  let msgId: string | undefined;
  if (flags & FLAG_HAS_MSG_ID) {
    const msgIdLen = buf.readUInt16LE(pos); pos += 2;
    msgId = buf.toString("utf8", pos, pos + msgIdLen); pos += msgIdLen;
  }
  const [value, valueLen] = readBytes(buf, pos); pos += valueLen;
  const [headers] = readHeaders(buf, pos);
  return { stream, subject, value, key, headers, msgId };
}

export function decodePublishOk(buf: Buffer): { offset: bigint; duplicate: boolean } {
  const offset = buf.readBigUInt64LE(0);
  const duplicate = buf.length >= 9 ? buf[8] !== 0 : false;
  return { offset, duplicate };
}
