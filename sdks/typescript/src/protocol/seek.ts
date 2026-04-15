import { writeString, readString, writeU64, readU64, stringByteLength } from "./primitives.js";
import type { SeekRequest } from "./types.js";

export function encodeSeek(req: SeekRequest): Buffer {
  const size = stringByteLength(req.consumerName) + 8;
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.consumerName);
  writeU64(buf, pos, req.timestamp);
  return buf;
}

export function decodeSeek(buf: Buffer): SeekRequest {
  let pos = 0;
  const [consumerName, nameLen] = readString(buf, pos); pos += nameLen;
  const [timestamp] = readU64(buf, pos);
  return { consumerName, timestamp };
}
