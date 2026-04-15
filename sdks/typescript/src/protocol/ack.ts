import { writeString, readString, writeU64, readU64, stringByteLength } from "./primitives.js";
import type { AckRequest } from "./types.js";

export function encodeAck(req: AckRequest): Buffer {
  const size = stringByteLength(req.consumerName) + 8;
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.consumerName);
  writeU64(buf, pos, req.offset);
  return buf;
}

export function decodeAck(buf: Buffer): AckRequest {
  let pos = 0;
  const [consumerName, nameLen] = readString(buf, pos); pos += nameLen;
  const [offset] = readU64(buf, pos);
  return { consumerName, offset };
}
