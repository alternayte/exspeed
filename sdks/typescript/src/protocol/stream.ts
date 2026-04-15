import { writeString, readString, writeU64, readU64, stringByteLength } from "./primitives.js";
import type { CreateStreamRequest } from "./types.js";

export function encodeCreateStream(req: CreateStreamRequest): Buffer {
  const size = stringByteLength(req.name) + 8 + 8;
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.name);
  pos += writeU64(buf, pos, req.maxAgeSecs);
  writeU64(buf, pos, req.maxBytes);
  return buf;
}

export function decodeCreateStream(buf: Buffer): CreateStreamRequest {
  let pos = 0;
  const [name, nameLen] = readString(buf, pos); pos += nameLen;
  const [maxAgeSecs] = readU64(buf, pos); pos += 8;
  const [maxBytes] = readU64(buf, pos);
  return { name, maxAgeSecs, maxBytes };
}
