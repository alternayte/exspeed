import { writeString, readString, writeU64, readU64, writeU32, readU32, stringByteLength } from "./primitives.js";
import type { FetchRequest } from "./types.js";

export function encodeFetch(req: FetchRequest): Buffer {
  const filter = req.subjectFilter ?? "";
  const size = stringByteLength(req.stream) + 8 + 4 + stringByteLength(filter);
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.stream);
  pos += writeU64(buf, pos, req.offset);
  pos += writeU32(buf, pos, req.maxRecords);
  writeString(buf, pos, filter);
  return buf;
}

export function decodeFetch(buf: Buffer): FetchRequest {
  let pos = 0;
  const [stream, streamLen] = readString(buf, pos); pos += streamLen;
  const [offset] = readU64(buf, pos); pos += 8;
  const [maxRecords] = readU32(buf, pos); pos += 4;
  const [subjectFilter] = readString(buf, pos);
  return { stream, offset, maxRecords, subjectFilter };
}
