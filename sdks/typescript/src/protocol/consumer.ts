import { writeString, readString, writeU64, readU64, stringByteLength } from "./primitives.js";
import type { CreateConsumerRequest } from "./types.js";

export function encodeCreateConsumer(req: CreateConsumerRequest): Buffer {
  const size = stringByteLength(req.name) + stringByteLength(req.stream) + stringByteLength(req.group) + stringByteLength(req.subjectFilter) + 1 + 8;
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.name);
  pos += writeString(buf, pos, req.stream);
  pos += writeString(buf, pos, req.group);
  pos += writeString(buf, pos, req.subjectFilter);
  buf[pos] = req.startFrom; pos += 1;
  writeU64(buf, pos, req.startOffset);
  return buf;
}

export function decodeCreateConsumer(buf: Buffer): CreateConsumerRequest {
  let pos = 0;
  const [name, nameLen] = readString(buf, pos); pos += nameLen;
  const [stream, streamLen] = readString(buf, pos); pos += streamLen;
  const [group, groupLen] = readString(buf, pos); pos += groupLen;
  const [subjectFilter, filterLen] = readString(buf, pos); pos += filterLen;
  const startFrom = buf[pos]; pos += 1;
  const [startOffset] = readU64(buf, pos);
  return { name, stream, group, subjectFilter, startFrom, startOffset };
}

export function encodeDeleteConsumer(name: string): Buffer {
  const buf = Buffer.alloc(stringByteLength(name));
  writeString(buf, 0, name);
  return buf;
}

export function decodeDeleteConsumer(buf: Buffer): string {
  const [name] = readString(buf, 0);
  return name;
}
