import { writeString, readString, stringByteLength } from "./primitives.js";

export function encodeSubscribe(consumerName: string): Buffer {
  const buf = Buffer.alloc(stringByteLength(consumerName));
  writeString(buf, 0, consumerName);
  return buf;
}

export function decodeSubscribe(buf: Buffer): string {
  const [name] = readString(buf, 0);
  return name;
}
