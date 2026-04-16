import { readString, stringByteLength, writeString } from "./primitives.js";

export function encodeSubscribe(consumerName: string, subscriberId: string): Buffer {
  const totalLen = stringByteLength(consumerName) + stringByteLength(subscriberId);
  const buf = Buffer.alloc(totalLen);
  const offset = writeString(buf, 0, consumerName);
  writeString(buf, offset, subscriberId);
  return buf;
}

export function encodeUnsubscribe(consumerName: string, subscriberId: string): Buffer {
  // Identical wire format to Subscribe — two length-prefixed strings.
  return encodeSubscribe(consumerName, subscriberId);
}

export function decodeSubscribe(buf: Buffer): { consumerName: string; subscriberId: string } {
  const [consumerName, consumed] = readString(buf, 0);
  // Backward-compat: if no more bytes, subscriberId defaults to empty.
  if (consumed >= buf.length) {
    return { consumerName, subscriberId: "" };
  }
  const [subscriberId] = readString(buf, consumed);
  return { consumerName, subscriberId };
}
