import type { ServerError } from "./types.js";

export function decodeErrorFrame(buf: Buffer): ServerError {
  const code = buf.readUInt16LE(0);
  const message = buf.toString("utf8", 2);
  return { code, message };
}

export function encodeErrorFrame(err: ServerError): Buffer {
  const msgBytes = Buffer.from(err.message, "utf8");
  const buf = Buffer.alloc(2 + msgBytes.length);
  buf.writeUInt16LE(err.code, 0);
  msgBytes.copy(buf, 2);
  return buf;
}
