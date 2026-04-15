import { writeString, readString, stringByteLength } from "./primitives.js";
import type { ConnectRequest } from "./types.js";

export function encodeConnect(req: ConnectRequest): Buffer {
  const size = stringByteLength(req.clientId) + 1 + req.authPayload.length;
  const buf = Buffer.alloc(size);
  let pos = 0;
  pos += writeString(buf, pos, req.clientId);
  buf[pos] = req.authType;
  pos += 1;
  req.authPayload.copy(buf, pos);
  return buf;
}

export function decodeConnect(buf: Buffer): ConnectRequest {
  let pos = 0;
  const [clientId, clientIdLen] = readString(buf, pos);
  pos += clientIdLen;
  const authType = buf[pos];
  pos += 1;
  const authPayload = Buffer.alloc(buf.length - pos);
  buf.copy(authPayload, 0, pos);
  return { clientId, authType, authPayload };
}
