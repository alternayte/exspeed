import { writeString, readString, stringByteLength } from "./primitives.js";
import type { ConnectRequest } from "./types.js";

export interface ConnectResponse {
  serverVersion: number;
}

export function decodeConnectResponse(payload: Buffer): ConnectResponse {
  // v2 payload: 1 byte server_version. Older servers send empty Ok — default to 1.
  const serverVersion = payload.length >= 1 ? payload.readUInt8(0) : 1;
  return { serverVersion };
}

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
