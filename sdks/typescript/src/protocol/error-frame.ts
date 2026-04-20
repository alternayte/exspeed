import type { ServerError } from "./types.js";

export const ERR_KEY_COLLISION = 0x1001;
export const ERR_DEDUP_MAP_FULL = 0x1002;

/**
 * Structured representation of a decoded error frame.
 * storedOffset is set for KeyCollision (0x1001).
 * retryAfterSecs is set for DedupMapFull (0x1002).
 */
export interface ParsedErrorFrame {
  code: number;
  message: string;
  storedOffset?: bigint;
  retryAfterSecs?: number;
}

/**
 * Decode an error frame payload.
 *
 * New format (v2): u16(code) + u16(msg_len) + msg bytes + optional extras
 * Old format (v1): u16(code) + raw message string (no length prefix)
 *
 * Extras are detected by reading beyond the message:
 *   KeyCollision  (0x1001): extras_flag=0x01 + u64(stored_offset)
 *   DedupMapFull  (0x1002): extras_flag=0x02 + u32(retry_after_secs)
 */
export function decodeErrorFrame(buf: Buffer): ParsedErrorFrame {
  const code = buf.readUInt16LE(0);

  // Detect new framing: the u16 at offset 2 should equal the remaining byte
  // count for the message.  If it does, use the new format; otherwise fall
  // back to the legacy format where offset 2 begins the raw message string.
  const candidateMsgLen = buf.readUInt16LE(2);
  const newFormatMinSize = 2 + 2 + candidateMsgLen; // code + len + msg

  let message: string;
  let extrasOffset: number;

  if (candidateMsgLen > 0 && newFormatMinSize <= buf.length) {
    // New format
    message = buf.toString("utf8", 4, 4 + candidateMsgLen);
    extrasOffset = 4 + candidateMsgLen;
  } else if (candidateMsgLen === 0 && buf.length === 4) {
    // New format with empty message
    message = "";
    extrasOffset = 4;
  } else {
    // Legacy format: remainder is the raw string
    message = buf.toString("utf8", 2);
    return { code, message };
  }

  const result: ParsedErrorFrame = { code, message };

  // Parse optional extras
  if (extrasOffset < buf.length) {
    const flag = buf[extrasOffset];
    const extrasData = extrasOffset + 1;

    if (flag === 0x01 && extrasData + 8 <= buf.length) {
      // KeyCollision: u64 stored_offset (LE)
      result.storedOffset = buf.readBigUInt64LE(extrasData);
    } else if (flag === 0x02 && extrasData + 4 <= buf.length) {
      // DedupMapFull: u32 retry_after_secs (LE)
      result.retryAfterSecs = buf.readUInt32LE(extrasData);
    }
  }

  return result;
}

export function encodeErrorFrame(err: ServerError): Buffer {
  const msgBytes = Buffer.from(err.message, "utf8");
  const buf = Buffer.alloc(2 + msgBytes.length);
  buf.writeUInt16LE(err.code, 0);
  msgBytes.copy(buf, 2);
  return buf;
}
