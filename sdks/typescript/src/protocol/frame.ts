import { ExspeedError, ProtocolError } from "../errors.js";
import { FRAME_HEADER_SIZE, MAX_PAYLOAD_SIZE, PROTOCOL_VERSION } from "./constants.js";

/** One protocol frame: header fields plus the raw payload. */
export interface Frame {
  opcode: number;
  correlationId: number;
  payload: Buffer;
}

/** Serialize a frame: `[version][opcode][corr u32 LE][len u32 LE][payload]`. */
export function encodeFrame(opcode: number, correlationId: number, payload: Uint8Array): Buffer {
  if (payload.length > MAX_PAYLOAD_SIZE) {
    throw new ExspeedError(
      `payload too large: ${payload.length} bytes (max ${MAX_PAYLOAD_SIZE}); split the batch`,
    );
  }
  const buf = Buffer.allocUnsafe(FRAME_HEADER_SIZE + payload.length);
  buf[0] = PROTOCOL_VERSION;
  buf[1] = opcode;
  buf.writeUInt32LE(correlationId >>> 0, 2);
  buf.writeUInt32LE(payload.length, 6);
  buf.set(payload, FRAME_HEADER_SIZE);
  return buf;
}

const EMPTY = Buffer.alloc(0);

/**
 * Incremental frame parser for a byte stream. Feed it socket chunks; it
 * returns every complete frame. A bad version or oversize length throws a
 * {@link ProtocolError}: the stream can't be resynchronised after that, so
 * the caller should drop the connection.
 */
export class FrameParser {
  private buf: Buffer = EMPTY;

  push(chunk: Buffer): Frame[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const frames: Frame[] = [];
    let pos = 0;
    while (this.buf.length - pos >= FRAME_HEADER_SIZE) {
      const version = this.buf[pos]!;
      if (version !== PROTOCOL_VERSION) {
        throw new ProtocolError(`unsupported protocol version 0x${version.toString(16)}`);
      }
      const opcode = this.buf[pos + 1]!;
      const correlationId = this.buf.readUInt32LE(pos + 2);
      const len = this.buf.readUInt32LE(pos + 6);
      if (len > MAX_PAYLOAD_SIZE) {
        throw new ProtocolError(`payload too large: ${len} bytes (max ${MAX_PAYLOAD_SIZE})`);
      }
      const end = pos + FRAME_HEADER_SIZE + len;
      if (this.buf.length < end) break;
      frames.push({ opcode, correlationId, payload: this.buf.subarray(pos + FRAME_HEADER_SIZE, end) });
      pos = end;
    }
    this.buf = pos === this.buf.length ? EMPTY : this.buf.subarray(pos);
    return frames;
  }

  /** Bytes buffered towards an incomplete frame. */
  get pending(): number {
    return this.buf.length;
  }
}
