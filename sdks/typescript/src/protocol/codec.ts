import { ProtocolError } from "../errors.js";
import { PROTOCOL_VERSION, FRAME_HEADER_SIZE, MAX_PAYLOAD_SIZE, OpCode } from "./opcodes.js";

export interface Frame {
  version: number;
  opcode: OpCode;
  correlationId: number;
  payload: Buffer;
}

export interface DecodeResult {
  frame: Frame;
  bytesConsumed: number;
}

export function encodeFrame(frame: Frame): Buffer {
  const buf = Buffer.alloc(FRAME_HEADER_SIZE + frame.payload.length);
  buf[0] = frame.version;
  buf[1] = frame.opcode;
  buf.writeUInt32LE(frame.correlationId, 2);
  buf.writeUInt32LE(frame.payload.length, 6);
  frame.payload.copy(buf, FRAME_HEADER_SIZE);
  return buf;
}

export function decodeFrame(buf: Buffer, offset: number): DecodeResult | null {
  if (buf.length - offset < FRAME_HEADER_SIZE) return null;
  const version = buf[offset];
  if (version !== PROTOCOL_VERSION) {
    throw new ProtocolError(`Unsupported protocol version: 0x${version.toString(16)}`);
  }
  const opcode: OpCode = buf[offset + 1];
  const correlationId = buf.readUInt32LE(offset + 2);
  const payloadLength = buf.readUInt32LE(offset + 6);
  if (payloadLength > MAX_PAYLOAD_SIZE) {
    throw new ProtocolError(`Payload too large: ${payloadLength} bytes (max ${MAX_PAYLOAD_SIZE})`);
  }
  const totalSize = FRAME_HEADER_SIZE + payloadLength;
  if (buf.length - offset < totalSize) return null;
  const payload = Buffer.alloc(payloadLength);
  buf.copy(payload, 0, offset + FRAME_HEADER_SIZE, offset + totalSize);
  return { frame: { version, opcode, correlationId, payload }, bytesConsumed: totalSize };
}
