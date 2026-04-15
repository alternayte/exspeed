import { describe, it, expect } from "vitest";
import { encodeFrame, decodeFrame } from "../../src/protocol/codec.js";
import { OpCode, PROTOCOL_VERSION } from "../../src/protocol/opcodes.js";

describe("frame codec", () => {
  it("round-trips a frame with payload", () => {
    const frame = { version: PROTOCOL_VERSION, opcode: OpCode.Ping, correlationId: 42, payload: Buffer.from([0x01, 0x02, 0x03]) };
    const encoded = encodeFrame(frame);
    expect(encoded.length).toBe(10 + 3);
    const result = decodeFrame(encoded, 0);
    expect(result).not.toBeNull();
    expect(result!.frame.version).toBe(PROTOCOL_VERSION);
    expect(result!.frame.opcode).toBe(OpCode.Ping);
    expect(result!.frame.correlationId).toBe(42);
    expect(Buffer.compare(result!.frame.payload, frame.payload)).toBe(0);
    expect(result!.bytesConsumed).toBe(13);
  });
  it("round-trips a frame with empty payload", () => {
    const frame = { version: PROTOCOL_VERSION, opcode: OpCode.Pong, correlationId: 1, payload: Buffer.alloc(0) };
    const encoded = encodeFrame(frame);
    expect(encoded.length).toBe(10);
    const result = decodeFrame(encoded, 0);
    expect(result).not.toBeNull();
    expect(result!.frame.opcode).toBe(OpCode.Pong);
    expect(result!.frame.payload.length).toBe(0);
  });
  it("returns null if buffer has incomplete header", () => {
    expect(decodeFrame(Buffer.alloc(5), 0)).toBeNull();
  });
  it("returns null if buffer has incomplete payload", () => {
    const frame = { version: PROTOCOL_VERSION, opcode: OpCode.Ping, correlationId: 1, payload: Buffer.from([0x01, 0x02, 0x03]) };
    const encoded = encodeFrame(frame);
    expect(decodeFrame(encoded.subarray(0, encoded.length - 1), 0)).toBeNull();
  });
  it("throws ProtocolError for unsupported version", () => {
    const buf = Buffer.alloc(10);
    buf[0] = 0xff;
    buf[1] = OpCode.Ping;
    buf.writeUInt32LE(1, 2);
    buf.writeUInt32LE(0, 6);
    expect(() => decodeFrame(buf, 0)).toThrow("Unsupported protocol version");
  });
  it("throws ProtocolError for payload exceeding max size", () => {
    const buf = Buffer.alloc(10);
    buf[0] = PROTOCOL_VERSION;
    buf[1] = OpCode.Ping;
    buf.writeUInt32LE(1, 2);
    buf.writeUInt32LE(16 * 1024 * 1024 + 1, 6);
    expect(() => decodeFrame(buf, 0)).toThrow("Payload too large");
  });
  it("encodes header bytes in correct order", () => {
    const frame = { version: PROTOCOL_VERSION, opcode: OpCode.Publish, correlationId: 0x01020304, payload: Buffer.alloc(0) };
    const buf = encodeFrame(frame);
    expect(buf[0]).toBe(PROTOCOL_VERSION);
    expect(buf[1]).toBe(OpCode.Publish);
    expect(buf[2]).toBe(0x04);
    expect(buf[3]).toBe(0x03);
    expect(buf.readUInt32LE(6)).toBe(0);
  });
  it("decodes multiple frames from a single buffer", () => {
    const f1 = encodeFrame({ version: PROTOCOL_VERSION, opcode: OpCode.Ping, correlationId: 1, payload: Buffer.alloc(0) });
    const f2 = encodeFrame({ version: PROTOCOL_VERSION, opcode: OpCode.Pong, correlationId: 2, payload: Buffer.alloc(0) });
    const combined = Buffer.concat([f1, f2]);
    const r1 = decodeFrame(combined, 0);
    expect(r1!.frame.opcode).toBe(OpCode.Ping);
    const r2 = decodeFrame(combined, r1!.bytesConsumed);
    expect(r2!.frame.opcode).toBe(OpCode.Pong);
  });
});
