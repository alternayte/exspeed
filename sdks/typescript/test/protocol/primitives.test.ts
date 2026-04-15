import { describe, it, expect } from "vitest";
import {
  writeString, readString, writeBytes, readBytes, writeHeaders, readHeaders,
  writeU16, readU16, writeU32, readU32, writeU64, readU64, stringByteLength,
} from "../../src/protocol/primitives.js";

describe("primitives", () => {
  describe("u16", () => {
    it("round-trips a u16 value", () => {
      const buf = Buffer.alloc(2);
      writeU16(buf, 0, 0x1234);
      expect(readU16(buf, 0)).toEqual([0x1234, 2]);
    });
    it("encodes as little-endian", () => {
      const buf = Buffer.alloc(2);
      writeU16(buf, 0, 0x0102);
      expect(buf[0]).toBe(0x02);
      expect(buf[1]).toBe(0x01);
    });
  });
  describe("u32", () => {
    it("round-trips a u32 value", () => {
      const buf = Buffer.alloc(4);
      writeU32(buf, 0, 0xdeadbeef);
      expect(readU32(buf, 0)).toEqual([0xdeadbeef, 4]);
    });
  });
  describe("u64", () => {
    it("round-trips a u64 bigint", () => {
      const buf = Buffer.alloc(8);
      writeU64(buf, 0, 0x0102030405060708n);
      expect(readU64(buf, 0)).toEqual([0x0102030405060708n, 8]);
    });
    it("encodes as little-endian", () => {
      const buf = Buffer.alloc(8);
      writeU64(buf, 0, 1n);
      expect(buf[0]).toBe(1);
      expect(buf[7]).toBe(0);
    });
  });
  describe("string", () => {
    it("round-trips an ASCII string", () => {
      const buf = Buffer.alloc(64);
      const written = writeString(buf, 0, "hello");
      expect(written).toBe(2 + 5);
      const [str, consumed] = readString(buf, 0);
      expect(str).toBe("hello");
      expect(consumed).toBe(7);
    });
    it("round-trips a UTF-8 string with multi-byte chars", () => {
      const buf = Buffer.alloc(64);
      const s = "héllo 🌍";
      const written = writeString(buf, 0, s);
      const [str, consumed] = readString(buf, 0);
      expect(str).toBe(s);
      expect(consumed).toBe(written);
    });
    it("round-trips an empty string", () => {
      const buf = Buffer.alloc(64);
      writeString(buf, 0, "");
      const [str, consumed] = readString(buf, 0);
      expect(str).toBe("");
      expect(consumed).toBe(2);
    });
    it("stringByteLength returns correct byte count", () => {
      expect(stringByteLength("hello")).toBe(2 + 5);
      expect(stringByteLength("")).toBe(2);
      expect(stringByteLength("🌍")).toBe(2 + 4);
    });
  });
  describe("bytes", () => {
    it("round-trips a buffer", () => {
      const data = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
      const buf = Buffer.alloc(64);
      const written = writeBytes(buf, 0, data);
      expect(written).toBe(4 + 4);
      const [result, consumed] = readBytes(buf, 0);
      expect(Buffer.compare(result, data)).toBe(0);
      expect(consumed).toBe(8);
    });
    it("round-trips empty bytes", () => {
      const buf = Buffer.alloc(64);
      writeBytes(buf, 0, Buffer.alloc(0));
      const [result, consumed] = readBytes(buf, 0);
      expect(result.length).toBe(0);
      expect(consumed).toBe(4);
    });
  });
  describe("headers", () => {
    it("round-trips headers", () => {
      const headers: [string, string][] = [["content-type", "application/json"], ["x-trace-id", "abc-123"]];
      const buf = Buffer.alloc(256);
      const written = writeHeaders(buf, 0, headers);
      const [result, consumed] = readHeaders(buf, 0);
      expect(result).toEqual(headers);
      expect(consumed).toBe(written);
    });
    it("round-trips empty headers", () => {
      const buf = Buffer.alloc(64);
      writeHeaders(buf, 0, []);
      const [result, consumed] = readHeaders(buf, 0);
      expect(result).toEqual([]);
      expect(consumed).toBe(2);
    });
  });
});
