import { describe, it, expect } from "vitest";
import { decodeRecordsBatch, encodeRecordsBatch } from "../../src/protocol/records-batch.js";
import type { BatchRecord } from "../../src/protocol/types.js";

describe("records batch", () => {
  it("round-trips a batch with multiple records", () => {
    const records: BatchRecord[] = [
      { offset: 0n, timestamp: 1000n, subject: "orders.created", key: Buffer.from("k1"), value: Buffer.from("v1"), headers: [["a", "1"]] },
      { offset: 1n, timestamp: 1001n, subject: "orders.updated", value: Buffer.from("v2"), headers: [] },
    ];
    const decoded = decodeRecordsBatch(encodeRecordsBatch(records));
    expect(decoded.length).toBe(2);
    expect(decoded[0].offset).toBe(0n);
    expect(decoded[0].key!.toString()).toBe("k1");
    expect(decoded[1].key).toBeUndefined();
  });
  it("round-trips an empty batch", () => {
    expect(decodeRecordsBatch(encodeRecordsBatch([]))).toEqual([]);
  });
  it("starts with u32 record count", () => {
    const buf = encodeRecordsBatch([{ offset: 0n, timestamp: 0n, subject: "s", value: Buffer.from("v"), headers: [] }]);
    expect(buf.readUInt32LE(0)).toBe(1);
  });
});
