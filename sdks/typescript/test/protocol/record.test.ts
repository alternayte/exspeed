import { describe, it, expect } from "vitest";
import { decodeRecord, encodeRecord } from "../../src/protocol/record.js";

describe("record delivery", () => {
  it("round-trips a record with key and headers", () => {
    const record = { consumerName: "my-consumer", offset: 42n, timestamp: 1713200000000n, subject: "orders.created", deliveryAttempt: 1, key: Buffer.from("customer-1"), value: Buffer.from('{"id":1}'), headers: [["source", "api"]] as [string, string][] };
    const decoded = decodeRecord(encodeRecord(record));
    expect(decoded.consumerName).toBe("my-consumer");
    expect(decoded.offset).toBe(42n);
    expect(decoded.timestamp).toBe(1713200000000n);
    expect(decoded.subject).toBe("orders.created");
    expect(decoded.deliveryAttempt).toBe(1);
    expect(decoded.key!.toString()).toBe("customer-1");
    expect(decoded.value.toString()).toBe('{"id":1}');
    expect(decoded.headers).toEqual([["source", "api"]]);
  });
  it("round-trips a record without key", () => {
    const record = { consumerName: "c", offset: 0n, timestamp: 0n, subject: "s", deliveryAttempt: 3, value: Buffer.from("v"), headers: [] as [string, string][] };
    const decoded = decodeRecord(encodeRecord(record));
    expect(decoded.key).toBeUndefined();
    expect(decoded.deliveryAttempt).toBe(3);
  });
  it("encodes delivery_attempt as u16 LE", () => {
    const record = { consumerName: "c", offset: 0n, timestamp: 0n, subject: "s", deliveryAttempt: 0x0102, value: Buffer.from("v"), headers: [] as [string, string][] };
    const buf = encodeRecord(record);
    const pos = 3 + 8 + 8 + 3; // consumerName("c":2+1) + offset(8) + timestamp(8) + subject("s":2+1)
    expect(buf.readUInt16LE(pos)).toBe(0x0102);
  });
});
