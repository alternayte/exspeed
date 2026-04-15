import { describe, it, expect, vi } from "vitest";
import { Subscription, Message } from "../src/subscription.js";

describe("Message", () => {
  it("json() parses value as JSON", () => {
    const msg = new Message(
      { consumerName: "c", offset: 0n, timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from('{"id":1}'), headers: [] },
      vi.fn(), vi.fn(),
    );
    expect(msg.json()).toEqual({ id: 1 });
  });

  it("raw() returns the buffer", () => {
    const value = Buffer.from("raw-data");
    const msg = new Message(
      { consumerName: "c", offset: 0n, timestamp: 0n, subject: "s", deliveryAttempt: 1, value, headers: [] },
      vi.fn(), vi.fn(),
    );
    expect(Buffer.compare(msg.raw(), value)).toBe(0);
  });

  it("ack() calls the ack callback", async () => {
    const ackFn = vi.fn().mockResolvedValue(undefined);
    const msg = new Message(
      { consumerName: "c", offset: 42n, timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from("v"), headers: [] },
      ackFn, vi.fn(),
    );
    await msg.ack();
    expect(ackFn).toHaveBeenCalledWith("c", 42n);
  });

  it("nack() calls the nack callback", async () => {
    const nackFn = vi.fn().mockResolvedValue(undefined);
    const msg = new Message(
      { consumerName: "c", offset: 10n, timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from("v"), headers: [] },
      vi.fn(), nackFn,
    );
    await msg.nack();
    expect(nackFn).toHaveBeenCalledWith("c", 10n);
  });

  it("toJSON() converts bigints to strings", () => {
    const msg = new Message(
      { consumerName: "c", offset: 42n, timestamp: 1000n, subject: "s", deliveryAttempt: 1, value: Buffer.from("v"), headers: [["a", "1"]] },
      vi.fn(), vi.fn(),
    );
    const json = msg.toJSON();
    expect(json).toEqual({ subject: "s", offset: "42", timestamp: "1000", deliveryAttempt: 1, headers: [["a", "1"]] });
  });
});

describe("Subscription", () => {
  it("yields messages pushed into it", async () => {
    const sub = new Subscription("test-consumer", { maxQueueSize: 10 });
    sub.push(
      { consumerName: "test-consumer", offset: 0n, timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from("v"), headers: [] as [string, string][] },
      vi.fn(), vi.fn(),
    );
    sub.close();

    const messages: Message[] = [];
    for await (const msg of sub) {
      messages.push(msg);
    }
    expect(messages.length).toBe(1);
    expect(messages[0].subject).toBe("s");
  });

  it("emits slow event at 80% capacity", () => {
    const sub = new Subscription("c", { maxQueueSize: 5 });
    const slowHandler = vi.fn();
    sub.on("slow", slowHandler);

    for (let i = 0; i < 4; i++) {
      sub.push(
        { consumerName: "c", offset: BigInt(i), timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from("v"), headers: [] },
        vi.fn(), vi.fn(),
      );
    }
    expect(slowHandler).toHaveBeenCalled();
  });

  it("emits error and drops oldest when queue overflows", () => {
    const sub = new Subscription("c", { maxQueueSize: 2 });
    const errorHandler = vi.fn();
    sub.on("error", errorHandler);

    for (let i = 0; i < 3; i++) {
      sub.push(
        { consumerName: "c", offset: BigInt(i), timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from(`v${i}`), headers: [] },
        vi.fn(), vi.fn(),
      );
    }
    expect(errorHandler).toHaveBeenCalled();
  });
});
