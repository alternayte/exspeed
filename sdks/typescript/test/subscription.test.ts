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
    const sub = new Subscription("test-consumer", "test-sub-id", { maxQueueSize: 10 });
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
    const sub = new Subscription("c", "test-sub-id", { maxQueueSize: 5 });
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

  it("emits typed overflow event and drops oldest when queue overflows", () => {
    const sub = new Subscription("c", "test-sub-id", { maxQueueSize: 2 });
    const handler = vi.fn();
    sub.on("overflow", handler);

    for (let i = 0; i < 3; i++) {
      sub.push(
        { consumerName: "c", offset: BigInt(i), timestamp: 0n, subject: "s", deliveryAttempt: 1, value: Buffer.from(`v${i}`), headers: [] },
        vi.fn(), vi.fn(),
      );
    }
    expect(handler).toHaveBeenCalled();
  });

  it("emits a typed QueueOverflowError on drop-oldest overflow", async () => {
    const { QueueOverflowError } = await import("../src/errors.js");
    const sub = new Subscription("c", "s-id", { maxQueueSize: 2, overflowPolicy: "drop-oldest" });

    const overflowEvents: any[] = [];
    sub.on("overflow", (e) => overflowEvents.push(e));

    const mk = (off: bigint) => ({
      consumerName: "c",
      offset: off,
      timestamp: 0n,
      subject: `s-${off}`,
      deliveryAttempt: 1,
      value: Buffer.from(""),
      headers: [] as [string, string][],
    });

    sub.push(mk(0n), () => Promise.resolve(), () => Promise.resolve());
    sub.push(mk(1n), () => Promise.resolve(), () => Promise.resolve());
    sub.push(mk(2n), () => Promise.resolve(), () => Promise.resolve()); // drops offset 0

    expect(overflowEvents).toHaveLength(1);
    expect(overflowEvents[0]).toBeInstanceOf(QueueOverflowError);
    expect(overflowEvents[0].offset).toBe(0n);
    expect(overflowEvents[0].subject).toBe("s-0");
  });

  it("'error' policy emits typed error and does NOT enqueue the new record", async () => {
    const { QueueOverflowError } = await import("../src/errors.js");
    const sub = new Subscription("c", "s-id", { maxQueueSize: 1, overflowPolicy: "error" });

    const errs: any[] = [];
    sub.on("error", (e) => errs.push(e));

    const mk = (off: bigint) => ({
      consumerName: "c", offset: off, timestamp: 0n, subject: "s",
      deliveryAttempt: 1, value: Buffer.from(""), headers: [] as [string, string][],
    });
    const noop = () => Promise.resolve();

    sub.push(mk(0n), noop, noop);
    sub.push(mk(1n), noop, noop); // overflow

    expect(errs).toHaveLength(1);
    expect(errs[0]).toBeInstanceOf(QueueOverflowError);
    expect(errs[0].offset).toBe(1n); // the rejected one is the incoming

    sub.close();
    const collected: bigint[] = [];
    for await (const m of sub) collected.push(m.offset);
    expect(collected).toEqual([0n]);
  });

  it("pause() blocks iterator; resume() unblocks", async () => {
    const sub = new Subscription("c", "s-id", { maxQueueSize: 10 });
    const mk = (off: bigint) => ({
      consumerName: "c", offset: off, timestamp: 0n, subject: "s",
      deliveryAttempt: 1, value: Buffer.from(""), headers: [] as [string, string][],
    });
    const noop = () => Promise.resolve();

    sub.push(mk(0n), noop, noop);
    sub.pause();

    const iter = sub[Symbol.asyncIterator]();
    const racer = iter.next();
    const winner = await Promise.race([
      racer.then(() => "iter-resolved"),
      new Promise((r) => setTimeout(() => r("timed-out"), 50)),
    ]);
    expect(winner).toBe("timed-out"); // paused — iterator did not resolve

    sub.resume();
    const result = await racer;
    expect(result.done).toBe(false);
    expect(result.value!.offset).toBe(0n);
  });
});
