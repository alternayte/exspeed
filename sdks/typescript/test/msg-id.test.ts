import { describe, it, expect } from "vitest";
import { newMsgId } from "../src/msg-id.js";

describe("newMsgId", () => {
  it("returns a non-empty string", () => {
    expect(newMsgId().length).toBeGreaterThan(0);
  });

  it("returns unique values across calls", () => {
    const ids = new Set([newMsgId(), newMsgId(), newMsgId()]);
    expect(ids.size).toBe(3);
  });

  it("matches UUIDv7 format", () => {
    expect(newMsgId()).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });

  it("later-generated ids sort after earlier ones", async () => {
    const a = newMsgId();
    await new Promise((r) => setTimeout(r, 2));
    expect(a < newMsgId()).toBe(true);
  });
});
