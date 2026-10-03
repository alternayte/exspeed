import { randomUUID } from "node:crypto";
import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { ExspeedClient } from "@exspeed/sdk";

const app = new Hono();

// Connect to Exspeed (override the address with EXSPEED_HOST / EXSPEED_PORT)
const exspeed = await ExspeedClient.connect({
  clientId: "order-api",
  host: process.env.EXSPEED_HOST ?? "localhost",
  port: Number(process.env.EXSPEED_PORT ?? 5933),
});
await exspeed.createStream("order-events"); // idempotent
console.log("Connected to Exspeed, stream 'order-events' ready");

app.post("/orders", async (c) => {
  const { customer_id, total, region } = await c.req.json();
  // The region becomes a subject token, so keep it to one plain token.
  if (typeof customer_id !== "string" || typeof total !== "number" || !/^[A-Za-z0-9_-]+$/.test(String(region))) {
    return c.json({ error: "expected {customer_id: string, total: number, region: string}" }, 400);
  }

  const orderId = randomUUID();
  const result = await exspeed.publish("order-events", {
    subject: `order.${region}.created`,
    key: orderId,
    value: {
      order_id: orderId,
      customer_id,
      total,
      region,
      created_at: new Date().toISOString(),
    },
  });

  return c.json({ order_id: orderId, offset: result.offset, status: "created" }, 201);
});

app.get("/health", (c) => c.json({ status: "ok" }));

const port = Number(process.env.PORT ?? 3000);
serve({ fetch: app.fetch, port }, () => {
  console.log(`Order API listening on http://localhost:${port}`);
});
