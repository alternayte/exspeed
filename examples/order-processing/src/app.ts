import { Hono } from "hono";
import { ExspeedClient } from "@exspeed/sdk";

const app = new Hono();

// Connect to Exspeed
const exspeed = new ExspeedClient({
  clientId: "order-api",
  host: "localhost",
  port: 5933,
});
await exspeed.connect();
await exspeed.createStream("order-events");
console.log("Connected to Exspeed, stream 'order-events' ready");

app.post("/orders", async (c) => {
  const body = await c.req.json();
  const { customer_id, total, region } = body;

  const orderId = crypto.randomUUID();
  const result = await exspeed.publish("order-events", {
    subject: `order.${region}.created`,
    key: orderId,
    data: {
      order_id: orderId,
      customer_id,
      total,
      region,
      created_at: new Date().toISOString(),
    },
  });

  return c.json({ order_id: orderId, offset: result.offset.toString(), status: "created" }, 201);
});

app.get("/health", (c) => c.json({ status: "ok" }));

console.log("Order API listening on http://localhost:3000");
export default { port: 3000, fetch: app.fetch };
