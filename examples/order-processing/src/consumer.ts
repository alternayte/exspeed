import { ExspeedClient } from "@exspeed/sdk";

// Override the address with EXSPEED_HOST / EXSPEED_PORT
const client = await ExspeedClient.connect({
  clientId: "order-processor",
  host: process.env.EXSPEED_HOST ?? "localhost",
  port: Number(process.env.EXSPEED_PORT ?? 5933),
});
console.log("Connected to Exspeed");

// The API server creates the stream too; this lets the worker start first (idempotent)
await client.createStream("order-events");

// A durable consumer that reads all order events from the beginning
// (idempotent: a restarted worker resumes where it left off)
await client.createConsumer({
  name: "order-processor",
  stream: "order-events",
  filterSubjects: ["order.>"],
  deliver: "all",
});
console.log("Consumer 'order-processor' ready");

// Subscribe to receive messages
const sub = await client.subscribe("order-processor");

console.log("Order processor running — consuming from 'order-events'...");

for await (const msg of sub) {
  const order = msg.json<{
    order_id: string;
    customer_id: string;
    total: number;
    region: string;
    created_at: string;
  }>();

  console.log(
    `[${msg.subject}] Order ${order.order_id} | customer=${order.customer_id} | $${order.total} | ${order.region}`,
  );

  // In a real app you would update the database, send notifications, etc.
  msg.ack();
}
