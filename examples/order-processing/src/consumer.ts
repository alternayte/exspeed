import { ExspeedClient } from "@exspeed/sdk";

const client = new ExspeedClient({
  clientId: "order-processor",
  host: "localhost",
  port: 5933,
});
await client.connect();
console.log("Connected to Exspeed");

// Create a consumer that reads all order events from the beginning
await client.createConsumer({
  name: "order-processor",
  stream: "order-events",
  subjectFilter: "order.>",
  startFrom: "earliest",
});
console.log("Consumer 'order-processor' created");

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
    `[${msg.subject}] Order ${order.order_id} | customer=${order.customer_id} | $${order.total} | ${order.region}`
  );

  // In a real app you would update the database, send notifications, etc.
  await msg.ack();
}
