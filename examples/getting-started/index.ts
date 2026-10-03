import { ExspeedClient } from "@exspeed/sdk";

// 1. Connect to Exspeed (override the address with EXSPEED_HOST / EXSPEED_PORT)
const client = await ExspeedClient.connect({
  clientId: "crypto-tracker",
  host: process.env.EXSPEED_HOST ?? "localhost",
  port: Number(process.env.EXSPEED_PORT ?? 5933),
});
console.log("Connected to Exspeed");

// 2. Create a stream to hold crypto price data (idempotent)
await client.createStream("crypto-prices");
console.log("Stream 'crypto-prices' ready");

// 3. Publish a sample record manually (objects are JSON-encoded)
const result = await client.publish("crypto-prices", {
  subject: "crypto.prices",
  value: {
    bitcoin: { usd: 67000 },
    ethereum: { usd: 3500 },
    solana: { usd: 150 },
  },
});
console.log(`Published at offset ${result.offset}`);

// 4. Read up to 10 records from the start of the stream
const page = await client.read("crypto-prices", { from: 0, maxRecords: 10 });
console.log(`\nRead ${page.records.length} record(s):`);
for (const record of page.records) {
  console.log(`  [${record.subject}] offset=${record.offset}`, record.json());
}

// 5. Create a durable consumer and subscribe for real-time updates
await client.createConsumer({
  name: "price-watcher",
  stream: "crypto-prices",
  deliver: "all",
});
console.log("\nConsumer 'price-watcher' ready");

const sub = await client.subscribe("price-watcher");

// Process messages as they arrive using async iteration
console.log("Subscribed — waiting for records (Ctrl+C to stop)...");
console.log("Tip: The HTTP poller connector will fetch live crypto prices every 60s");
console.log("     Place crypto-prices.toml in your server's connectors.d/ folder");

for await (const msg of sub) {
  console.log(`[${msg.subject}] offset=${msg.offset}`, msg.json());
  msg.ack();
}
