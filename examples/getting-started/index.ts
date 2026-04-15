import { ExspeedClient } from "@exspeed/sdk";

// 1. Connect to Exspeed
const client = new ExspeedClient({
  clientId: "crypto-tracker",
  host: "localhost",
  port: 5933,
});

await client.connect();
console.log("Connected to Exspeed");

// 2. Create a stream to hold crypto price data
await client.createStream("crypto-prices");
console.log("Stream 'crypto-prices' created");

// 3. Publish a sample record manually
const result = await client.publish("crypto-prices", {
  subject: "crypto.prices",
  data: {
    bitcoin: { usd: 67000 },
    ethereum: { usd: 3500 },
    solana: { usd: 150 },
  },
});
console.log(`Published at offset ${result.offset}`);

// 4. Fetch the last few records from the stream
const records = await client.fetch("crypto-prices", {
  offset: 0n,
  maxRecords: 10,
});
console.log(`\nFetched ${records.length} record(s):`);
for (const record of records) {
  console.log(`  [${record.subject}] offset=${record.offset}`, record.json());
}

// 5. Create a consumer and subscribe for real-time updates
await client.createConsumer({
  name: "price-watcher",
  stream: "crypto-prices",
  startFrom: "earliest",
});
console.log("\nConsumer 'price-watcher' created");

const sub = await client.subscribe("price-watcher");

// Process messages as they arrive using async iteration
console.log("Subscribed — waiting for records (Ctrl+C to stop)...");
console.log("Tip: The HTTP poller connector will fetch live crypto prices every 60s");
console.log("     Place crypto-prices.toml in your server's connectors.d/ folder");

for await (const msg of sub) {
  console.log(`[${msg.subject}] offset=${msg.offset}`, msg.json());
  await msg.ack();
}
