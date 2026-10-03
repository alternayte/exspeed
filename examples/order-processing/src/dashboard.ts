// Dashboard — a materialized table over the HTTP API, plus an ad-hoc ExQL
// query through the SDK.
import { ExspeedClient } from "@exspeed/sdk";

// Override the addresses with EXSPEED_URL (HTTP API) and EXSPEED_HOST / EXSPEED_PORT
const EXSPEED_URL = process.env.EXSPEED_URL ?? "http://localhost:8080";

async function api(path: string, init?: RequestInit): Promise<unknown> {
  const res = await fetch(`${EXSPEED_URL}${path}`, init);
  const body = await res.json();
  if (!res.ok) throw new Error(`${init?.method ?? "GET"} ${path}: ${res.status} ${JSON.stringify(body)}`);
  return body;
}

// Create a materialized view for live order stats by region (kept if it already exists)
console.log("Creating materialized view 'order_stats'...");
const view = (await api("/api/v1/views", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    sql: `CREATE MATERIALIZED VIEW IF NOT EXISTS order_stats AS
          SELECT payload->>'region' AS region,
                 COUNT(*) AS order_count,
                 SUM((payload->>'total')::DECIMAL) AS revenue
          FROM "order-events"
          GROUP BY payload->>'region'`,
  }),
})) as { name: string; status: string };
console.log(`Materialized view '${view.name}' is ${view.status}`);

// Give the view a moment to catch up with existing data
await new Promise((r) => setTimeout(r, 2000));

// Query the materialized view
console.log("\nOrder Stats by Region:");
console.log("─".repeat(50));
const stats = await api("/api/v1/views/order_stats");
console.log(JSON.stringify(stats, null, 2));

// Run an ad-hoc ExQL query directly against the stream
console.log("\nAd-hoc ExQL Query — Orders by Region:");
console.log("─".repeat(50));
const client = await ExspeedClient.connect({
  clientId: "order-dashboard",
  host: process.env.EXSPEED_HOST ?? "localhost",
  port: Number(process.env.EXSPEED_PORT ?? 5933),
});
const result = await client.query(
  `SELECT payload->>'region' AS region,
          COUNT(*) AS orders
   FROM "order-events"
   GROUP BY payload->>'region'
   ORDER BY orders DESC`,
);
console.log(JSON.stringify(result, null, 2));
await client.close();
