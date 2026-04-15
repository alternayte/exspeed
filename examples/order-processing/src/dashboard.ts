// Dashboard — uses the Exspeed HTTP API for ExQL queries
// (The SDK doesn't have query support yet, so we use fetch directly)

const EXSPEED_URL = "http://localhost:8080";

// Create a materialized view for live order stats by region
console.log("Creating materialized view 'order_stats'...");
const mvResult = await fetch(`${EXSPEED_URL}/api/v1/views`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    sql: `CREATE MATERIALIZED VIEW order_stats AS
          SELECT payload->>'region' AS region,
                 COUNT(*) AS order_count,
                 SUM((payload->>'total')::DECIMAL) AS revenue
          FROM "order-events"
          GROUP BY payload->>'region'`,
  }),
});
console.log("Materialized view created:", await mvResult.json());

// Give the view a moment to catch up with existing data
await new Promise((r) => setTimeout(r, 2000));

// Query the materialized view
console.log("\nOrder Stats by Region:");
console.log("\u2500".repeat(50));
const stats = await fetch(`${EXSPEED_URL}/api/v1/views/order_stats`);
const statsData = await stats.json();
console.log(JSON.stringify(statsData, null, 2));

// Run an ad-hoc ExQL query directly against the stream
console.log("\nAd-hoc ExQL Query — Orders by Region:");
console.log("\u2500".repeat(50));
const query = await fetch(`${EXSPEED_URL}/api/v1/queries`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    sql: `SELECT payload->>'region' AS region,
                 COUNT(*) AS orders
          FROM "order-events"
          GROUP BY payload->>'region'
          ORDER BY orders DESC`,
  }),
});
console.log(JSON.stringify(await query.json(), null, 2));
