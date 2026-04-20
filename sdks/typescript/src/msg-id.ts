/**
 * Generate a sortable UUIDv7 suitable for use as an idempotency key.
 * Uses crypto.getRandomValues for the random bits and Date.now() for the
 * millisecond timestamp, so IDs generated later will sort lexicographically
 * after earlier ones.
 *
 * Format: xxxxxxxx-xxxx-7xxx-[89ab]xxx-xxxxxxxxxxxx
 *   48 bits  unix_ts_ms
 *    4 bits  version = 7
 *   12 bits  rand_a
 *    2 bits  variant = 10
 *   62 bits  rand_b
 */
export function newMsgId(): string {
  const now = BigInt(Date.now());
  const rand = new Uint8Array(10);
  crypto.getRandomValues(rand);

  // 48-bit timestamp → 12 hex chars
  const tsHex = now.toString(16).padStart(12, "0");

  // rand_a: 12 bits with version nibble 0x7 in the top 4 bits
  const rawRandA = ((rand[0]! << 8) | rand[1]!) & 0x0fff;
  const randA = rawRandA | 0x7000;

  // rand_b high byte: 6 bits with variant 0b10 in the top 2 bits
  const rawRandBHi = rand[2]! & 0x3f;
  const randBHi = rawRandBHi | 0x80;

  // Remaining 8 bytes of rand_b
  const tail = Array.from(rand.slice(3))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  const hex = (n: number, len: number) => n.toString(16).padStart(len, "0");

  return [
    tsHex.slice(0, 8),                                 // time_high
    tsHex.slice(8, 12),                                // time_low
    hex(randA, 4),                                     // ver + rand_a
    hex(randBHi, 2) + rand[3]!.toString(16).padStart(2, "0"), // var + rand_b[0]
    tail.slice(2, 14),                                 // rand_b[1..7]
  ].join("-");
}
