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

  // Segments 1-2: 48-bit unix_ts_ms
  const tsHex = now.toString(16).padStart(12, "0");
  const seg1 = tsHex.slice(0, 8);
  const seg2 = tsHex.slice(8, 12);

  // Segment 3: version nibble 0x7 + 12 random bits (rand[0..1])
  let randA = ((rand[0]! << 8) | rand[1]!) & 0x0fff;
  randA |= 0x7000;
  const seg3 = randA.toString(16).padStart(4, "0");

  // Segment 4: variant 0b10 + 14 random bits (rand[2..3])
  const randBHi = (rand[2]! & 0x3f) | 0x80;
  const seg4 = randBHi.toString(16).padStart(2, "0") + rand[3]!.toString(16).padStart(2, "0");

  // Segment 5: 48 random bits (rand[4..9], 6 bytes = 12 hex chars)
  const seg5 = Array.from(rand.slice(4, 10))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  return [seg1, seg2, seg3, seg4, seg5].join("-");
}
