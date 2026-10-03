/**
 * CRC32C (Castagnoli), as used by the record encoding: every `WireRecord`
 * carries the CRC32C of its bytes after `delivery_count`. The SDK computes
 * it when encoding (the unit tests' fake server) and can verify it with
 * {@link verifyRecordCrc}; decoding does not verify by default because a
 * table-driven CRC in JavaScript costs more than the rest of decoding.
 */

const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
    t[i] = c >>> 0;
  }
  return t;
})();

/** CRC32C of `data` (unsigned 32-bit). */
export function crc32c(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = TABLE[(crc ^ data[i]!) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
