/**
 * Little-endian encoding primitives of client protocol v2, mirroring
 * `Writer` / `Reader` in `crates/exspeed-protocol/src/client.rs`:
 *
 * - `str`     = u16 length + UTF-8 bytes
 * - `lstr`    = u32 length + UTF-8 bytes (SQL text)
 * - `bytes`   = u32 length + raw bytes
 * - `opt<T>`  = u8 flag (0 = absent, 1 = present) + T
 * - `headers` = u16 count + (str key, str value) pairs
 * - `vec<T>`  = u32 count + items
 *
 * Integers that are `u64` on the wire are JavaScript `number`s here. Values
 * above `Number.MAX_SAFE_INTEGER` (2^53 - 1) are rejected rather than
 * silently rounded.
 */
import { ExspeedError, ProtocolError } from "../errors.js";

export type Headers = [string, string][];

const utf8 = new TextDecoder("utf-8", { fatal: true });
const EMPTY = Buffer.alloc(0);

export class Writer {
  private buf: Buffer;
  private pos = 0;

  constructor(initialSize = 128) {
    this.buf = Buffer.allocUnsafe(initialSize);
  }

  private ensure(n: number): void {
    const need = this.pos + n;
    if (need <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < need) size *= 2;
    const next = Buffer.allocUnsafe(size);
    this.buf.copy(next, 0, 0, this.pos);
    this.buf = next;
  }

  /** The bytes written so far. */
  finish(): Buffer {
    return this.buf.subarray(0, this.pos);
  }

  u8(v: number): this {
    this.ensure(1);
    this.buf.writeUInt8(v, this.pos);
    this.pos += 1;
    return this;
  }

  u16(v: number): this {
    this.ensure(2);
    this.buf.writeUInt16LE(v, this.pos);
    this.pos += 2;
    return this;
  }

  u32(v: number): this {
    this.ensure(4);
    this.buf.writeUInt32LE(v, this.pos);
    this.pos += 4;
    return this;
  }

  u64(v: number | bigint): this {
    let big: bigint;
    if (typeof v === "bigint") {
      big = v;
    } else {
      if (!Number.isSafeInteger(v) || v < 0) {
        throw new ExspeedError(`not a valid u64: ${v}`);
      }
      big = BigInt(v);
    }
    if (big < 0n || big > 0xffff_ffff_ffff_ffffn) throw new ExspeedError(`not a valid u64: ${v}`);
    this.ensure(8);
    this.buf.writeBigUInt64LE(big, this.pos);
    this.pos += 8;
    return this;
  }

  raw(b: Uint8Array): this {
    this.ensure(b.length);
    this.buf.set(b, this.pos);
    this.pos += b.length;
    return this;
  }

  str(s: string): this {
    const b = Buffer.from(s, "utf8");
    if (b.length > 0xffff) {
      throw new ExspeedError(`string too long (${b.length} bytes, max 65535): ${s.slice(0, 40)}...`);
    }
    return this.u16(b.length).raw(b);
  }

  lstr(s: string): this {
    return this.bytes(Buffer.from(s, "utf8"));
  }

  bytes(b: Uint8Array): this {
    return this.u32(b.length).raw(b);
  }

  opt<T>(v: T | null | undefined, f: (w: this, v: T) => void): this {
    if (v === null || v === undefined) return this.u8(0);
    this.u8(1);
    f(this, v);
    return this;
  }

  headers(h: Headers): this {
    if (h.length > 0xffff) throw new ExspeedError(`too many headers (${h.length}, max 65535)`);
    this.u16(h.length);
    for (const [k, v] of h) this.str(k).str(v);
    return this;
  }
}

/** Bounds-checked reader; every failure is a {@link ProtocolError}. */
export class Reader {
  private pos = 0;

  constructor(private readonly buf: Buffer) {}

  get remaining(): number {
    return this.buf.length - this.pos;
  }

  private need(n: number): void {
    if (this.remaining < n) {
      throw new ProtocolError(`truncated payload: need ${n} bytes, have ${this.remaining}`);
    }
  }

  /** Fail if bytes are left over (catches encoder/decoder drift). */
  finish(): void {
    if (this.remaining > 0) throw new ProtocolError(`${this.remaining} trailing bytes`);
  }

  u8(): number {
    this.need(1);
    return this.buf[this.pos++]!;
  }

  u16(): number {
    this.need(2);
    const v = this.buf.readUInt16LE(this.pos);
    this.pos += 2;
    return v;
  }

  u32(): number {
    this.need(4);
    const v = this.buf.readUInt32LE(this.pos);
    this.pos += 4;
    return v;
  }

  u64(): number {
    this.need(8);
    const v = this.buf.readBigUInt64LE(this.pos);
    this.pos += 8;
    if (v > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new ProtocolError(`u64 value ${v} exceeds Number.MAX_SAFE_INTEGER`);
    }
    return Number(v);
  }

  private take(n: number): Buffer {
    this.need(n);
    const out = n === 0 ? EMPTY : this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }

  private static utf8(b: Uint8Array): string {
    try {
      return utf8.decode(b);
    } catch {
      throw new ProtocolError("invalid UTF-8");
    }
  }

  str(): string {
    return Reader.utf8(this.take(this.u16()));
  }

  lstr(): string {
    return Reader.utf8(this.bytes());
  }

  bytes(): Buffer {
    return this.take(this.u32());
  }

  opt<T>(f: (r: this) => T): T | null {
    const flag = this.u8();
    if (flag === 0) return null;
    if (flag === 1) return f(this);
    throw new ProtocolError(`invalid option flag ${flag}`);
  }

  /** A u32 element count, rejected when the rest of the payload can't hold that many items of `minSize` bytes. */
  count(minSize: number): number {
    const n = this.u32();
    if (n * Math.max(1, minSize) > this.remaining) {
      throw new ProtocolError(`count ${n} exceeds payload size`);
    }
    return n;
  }

  headers(): Headers {
    const n = this.u16();
    if (n * 4 > this.remaining) throw new ProtocolError("header count exceeds payload");
    const out: Headers = [];
    for (let i = 0; i < n; i++) out.push([this.str(), this.str()]);
    return out;
  }
}
