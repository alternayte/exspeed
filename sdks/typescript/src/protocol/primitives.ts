export function writeU16(buf: Buffer, offset: number, value: number): number {
  buf.writeUInt16LE(value, offset);
  return 2;
}
export function readU16(buf: Buffer, offset: number): [number, number] {
  return [buf.readUInt16LE(offset), 2];
}
export function writeU32(buf: Buffer, offset: number, value: number): number {
  buf.writeUInt32LE(value, offset);
  return 4;
}
export function readU32(buf: Buffer, offset: number): [number, number] {
  return [buf.readUInt32LE(offset), 4];
}
export function writeU64(buf: Buffer, offset: number, value: bigint): number {
  buf.writeBigUInt64LE(value, offset);
  return 8;
}
export function readU64(buf: Buffer, offset: number): [bigint, number] {
  return [buf.readBigUInt64LE(offset), 8];
}
export function writeString(buf: Buffer, offset: number, s: string): number {
  const bytes = Buffer.from(s, "utf8");
  buf.writeUInt16LE(bytes.length, offset);
  bytes.copy(buf, offset + 2);
  return 2 + bytes.length;
}
export function readString(buf: Buffer, offset: number): [string, number] {
  const len = buf.readUInt16LE(offset);
  const str = buf.toString("utf8", offset + 2, offset + 2 + len);
  return [str, 2 + len];
}
export function stringByteLength(s: string): number {
  return 2 + Buffer.byteLength(s, "utf8");
}
export function writeBytes(buf: Buffer, offset: number, b: Buffer): number {
  buf.writeUInt32LE(b.length, offset);
  b.copy(buf, offset + 4);
  return 4 + b.length;
}
export function readBytes(buf: Buffer, offset: number): [Buffer, number] {
  const len = buf.readUInt32LE(offset);
  const data = Buffer.alloc(len);
  buf.copy(data, 0, offset + 4, offset + 4 + len);
  return [data, 4 + len];
}
export function writeHeaders(buf: Buffer, offset: number, headers: [string, string][]): number {
  let pos = 0;
  buf.writeUInt16LE(headers.length, offset);
  pos += 2;
  for (const [key, val] of headers) {
    pos += writeString(buf, offset + pos, key);
    pos += writeString(buf, offset + pos, val);
  }
  return pos;
}
export function readHeaders(buf: Buffer, offset: number): [[string, string][], number] {
  const count = buf.readUInt16LE(offset);
  let pos = 2;
  const headers: [string, string][] = [];
  for (let i = 0; i < count; i++) {
    const [key, keyLen] = readString(buf, offset + pos);
    pos += keyLen;
    const [val, valLen] = readString(buf, offset + pos);
    pos += valLen;
    headers.push([key, val]);
  }
  return [headers, pos];
}
export function headersSize(headers: [string, string][]): number {
  let size = 2;
  for (const [key, val] of headers) {
    size += stringByteLength(key) + stringByteLength(val);
  }
  return size;
}
