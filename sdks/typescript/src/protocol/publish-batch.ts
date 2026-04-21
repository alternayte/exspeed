// Matches crates/exspeed-protocol/src/messages/publish_batch.rs.

export interface BatchRecord {
  subject: string;
  key?: Buffer;
  msgId?: string;
  value: Buffer;
  headers: Array<[string, string]>;
}

export interface PublishBatchRequest {
  stream: string;
  records: BatchRecord[];
}

export type BatchResult =
  | { type: "Written"; offset: bigint }
  | { type: "Duplicate"; offset: bigint; duplicateOf: bigint }
  | { type: "Error"; code: number; message: string };

export interface PublishBatchOkResponse {
  results: BatchResult[];
}

const FLAG_HAS_KEY = 0x01;
const FLAG_HAS_MSG_ID = 0x02;

export function encodePublishBatch(req: PublishBatchRequest): Buffer {
  const chunks: Buffer[] = [];
  const streamBuf = Buffer.from(req.stream, "utf8");
  const header = Buffer.alloc(2);
  header.writeUInt16LE(streamBuf.length, 0);
  chunks.push(header, streamBuf);

  const countBuf = Buffer.alloc(2);
  countBuf.writeUInt16LE(req.records.length, 0);
  chunks.push(countBuf);

  for (const r of req.records) {
    const subjectBuf = Buffer.from(r.subject, "utf8");
    const subjectHdr = Buffer.alloc(2);
    subjectHdr.writeUInt16LE(subjectBuf.length, 0);
    chunks.push(subjectHdr, subjectBuf);

    let flags = 0;
    if (r.key !== undefined) flags |= FLAG_HAS_KEY;
    if (r.msgId !== undefined) flags |= FLAG_HAS_MSG_ID;
    chunks.push(Buffer.from([flags]));

    if (r.key !== undefined) {
      const kl = Buffer.alloc(4);
      kl.writeUInt32LE(r.key.length, 0);
      chunks.push(kl, r.key);
    }
    if (r.msgId !== undefined) {
      const idBuf = Buffer.from(r.msgId, "utf8");
      const idHdr = Buffer.alloc(2);
      idHdr.writeUInt16LE(idBuf.length, 0);
      chunks.push(idHdr, idBuf);
    }

    const vl = Buffer.alloc(4);
    vl.writeUInt32LE(r.value.length, 0);
    chunks.push(vl, r.value);

    const hc = Buffer.alloc(2);
    hc.writeUInt16LE(r.headers.length, 0);
    chunks.push(hc);

    for (const [k, v] of r.headers) {
      const kb = Buffer.from(k, "utf8");
      const kh = Buffer.alloc(2);
      kh.writeUInt16LE(kb.length, 0);
      chunks.push(kh, kb);

      const vb = Buffer.from(v, "utf8");
      const vh = Buffer.alloc(2);
      vh.writeUInt16LE(vb.length, 0);
      chunks.push(vh, vb);
    }
  }
  return Buffer.concat(chunks);
}

export function decodePublishBatchOk(payload: Buffer): PublishBatchOkResponse {
  let offset = 0;
  const count = payload.readUInt16LE(offset); offset += 2;
  const results: BatchResult[] = [];
  for (let i = 0; i < count; i++) {
    const status = payload.readUInt8(offset); offset += 1;
    if (status === 0) {
      const off = payload.readBigUInt64LE(offset); offset += 8;
      results.push({ type: "Written", offset: off });
    } else if (status === 1) {
      const off = payload.readBigUInt64LE(offset); offset += 8;
      const dupe = payload.readBigUInt64LE(offset); offset += 8;
      results.push({ type: "Duplicate", offset: off, duplicateOf: dupe });
    } else if (status === 2) {
      const code = payload.readUInt16LE(offset); offset += 2;
      const ml = payload.readUInt16LE(offset); offset += 2;
      const message = payload.slice(offset, offset + ml).toString("utf8"); offset += ml;
      results.push({ type: "Error", code, message });
    } else {
      throw new Error(`unknown batch result status: ${status}`);
    }
  }
  return { results };
}
