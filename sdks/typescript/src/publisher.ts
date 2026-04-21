import { Connection } from "./connection.js";
import { OpCode } from "./protocol/opcodes.js";
import { encodePublishBatch, decodePublishBatchOk } from "./protocol/publish-batch.js";
import type { PublishOptions, PublishResult, BrokerEndpoint } from "./types.js";

export interface PublisherOptions {
  endpoints: BrokerEndpoint[];
  clientId?: string;
  maxInFlight?: number;
  batchWindowMs?: number;      // default 0.1 (100µs)
  maxBatchRecords?: number;    // default 256
}

interface Queued {
  stream: string;
  subject: string;
  key?: Buffer;
  msgId?: string;
  value: Buffer;
  headers: Array<[string, string]>;
  resolve: (r: PublishResult) => void;
  reject: (e: Error) => void;
}

/**
 * Pipelined publisher with transparent coalescing. Owns one Connection.
 * Multiple `publish()` calls coalesce into one PublishBatch frame per
 * batchWindowMs (default 100µs) or when maxBatchRecords queue up.
 */
export class Publisher {
  private conn: Connection;
  private closed = false;
  private queue: Queued[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly batchWindowMs: number;
  private readonly maxBatchRecords: number;

  constructor(opts: PublisherOptions) {
    this.conn = new Connection({
      endpoints: opts.endpoints,
      clientId: opts.clientId ?? "exspeed-publisher",
      reconnect: true,
      requestTimeout: 30_000,
      pingInterval: 30_000,
    });
    this.batchWindowMs = opts.batchWindowMs ?? 0.1;
    this.maxBatchRecords = opts.maxBatchRecords ?? 256;
  }

  static async open(opts: PublisherOptions): Promise<Publisher> {
    const p = new Publisher(opts);
    await p.conn.connect();
    return p;
  }

  async publish(stream: string, options: PublishOptions): Promise<PublishResult> {
    if (this.closed) throw new Error("Publisher closed");
    return new Promise<PublishResult>((resolve, reject) => {
      this.queue.push({
        stream,
        subject: options.subject,
        key: this.coerceKey(options),
        msgId: options.msgId,
        value: this.coerceValue(options),
        headers: options.headers ?? [],
        resolve,
        reject,
      });
      if (this.queue.length >= this.maxBatchRecords) {
        this.flushNow();
      } else if (this.flushTimer === null) {
        this.flushTimer = setTimeout(() => this.flushNow(), this.batchWindowMs);
      }
    });
  }

  async publishBatch(stream: string, reqs: PublishOptions[]): Promise<PublishResult[]> {
    if (this.closed) throw new Error("Publisher closed");
    if (reqs.length === 0) return [];

    const records = reqs.map((r) => ({
      subject: r.subject,
      key: this.coerceKey(r),
      msgId: r.msgId,
      value: this.coerceValue(r),
      headers: r.headers ?? [],
    }));
    const payload = encodePublishBatch({ stream, records });
    const response = await this.conn.request(OpCode.PublishBatch, payload);
    const decoded = decodePublishBatchOk(response.payload);
    return decoded.results.map((r) => {
      if (r.type === "Written" || r.type === "Duplicate") {
        return { offset: r.offset, duplicate: r.type === "Duplicate", toJSON: () => ({ offset: r.offset.toString() }) };
      }
      throw new Error(`Publish error code=${r.code} msg=${r.message}`);
    });
  }

  private flushNow(): void {
    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.queue.length === 0) return;

    const byStream = new Map<string, Queued[]>();
    for (const q of this.queue) {
      const arr = byStream.get(q.stream);
      if (arr) arr.push(q);
      else byStream.set(q.stream, [q]);
    }
    this.queue = [];

    for (const [stream, group] of byStream) {
      this.sendBatch(stream, group).catch((e) => {
        for (const q of group) q.reject(e);
      });
    }
  }

  private async sendBatch(stream: string, group: Queued[]): Promise<void> {
    const records = group.map((q) => ({
      subject: q.subject,
      key: q.key,
      msgId: q.msgId,
      value: q.value,
      headers: q.headers,
    }));
    const payload = encodePublishBatch({ stream, records });
    try {
      const response = await this.conn.request(OpCode.PublishBatch, payload);
      const decoded = decodePublishBatchOk(response.payload);
      group.forEach((q, i) => {
        const r = decoded.results[i];
        if (!r) {
          q.reject(new Error("server returned fewer results than records in batch"));
          return;
        }
        if (r.type === "Written" || r.type === "Duplicate") {
          q.resolve({
            offset: r.offset,
            duplicate: r.type === "Duplicate",
            toJSON: () => ({ offset: r.offset.toString() }),
          });
        } else {
          q.reject(new Error(`Publish error code=${r.code} msg=${r.message}`));
        }
      });
    } catch (e) {
      for (const q of group) q.reject(e as Error);
    }
  }

  async flush(): Promise<void> {
    if (this.queue.length > 0) this.flushNow();
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.flush();
    await this.conn.close();
  }

  private coerceValue(options: PublishOptions): Buffer {
    if ("data" in options && options.data !== undefined) {
      return Buffer.from(JSON.stringify(options.data), "utf8");
    }
    if ("value" in options && options.value !== undefined) {
      return Buffer.isBuffer(options.value) ? options.value : Buffer.from(options.value);
    }
    throw new Error("Either data or value must be provided");
  }

  private coerceKey(options: PublishOptions): Buffer | undefined {
    if (options.key === undefined) return undefined;
    return typeof options.key === "string" ? Buffer.from(options.key, "utf8") : options.key;
  }
}
