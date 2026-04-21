import { Connection } from "./connection.js";
import { OpCode } from "./protocol/opcodes.js";
import { encodePublish, decodePublishOk } from "./protocol/publish.js";
import type { PublishOptions, PublishResult, BrokerEndpoint } from "./types.js";

export interface PublisherOptions {
  endpoints: BrokerEndpoint[];
  clientId?: string;
  maxInFlight?: number;
}

/**
 * Pipelined publisher. Owns one Connection. Multiple publish() calls share
 * the same TCP socket via correlation-ID demultiplexing in Connection.
 *
 * For one-shot publishes, prefer `client.publish()` which uses a shared
 * connection internally. Use Publisher when sending a high-throughput
 * stream of messages and you want an explicit Publisher handle.
 */
export class Publisher {
  private conn: Connection;
  private closed = false;

  constructor(opts: PublisherOptions) {
    this.conn = new Connection({
      endpoints: opts.endpoints,
      clientId: opts.clientId ?? "exspeed-publisher",
      reconnect: true,
      requestTimeout: 30_000,
      pingInterval: 30_000,
    });
  }

  static async open(opts: PublisherOptions): Promise<Publisher> {
    const p = new Publisher(opts);
    await p.conn.connect();
    return p;
  }

  async publish(stream: string, options: PublishOptions): Promise<PublishResult> {
    if (this.closed) throw new Error("Publisher closed");
    const payload = encodePublish({
      stream,
      subject: options.subject,
      value: this.coerceValue(options),
      key: this.coerceKey(options),
      headers: options.headers,
      msgId: options.msgId,
    });
    const response = await this.conn.request(OpCode.Publish, payload);
    const { offset, duplicate } = decodePublishOk(response.payload);
    return { offset, duplicate, toJSON: () => ({ offset: offset.toString() }) };
  }

  async flush(): Promise<void> {
    // No-op: Connection.request already awaits per call. Provided for API parity with the Rust SDK.
  }

  async close(): Promise<void> {
    this.closed = true;
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
