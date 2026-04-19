import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { Connection, type ConnectionOptions } from "./connection.js";
import {
  OpCode, encodePublish, encodeAck, encodeFetch, encodeSeek,
  encodeCreateStream, encodeCreateConsumer, encodeDeleteConsumer,
  encodeSubscribe, encodeUnsubscribe, decodePublishOk, decodeRecord, decodeRecordsBatch,
  type Frame, DEFAULT_PORT, START_FROM_EARLIEST, START_FROM_LATEST, START_FROM_OFFSET,
} from "./protocol/index.js";
import { Subscription } from "./subscription.js";
import { ServerError, ValidationError } from "./errors.js";
import type {
  BrokerEndpoint, ClientOptions, PublishOptions, PublishResult, CreateStreamOptions,
  CreateConsumerOptions, SubscribeOptions, FetchOptions, FetchRecord,
  SeekOptions, SeekResult,
} from "./types.js";

export class ExspeedClient extends EventEmitter {
  private mainConn: Connection;
  private subscriptions = new Map<string, { sub: Subscription; conn: Connection }>();
  private readonly opts: ClientOptions;

  constructor(options: ClientOptions) {
    super();
    this.opts = options;
    this.mainConn = this.createConnection();
  }

  async connect(): Promise<void> {
    await this.mainConn.connect();
  }

  async close(drainTimeout = 5000): Promise<void> {
    const timer = setTimeout(() => this.forceClose(), drainTimeout);
    try {
      const unsubs = Array.from(this.subscriptions.entries()).map(
        async ([name, { sub, conn }]) => {
          try {
            const payload = encodeUnsubscribe(name, sub.subscriberId);
            await conn.request(OpCode.Unsubscribe, payload);
          } catch {}
          sub.close();
          await conn.close();
        },
      );
      await Promise.allSettled(unsubs);
      this.subscriptions.clear();
      await this.mainConn.close();
    } finally {
      clearTimeout(timer);
    }
  }

  async publish(stream: string, options: PublishOptions): Promise<PublishResult> {
    if (!stream) throw new ValidationError("Stream name is required");
    if (!options.subject) throw new ValidationError("Subject is required");

    let value: Buffer;
    if ("data" in options && options.data !== undefined) {
      value = Buffer.from(JSON.stringify(options.data), "utf8");
    } else if ("value" in options && options.value !== undefined) {
      value = Buffer.isBuffer(options.value) ? options.value : Buffer.from(options.value);
    } else {
      throw new ValidationError("Either data or value must be provided");
    }

    let key: Buffer | undefined;
    if (options.key !== undefined) {
      key = typeof options.key === "string" ? Buffer.from(options.key, "utf8") : options.key;
    }

    let headers = options.headers;
    let msgId: string | undefined;

    if (options.msgId) {
      if (this.mainConn.getServerVersion() >= 2) {
        msgId = options.msgId;
      } else {
        const filtered = (headers ?? []).filter(([k]) => k.toLowerCase() !== 'x-idempotency-key');
        headers = [...filtered, ["x-idempotency-key", options.msgId]];
      }
    }

    const payload = encodePublish({ stream, subject: options.subject, value, key, headers, msgId });
    const response = await this.mainConn.request(OpCode.Publish, payload);
    const { offset, duplicate } = decodePublishOk(response.payload);
    return { offset, duplicate, toJSON: () => ({ offset: offset.toString() }) };
  }

  async subscribe(consumerName: string, options?: SubscribeOptions): Promise<Subscription & { unsubscribe(): Promise<void> }> {
    if (!consumerName) throw new ValidationError("Consumer name is required");

    const subscriberId =
      options?.subscriberId ?? `${this.opts.clientId}-${randomUUID()}`;

    const conn = this.createConnection();
    await conn.connect();

    const sub = new Subscription(consumerName, subscriberId, {
      maxQueueSize: options?.maxQueueSize ?? 1000,
      overflowPolicy: options?.overflowPolicy ?? "drop-oldest",
    });

    const ackFn = async (cn: string, offset: bigint) => {
      const payload = encodeAck({ consumerName: cn, offset });
      await conn.request(OpCode.Ack, payload);
    };
    const nackFn = async (cn: string, offset: bigint) => {
      const payload = encodeAck({ consumerName: cn, offset });
      await conn.request(OpCode.Nack, payload);
    };

    conn.on("push", (frame: Frame) => {
      if (frame.opcode === OpCode.Record) {
        try {
          const record = decodeRecord(frame.payload);
          if (record.consumerName === consumerName) {
            sub.push(record, ackFn, nackFn);
          }
        } catch (err) {
          sub.emit("error", err);
        }
      }
    });

    conn.on("reconnected", async () => {
      try {
        // Re-subscribe with the SAME subscriber_id so the server can rebuild
        // the same subscription identity after reconnect.
        const payload = encodeSubscribe(consumerName, subscriberId);
        await conn.request(OpCode.Subscribe, payload);
      } catch (err: any) {
        if (err?.code === 404) {
          sub.emit("subscription_lost", consumerName);
        } else {
          sub.emit("error", err);
        }
      }
    });

    const payload = encodeSubscribe(consumerName, subscriberId);
    await conn.request(OpCode.Subscribe, payload);
    this.subscriptions.set(consumerName, { sub, conn });

    const unsubscribe = async () => {
      try {
        const p = encodeUnsubscribe(consumerName, subscriberId);
        await conn.request(OpCode.Unsubscribe, p);
      } catch {}
      sub.close();
      await conn.close();
      this.subscriptions.delete(consumerName);
    };

    return Object.assign(sub, { unsubscribe });
  }

  async createStream(name: string, options?: CreateStreamOptions): Promise<void> {
    if (!name) throw new ValidationError("Stream name is required");
    const payload = encodeCreateStream({
      name,
      maxAgeSecs: BigInt(options?.maxAgeSecs ?? 0),
      maxBytes: BigInt(options?.maxBytes ?? 0),
    });
    try {
      await this.mainConn.request(OpCode.CreateStream, payload);
    } catch (e) {
      if (options?.ifNotExists && e instanceof ServerError && e.code === 409) return;
      throw e;
    }
  }

  async createConsumer(options: CreateConsumerOptions): Promise<void> {
    if (!options.name) throw new ValidationError("Consumer name is required");
    if (!options.stream) throw new ValidationError("Stream name is required");

    let startFrom: number;
    let startOffset: bigint;
    if (options.startFrom === undefined || options.startFrom === "earliest") {
      startFrom = START_FROM_EARLIEST; startOffset = 0n;
    } else if (options.startFrom === "latest") {
      startFrom = START_FROM_LATEST; startOffset = 0n;
    } else {
      startFrom = START_FROM_OFFSET; startOffset = options.startFrom.offset;
    }

    const payload = encodeCreateConsumer({
      name: options.name, stream: options.stream,
      group: options.group ?? "", subjectFilter: options.subjectFilter ?? "",
      startFrom, startOffset,
    });
    try {
      await this.mainConn.request(OpCode.CreateConsumer, payload);
    } catch (e) {
      if (options.ifNotExists && e instanceof ServerError && e.code === 409) return;
      throw e;
    }
  }

  async deleteConsumer(name: string): Promise<void> {
    if (!name) throw new ValidationError("Consumer name is required");
    await this.mainConn.request(OpCode.DeleteConsumer, encodeDeleteConsumer(name));
  }

  async fetch(stream: string, options: FetchOptions): Promise<FetchRecord[]> {
    if (!stream) throw new ValidationError("Stream name is required");
    const payload = encodeFetch({
      stream, offset: options.offset, maxRecords: options.maxRecords, subjectFilter: options.subjectFilter,
    });
    const response = await this.mainConn.request(OpCode.Fetch, payload);
    const records = decodeRecordsBatch(response.payload);
    return records.map((r) => ({
      offset: r.offset, timestamp: r.timestamp, subject: r.subject,
      key: r.key, headers: r.headers,
      json: <T = unknown>() => JSON.parse(r.value.toString("utf8")) as T,
      raw: () => r.value,
      toJSON: () => ({ offset: r.offset.toString(), timestamp: r.timestamp.toString(), subject: r.subject, headers: r.headers }),
    }));
  }

  async seek(consumerName: string, options: SeekOptions): Promise<SeekResult> {
    if (!consumerName) throw new ValidationError("Consumer name is required");
    const payload = encodeSeek({ consumerName, timestamp: options.timestamp });
    const response = await this.mainConn.request(OpCode.Seek, payload);
    return { offset: response.payload.readBigUInt64LE(0) };
  }

  async ping(): Promise<number> {
    const start = performance.now();
    await this.mainConn.request(OpCode.Ping, Buffer.alloc(0));
    return performance.now() - start;
  }

  private createConnection(): Connection {
    const endpoints = this.resolveEndpoints();
    const connOpts: ConnectionOptions = {
      endpoints,
      clientId: this.opts.clientId,
      auth: this.opts.auth,
      reconnect: this.opts.reconnect ?? true,
      maxReconnectAttempts: this.opts.maxReconnectAttempts,
      reconnectBaseDelay: this.opts.reconnectBaseDelay,
      reconnectMaxDelay: this.opts.reconnectMaxDelay,
      requestTimeout: this.opts.requestTimeout ?? 10000,
      pingInterval: this.opts.pingInterval ?? 30000,
      tls: this.opts.tls ?? false,
    };
    const conn = new Connection(connOpts);
    conn.on("connected", (ep) => this.emit("connected", ep));
    conn.on("disconnected", (info) => this.emit("disconnected", info));
    conn.on("reconnecting", (info) => this.emit("reconnecting", info));
    conn.on("reconnected", (info) => this.emit("reconnected", info));
    conn.on("endpoint_changed", (info) => this.emit("endpoint_changed", info));
    conn.on("close", (info) => this.emit("close", info));
    return conn;
  }

  private resolveEndpoints(): BrokerEndpoint[] {
    if (this.opts.brokers && this.opts.brokers.length > 0) {
      return this.opts.brokers.map((b) => ({
        host: b.host,
        port: b.port ?? DEFAULT_PORT,
      }));
    }
    return [{
      host: this.opts.host ?? "localhost",
      port: this.opts.port ?? DEFAULT_PORT,
    }];
  }

  private forceClose(): void {
    for (const [, { sub, conn }] of this.subscriptions) {
      sub.close();
      conn.close().catch(() => {});
    }
    this.subscriptions.clear();
    this.mainConn.close().catch(() => {});
  }
}
