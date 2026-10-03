import { EventEmitter } from "node:events";
import { Connection, type ConnectionOptions, type RequestOptions } from "./connection.js";
import { ConnectionError, ExspeedError, ProtocolError, ServerError } from "./errors.js";
import { Message, StreamRecord } from "./message.js";
import { DEFAULT_PORT, ErrorCode } from "./protocol/constants.js";
import { SeekKind, type Request, type Response } from "./protocol/messages.js";
import { Publisher, type PublisherOptions } from "./publisher.js";
import { Subscription, type SubscriptionHost } from "./subscription.js";
import {
  camelize,
  toWireConsumerSpec,
  toWirePublishRecord,
  toWireStreamSpec,
  type ClientOptions,
  type ConsumerInfo,
  type ConsumerSpec,
  type Metadata,
  type PublishInput,
  type PublishResult,
  type PullOptions,
  type QueryResult,
  type ReadOptions,
  type ReconnectOptions,
  type SeekTarget,
  type ServerInfo,
  type StreamInfo,
  type StreamSpec,
  type SubscribeOptions,
} from "./types.js";

type ResponseOf<T extends Response["type"]> = Extract<Response, { type: T }>;

/** Result of a stateless {@link ExspeedClient.read}. */
export interface ReadResult {
  records: StreamRecord[];
  /** Pass as `from` to continue. */
  nextOffset: number;
  /** The stream's next offset at the time of the read. */
  highWatermark: number;
}

type State = "connected" | "reconnecting" | "closed";

/**
 * A connection to an Exspeed server (client protocol v2).
 *
 * One client is one TCP/TLS connection. Requests are multiplexed by
 * correlation id, so a slow pull or long-poll read never blocks other
 * calls; share one client across your application.
 *
 * Events:
 * - `"disconnect"` `(err: Error)`: the connection dropped; reconnecting.
 * - `"reconnect"` `(info: ServerInfo)`: reconnected; subscriptions restored.
 * - `"close"` `(err?: Error)`: the client is closed for good (by `close()`,
 *   or because reconnecting gave up).
 * - `"error"` `(err: ServerError | ProtocolError)`: a fire-and-forget request
 *   (ack, credit) failed. Only emitted when a listener is attached.
 */
export class ExspeedClient extends EventEmitter {
  private conn: Connection;
  private state: State = "connected";
  private readonly connOpts: ConnectionOptions;
  private readonly reconnectOpts: Required<ReconnectOptions> | null;
  private readonly subs = new Set<Subscription>();
  /** Ephemeral consumers this client created, re-created after a reconnect. */
  private readonly ephemeral = new Map<string, ConsumerSpec>();
  private readonly host: SubscriptionHost;
  private pendingAcks = new Map<string, number[]>();
  private ackFlushScheduled = false;

  private readonly servers: string[];

  private constructor(
    conn: Connection,
    connOpts: ConnectionOptions,
    reconnect: Required<ReconnectOptions> | null,
    servers: string[],
  ) {
    super();
    this.servers = servers;
    this.conn = conn;
    this.connOpts = connOpts;
    this.reconnectOpts = reconnect;
    this.host = {
      forget: (sub) => this.subs.delete(sub),
      ackNowait: (consumer, offsets) => this.ackNowait(consumer, offsets),
      nack: (consumer, offset, delayMs) => this.nack(consumer, offset, delayMs),
      term: (consumer, offset, reason) => this.term(consumer, offset, reason),
      inProgress: (consumer, offsets) => this.inProgress(consumer, offsets),
    };
  }

  /** Connect and authenticate. The first connection attempt is not retried. */
  static async connect(options: ClientOptions = {}): Promise<ExspeedClient> {
    const connOpts: ConnectionOptions = {
      host: options.host ?? "127.0.0.1",
      port: options.port ?? DEFAULT_PORT,
      tls: options.tls,
      clientId: options.clientId ?? "exspeed-ts",
      token: options.token,
      requestTimeoutMs: options.requestTimeoutMs ?? 30_000,
      keepaliveMs: options.keepaliveMs ?? 20_000,
    };
    const r = options.reconnect ?? true;
    const reconnect =
      r === false
        ? null
        : {
            maxAttempts: Infinity,
            initialDelayMs: 100,
            maxDelayMs: 5_000,
            ...(typeof r === "object" ? r : {}),
          };
    const servers = options.servers ?? [];
    let client: ExspeedClient | null = null;
    const conn = await openLeader(connOpts, servers, null, {
      onClose: (c, err) => {
        if (client && client.conn === c) client.onConnectionLost(err);
      },
      onAsyncError: (err) => client?.onAsyncError(err),
    });
    client = new ExspeedClient(conn, connOpts, reconnect, servers);
    return client;
  }

  /** Handshake info from the current connection. */
  get serverInfo(): ServerInfo {
    return this.conn.info;
  }

  /** True while a connection is up (false while reconnecting or after `close()`). */
  get connected(): boolean {
    return this.state === "connected" && !this.conn.closed;
  }

  /**
   * Close the connection. Pending requests fail with `ConnectionError`,
   * subscriptions end (code 0), and the server deletes this connection's
   * ephemeral consumers.
   */
  async close(): Promise<void> {
    if (this.state === "closed") return;
    this.flushAcks(); // acks made just before close() still go out
    this.state = "closed";
    for (const sub of [...this.subs]) sub.end({ code: 0, message: "client closed" }, false);
    await this.conn.close();
    this.emit("close");
  }

  // ---- basics ---------------------------------------------------------------

  /** Round trip to the server; resolves with the latency in ms. */
  async ping(): Promise<number> {
    const start = performance.now();
    await this.call({ type: "Ping" }, "Pong");
    return performance.now() - start;
  }

  /** Node id, leadership and server version. */
  async metadata(): Promise<Metadata> {
    return camelize<Metadata>(await this.json({ type: "Metadata" }));
  }

  // ---- streams --------------------------------------------------------------

  /**
   * Create a stream. Idempotent when it exists with the same settings; a
   * `ServerError` 409 when the settings differ.
   */
  async createStream(spec: StreamSpec | string): Promise<void> {
    await this.call({ type: "CreateStream", spec: toWireStreamSpec(spec) }, "Ok");
  }

  /** Replace a stream's settings (omitted fields reset to the server defaults). */
  async updateStream(spec: StreamSpec): Promise<void> {
    await this.call({ type: "UpdateStream", spec: toWireStreamSpec(spec) }, "Ok");
  }

  /** Delete a stream. Fails with 409 while consumers exist (`detail.consumers`). */
  async deleteStream(name: string): Promise<void> {
    await this.call({ type: "DeleteStream", name }, "Ok");
  }

  async streamInfo(name: string): Promise<StreamInfo> {
    return camelize<StreamInfo>(await this.json({ type: "StreamInfo", name }));
  }

  /** The streams this credential can see. */
  async listStreams(): Promise<StreamInfo[]> {
    return camelize<StreamInfo[]>(await this.json({ type: "ListStreams" }));
  }

  // ---- publishing -----------------------------------------------------------

  /** Publish one record and wait for its offset. */
  async publish(stream: string, record: PublishInput): Promise<PublishResult> {
    const r = await this.call({ type: "Publish", stream, record: toWirePublishRecord(record) }, "PublishOk");
    return { offset: r.offset, duplicate: r.duplicate };
  }

  /** Publish several records in one request; one result per record, in order. */
  async publishBatch(stream: string, records: PublishInput[]): Promise<PublishResult[]> {
    if (records.length === 0) return [];
    const r = await this.call(
      { type: "PublishBatch", stream, records: records.map(toWirePublishRecord) },
      "PublishBatchOk",
    );
    return r.results.map((x) => ({ offset: x.offset, duplicate: x.duplicate }));
  }

  /** A coalescing, order-preserving publisher on this client; see {@link Publisher}. */
  publisher(options: PublisherOptions = {}): Publisher {
    return new Publisher({ request: (req) => this.request(req) }, options);
  }

  // ---- stateless reads ------------------------------------------------------

  /**
   * Read records without a consumer. With `waitMs`, waits for new records
   * when caught up. Continue from `nextOffset`.
   */
  async read(stream: string, opts: ReadOptions = {}): Promise<ReadResult> {
    const waitMs = opts.waitMs ?? 0;
    const r = await this.call(
      {
        type: "Read",
        stream,
        from: opts.from ?? 0,
        maxRecords: opts.maxRecords ?? 100,
        maxBytes: opts.maxBytes ?? 0,
        waitMs,
        filter: opts.filter ?? "",
      },
      "ReadResult",
      { timeoutMs: this.connOpts.requestTimeoutMs + waitMs },
    );
    return {
      records: r.records.map((w) => new StreamRecord(w)),
      nextOffset: r.nextOffset,
      highWatermark: r.highWatermark,
    };
  }

  // ---- SQL ------------------------------------------------------------------

  /** Run a bounded ExQL query. Needs global admin when auth is on. */
  async query(sql: string): Promise<QueryResult> {
    const v = (await this.json({ type: "Query", sql })) as {
      columns: string[];
      rows: unknown[][];
      row_count: number;
      execution_time_ms: number;
    };
    return { columns: v.columns, rows: v.rows, rowCount: v.row_count, executionTimeMs: v.execution_time_ms };
  }

  // ---- consumers ------------------------------------------------------------

  /**
   * Create a consumer. Idempotent for an identical spec; 409 when a
   * consumer of that name exists with a different spec.
   */
  async createConsumer(spec: ConsumerSpec): Promise<ConsumerInfo> {
    const info = camelize<ConsumerInfo>(await this.json({ type: "CreateConsumer", spec: toWireConsumerSpec(spec) }));
    if (spec.ephemeral) this.ephemeral.set(spec.name, spec);
    return info;
  }

  async deleteConsumer(name: string): Promise<void> {
    await this.call({ type: "DeleteConsumer", name }, "Ok");
    this.ephemeral.delete(name);
  }

  async consumerInfo(name: string): Promise<ConsumerInfo> {
    return camelize<ConsumerInfo>(await this.json({ type: "ConsumerInfo", name }));
  }

  /** Consumers this credential can see, optionally only those on `stream`. */
  async listConsumers(stream?: string): Promise<ConsumerInfo[]> {
    return camelize<ConsumerInfo[]>(await this.json({ type: "ListConsumers", stream: stream ?? null }));
  }

  /** Move a consumer's cursor. */
  async seek(consumer: string, to: SeekTarget): Promise<void> {
    let kind: SeekKind;
    let value = 0;
    if (to === "earliest" || (typeof to === "object" && "earliest" in to)) kind = SeekKind.Earliest;
    else if (to === "latest" || (typeof to === "object" && "latest" in to)) kind = SeekKind.Latest;
    else if (typeof to === "object" && "offset" in to) {
      kind = SeekKind.Offset;
      value = to.offset;
    } else if (typeof to === "object" && "timeMs" in to) {
      kind = SeekKind.Time;
      value = to.timeMs instanceof Date ? to.timeMs.getTime() : to.timeMs;
    } else {
      throw new ExspeedError(`invalid seek target: ${JSON.stringify(to)}`);
    }
    await this.call({ type: "SeekConsumer", consumer, kind, value }, "Ok");
  }

  /**
   * Start push delivery from a consumer. Any number of subscriptions (on
   * any connection, in any process) can share one consumer; each record goes
   * to one of them.
   */
  async subscribe(consumer: string, opts: SubscribeOptions = {}): Promise<Subscription> {
    const window = Math.max(1, Math.min(opts.window ?? 256, 0xffffffff));
    const sub = new Subscription(this.host, consumer, window);
    // The connection binds `sub` to its id as soon as SubscribeOk arrives,
    // before any Deliver behind it is routed.
    await this.call({ type: "Subscribe", consumer, credits: window }, "SubscribeOk", { sink: sub });
    this.subs.add(sub);
    return sub;
  }

  /**
   * Fetch up to `maxMessages`, waiting up to `expiresMs` for at least one.
   * Resolves with an empty array on timeout.
   */
  async pull(consumer: string, opts: PullOptions = {}): Promise<Message[]> {
    const expiresMs = opts.expiresMs ?? 5_000;
    const r = await this.call(
      {
        type: "Pull",
        consumer,
        maxMessages: opts.maxMessages ?? 100,
        maxBytes: opts.maxBytes ?? 0,
        expiresMs,
      },
      "Messages",
      { timeoutMs: this.connOpts.requestTimeoutMs + expiresMs },
    );
    return r.records.map((w) => new Message(w, consumer, this.host));
  }

  /** Acknowledge records and wait for the server to confirm. */
  async ack(consumer: string, offsets: number[]): Promise<void> {
    await this.call({ type: "Ack", consumer, offsets }, "Ok");
  }

  /** Redeliver after `delayMs` (default 0 = the consumer's backoff). */
  async nack(consumer: string, offset: number, delayMs = 0): Promise<void> {
    await this.call({ type: "Nack", consumer, offset, delayMs }, "Ok");
  }

  /** Dead-letter now (to the consumer's `dlqStream`, if set). */
  async term(consumer: string, offset: number, reason = ""): Promise<void> {
    await this.call({ type: "Term", consumer, offset, reason }, "Ok");
  }

  /** Reset the ack deadlines of records still being worked on. */
  async inProgress(consumer: string, offsets: number[]): Promise<void> {
    await this.call({ type: "InProgress", consumer, offsets }, "Ok");
  }

  // ---- plumbing -------------------------------------------------------------

  /**
   * Queue a fire-and-forget ack. Acks made in the same event-loop turn go
   * out as one `Ack` frame per consumer (flushed on `setImmediate`), and
   * always before any later request, so wire order matches call order.
   * Coalescing matters: the server does work per `Ack` command, so one ack
   * per frame throttles consumption badly.
   */
  private ackNowait(consumer: string, offsets: number[]): void {
    if (this.state !== "connected") return; // redelivered after the reconnect
    const queued = this.pendingAcks.get(consumer);
    if (queued) queued.push(...offsets);
    else this.pendingAcks.set(consumer, [...offsets]);
    if (!this.ackFlushScheduled) {
      this.ackFlushScheduled = true;
      setImmediate(() => this.flushAcks());
    }
  }

  private flushAcks(): void {
    this.ackFlushScheduled = false;
    if (this.pendingAcks.size === 0) return;
    const acks = [...this.pendingAcks];
    this.pendingAcks.clear();
    if (this.state !== "connected") return;
    for (const [consumer, offsets] of acks) this.conn.send({ type: "Ack", consumer, offsets });
  }

  /** @internal Send a request on the current connection. */
  request(req: Request, opts?: RequestOptions): Promise<Response> {
    if (this.state === "closed") return Promise.reject(new ConnectionError("client is closed"));
    if (this.state === "reconnecting") return Promise.reject(new ConnectionError("not connected (reconnecting)"));
    this.flushAcks();
    return this.conn.request(req, opts);
  }

  private async call<T extends Response["type"]>(
    req: Request,
    expect: T,
    opts?: RequestOptions,
  ): Promise<ResponseOf<T>> {
    const resp = await this.request(req, opts);
    if (resp.type !== expect) throw new ProtocolError(`unexpected reply to ${req.type}: ${resp.type}`);
    return resp as ResponseOf<T>;
  }

  private async json(req: Request): Promise<unknown> {
    const r = await this.call(req, "Json");
    try {
      return JSON.parse(r.json.toString("utf8"));
    } catch (e) {
      throw new ProtocolError(`bad JSON in reply to ${req.type}: ${(e as Error).message}`);
    }
  }

  private onAsyncError(err: ServerError | ProtocolError): void {
    if (this.listenerCount("error") > 0) this.emit("error", err);
  }

  private onConnectionLost(err: Error): void {
    if (this.state !== "connected") return;
    if (!this.reconnectOpts) {
      this.state = "closed";
      for (const sub of [...this.subs]) sub.end({ code: ErrorCode.Unavailable, message: "connection closed" }, false);
      this.emit("close", err);
      return;
    }
    this.state = "reconnecting";
    this.pendingAcks.clear(); // those records will be redelivered
    for (const sub of this.subs) sub.suspend();
    this.emit("disconnect", err);
    void this.reconnectLoop(this.reconnectOpts);
  }

  private async reconnectLoop(opts: Required<ReconnectOptions>): Promise<void> {
    let lastErr: Error = new ConnectionError("connection lost");
    for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
      const delay = Math.min(opts.initialDelayMs * 2 ** (attempt - 1), opts.maxDelayMs);
      await new Promise((r) => setTimeout(r, delay * (0.75 + Math.random() * 0.5)));
      if (this.state !== "reconnecting") return; // closed meanwhile
      let conn: Connection;
      try {
        conn = await openLeader(this.connOpts, this.servers, this.conn.info.leader, {
          onClose: (c, e) => {
            if (this.conn === c) this.onConnectionLost(e);
          },
          onAsyncError: (e) => this.onAsyncError(e),
        });
      } catch (err) {
        lastErr = err as Error;
        // A rejected credential won't get better by retrying.
        if (err instanceof ServerError && (err.code === ErrorCode.Unauthorized || err.code === ErrorCode.Forbidden)) {
          break;
        }
        continue;
      }
      if (this.state !== "reconnecting") {
        void conn.close();
        return;
      }
      this.conn = conn;
      this.state = "connected";
      await this.restore(conn);
      if (this.conn === conn && this.state === "connected") this.emit("reconnect", conn.info);
      return;
    }
    if (this.state !== "reconnecting") return;
    this.state = "closed";
    for (const sub of [...this.subs]) {
      sub.end({ code: ErrorCode.Unavailable, message: `connection lost: ${lastErr.message}` }, false);
    }
    this.emit("close", lastErr);
  }

  /** Re-create ephemeral consumers, then re-subscribe every live subscription. */
  private async restore(conn: Connection): Promise<void> {
    for (const spec of this.ephemeral.values()) {
      try {
        await conn.request({ type: "CreateConsumer", spec: toWireConsumerSpec(spec) });
      } catch (err) {
        if (err instanceof ConnectionError) return; // lost again; the next loop retries
      }
    }
    await Promise.all(
      [...this.subs].map(async (sub) => {
        try {
          await conn.request({ type: "Subscribe", consumer: sub.consumer, credits: sub.window }, { sink: sub });
        } catch (err) {
          if (err instanceof ConnectionError) return; // stays suspended for the next attempt
          const e = err as Error;
          sub.end({ code: err instanceof ServerError ? err.code : ErrorCode.Internal, message: e.message }, false);
        }
      }),
    );
  }
}

interface LeaderHandlers {
  onClose(conn: Connection, err: Error): void;
  onAsyncError(err: ServerError | ProtocolError): void;
}

function parseAddr(addr: string, fallbackPort: number): { host: string; port: number } {
  const i = addr.lastIndexOf(":");
  if (i <= 0) return { host: addr, port: fallbackPort };
  const port = Number(addr.slice(i + 1));
  return { host: addr.slice(0, i).replace(/^\[|\]$/g, ""), port: Number.isFinite(port) ? port : fallbackPort };
}

/**
 * Open a connection to the cluster leader. Without seed `servers` this is a
 * plain connect to `opts.host:opts.port`, except that a node naming another
 * node as leader in its handshake is followed. With seeds, each candidate
 * (the last known leader first) is asked whether it leads; leader hints are
 * followed, and a follower is accepted only when no node claims to lead.
 */
async function openLeader(
  opts: ConnectionOptions,
  servers: string[],
  hint: string | null,
  handlers: LeaderHandlers,
): Promise<Connection> {
  const queue: string[] = [];
  if (hint) queue.push(hint);
  if (servers.length > 0) queue.push(...servers);
  else queue.push(`${opts.host}:${opts.port}`);
  const tried = new Set<string>();
  let fallback: Connection | null = null;
  let lastErr: Error | null = null;
  while (queue.length > 0) {
    const addr = queue.shift()!;
    if (tried.has(addr)) continue;
    tried.add(addr);
    const { host, port } = parseAddr(addr, opts.port);
    let conn: Connection;
    // `onClose` can fire while the handshake is still failing, before
    // `open` resolves; such a connection was never handed out, so ignore it.
    let opened: Connection | undefined;
    try {
      opened = await Connection.open(
        { ...opts, host, port },
        {
          onClose: (err) => {
            if (opened) handlers.onClose(opened, err);
          },
          onAsyncError: (err) => handlers.onAsyncError(err),
        },
      );
      conn = opened;
    } catch (err) {
      if (err instanceof ServerError && (err.code === ErrorCode.Unauthorized || err.code === ErrorCode.Forbidden)) {
        throw err;
      }
      lastErr = err as Error;
      continue;
    }
    let isLeader = conn.info.leader === null;
    let leader = conn.info.leader;
    if (servers.length > 0) {
      try {
        const resp = await conn.request({ type: "Metadata" });
        if (resp.type === "Json") {
          const m = JSON.parse(resp.json.toString("utf8")) as { is_leader?: boolean; leader?: string | null };
          isLeader = m.is_leader === true;
          leader = m.leader ?? null;
        }
      } catch {
        // An old server without Metadata: trust the handshake.
      }
    }
    if (isLeader) {
      if (fallback) void fallback.close();
      return conn;
    }
    if (leader && !tried.has(leader)) queue.unshift(leader);
    if (!fallback) fallback = conn;
    else void conn.close();
  }
  if (fallback) return fallback;
  throw lastErr ?? new ConnectionError("no server reachable");
}
