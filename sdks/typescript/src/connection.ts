/**
 * One TCP (or TLS) connection: handshake, frame parsing, correlation-id
 * multiplexing, push routing and keepalive. Reconnection lives one level up,
 * in `ExspeedClient`.
 */
import * as net from "node:net";
import * as tls from "node:tls";
import { ConnectionError, ProtocolError, ServerError, TimeoutError } from "./errors.js";
import { DEFAULT_PORT } from "./protocol/constants.js";
import { FrameParser } from "./protocol/frame.js";
import {
  decodeResponse,
  requestFrame,
  type Request,
  type Response,
  type WireRecord,
} from "./protocol/messages.js";
import type { ServerInfo } from "./types.js";

export interface ConnectionOptions {
  host: string;
  port: number;
  tls?: boolean | tls.ConnectionOptions;
  clientId: string;
  token?: string;
  requestTimeoutMs: number;
  keepaliveMs: number;
}

/** Receives a subscription's pushes. */
export interface SubscriptionSink {
  /** Called synchronously when `SubscribeOk` arrives, before any `Deliver` for it is routed. */
  onSubscribed(conn: Connection, subId: number): void;
  onDeliver(records: WireRecord[]): void;
  onEnded(code: number, message: string): void;
}

export interface ConnectionHandlers {
  /** The connection closed (for any reason other than `close()`). Called once. */
  onClose(err: Error): void;
  /** An `Error` with correlation id 0: a fire-and-forget request failed. */
  onAsyncError(err: ServerError | ProtocolError): void;
}

interface Pending {
  resolve(resp: Response): void;
  reject(err: Error): void;
  timer: ReturnType<typeof setTimeout> | null;
  sink?: SubscriptionSink;
}

export interface RequestOptions {
  /** Overrides the default request timeout. */
  timeoutMs?: number;
  /** For `Subscribe`: where the subscription's pushes go. */
  sink?: SubscriptionSink;
}

export function errorFromResponse(resp: Extract<Response, { type: "Error" }>): ServerError {
  let detail: unknown = null;
  if (resp.detail && resp.detail.length > 0) {
    try {
      detail = JSON.parse(resp.detail.toString("utf8"));
    } catch {
      detail = resp.detail.toString("utf8");
    }
  }
  return new ServerError(resp.code, resp.message, detail);
}

export class Connection {
  private readonly parser = new FrameParser();
  private readonly pending = new Map<number, Pending>();
  private readonly subs = new Map<number, SubscriptionSink>();
  private nextCorr = 1;
  private keepalive: ReturnType<typeof setInterval> | null = null;
  private _closed = false;
  private closedByUser = false;
  private _info: ServerInfo | null = null;

  private constructor(
    private readonly socket: net.Socket,
    private readonly opts: ConnectionOptions,
    private handlers: ConnectionHandlers,
  ) {}

  /** Open a socket and run the `Connect` handshake. */
  static async open(opts: ConnectionOptions, handlers: ConnectionHandlers): Promise<Connection> {
    const socket = await openSocket(opts);
    const conn = new Connection(socket, opts, handlers);
    conn.attach();
    try {
      const resp = await conn.request({
        type: "Connect",
        clientId: opts.clientId,
        token: opts.token ?? null,
      });
      if (resp.type !== "ConnectOk") throw new ProtocolError(`unexpected handshake reply ${resp.type}`);
      conn._info = { serverVersion: resp.serverVersion, nodeId: resp.nodeId, leader: resp.leader };
    } catch (err) {
      conn.closedByUser = true;
      conn.teardown(err as Error);
      throw err;
    }
    conn.startKeepalive();
    return conn;
  }

  get info(): ServerInfo {
    return this._info!;
  }

  get closed(): boolean {
    return this._closed;
  }

  private attach(): void {
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (err) => this.teardown(new ConnectionError(`connection error: ${err.message}`)));
    this.socket.on("close", () => this.teardown(new ConnectionError("connection closed")));
  }

  /** Send a request and wait for its response. Error responses reject with {@link ServerError}. */
  request(req: Request, opts: RequestOptions = {}): Promise<Response> {
    if (this._closed) return Promise.reject(new ConnectionError("connection closed"));
    const corr = this.allocCorr();
    let frame: Buffer;
    try {
      frame = requestFrame(req, corr);
    } catch (err) {
      return Promise.reject(err);
    }
    return new Promise<Response>((resolve, reject) => {
      const timeoutMs = opts.timeoutMs ?? this.opts.requestTimeoutMs;
      const timer =
        timeoutMs > 0
          ? setTimeout(() => {
              this.pending.delete(corr);
              reject(new TimeoutError(`${req.type} timed out after ${timeoutMs} ms`));
            }, timeoutMs)
          : null;
      this.pending.set(corr, { resolve, reject, timer, sink: opts.sink });
      this.socket.write(frame);
    });
  }

  /**
   * Send with correlation id 0 (fire-and-forget): no reply on success; a
   * failure arrives as an async error. Returns false when not sent.
   */
  send(req: Request): boolean {
    if (this._closed) return false;
    this.socket.write(requestFrame(req, 0));
    return true;
  }

  /** Stop routing pushes for a subscription. */
  removeSub(subId: number): void {
    this.subs.delete(subId);
  }

  /** Close gracefully: flush what was written (acks), then drop the socket. */
  async close(): Promise<void> {
    if (this._closed) return;
    this.closedByUser = true;
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(t);
        resolve();
      };
      const t = setTimeout(() => {
        this.socket.destroy();
        resolve();
      }, 1000);
      this.socket.once("close", done);
      this.socket.end();
    });
    this.teardown(new ConnectionError("connection closed"));
  }

  private allocCorr(): number {
    const c = this.nextCorr;
    this.nextCorr = this.nextCorr >= 0xffffffff ? 1 : this.nextCorr + 1;
    return c;
  }

  private onData(chunk: Buffer): void {
    let frames;
    try {
      frames = this.parser.push(chunk);
    } catch (err) {
      // The byte stream can't be resynchronised: drop the connection.
      this.socket.destroy();
      this.teardown(err as Error);
      return;
    }
    for (const f of frames) {
      if (this._closed) return;
      let resp: Response;
      try {
        resp = decodeResponse(f.opcode, f.payload);
      } catch (err) {
        const p = f.correlationId !== 0 ? this.takePending(f.correlationId) : undefined;
        if (p) p.reject(err as Error);
        else this.handlers.onAsyncError(err as ProtocolError);
        continue;
      }
      this.route(f.correlationId, resp);
    }
  }

  private takePending(corr: number): Pending | undefined {
    const p = this.pending.get(corr);
    if (p) {
      this.pending.delete(corr);
      if (p.timer) clearTimeout(p.timer);
    }
    return p;
  }

  private route(corr: number, resp: Response): void {
    if (corr === 0) {
      switch (resp.type) {
        case "Deliver":
          this.subs.get(resp.subId)?.onDeliver(resp.records);
          return;
        case "SubscriptionEnded": {
          const sink = this.subs.get(resp.subId);
          this.subs.delete(resp.subId);
          sink?.onEnded(resp.code, resp.message);
          return;
        }
        case "Error":
          this.handlers.onAsyncError(errorFromResponse(resp));
          return;
        default:
          return;
      }
    }
    const p = this.takePending(corr);
    if (resp.type === "SubscribeOk") {
      if (p?.sink) {
        // Register before resolving so a Deliver in the same chunk is not lost.
        this.subs.set(resp.subId, p.sink);
        p.sink.onSubscribed(this, resp.subId);
      } else {
        // The subscribe call timed out or was abandoned; release the server side.
        this.send({ type: "Unsubscribe", subId: resp.subId });
      }
    }
    if (!p) return;
    if (resp.type === "Error") p.reject(errorFromResponse(resp));
    else p.resolve(resp);
  }

  private startKeepalive(): void {
    if (this.opts.keepaliveMs <= 0) return;
    this.keepalive = setInterval(() => {
      this.request({ type: "Ping" }).catch((err) => {
        // A ping that times out means the peer is gone (half-open socket).
        if (err instanceof TimeoutError) this.socket.destroy();
      });
    }, this.opts.keepaliveMs);
    this.keepalive.unref();
  }

  private teardown(err: Error): void {
    if (this._closed) return;
    this._closed = true;
    if (this.keepalive) clearInterval(this.keepalive);
    this.keepalive = null;
    if (!this.socket.destroyed) this.socket.destroy();
    const pending = [...this.pending.values()];
    this.pending.clear();
    this.subs.clear();
    for (const p of pending) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err instanceof ConnectionError ? err : new ConnectionError(err.message));
    }
    if (!this.closedByUser) this.handlers.onClose(err);
  }
}

function openSocket(opts: ConnectionOptions): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const port = opts.port ?? DEFAULT_PORT;
    const useTls = opts.tls !== undefined && opts.tls !== false;
    let socket: net.Socket;
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new ConnectionError(`connect to ${opts.host}:${port} timed out`));
    }, opts.requestTimeoutMs);
    const onError = (err: Error) => {
      clearTimeout(timer);
      reject(new ConnectionError(`connect to ${opts.host}:${port} failed: ${err.message}`));
    };
    const onReady = () => {
      clearTimeout(timer);
      socket.removeListener("error", onError);
      socket.setNoDelay(true);
      socket.setKeepAlive(true, 10_000);
      resolve(socket);
    };
    if (useTls) {
      const extra = typeof opts.tls === "object" ? opts.tls : {};
      const servername = extra.servername ?? (net.isIP(opts.host) ? undefined : opts.host);
      socket = tls.connect({ host: opts.host, port, servername, ...extra }, onReady);
    } else {
      socket = net.createConnection({ host: opts.host, port }, onReady);
    }
    socket.once("error", onError);
  });
}
