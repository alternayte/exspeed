import { EventEmitter } from "node:events";
import * as net from "node:net";
import * as tls from "node:tls";
import {
  OpCode,
  PROTOCOL_VERSION,
  DEFAULT_PORT,
  encodeFrame,
  decodeFrame,
  type Frame,
} from "./protocol/index.js";
import { encodeConnect } from "./protocol/connect.js";
import { decodeErrorFrame } from "./protocol/error-frame.js";
import {
  ConnectionError,
  ServerError,
  TimeoutError,
} from "./errors.js";
import { AUTH_NONE, AUTH_TOKEN } from "./protocol/types.js";
import type { BrokerEndpoint } from "./types.js";

export interface ConnectionOptions {
  endpoints: BrokerEndpoint[];
  clientId: string;
  auth?: { type: "token"; token: string };
  reconnect: boolean;
  maxReconnectAttempts?: number;
  reconnectBaseDelay?: number;
  reconnectMaxDelay?: number;
  requestTimeout: number;
  pingInterval: number;
  tls?: boolean | {
    rejectUnauthorized?: boolean;
    ca?: Buffer | Buffer[];
  };
}

interface PendingRequest {
  resolve: (frame: Frame) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class Connection extends EventEmitter {
  private socket: net.Socket | null = null;
  private readBuffer = Buffer.alloc(0);
  private correlationCounter = 0;
  private pending = new Map<number, PendingRequest>();
  private closed = false;
  private connected = false;
  private socketReady = false;
  private reconnecting = false;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private currentEndpointIndex = 0;

  private readonly opts: Required<Omit<ConnectionOptions, "auth" | "tls">> & {
    auth?: ConnectionOptions["auth"];
    tls?: ConnectionOptions["tls"];
  };

  constructor(options: ConnectionOptions) {
    super();
    this.opts = {
      maxReconnectAttempts: 10,
      reconnectBaseDelay: 100,
      reconnectMaxDelay: 5000,
      ...options,
    };
  }

  currentEndpoint(): BrokerEndpoint {
    return this.opts.endpoints[this.currentEndpointIndex]!;
  }

  async connect(): Promise<void> {
    const totalEndpoints = this.opts.endpoints.length;
    if (totalEndpoints === 0) {
      throw new ConnectionError("No broker endpoints configured");
    }

    let lastError: unknown;
    for (let attempt = 0; attempt < totalEndpoints; attempt++) {
      try {
        await this.openSocket();
        await this.handshake();
        this.connected = true;
        this.startPing();
        this.emit("connected", this.currentEndpoint());
        return;
      } catch (err) {
        lastError = err;
        // openSocket advanced the index; try the next one.
      }
    }

    throw new ConnectionError(
      `Failed to connect to any of ${totalEndpoints} endpoints: ${(lastError as Error)?.message ?? "unknown"}`,
    );
  }

  async close(): Promise<void> {
    this.closed = true;
    this.socketReady = false;
    this.stopPing();
    this.rejectAllPending(new ConnectionError("Connection closed"));
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }

  async request(opcode: OpCode, payload: Buffer): Promise<Frame> {
    if (this.closed || !this.socket || !this.socketReady) {
      throw new ConnectionError("Connection is closed");
    }

    const correlationId = this.nextCorrelationId();
    const frame = encodeFrame({
      version: PROTOCOL_VERSION,
      opcode,
      correlationId,
      payload,
    });

    return new Promise<Frame>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new TimeoutError(`Request timed out (opcode 0x${opcode.toString(16)})`));
      }, this.opts.requestTimeout);

      this.pending.set(correlationId, { resolve, reject, timer });
      this.socket!.write(frame);
    });
  }

  send(opcode: OpCode, payload: Buffer, correlationId: number): void {
    if (this.closed || !this.socket) return;
    const frame = encodeFrame({
      version: PROTOCOL_VERSION,
      opcode,
      correlationId,
      payload,
    });
    this.socket.write(frame);
  }

  private nextCorrelationId(): number {
    this.correlationCounter = (this.correlationCounter + 1) & 0xffffffff;
    if (this.correlationCounter === 0) this.correlationCounter = 1;
    return this.correlationCounter;
  }

  private openSocket(): Promise<void> {
    return new Promise((resolve, reject) => {
      const endpoint = this.currentEndpoint();
      const port = endpoint.port ?? DEFAULT_PORT;
      const useTls = this.opts.tls !== undefined && this.opts.tls !== false;
      const baseOpts = { host: endpoint.host, port };

      // onReady fires on 'connect' for plain TCP, 'secureConnect' for TLS —
      // both mean the socket is ready for application I/O.
      const onReady = () => {
        socket.removeListener("error", onInitialError);
        this.socketReady = true;
        resolve();
      };

      const onInitialError = (err: Error) => {
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.opts.endpoints.length;
        reject(err);
      };

      let socket: net.Socket;
      if (useTls) {
        const tlsOpts: tls.ConnectionOptions = {
          ...baseOpts,
          rejectUnauthorized:
            typeof this.opts.tls === "object" && this.opts.tls !== null && this.opts.tls.rejectUnauthorized === false
              ? false
              : true,
          ca:
            typeof this.opts.tls === "object" && this.opts.tls !== null && this.opts.tls.ca
              ? (Array.isArray(this.opts.tls.ca) ? this.opts.tls.ca : [this.opts.tls.ca])
              : undefined,
        };
        socket = tls.connect(tlsOpts, onReady);
      } else {
        socket = net.createConnection(baseOpts, onReady);
      }

      // Register error listener BEFORE other listeners so synchronous errors
      // during socket setup properly reject the promise instead of being
      // absorbed by onSocketError with no listener.
      socket.once("error", onInitialError);
      socket.setKeepAlive(true, 10000);
      socket.on("data", (data) => this.onData(data));
      socket.on("error", (err) => this.onSocketError(err));
      socket.on("close", () => this.onSocketClose());
      this.socket = socket;
    });
  }

  private async handshake(): Promise<void> {
    const authType = this.opts.auth ? AUTH_TOKEN : AUTH_NONE;
    const authPayload = this.opts.auth
      ? Buffer.from(this.opts.auth.token, "utf8")
      : Buffer.alloc(0);

    const payload = encodeConnect({
      clientId: this.opts.clientId,
      authType,
      authPayload,
    });

    const response = await this.request(OpCode.Connect, payload);
    if (response.opcode === OpCode.Error) {
      const err = decodeErrorFrame(response.payload);
      throw new ServerError(err.code, err.message);
    }
  }

  private onData(data: Buffer): void {
    this.readBuffer = Buffer.concat([this.readBuffer, data]);
    let offset = 0;

    while (true) {
      let result;
      try {
        result = decodeFrame(this.readBuffer, offset);
      } catch (err) {
        this.emit("error", err);
        offset += 1;
        continue;
      }
      if (!result) break;
      offset += result.bytesConsumed;
      this.dispatchFrame(result.frame);
    }

    if (offset > 0) {
      this.readBuffer = this.readBuffer.subarray(offset);
    }
  }

  private dispatchFrame(frame: Frame): void {
    if (frame.correlationId === 0) {
      this.emit("push", frame);
      return;
    }

    const pending = this.pending.get(frame.correlationId);
    if (!pending) return;

    this.pending.delete(frame.correlationId);
    clearTimeout(pending.timer);

    if (frame.opcode === OpCode.Error) {
      const err = decodeErrorFrame(frame.payload);
      pending.reject(new ServerError(err.code, err.message));
    } else {
      pending.resolve(frame);
    }
  }

  private onSocketError(err: Error): void {
    if (this.closed) return;
    // Only emit "error" if there's a listener; otherwise swallow it
    // (the socket "close" event that follows will handle cleanup)
    if (this.listenerCount("error") > 0) {
      this.emit("error", err);
    }
  }

  private onSocketClose(): void {
    if (this.closed) return;
    this.connected = false;
    this.socketReady = false;
    this.stopPing();
    this.emit("disconnected", { error: new ConnectionError("Socket closed") });

    if (this.opts.reconnect && !this.reconnecting) {
      this.attemptReconnect();
    } else {
      this.rejectAllPending(new ConnectionError("Connection lost"));
    }
  }

  private async attemptReconnect(): Promise<void> {
    this.reconnecting = true;
    const max = this.opts.maxReconnectAttempts;

    for (let attempt = 1; attempt <= max; attempt++) {
      const delay = Math.min(
        this.opts.reconnectBaseDelay * Math.pow(2, attempt - 1) +
          Math.random() * 100,
        this.opts.reconnectMaxDelay,
      );

      this.emit("reconnecting", { attempt, delay });
      await new Promise((r) => setTimeout(r, delay));

      if (this.closed) break;

      const before = this.currentEndpoint();

      try {
        if (this.socket) {
          this.socket.removeAllListeners();
          this.socket.destroy();
        }
        await this.openSocket();
        await this.handshake();
        this.connected = true;
        this.reconnecting = false;
        this.startPing();
        const after = this.currentEndpoint();
        this.emit("reconnected", { attempt });
        this.emit("connected", after);
        if (after.host !== before.host || after.port !== before.port) {
          this.emit("endpoint_changed", { from: before, to: after });
        }
        return;
      } catch {
        // Try again
      }
    }

    this.reconnecting = false;
    this.rejectAllPending(new ConnectionError("Reconnection exhausted"));
    this.emit("close", {
      error: new ConnectionError("Failed to reconnect"),
    });
  }

  private rejectAllPending(err: Error): void {
    for (const [, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(err);
    }
    this.pending.clear();
  }

  private startPing(): void {
    if (this.opts.pingInterval <= 0) return;
    this.pingTimer = setInterval(() => {
      this.request(OpCode.Ping, Buffer.alloc(0)).catch(() => {});
    }, this.opts.pingInterval);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
}
