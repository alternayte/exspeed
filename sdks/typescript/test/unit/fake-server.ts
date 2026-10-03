import * as net from "node:net";
import { FrameParser, decodeRequest, responseFrame, type Request, type Response } from "../../src/protocol/index.js";

export interface Received {
  corr: number;
  req: Request;
}

/** One accepted client connection on the fake server. */
export class FakeConn {
  readonly received: Received[] = [];
  private readonly parser = new FrameParser();

  constructor(
    readonly socket: net.Socket,
    private readonly server: FakeServer,
  ) {
    socket.on("data", (chunk) => {
      for (const f of this.parser.push(chunk)) {
        let req: Request;
        try {
          req = decodeRequest(f.opcode, f.payload);
        } catch (e) {
          this.reply(f.correlationId, { type: "Error", code: 400, message: String(e), detail: null });
          continue;
        }
        this.received.push({ corr: f.correlationId, req });
        this.server.handle(this, f.correlationId, req);
        this.server.notify();
      }
    });
    socket.on("error", () => {});
  }

  reply(corr: number, resp: Response): void {
    this.socket.write(responseFrame(resp, corr));
  }

  /** Write several frames in a single chunk. */
  replyMany(frames: [number, Response][]): void {
    this.socket.write(Buffer.concat(frames.map(([c, r]) => responseFrame(r, c))));
  }

  of<T extends Request["type"]>(type: T): (Received & { req: Extract<Request, { type: T }> })[] {
    return this.received.filter((r) => r.req.type === type) as (Received & {
      req: Extract<Request, { type: T }>;
    })[];
  }
}

export type Handler = (conn: FakeConn, corr: number, req: Request) => boolean | void;

/**
 * A scriptable protocol-v2 server for unit tests. Connect and Ping are
 * answered automatically unless `handler` returns true for them.
 */
export class FakeServer {
  readonly conns: FakeConn[] = [];
  handler: Handler = () => {};
  private waiters: Array<() => void> = [];

  private constructor(private readonly server: net.Server) {}

  static async start(handler?: Handler): Promise<FakeServer> {
    let fake!: FakeServer;
    const server = net.createServer((socket) => {
      fake.conns.push(new FakeConn(socket, fake));
      fake.notify();
    });
    fake = new FakeServer(server);
    if (handler) fake.handler = handler;
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    return fake;
  }

  get port(): number {
    return (this.server.address() as net.AddressInfo).port;
  }

  get last(): FakeConn {
    return this.conns[this.conns.length - 1]!;
  }

  handle(conn: FakeConn, corr: number, req: Request): void {
    if (this.handler(conn, corr, req) === true) return;
    if (req.type === "Connect") conn.reply(corr, { type: "ConnectOk", serverVersion: "test", nodeId: "n1", leader: null });
    else if (req.type === "Ping") conn.reply(corr, { type: "Pong" });
  }

  notify(): void {
    const w = this.waiters;
    this.waiters = [];
    for (const f of w) f();
  }

  /** Wait until `cond` holds (re-checked whenever a request or connection arrives). */
  async until(cond: () => boolean, timeoutMs = 2000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!cond()) {
      if (Date.now() > deadline) throw new Error("FakeServer.until: timed out");
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, 20);
        this.waiters.push(() => {
          clearTimeout(t);
          resolve();
        });
      });
    }
  }

  async close(): Promise<void> {
    for (const c of this.conns) c.socket.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
