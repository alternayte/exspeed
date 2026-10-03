/**
 * Starts a real exspeed server for the end-to-end tests.
 *
 * The binary comes from `EXSPEED_BIN`, or else `target/debug/exspeed` at the
 * repository root (`cargo build -p exspeed --bin exspeed`). When neither
 * exists the e2e suites are skipped.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import * as http from "node:http";
import * as https from "node:https";
import * as net from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { ExspeedClient, type ClientOptions } from "../../src/index.js";

const here = dirname(fileURLToPath(import.meta.url));
const defaultBin = resolve(here, "../../../../target/debug/exspeed");

export const serverBin: string | null = process.env.EXSPEED_BIN
  ? resolve(process.env.EXSPEED_BIN)
  : existsSync(defaultBin)
    ? defaultBin
    : null;

export const skipReason = serverBin
  ? null
  : `e2e tests skipped: set EXSPEED_BIN to an exspeed server binary, or build one with ` +
    `\`cargo build -p exspeed --bin exspeed\` (looked for ${defaultBin})`;

if (skipReason) console.warn(`\n[exspeed-sdk] ${skipReason}\n`);
else if (process.env.EXSPEED_BIN && !existsSync(serverBin!)) {
  throw new Error(`EXSPEED_BIN=${process.env.EXSPEED_BIN} does not exist`);
}

export interface ServerOptions {
  authToken?: string;
  tlsCert?: string;
  tlsKey?: string;
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const port = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(port));
    });
  });
}

export class TestServer {
  private log: string[] = [];

  private constructor(
    readonly port: number,
    readonly apiPort: number,
    readonly dataDir: string,
    private readonly child: ChildProcess,
    private readonly opts: ServerOptions,
  ) {
    const keep = (chunk: Buffer) => {
      this.log.push(chunk.toString());
      if (this.log.length > 200) this.log.shift();
    };
    child.stdout?.on("data", keep);
    child.stderr?.on("data", keep);
  }

  static async start(opts: ServerOptions = {}, reuse?: TestServer): Promise<TestServer> {
    if (!serverBin) throw new Error(skipReason!);
    const port = reuse?.port ?? (await freePort());
    const apiPort = reuse?.apiPort ?? (await freePort());
    const dataDir = reuse?.dataDir ?? mkdtempSync(join(tmpdir(), "exspeed-sdk-e2e-"));
    const args = ["server", "--bind", `127.0.0.1:${port}`, "--api-bind", `127.0.0.1:${apiPort}`, "--data-dir", dataDir];
    if (opts.authToken) args.push("--auth-token", opts.authToken);
    if (opts.tlsCert && opts.tlsKey) args.push("--tls-cert", opts.tlsCert, "--tls-key", opts.tlsKey);
    // Don't let the developer's environment change the server under test.
    const env = { ...process.env };
    for (const k of Object.keys(env)) if (k.startsWith("EXSPEED_")) delete env[k];
    env.RUST_LOG ??= "warn";
    const child = spawn(serverBin, args, { env, stdio: ["ignore", "pipe", "pipe"] });
    const server = new TestServer(port, apiPort, dataDir, child, opts);
    try {
      await server.waitReady();
    } catch (err) {
      await server.stop();
      throw new Error(`${(err as Error).message}\n--- server log ---\n${server.log.join("")}`);
    }
    return server;
  }

  private async waitReady(timeoutMs = 30_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (this.child.exitCode !== null) throw new Error(`server exited with code ${this.child.exitCode}`);
      if ((await this.readyz()) === 200) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error("server did not become ready");
  }

  /** GET /readyz (over HTTPS when the server runs with TLS); 0 when unreachable. */
  private readyz(): Promise<number> {
    const tls = Boolean(this.opts.tlsCert);
    const get = tls ? https.get : http.get;
    return new Promise((resolve) => {
      const req = get(
        `${tls ? "https" : "http"}://127.0.0.1:${this.apiPort}/readyz`,
        { rejectUnauthorized: false, timeout: 1_000 },
        (res) => {
          res.resume();
          resolve(res.statusCode ?? 0);
        },
      );
      req.on("error", () => resolve(0));
      req.on("timeout", () => req.destroy());
    });
  }

  /** Connect a client to this server (reconnect off unless asked). */
  connect(opts: ClientOptions = {}): Promise<ExspeedClient> {
    return ExspeedClient.connect({ port: this.port, reconnect: false, ...opts });
  }

  /** Stop the process and start a new one on the same ports and data directory. */
  async restart(): Promise<TestServer> {
    await this.kill();
    return TestServer.start(this.opts, this);
  }

  async stop(): Promise<void> {
    await this.kill();
    rmSync(this.dataDir, { recursive: true, force: true });
  }

  private async kill(): Promise<void> {
    if (this.child.exitCode === null && this.child.signalCode === null) {
      const exited = new Promise((r) => this.child.once("exit", r));
      this.child.kill("SIGTERM");
      const t = setTimeout(() => this.child.kill("SIGKILL"), 5_000);
      await exited;
      clearTimeout(t);
    }
  }
}

let counter = 0;
/** A unique, valid stream/consumer name. */
export function uniq(prefix: string): string {
  return `${prefix}-${process.pid}-${Date.now().toString(36)}-${counter++}`;
}

/** Poll `fn` until it returns a truthy value. */
export async function eventually<T>(fn: () => Promise<T>, timeoutMs = 10_000): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v) return v as NonNullable<T>;
    } catch (e) {
      last = e;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`eventually: timed out${last ? ` (last error: ${String(last)})` : ""}`);
}
