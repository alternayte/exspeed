import { ProtocolError, ServerError } from "./errors.js";
import { ErrorCode } from "./protocol/constants.js";
import type { Request, Response } from "./protocol/messages.js";
import {
  encodeValue,
  type KvBucketOptions,
  type KvDeleteOptions,
  type KvPutOptions,
  type ReadOptions,
  type Value,
} from "./types.js";

/** Header marking a tombstone: `DEL` or `PURGE`. */
export const KV_OP_HEADER = "exspeed-kv-op";

/** What a revision holds. */
export type KvOp = "put" | "delete" | "purge";

/** What an entry is built from: a record as returned by KvGet/KvHistory or a stateless read. */
interface KvRecord {
  offset: number;
  timestampNs: bigint;
  subject: string;
  value: Buffer;
  headers: [string, string][];
}

/** One revision of a key. */
export class KvEntry {
  readonly key: string;
  readonly value: Buffer;
  /** The record's offset in the bucket's stream plus one (0 = absent). */
  readonly revision: number;
  /** Write time, ms since the Unix epoch. */
  readonly timestamp: number;
  /** Write time, ns since the Unix epoch (full precision). */
  readonly timestampNs: bigint;
  /** `"delete"` and `"purge"` entries are tombstones with an empty value. */
  readonly op: KvOp;

  /** @internal */
  constructor(r: KvRecord) {
    const op = r.headers.find(([k]) => k === KV_OP_HEADER)?.[1];
    this.key = r.subject;
    this.value = r.value;
    this.revision = r.offset + 1;
    this.timestamp = Number(r.timestampNs / 1_000_000n);
    this.timestampNs = r.timestampNs;
    this.op = op === "DEL" ? "delete" : op === "PURGE" ? "purge" : "put";
  }

  /** The value as UTF-8 text. */
  text(): string {
    return this.value.toString("utf8");
  }

  /** The value parsed as JSON. */
  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }
}

/** @internal What a bucket needs from the client. */
export interface KvHost {
  rawRequest(req: Request): Promise<Response>;
  read(stream: string, opts: ReadOptions): Promise<{ records: KvRecord[]; nextOffset: number; highWatermark: number }>;
  deleteStream(name: string): Promise<void>;
}

function expect<T extends Response["type"]>(req: Request, resp: Response, type: T): Extract<Response, { type: T }> {
  if (resp.type !== type) throw new ProtocolError(`unexpected reply to ${req.type}: ${resp.type}`);
  return resp as Extract<Response, { type: T }>;
}

/**
 * A key-value bucket, from `client.kv(bucket)`. The bucket `B` is the
 * stream `KV_B`: each key is a subject, each put a record, and a key's
 * revision is its record's offset plus one.
 */
export class KvBucket {
  readonly bucket: string;

  /** @internal */
  constructor(
    private readonly host: KvHost,
    bucket: string,
  ) {
    this.bucket = bucket;
  }

  /** The bucket's stream. */
  get stream(): string {
    return `KV_${this.bucket}`;
  }

  /** Create the bucket. Idempotent for the same settings; 400 when it exists with others. */
  async create(opts: KvBucketOptions = {}): Promise<void> {
    const req: Request = {
      type: "KvCreateBucket",
      bucket: this.bucket,
      history: opts.history ?? 0,
      ttlMs: opts.ttlMs ?? 0,
      maxBytes: opts.maxBytes ?? 0,
    };
    expect(req, await this.host.rawRequest(req), "Ok");
  }

  /** Delete the bucket and every key in it. */
  destroy(): Promise<void> {
    return this.host.deleteStream(this.stream);
  }

  /** The current value of `key`, or `null` when it is absent or deleted. */
  get(key: string): Promise<KvEntry | null> {
    return this.getAt(key, null);
  }

  /** `key` at `revision`, while the bucket still keeps it; else `null`. */
  getRevision(key: string, revision: number): Promise<KvEntry | null> {
    return this.getAt(key, revision);
  }

  private async getAt(key: string, revision: number | null): Promise<KvEntry | null> {
    const req: Request = { type: "KvGet", bucket: this.bucket, key, revision };
    let resp: Response;
    try {
      resp = await this.host.rawRequest(req);
    } catch (err) {
      // 404 is "key not found" (message "key '...' not found") or "bucket not found".
      if (err instanceof ServerError && err.code === ErrorCode.NotFound && err.message.startsWith("key ")) return null;
      throw err;
    }
    const first = expect(req, resp, "Messages").records[0];
    return first ? new KvEntry(first) : null;
  }

  /** Set `key`; resolves with the new revision. */
  put(key: string, value: Value, opts: KvPutOptions = {}): Promise<number> {
    return this.write({
      type: "KvPut",
      bucket: this.bucket,
      key,
      value: encodeValue(value),
      expectedRevision: opts.expectedRevision ?? null,
      ttlMs: opts.ttlMs === undefined ? null : Math.max(1, Math.ceil(opts.ttlMs)),
    });
  }

  /** Set `key` only if it doesn't exist (or was deleted); `ServerError` 409 otherwise. */
  createKey(key: string, value: Value, opts: Omit<KvPutOptions, "expectedRevision"> = {}): Promise<number> {
    return this.put(key, value, { ...opts, expectedRevision: 0 });
  }

  /**
   * Set `key` only if it is at `revision` (compare-and-set). `ServerError`
   * 409 otherwise, with `detail.current_revision`.
   */
  update(key: string, value: Value, revision: number, opts: Omit<KvPutOptions, "expectedRevision"> = {}): Promise<number> {
    return this.put(key, value, { ...opts, expectedRevision: revision });
  }

  /** Delete `key` (its history stays until it ages out); resolves with the tombstone's revision. */
  delete(key: string, opts: KvDeleteOptions = {}): Promise<number> {
    return this.write({
      type: "KvDelete",
      bucket: this.bucket,
      key,
      purge: false,
      expectedRevision: opts.expectedRevision ?? null,
    });
  }

  /** Delete `key` and hide its older values. */
  purge(key: string, opts: KvDeleteOptions = {}): Promise<number> {
    return this.write({
      type: "KvDelete",
      bucket: this.bucket,
      key,
      purge: true,
      expectedRevision: opts.expectedRevision ?? null,
    });
  }

  private async write(req: Request): Promise<number> {
    return expect(req, await this.host.rawRequest(req), "PublishOk").offset;
  }

  /** Keys that have a value, matching `filter` (NATS-style; `""` = all), sorted. */
  async keys(filter = ""): Promise<string[]> {
    const req: Request = { type: "KvKeys", bucket: this.bucket, filter };
    const r = expect(req, await this.host.rawRequest(req), "Json");
    try {
      return JSON.parse(r.json.toString("utf8")) as string[];
    } catch (e) {
      throw new ProtocolError(`bad JSON in reply to KvKeys: ${(e as Error).message}`);
    }
  }

  /** Kept revisions of `key`, oldest first (deletes included). */
  async history(key: string): Promise<KvEntry[]> {
    const req: Request = { type: "KvHistory", bucket: this.bucket, key };
    return expect(req, await this.host.rawRequest(req), "Messages").records.map((r) => new KvEntry(r));
  }

  /**
   * Watch keys matching `filter` (`""` = all): first the current value of
   * every matching key (deleted keys left out), then every change as it
   * happens, deletes included.
   */
  watch(filter = ""): KvWatch {
    return new KvWatch(this.host, this.stream, filter);
  }
}

/** Long-poll wait of each follow-up read. */
const WATCH_WAIT_MS = 10_000;
const WATCH_BATCH = 1000;

/**
 * Changes to a bucket's keys, from {@link KvBucket.watch}. Iterate it with
 * `for await`, or call {@link next}. It reads the bucket's stream with
 * stateless reads, so it holds no server-side state; a failed read (for
 * example `ConnectionError` when the connection drops) rejects `next()`.
 */
export class KvWatch implements AsyncIterable<KvEntry> {
  private from = 0;
  private snapshotDone = false;
  private pending: KvEntry[] = [];
  private fetching: Promise<void> | null = null;
  private stopped = false;

  /** @internal */
  constructor(
    private readonly host: KvHost,
    private readonly stream: string,
    private readonly filter: string,
  ) {}

  /** True after {@link stop}. */
  get closed(): boolean {
    return this.stopped;
  }

  /**
   * The next entry, waiting for changes as long as it takes; with
   * `timeoutMs`, `null` when nothing arrived in time. `null` after `stop()`.
   */
  async next(opts: { timeoutMs?: number } = {}): Promise<KvEntry | null> {
    const deadline = opts.timeoutMs === undefined ? Infinity : Date.now() + opts.timeoutMs;
    for (;;) {
      if (this.stopped) return null;
      if (this.pending.length > 0) return this.pending.shift()!;
      const f = this.fill();
      if (deadline === Infinity) {
        await f;
        continue;
      }
      const left = deadline - Date.now();
      if (left <= 0) return null;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedOut = await Promise.race([
        f.then(() => false),
        new Promise<boolean>((resolve) => {
          timer = setTimeout(() => resolve(true), left);
        }),
      ]).finally(() => clearTimeout(timer));
      if (timedOut) return null;
    }
  }

  /** Stop watching: `next()` resolves with `null` from now on. */
  stop(): void {
    this.stopped = true;
    this.pending = [];
  }

  [Symbol.asyncIterator](): AsyncIterator<KvEntry> {
    return {
      next: async (): Promise<IteratorResult<KvEntry>> => {
        const e = await this.next();
        return e ? { value: e, done: false } : { value: undefined, done: true };
      },
      return: async (): Promise<IteratorResult<KvEntry>> => {
        this.stop();
        return { value: undefined, done: true };
      },
    };
  }

  /** One read at a time; a `next()` that timed out leaves it running, and its records are kept. */
  private fill(): Promise<void> {
    if (!this.fetching) {
      const p = (this.snapshotDone ? this.follow() : this.loadSnapshot()).finally(() => {
        this.fetching = null;
      });
      p.catch(() => {}); // surfaced through the next() that awaits it
      this.fetching = p;
    }
    return this.fetching;
  }

  /**
   * Read up to the high watermark seen by the first read, keep the last
   * record per key, and queue the live keys sorted by revision.
   */
  private async loadSnapshot(): Promise<void> {
    const latest = new Map<string, KvEntry>();
    let end: number | null = null;
    for (;;) {
      const r = await this.host.read(this.stream, {
        from: this.from,
        maxRecords: WATCH_BATCH,
        filter: this.filter,
      });
      end ??= r.highWatermark;
      for (const rec of r.records) {
        const e = new KvEntry(rec);
        latest.set(e.key, e);
      }
      const progressed = r.nextOffset > this.from;
      this.from = Math.max(this.from, r.nextOffset);
      if (this.from >= end || !progressed) break;
    }
    if (this.stopped) return;
    const live = [...latest.values()].filter((e) => e.op === "put").sort((a, b) => a.revision - b.revision);
    this.pending.push(...live);
    this.snapshotDone = true;
  }

  private async follow(): Promise<void> {
    const r = await this.host.read(this.stream, {
      from: this.from,
      maxRecords: WATCH_BATCH,
      waitMs: WATCH_WAIT_MS,
      filter: this.filter,
    });
    this.from = Math.max(this.from, r.nextOffset);
    if (this.stopped) return;
    for (const rec of r.records) this.pending.push(new KvEntry(rec));
  }
}
