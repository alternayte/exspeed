import { ConnectionError, ProtocolError } from "./errors.js";
import type { Request, Response, WirePublishRecord } from "./protocol/messages.js";
import { toWirePublishRecord, type PublishInput, type PublishResult } from "./types.js";

export interface PublisherOptions {
  /**
   * How long to gather records before sending a batch. `0` (default) sends
   * everything published in the same event-loop turn as one batch, without
   * adding latency.
   */
  batchWindowMs?: number;
  /** Most records per batch request. Default 512. */
  maxBatchRecords?: number;
  /** Records accepted but not yet acknowledged; `publish` waits when full. Default 4096. */
  maxInFlight?: number;
}

/** @internal */
export interface PublisherTransport {
  request(req: Request): Promise<Response>;
}

interface Queued {
  stream: string;
  record: WirePublishRecord;
  size: number;
  resolve(r: PublishResult): void;
  reject(e: Error): void;
}

/** Keep each batch frame well below the 16 MiB frame limit. */
const MAX_BATCH_BYTES = 4 * 1024 * 1024;

/**
 * A pipelined, coalescing publisher. Concurrent `publish` calls are
 * gathered into `PublishBatch` requests (one per run of records for the
 * same stream) and many batches can be in flight at once. Records reach the
 * stream in the order `publish` was called, and every call gets its own
 * record's result.
 *
 * Create one with `client.publisher(options)`.
 */
export class Publisher {
  private readonly batchWindowMs: number;
  private readonly maxBatchRecords: number;
  private readonly maxInFlight: number;

  private queue: Queued[] = [];
  private scheduled = false;
  private inFlight = 0;
  private permitWaiters: Array<() => void> = [];
  private idleWaiters: Array<() => void> = [];
  private closed = false;

  /** @internal */
  constructor(
    private readonly transport: PublisherTransport,
    opts: PublisherOptions = {},
  ) {
    this.batchWindowMs = Math.max(0, opts.batchWindowMs ?? 0);
    this.maxBatchRecords = Math.max(1, opts.maxBatchRecords ?? 512);
    this.maxInFlight = Math.max(1, opts.maxInFlight ?? 4096);
  }

  /** Records accepted and not yet acknowledged. */
  get pending(): number {
    return this.inFlight;
  }

  /** Publish one record; resolves with its offset once the server has it. */
  async publish(stream: string, record: PublishInput): Promise<PublishResult> {
    if (this.closed) throw new ConnectionError("publisher is closed");
    const wire = toWirePublishRecord(record);
    await this.acquire();
    if (this.closed) {
      this.release();
      throw new ConnectionError("publisher is closed");
    }
    return new Promise<PublishResult>((resolve, reject) => {
      const size =
        64 + wire.subject.length + wire.value.length + (wire.key?.length ?? 0) + (wire.msgId?.length ?? 0) +
        wire.headers.reduce((n, [k, v]) => n + 4 + k.length * 3 + v.length * 3, 0);
      this.queue.push({ stream, record: wire, size, resolve, reject });
      if (this.queue.length >= this.maxBatchRecords) this.flushQueue();
      else this.schedule();
    });
  }

  /** Wait until every accepted record has been acknowledged (or failed). */
  async flush(): Promise<void> {
    if (this.queue.length > 0) this.flushQueue();
    if (this.inFlight === 0 && this.permitWaiters.length === 0) return;
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  /** Flush, then reject further publishes. */
  async close(): Promise<void> {
    await this.flush();
    this.closed = true;
  }

  private acquire(): Promise<void> {
    if (this.inFlight < this.maxInFlight && this.permitWaiters.length === 0) {
      this.inFlight++;
      return Promise.resolve();
    }
    // FIFO hand-off keeps call order intact while waiting for capacity.
    return new Promise((resolve) => this.permitWaiters.push(resolve));
  }

  private release(): void {
    const next = this.permitWaiters.shift();
    if (next) {
      next(); // the permit passes straight to the next waiter
      return;
    }
    this.inFlight--;
    if (this.inFlight === 0) {
      const waiters = this.idleWaiters;
      this.idleWaiters = [];
      for (const w of waiters) w();
    }
  }

  private schedule(): void {
    if (this.scheduled) return;
    this.scheduled = true;
    const run = () => {
      this.scheduled = false;
      this.flushQueue();
    };
    if (this.batchWindowMs === 0) setImmediate(run);
    else setTimeout(run, this.batchWindowMs);
  }

  /** Send everything queued, as runs of the same stream, in arrival order. */
  private flushQueue(): void {
    while (this.queue.length > 0) {
      const stream = this.queue[0]!.stream;
      const run: Queued[] = [];
      let bytes = 0;
      while (
        this.queue.length > 0 &&
        this.queue[0]!.stream === stream &&
        run.length < this.maxBatchRecords &&
        (run.length === 0 || bytes + this.queue[0]!.size <= MAX_BATCH_BYTES)
      ) {
        const q = this.queue.shift()!;
        bytes += q.size;
        run.push(q);
      }
      this.sendRun(stream, run);
    }
  }

  /** The request is written synchronously, so wire order matches arrival order. */
  private sendRun(stream: string, run: Queued[]): void {
    const single = run.length === 1;
    const req: Request = single
      ? { type: "Publish", stream, record: run[0]!.record }
      : { type: "PublishBatch", stream, records: run.map((q) => q.record) };
    let sent: Promise<Response>;
    try {
      sent = this.transport.request(req);
    } catch (err) {
      sent = Promise.reject(err);
    }
    sent.then(
      (resp) => {
        if (single && resp.type === "PublishOk") {
          run[0]!.resolve({ offset: resp.offset, duplicate: resp.duplicate });
        } else if (resp.type === "PublishBatchOk" && resp.results.length === run.length) {
          resp.results.forEach((r, i) => run[i]!.resolve({ offset: r.offset, duplicate: r.duplicate }));
        } else {
          const err = new ProtocolError(`unexpected reply to ${req.type}: ${resp.type}`);
          for (const q of run) q.reject(err);
        }
        for (let i = 0; i < run.length; i++) this.release();
      },
      (err: Error) => {
        for (const q of run) q.reject(err);
        for (let i = 0; i < run.length; i++) this.release();
      },
    );
  }
}
