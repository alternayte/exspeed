import type { Connection, CoreMsgPush, SubscriptionSink } from "./connection.js";
import { ExspeedError } from "./errors.js";
import type { EndReason } from "./subscription.js";
import type { CorePublishOptions, Value } from "./types.js";

/** @internal What core messages and subscriptions need from the client. */
export interface CoreHost {
  publishCore(subject: string, value: Value, opts?: CorePublishOptions): Promise<void>;
  forgetCore(sub: CoreSubscription): void;
}

/**
 * A core (non-persistent) message: from a core subscription, or the
 * response to a `request()`.
 */
export class CoreMessage {
  readonly subject: string;
  /** Set on a request: answer it with {@link respond}. */
  readonly replyTo: string | null;
  /** In wire order; a key can repeat. See {@link header}. */
  readonly headers: [string, string][];
  readonly value: Buffer;

  /** @internal */
  constructor(
    m: CoreMsgPush,
    private readonly host: CoreHost,
  ) {
    this.subject = m.subject;
    this.replyTo = m.replyTo;
    this.headers = m.headers;
    this.value = m.value;
  }

  /** The value as UTF-8 text. */
  text(): string {
    return this.value.toString("utf8");
  }

  /** The value parsed as JSON. */
  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  /** The first header named `name`. */
  header(name: string): string | undefined {
    return this.headers.find(([k]) => k === name)?.[1];
  }

  /** Answer a request: publish `value` to its `replyTo` subject. */
  respond(value: Value, opts: { headers?: CorePublishOptions["headers"] } = {}): Promise<void> {
    if (!this.replyTo) return Promise.reject(new ExspeedError("message has no replyTo"));
    return this.host.publishCore(this.replyTo, value, { headers: opts.headers });
  }
}

type Waiter = { resolve: (m: CoreMessage | null) => void; timer: ReturnType<typeof setTimeout> | null };

/**
 * A core-message subscription. Iterate it with `for await`; it ends on
 * `unsubscribe()` (or `break`), when the server ends it (503 when
 * leadership moves), or when the client closes.
 *
 * Core messages are not stored: a subscription receives what is published
 * while it is live. After a reconnect the client subscribes again with the
 * same subject and queue group; messages published in the gap are missed.
 */
export class CoreSubscription implements AsyncIterable<CoreMessage>, SubscriptionSink {
  readonly subject: string;
  readonly queue: string | null;

  private conn: Connection | null = null;
  private subId = 0;
  private buffer: CoreMessage[] = [];
  private head = 0;
  private waiters: Waiter[] = [];
  private _endReason: EndReason | null = null;

  /** @internal */
  constructor(
    private readonly host: CoreHost,
    subject: string,
    queue: string | null,
  ) {
    this.subject = subject;
    this.queue = queue;
  }

  /** Server-assigned id of the current subscription (changes after a reconnect). */
  get id(): number {
    return this.subId;
  }

  /** Why the subscription ended, or `null` while it is active. */
  get endReason(): EndReason | null {
    return this._endReason;
  }

  get closed(): boolean {
    return this._endReason !== null;
  }

  /** Messages received but not yet taken. */
  get buffered(): number {
    return this.buffer.length - this.head;
  }

  /**
   * The next message, or `null` once the subscription has ended or, with
   * `timeoutMs`, when nothing arrived in time.
   */
  next(opts: { timeoutMs?: number } = {}): Promise<CoreMessage | null> {
    if (this.buffered > 0) return Promise.resolve(this.take());
    if (this._endReason) return Promise.resolve(null);
    return new Promise((resolve) => {
      const waiter: Waiter = { resolve, timer: null };
      if (opts.timeoutMs !== undefined) {
        waiter.timer = setTimeout(() => {
          this.waiters = this.waiters.filter((w) => w !== waiter);
          resolve(null);
        }, opts.timeoutMs);
      }
      this.waiters.push(waiter);
    });
  }

  [Symbol.asyncIterator](): AsyncIterator<CoreMessage> {
    return {
      next: async (): Promise<IteratorResult<CoreMessage>> => {
        const m = await this.next();
        return m ? { value: m, done: false } : { value: undefined, done: true };
      },
      return: async (): Promise<IteratorResult<CoreMessage>> => {
        await this.unsubscribe();
        return { value: undefined, done: true };
      },
    };
  }

  /** Stop delivery. Buffered messages are dropped. */
  async unsubscribe(): Promise<void> {
    if (this._endReason) return;
    const conn = this.conn;
    const subId = this.subId;
    this.end({ code: 0, message: "unsubscribed" }, false);
    if (conn && !conn.closed) {
      conn.removeSub(subId);
      await conn.request({ type: "Unsubscribe", subId }).catch(() => {});
    }
  }

  // ---- SubscriptionSink (called by the connection) ------------------------

  /** @internal */
  onSubscribed(conn: Connection, subId: number): void {
    if (this._endReason) {
      // Unsubscribed while a (re-)subscribe was in flight.
      conn.removeSub(subId);
      conn.send({ type: "Unsubscribe", subId });
      return;
    }
    this.conn = conn;
    this.subId = subId;
  }

  /** @internal */
  onCoreMsg(m: CoreMsgPush): void {
    if (this._endReason) return;
    const msg = new CoreMessage(m, this.host);
    const w = this.waiters.shift();
    if (w) {
      if (w.timer) clearTimeout(w.timer);
      w.resolve(msg);
    } else {
      this.buffer.push(msg);
    }
  }

  /** @internal Server-side end: buffered messages are still yielded. */
  onEnded(code: number, message: string): void {
    this.end({ code, message }, true);
  }

  // ---- client hooks ---------------------------------------------------------

  /** @internal The connection was lost; a re-subscribe will follow. Buffered messages are kept. */
  suspend(): void {
    this.conn = null;
  }

  /** @internal */
  end(reason: EndReason, keepBuffered: boolean): void {
    if (this._endReason) return;
    this._endReason = reason;
    this.conn = null;
    if (!keepBuffered) {
      this.buffer = [];
      this.head = 0;
    }
    this.host.forgetCore(this);
    if (this.buffered === 0) {
      for (const w of this.waiters) {
        if (w.timer) clearTimeout(w.timer);
        w.resolve(null);
      }
      this.waiters = [];
    }
  }

  private take(): CoreMessage {
    const m = this.buffer[this.head]!;
    this.head++;
    if (this.head === this.buffer.length) {
      this.buffer = [];
      this.head = 0;
    } else if (this.head > 1024 && this.head * 2 > this.buffer.length) {
      this.buffer = this.buffer.slice(this.head);
      this.head = 0;
    }
    return m;
  }
}
