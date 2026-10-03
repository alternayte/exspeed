import type { Connection, SubscriptionSink } from "./connection.js";
import { Message, type MessageSettler } from "./message.js";
import type { WireRecord } from "./protocol/messages.js";

/** Why a subscription ended. */
export interface EndReason {
  /**
   * `404`: the consumer or its stream was deleted. `503`: the node lost
   * leadership, or the connection was lost and not re-established. `0`:
   * ended locally (`unsubscribe()` or `client.close()`). Other codes come
   * from a failed re-subscribe after a reconnect.
   */
  code: number;
  message: string;
}

/** @internal What a subscription needs from its client. */
export interface SubscriptionHost extends MessageSettler {
  forget(sub: Subscription): void;
}

type Waiter = { resolve: (m: Message | null) => void; timer: ReturnType<typeof setTimeout> | null };

/**
 * A push subscription to a consumer. Iterate it with `for await`; it ends
 * when the server ends it (see {@link endReason}), on `unsubscribe()`, or
 * when the client closes. Breaking out of a `for await` loop unsubscribes.
 *
 * Credit flow: the server pushes at most `window` records ahead of your
 * code. Each record you take returns one credit (sent in batches of half
 * the window), so a slow consumer slows delivery instead of buffering
 * without bound.
 */
export class Subscription implements AsyncIterable<Message>, SubscriptionSink {
  readonly consumer: string;
  readonly window: number;

  private conn: Connection | null = null;
  private subId = 0;
  private buffer: WireRecord[] = [];
  private head = 0;
  private consumed = 0;
  private waiters: Waiter[] = [];
  private _endReason: EndReason | null = null;

  /** @internal */
  constructor(
    private readonly host: SubscriptionHost,
    consumer: string,
    window: number,
  ) {
    this.consumer = consumer;
    this.window = window;
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

  /** Records received but not yet taken. */
  get buffered(): number {
    return this.buffer.length - this.head;
  }

  /**
   * The next message, or `null` once the subscription has ended or, with
   * `timeoutMs`, when nothing arrived in time.
   */
  next(opts: { timeoutMs?: number } = {}): Promise<Message | null> {
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

  [Symbol.asyncIterator](): AsyncIterator<Message> {
    return {
      next: async (): Promise<IteratorResult<Message>> => {
        const m = await this.next();
        return m ? { value: m, done: false } : { value: undefined, done: true };
      },
      return: async (): Promise<IteratorResult<Message>> => {
        await this.unsubscribe();
        return { value: undefined, done: true };
      },
    };
  }

  /**
   * Stop delivery. Records delivered to this subscription and not yet acked
   * (including buffered ones your code never saw) are redelivered by the
   * server.
   */
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
    this.consumed = 0;
  }

  /** @internal */
  onDeliver(records: WireRecord[]): void {
    if (this._endReason) return;
    for (const r of records) this.buffer.push(r);
    while (this.waiters.length > 0 && this.buffered > 0) {
      const w = this.waiters.shift()!;
      if (w.timer) clearTimeout(w.timer);
      w.resolve(this.take());
    }
  }

  /** @internal Server-side end: buffered records are still yielded. */
  onEnded(code: number, message: string): void {
    this.end({ code, message }, true);
  }

  // ---- client hooks ---------------------------------------------------------

  /** @internal The connection was lost; a re-subscribe will follow. Buffered records are dropped (the server redelivers them). */
  suspend(): void {
    this.conn = null;
    this.buffer = [];
    this.head = 0;
    this.consumed = 0;
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
    this.host.forget(this);
    if (this.buffered === 0) {
      for (const w of this.waiters) {
        if (w.timer) clearTimeout(w.timer);
        w.resolve(null);
      }
      this.waiters = [];
    }
  }

  private take(): Message {
    const r = this.buffer[this.head]!;
    this.head++;
    if (this.head === this.buffer.length) {
      this.buffer = [];
      this.head = 0;
    } else if (this.head > 1024 && this.head * 2 > this.buffer.length) {
      this.buffer = this.buffer.slice(this.head);
      this.head = 0;
    }
    this.returnCredit();
    return new Message(r, this.consumer, this.host);
  }

  private returnCredit(): void {
    const conn = this.conn;
    if (!conn || conn.closed) return;
    this.consumed++;
    if (this.consumed >= Math.max(1, Math.floor(this.window / 2))) {
      const credits = this.consumed;
      this.consumed = 0;
      conn.send({ type: "Credit", subId: this.subId, credits });
    }
  }
}
