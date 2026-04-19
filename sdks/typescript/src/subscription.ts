import { EventEmitter } from "node:events";
import type { RecordDelivery } from "./protocol/types.js";
import { QueueOverflowError } from "./errors.js";

type AckFn = (consumerName: string, offset: bigint) => Promise<void>;
type NackFn = (consumerName: string, offset: bigint) => Promise<void>;

interface QueueEntry {
  record: RecordDelivery;
  ackFn: AckFn;
  nackFn: NackFn;
}

export class Message {
  readonly subject: string;
  readonly offset: bigint;
  readonly timestamp: bigint;
  readonly deliveryAttempt: number;
  readonly key: Buffer | undefined;
  readonly headers: [string, string][];

  private readonly _value: Buffer;
  private readonly _consumerName: string;
  private readonly _ackFn: AckFn;
  private readonly _nackFn: NackFn;

  constructor(record: RecordDelivery, ackFn: AckFn, nackFn: NackFn) {
    this.subject = record.subject;
    this.offset = record.offset;
    this.timestamp = record.timestamp;
    this.deliveryAttempt = record.deliveryAttempt;
    this.key = record.key;
    this.headers = record.headers;
    this._value = record.value;
    this._consumerName = record.consumerName;
    this._ackFn = ackFn;
    this._nackFn = nackFn;
  }

  json<T = unknown>(): T {
    return JSON.parse(this._value.toString("utf8")) as T;
  }

  raw(): Buffer {
    return this._value;
  }

  async ack(): Promise<void> {
    return this._ackFn(this._consumerName, this.offset);
  }

  async nack(): Promise<void> {
    return this._nackFn(this._consumerName, this.offset);
  }

  toJSON(): object {
    return {
      subject: this.subject,
      offset: this.offset.toString(),
      timestamp: this.timestamp.toString(),
      deliveryAttempt: this.deliveryAttempt,
      headers: this.headers,
    };
  }
}

export interface SubscriptionOptions {
  maxQueueSize: number;
  overflowPolicy?: "drop-oldest" | "error";
}

export class Subscription extends EventEmitter implements AsyncIterable<Message> {
  readonly consumerName: string;
  readonly subscriberId: string;
  private queue: QueueEntry[] = [];
  private readonly maxQueueSize: number;
  private readonly overflowPolicy: "drop-oldest" | "error";
  private _closed = false;
  private _paused = false;
  private waiter: ((value: IteratorResult<Message>) => void) | null = null;

  constructor(
    consumerName: string,
    subscriberId: string,
    options: SubscriptionOptions = { maxQueueSize: 1000 },
  ) {
    super();
    this.consumerName = consumerName;
    this.subscriberId = subscriberId;
    this.maxQueueSize = options.maxQueueSize;
    this.overflowPolicy = options.overflowPolicy ?? "drop-oldest";
  }

  push(record: RecordDelivery, ackFn: AckFn, nackFn: NackFn): void {
    if (this._closed) return;

    // Fast path: a waiter is parked AND we are not paused -> deliver directly.
    if (this.waiter && !this._paused) {
      const msg = new Message(record, ackFn, nackFn);
      const w = this.waiter;
      this.waiter = null;
      w({ value: msg, done: false });
      return;
    }

    if (this.queue.length >= this.maxQueueSize) {
      if (this.overflowPolicy === "error") {
        // Reject the incoming record; do NOT enqueue.
        this.emit("error", new QueueOverflowError(record.offset, record.subject));
        return;
      }
      // drop-oldest: shift the oldest, emit typed event referencing it.
      const dropped = this.queue.shift()!;
      this.emit(
        "overflow",
        new QueueOverflowError(dropped.record.offset, dropped.record.subject),
      );
    }

    this.queue.push({ record, ackFn, nackFn });

    if (this.queue.length >= this.maxQueueSize * 0.8) {
      this.emit("slow");
    }
  }

  /** Pause iteration. While paused, `next()` awaits a `resume()` call. */
  pause(): void {
    this._paused = true;
  }

  /** Resume iteration. Wakes a parked iterator if the queue has work. */
  resume(): void {
    this._paused = false;
    if (this.waiter && this.queue.length > 0) {
      const entry = this.queue.shift()!;
      const msg = new Message(entry.record, entry.ackFn, entry.nackFn);
      const w = this.waiter;
      this.waiter = null;
      w({ value: msg, done: false });
    }
  }

  get paused(): boolean {
    return this._paused;
  }

  close(): void {
    this._closed = true;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: undefined as any, done: true });
    }
  }

  get closed(): boolean {
    return this._closed;
  }

  [Symbol.asyncIterator](): AsyncIterator<Message> {
    return {
      next: (): Promise<IteratorResult<Message>> => {
        if (this.queue.length > 0 && !this._paused) {
          const entry = this.queue.shift()!;
          const msg = new Message(entry.record, entry.ackFn, entry.nackFn);
          return Promise.resolve({ value: msg, done: false });
        }

        if (this._closed) {
          return Promise.resolve({ value: undefined as any, done: true });
        }

        return new Promise((resolve) => {
          this.waiter = resolve;
        });
      },
      return: (): Promise<IteratorResult<Message>> => {
        this.close();
        return Promise.resolve({ value: undefined as any, done: true });
      },
    };
  }
}
