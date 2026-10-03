import type { WireRecord } from "./protocol/messages.js";

/** A record read from a stream. */
export class StreamRecord {
  readonly offset: number;
  /** Append time, ms since the Unix epoch. */
  readonly timestamp: number;
  readonly subject: string;
  readonly key: Buffer | null;
  readonly value: Buffer;
  /** In wire order; a key can repeat. See {@link header}. */
  readonly headers: [string, string][];

  constructor(r: WireRecord) {
    this.offset = r.offset;
    this.timestamp = r.timestampMs;
    this.subject = r.subject;
    this.key = r.key;
    this.value = r.value;
    this.headers = r.headers;
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
}

/** What a message needs from the client to settle itself. */
export interface MessageSettler {
  ackNowait(consumer: string, offsets: number[]): void;
  nack(consumer: string, offset: number, delayMs?: number): Promise<void>;
  term(consumer: string, offset: number, reason: string): Promise<void>;
  inProgress(consumer: string, offsets: number[]): Promise<void>;
}

/** A record delivered by a consumer (push or pull), with the means to settle it. */
export class Message extends StreamRecord {
  /** 1 on first delivery, incremented on each redelivery. */
  readonly deliveryCount: number;
  /** The consumer that delivered it. */
  readonly consumer: string;

  constructor(
    r: WireRecord,
    consumer: string,
    private readonly settler: MessageSettler,
  ) {
    super(r);
    this.deliveryCount = r.deliveryCount;
    this.consumer = consumer;
  }

  /**
   * Acknowledge (fire-and-forget: no round trip). If the connection is down
   * the ack is dropped and the record will be redelivered. Failures reported
   * by the server surface as the client's `"error"` event. Use
   * `client.ack(consumer, offsets)` to wait for confirmation.
   */
  ack(): void {
    this.settler.ackNowait(this.consumer, [this.offset]);
  }

  /** Ask for redelivery after `delayMs` (default: the consumer's backoff). */
  nack(delayMs?: number): Promise<void> {
    return this.settler.nack(this.consumer, this.offset, delayMs);
  }

  /** Never redeliver: dead-letter now (to the consumer's `dlqStream`, if set). */
  term(reason = ""): Promise<void> {
    return this.settler.term(this.consumer, this.offset, reason);
  }

  /** Still working on it: reset the ack deadline. */
  inProgress(): Promise<void> {
    return this.settler.inProgress(this.consumer, [this.offset]);
  }
}
