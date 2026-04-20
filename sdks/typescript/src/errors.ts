export class ExspeedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ExspeedError";
  }
}

export class ServerError extends ExspeedError {
  readonly code: number;

  constructor(code: number, message: string) {
    super(message);
    this.name = "ServerError";
    this.code = code;
  }
}

export class ProtocolError extends ExspeedError {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export class TimeoutError extends ExspeedError {
  constructor(message: string = "Request timed out") {
    super(message);
    this.name = "TimeoutError";
  }
}

export class ConnectionError extends ExspeedError {
  constructor(message: string) {
    super(message);
    this.name = "ConnectionError";
  }
}

export class ValidationError extends ExspeedError {
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

export class BufferFullError extends ExspeedError {
  constructor(message: string = "Publish buffer full") {
    super(message);
    this.name = "BufferFullError";
  }
}

/**
 * Thrown when a publish with a msgId is rejected because the broker already
 * has a record with the same msgId but a different body. This indicates a
 * bug — the same msgId must always carry the same payload.
 * The SDK does NOT retry this error.
 */
export class KeyCollisionError extends ExspeedError {
  readonly storedOffset: bigint;
  readonly msgId: string;

  constructor(msgId: string, storedOffset: bigint) {
    super(
      `key collision for msgId=${msgId}: body differs from stored record at offset ${storedOffset}`,
    );
    this.name = "KeyCollisionError";
    this.msgId = msgId;
    this.storedOffset = storedOffset;
  }
}

/**
 * Thrown when the broker's dedup map is full and cannot accept new msgIds
 * until space is freed. The SDK auto-retries after retryAfterSecs; you
 * only see this error if retries are exhausted.
 */
export class DedupMapFullError extends ExspeedError {
  readonly retryAfterSecs: number;
  readonly stream: string;

  constructor(stream: string, retryAfterSecs: number) {
    super(`dedup map full for stream=${stream}: retry after ${retryAfterSecs}s`);
    this.name = "DedupMapFullError";
    this.stream = stream;
    this.retryAfterSecs = retryAfterSecs;
  }
}

/**
 * Thrown by Subscription.push (when overflowPolicy === 'error') and emitted
 * as the "overflow" event payload (when overflowPolicy === 'drop-oldest')
 * to surface that a record was dropped.
 */
export class QueueOverflowError extends ExspeedError {
  readonly offset: bigint;
  readonly subject: string;
  readonly droppedCount: number;

  constructor(offset: bigint, subject: string, droppedCount: number = 1) {
    super(
      `Subscription queue overflow: dropped record offset=${offset} subject=${subject}`,
    );
    this.name = "QueueOverflowError";
    this.offset = offset;
    this.subject = subject;
    this.droppedCount = droppedCount;
  }
}
