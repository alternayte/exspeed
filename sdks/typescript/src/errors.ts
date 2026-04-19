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
