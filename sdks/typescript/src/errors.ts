/** Base class of every error this SDK throws. */
export class ExspeedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ExspeedError";
  }
}

/**
 * The server answered a request with an `Error` frame.
 *
 * `code` is HTTP-like (see {@link ErrorCode}); `detail` is the optional
 * machine-readable JSON the server attached, for example
 * `{"leader": "host:5933"}` (503), `{"stored_offset": 7}` (409) or
 * `{"retry_after_secs": 30}` (429). It is passed through as the server sent
 * it (snake_case keys).
 */
export class ServerError extends ExspeedError {
  readonly code: number;
  readonly detail: unknown;

  constructor(code: number, message: string, detail: unknown = null) {
    super(message);
    this.name = "ServerError";
    this.code = code;
    this.detail = detail ?? null;
  }

  /** `detail.leader` of a 503 "not the leader" error: the leader's client address, when known. */
  get leaderHint(): string | null {
    const d = this.detail as { leader?: unknown } | null;
    return d && typeof d === "object" && typeof d.leader === "string" ? d.leader : null;
  }

  override toString(): string {
    return `ServerError ${this.code}: ${this.message}`;
  }
}

/** The connection is closed, was lost, or is being re-established. */
export class ConnectionError extends ExspeedError {
  constructor(message: string) {
    super(message);
    this.name = "ConnectionError";
  }
}

/** No response arrived within the request timeout. */
export class TimeoutError extends ExspeedError {
  constructor(message = "request timed out") {
    super(message);
    this.name = "TimeoutError";
  }
}

/** The peer sent bytes this SDK cannot decode, or a reply of the wrong type. */
export class ProtocolError extends ExspeedError {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}
