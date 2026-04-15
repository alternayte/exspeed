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
