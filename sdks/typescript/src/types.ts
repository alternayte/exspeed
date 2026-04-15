export interface ClientOptions {
  host?: string;
  port?: number;
  clientId: string;
  auth?: { type: "token"; token: string };
  reconnect?: boolean;
  maxReconnectAttempts?: number;
  reconnectBaseDelay?: number;
  reconnectMaxDelay?: number;
  requestTimeout?: number;
  pingInterval?: number;
}

export type PublishOptions =
  | { subject: string; data: unknown; key?: string | Buffer; headers?: [string, string][] }
  | { subject: string; value: Buffer | Uint8Array; key?: string | Buffer; headers?: [string, string][] };

export interface PublishResult {
  offset: bigint;
  toJSON(): { offset: string };
}

export interface CreateStreamOptions {
  maxAgeSecs?: number;
  maxBytes?: number;
}

export interface CreateConsumerOptions {
  name: string;
  stream: string;
  group?: string;
  subjectFilter?: string;
  startFrom?: "earliest" | "latest" | { offset: bigint };
}

export interface SubscribeOptions {
  maxQueueSize?: number;
}

export interface FetchOptions {
  offset: bigint;
  maxRecords: number;
  subjectFilter?: string;
}

export interface FetchRecord {
  offset: bigint;
  timestamp: bigint;
  subject: string;
  key: Buffer | undefined;
  headers: [string, string][];
  json<T = unknown>(): T;
  raw(): Buffer;
  toJSON(): object;
}

export interface SeekOptions {
  timestamp: bigint;
}

export interface SeekResult {
  offset: bigint;
}
