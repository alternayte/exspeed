/**
 * A single broker endpoint. The SDK accepts a list and cycles through them
 * for failover. `port` defaults to the protocol DEFAULT_PORT (5933) when
 * omitted.
 */
export interface BrokerEndpoint {
  host: string;
  port?: number;
}

export interface ClientOptions {
  /**
   * @deprecated Use `brokers` instead. Provided for backwards compat with
   * single-broker setups. If both are supplied, `brokers` wins.
   */
  host?: string;
  /** @deprecated Use `brokers` instead. */
  port?: number;

  /** List of broker endpoints to try, in order. Cycles on connection failure. */
  brokers?: BrokerEndpoint[];

  clientId: string;
  auth?: { type: "token"; token: string };
  reconnect?: boolean;
  maxReconnectAttempts?: number;
  reconnectBaseDelay?: number;
  reconnectMaxDelay?: number;
  requestTimeout?: number;
  pingInterval?: number;
  /**
   * TLS configuration.
   * - `true` = TLS with full cert verification against Node's trust store.
   * - `false` or undefined = plain TCP (default, current behaviour).
   * - object = TLS with overrides for self-signed certs or custom CAs.
   */
  tls?: boolean | {
    rejectUnauthorized?: boolean;
    ca?: Buffer | Buffer[];
  };
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
  ifNotExists?: boolean;
}

export interface CreateConsumerOptions {
  name: string;
  stream: string;
  group?: string;
  subjectFilter?: string;
  startFrom?: "earliest" | "latest" | { offset: bigint };
  ifNotExists?: boolean;
}

export interface SubscribeOptions {
  maxQueueSize?: number;
  subscriberId?: string;
  /**
   * What to do when the subscription's local buffer fills up.
   * - 'drop-oldest' (default): drop the oldest queued record and emit a
   *   typed `QueueOverflowError` via the "overflow" event.
   * - 'error': reject the incoming push by throwing `QueueOverflowError`,
   *   which propagates to the caller as a Subscription "error" event.
   */
  overflowPolicy?: "drop-oldest" | "error";
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
