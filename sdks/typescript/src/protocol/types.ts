export interface ConnectRequest {
  clientId: string;
  authType: number;
  authPayload: Buffer;
}

export interface PublishRequest {
  stream: string;
  subject: string;
  value: Buffer;
  key?: Buffer;
  headers?: [string, string][];
}

export interface AckRequest {
  consumerName: string;
  offset: bigint;
}

export interface FetchRequest {
  stream: string;
  offset: bigint;
  maxRecords: number;
  subjectFilter?: string;
}

export interface SeekRequest {
  consumerName: string;
  timestamp: bigint;
}

export interface CreateStreamRequest {
  name: string;
  maxAgeSecs: bigint;
  maxBytes: bigint;
}

export interface CreateConsumerRequest {
  name: string;
  stream: string;
  group: string;
  subjectFilter: string;
  startFrom: number;
  startOffset: bigint;
}

export interface RecordDelivery {
  consumerName: string;
  offset: bigint;
  timestamp: bigint;
  subject: string;
  deliveryAttempt: number;
  key?: Buffer;
  value: Buffer;
  headers: [string, string][];
}

export interface BatchRecord {
  offset: bigint;
  timestamp: bigint;
  subject: string;
  key?: Buffer;
  value: Buffer;
  headers: [string, string][];
}

export interface ServerError {
  code: number;
  message: string;
}

export const AUTH_NONE = 0x00;
export const AUTH_TOKEN = 0x01;
export const AUTH_MTLS = 0x02;
export const AUTH_SASL = 0x03;

export const START_FROM_EARLIEST = 0;
export const START_FROM_LATEST = 1;
export const START_FROM_OFFSET = 2;

export const FLAG_HAS_KEY = 0x01;
