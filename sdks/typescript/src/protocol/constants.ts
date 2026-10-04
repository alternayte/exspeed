/** Wire protocol version spoken by this SDK (byte 0 of every frame). */
export const PROTOCOL_VERSION = 0x02;
/** `[version u8][opcode u8][correlation id u32][payload length u32]`. */
export const FRAME_HEADER_SIZE = 10;
/** Largest payload either side accepts (16 MiB). */
export const MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;
/** Default client port. */
export const DEFAULT_PORT = 5933;

/** Operation codes of client protocol v2 (see `docs/protocol.md`). */
export enum OpCode {
  // Requests (client -> server)
  Connect = 0x01,
  Metadata = 0x03,
  Publish = 0x10,
  PublishBatch = 0x11,
  CreateStream = 0x18,
  UpdateStream = 0x19,
  DeleteStream = 0x1a,
  StreamInfo = 0x1b,
  ListStreams = 0x1c,
  Query = 0x20,
  CreateConsumer = 0x40,
  DeleteConsumer = 0x41,
  ConsumerInfo = 0x42,
  ListConsumers = 0x43,
  SeekConsumer = 0x44,
  Subscribe = 0x50,
  Credit = 0x51,
  Unsubscribe = 0x52,
  Pull = 0x53,
  Ack = 0x54,
  Nack = 0x55,
  Term = 0x56,
  InProgress = 0x57,
  Read = 0x60,
  CorePublish = 0x70,
  CoreSubscribe = 0x71,
  KvPut = 0x74,
  KvGet = 0x75,
  KvDelete = 0x76,
  KvKeys = 0x77,
  KvHistory = 0x78,
  KvCreateBucket = 0x79,
  Ping = 0xf0,

  // Responses and pushes (server -> client)
  Ok = 0x80,
  Error = 0x81,
  Deliver = 0x82,
  Messages = 0x83,
  ReadResult = 0x84,
  Json = 0x85,
  PublishOk = 0x86,
  PublishBatchOk = 0x87,
  ConnectOk = 0x88,
  SubscribeOk = 0x89,
  SubscriptionEnded = 0x8a,
  CoreMsg = 0x8b,
  Pong = 0xf1,
}

/** Error codes the server returns (HTTP-like). */
export const ErrorCode = {
  /** Malformed request, invalid name or filter, invalid config. */
  BadRequest: 400,
  /** Not authenticated (bad or missing token). */
  Unauthorized: 401,
  /** The credential lacks the needed action on the stream. */
  Forbidden: 403,
  /** Stream, consumer, bucket or key not found; or a request (core publish with a reply subject) had no responders. */
  NotFound: 404,
  /**
   * Exists with different settings, stream still has consumers, `msgId`
   * reused with a different body, or a KV key is not at the expected revision.
   */
  Conflict: 409,
  /** Retry later (dedup map full, too many concurrent waiting requests). */
  TooManyRequests: 429,
  Internal: 500,
  /** Not the leader (see `ServerError.leaderHint`), or still starting. */
  Unavailable: 503,
  /** The server's disk is full; nothing was written. Retry once space is freed. */
  InsufficientStorage: 507,
} as const;
