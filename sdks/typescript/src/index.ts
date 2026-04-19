export { ExspeedClient } from "./client.js";
export { Subscription, Message } from "./subscription.js";
export {
  ExspeedError,
  ServerError,
  ProtocolError,
  TimeoutError,
  ConnectionError,
  ValidationError,
  BufferFullError,
  QueueOverflowError,
} from "./errors.js";
export type {
  BrokerEndpoint,
  ClientOptions,
  PublishOptions,
  PublishResult,
  CreateStreamOptions,
  CreateConsumerOptions,
  SubscribeOptions,
  FetchOptions,
  FetchRecord,
  SeekOptions,
  SeekResult,
} from "./types.js";
export { OpCode } from "./protocol/opcodes.js";
