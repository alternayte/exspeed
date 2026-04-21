export enum OpCode {
  // Client → Server
  Connect = 0x01,
  Publish = 0x02,
  Subscribe = 0x03,
  Unsubscribe = 0x04,
  Ack = 0x05,
  Nack = 0x06,
  Fetch = 0x07,
  Seek = 0x08,
  RebalanceAck = 0x09,
  PublishBatch = 0x0A,
  CreateStream = 0x10,
  DeleteStream = 0x11,
  StreamInfo = 0x12,
  CreateConsumer = 0x13,
  DeleteConsumer = 0x14,
  Query = 0x20,
  QueryCancel = 0x21,
  Ping = 0xf0,

  // Server → Client
  Ok = 0x80,
  Error = 0x81,
  Record = 0x82,
  RecordsBatch = 0x83,
  StreamInfoResp = 0x84,
  QueryResult = 0x85,
  Rebalance = 0x86,
  Drain = 0x87,
  PublishOk = 0x88,
  ConnectOk = 0x89,
  PublishBatchOk = 0x8A,
  Pong = 0xf1,
}

export const PROTOCOL_VERSION = 0x01;
export const FRAME_HEADER_SIZE = 10;
export const MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 16 MB
export const DEFAULT_PORT = 5933;
