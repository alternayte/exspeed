package io.exspeed.client.protocol;

/** Operation codes of client protocol v2 (see {@code docs/protocol.md}). */
public final class OpCode {
  private OpCode() {}

  /** Handshake; must be the first frame. */
  public static final int CONNECT = 0x01;
  /** Node id, leadership and server version. */
  public static final int METADATA = 0x03;
  /** Publish one record. */
  public static final int PUBLISH = 0x10;
  /** Publish several records. */
  public static final int PUBLISH_BATCH = 0x11;
  /** Create a stream. */
  public static final int CREATE_STREAM = 0x18;
  /** Replace a stream's settings. */
  public static final int UPDATE_STREAM = 0x19;
  /** Delete a stream. */
  public static final int DELETE_STREAM = 0x1A;
  /** Stream info (JSON). */
  public static final int STREAM_INFO = 0x1B;
  /** List streams (JSON). */
  public static final int LIST_STREAMS = 0x1C;
  /** Bounded ExQL query. */
  public static final int QUERY = 0x20;
  /** Create a consumer. */
  public static final int CREATE_CONSUMER = 0x40;
  /** Delete a consumer. */
  public static final int DELETE_CONSUMER = 0x41;
  /** Consumer info (JSON). */
  public static final int CONSUMER_INFO = 0x42;
  /** List consumers (JSON). */
  public static final int LIST_CONSUMERS = 0x43;
  /** Move a consumer's cursor. */
  public static final int SEEK_CONSUMER = 0x44;
  /** Start push delivery. */
  public static final int SUBSCRIBE = 0x50;
  /** Grant push credit. */
  public static final int CREDIT = 0x51;
  /** End a subscription. */
  public static final int UNSUBSCRIBE = 0x52;
  /** Fetch a batch from a consumer. */
  public static final int PULL = 0x53;
  /** Acknowledge records. */
  public static final int ACK = 0x54;
  /** Ask for redelivery. */
  public static final int NACK = 0x55;
  /** Dead-letter a record. */
  public static final int TERM = 0x56;
  /** Reset ack deadlines. */
  public static final int IN_PROGRESS = 0x57;
  /** Stateless read. */
  public static final int READ = 0x60;
  /** Publish a core message. */
  public static final int CORE_PUBLISH = 0x70;
  /** Subscribe to core messages. */
  public static final int CORE_SUBSCRIBE = 0x71;
  /** Set a KV key. */
  public static final int KV_PUT = 0x74;
  /** Read a KV key. */
  public static final int KV_GET = 0x75;
  /** Delete or purge a KV key. */
  public static final int KV_DELETE = 0x76;
  /** List KV keys. */
  public static final int KV_KEYS = 0x77;
  /** A KV key's history. */
  public static final int KV_HISTORY = 0x78;
  /** Create a KV bucket. */
  public static final int KV_CREATE_BUCKET = 0x79;
  /** Keepalive request. */
  public static final int PING = 0xF0;

  /** Success without a payload. */
  public static final int OK = 0x80;
  /** Failure: code, message, optional detail. */
  public static final int ERROR = 0x81;
  /** Push delivery of records to a subscription. */
  public static final int DELIVER = 0x82;
  /** Records (pull, KV get and history). */
  public static final int MESSAGES = 0x83;
  /** Records of a stateless read. */
  public static final int READ_RESULT = 0x84;
  /** Raw UTF-8 JSON. */
  public static final int JSON = 0x85;
  /** Offset of a published record. */
  public static final int PUBLISH_OK = 0x86;
  /** Offsets of a published batch. */
  public static final int PUBLISH_BATCH_OK = 0x87;
  /** Handshake reply. */
  public static final int CONNECT_OK = 0x88;
  /** Subscription id. */
  public static final int SUBSCRIBE_OK = 0x89;
  /** Push: a subscription ended. */
  public static final int SUBSCRIPTION_ENDED = 0x8A;
  /** Push: a core message. */
  public static final int CORE_MSG = 0x8B;
  /** Keepalive reply. */
  public static final int PONG = 0xF1;

  /**
   * A readable name for an opcode.
   *
   * @param op the opcode
   * @return its name, or {@code 0x..} when unknown
   */
  public static String name(int op) {
    return switch (op) {
      case CONNECT -> "Connect";
      case METADATA -> "Metadata";
      case PUBLISH -> "Publish";
      case PUBLISH_BATCH -> "PublishBatch";
      case CREATE_STREAM -> "CreateStream";
      case UPDATE_STREAM -> "UpdateStream";
      case DELETE_STREAM -> "DeleteStream";
      case STREAM_INFO -> "StreamInfo";
      case LIST_STREAMS -> "ListStreams";
      case QUERY -> "Query";
      case CREATE_CONSUMER -> "CreateConsumer";
      case DELETE_CONSUMER -> "DeleteConsumer";
      case CONSUMER_INFO -> "ConsumerInfo";
      case LIST_CONSUMERS -> "ListConsumers";
      case SEEK_CONSUMER -> "SeekConsumer";
      case SUBSCRIBE -> "Subscribe";
      case CREDIT -> "Credit";
      case UNSUBSCRIBE -> "Unsubscribe";
      case PULL -> "Pull";
      case ACK -> "Ack";
      case NACK -> "Nack";
      case TERM -> "Term";
      case IN_PROGRESS -> "InProgress";
      case READ -> "Read";
      case CORE_PUBLISH -> "CorePublish";
      case CORE_SUBSCRIBE -> "CoreSubscribe";
      case KV_PUT -> "KvPut";
      case KV_GET -> "KvGet";
      case KV_DELETE -> "KvDelete";
      case KV_KEYS -> "KvKeys";
      case KV_HISTORY -> "KvHistory";
      case KV_CREATE_BUCKET -> "KvCreateBucket";
      case PING -> "Ping";
      case OK -> "Ok";
      case ERROR -> "Error";
      case DELIVER -> "Deliver";
      case MESSAGES -> "Messages";
      case READ_RESULT -> "ReadResult";
      case JSON -> "Json";
      case PUBLISH_OK -> "PublishOk";
      case PUBLISH_BATCH_OK -> "PublishBatchOk";
      case CONNECT_OK -> "ConnectOk";
      case SUBSCRIBE_OK -> "SubscribeOk";
      case SUBSCRIPTION_ENDED -> "SubscriptionEnded";
      case CORE_MSG -> "CoreMsg";
      case PONG -> "Pong";
      default -> String.format("0x%02x", op);
    };
  }
}
