package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Every request a client can send, with its binary encoding. This mirrors
 * {@code Request} in {@code crates/exspeed-protocol/src/client.rs} in both
 * directions: {@link #encode()} is what a client sends, {@link #decode(int, byte[])}
 * is what a server does.
 */
public sealed interface Request {
  /**
   * The request's opcode.
   *
   * @return the opcode
   */
  int opcode();

  /**
   * Writes the payload.
   *
   * @param w the writer
   */
  void encodeTo(WireWriter w);

  /**
   * The request type's name, such as {@code "Publish"}.
   *
   * @return the name
   */
  default String typeName() {
    return OpCode.name(opcode());
  }

  /**
   * Encodes the payload (no frame header).
   *
   * @return the payload bytes
   */
  default byte[] encode() {
    WireWriter w = new WireWriter();
    encodeTo(w);
    return w.finish();
  }

  /**
   * Encodes a complete frame.
   *
   * @param correlationId the correlation id (0 = fire-and-forget)
   * @return the frame bytes
   */
  default byte[] frame(int correlationId) {
    return Frame.encode(opcode(), correlationId, encode());
  }

  /** Seek kind: the first retained record. */
  int SEEK_EARLIEST = 0;
  /** Seek kind: the end of the stream. */
  int SEEK_LATEST = 1;
  /** Seek kind: an offset. */
  int SEEK_OFFSET = 2;
  /** Seek kind: a time in ms since the epoch. */
  int SEEK_TIME = 3;

  /**
   * Handshake.
   *
   * @param clientId shown in server logs
   * @param token bearer token, or {@code null}
   */
  record Connect(String clientId, String token) implements Request {
    @Override
    public int opcode() {
      return OpCode.CONNECT;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(clientId);
      w.optStr(token);
    }
  }

  /** Keepalive. */
  record Ping() implements Request {
    @Override
    public int opcode() {
      return OpCode.PING;
    }

    @Override
    public void encodeTo(WireWriter w) {}
  }

  /** Node id, leadership and server version. */
  record Metadata() implements Request {
    @Override
    public int opcode() {
      return OpCode.METADATA;
    }

    @Override
    public void encodeTo(WireWriter w) {}
  }

  /**
   * Publish one record.
   *
   * @param stream the stream
   * @param record the record
   */
  record Publish(String stream, WirePublishRecord record) implements Request {
    @Override
    public int opcode() {
      return OpCode.PUBLISH;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(stream);
      record.encode(w);
    }
  }

  /**
   * Publish several records.
   *
   * @param stream the stream
   * @param records the records, in order
   */
  record PublishBatch(String stream, List<WirePublishRecord> records) implements Request {
    @Override
    public int opcode() {
      return OpCode.PUBLISH_BATCH;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(stream);
      w.u32(records.size());
      for (WirePublishRecord r : records) {
        r.encode(w);
      }
    }
  }

  /**
   * Create a stream.
   *
   * @param spec the settings
   */
  record CreateStream(WireStreamSpec spec) implements Request {
    @Override
    public int opcode() {
      return OpCode.CREATE_STREAM;
    }

    @Override
    public void encodeTo(WireWriter w) {
      spec.encode(w);
    }
  }

  /**
   * Replace a stream's settings.
   *
   * @param spec the settings
   */
  record UpdateStream(WireStreamSpec spec) implements Request {
    @Override
    public int opcode() {
      return OpCode.UPDATE_STREAM;
    }

    @Override
    public void encodeTo(WireWriter w) {
      spec.encode(w);
    }
  }

  /**
   * Delete a stream.
   *
   * @param name the stream
   */
  record DeleteStream(String name) implements Request {
    @Override
    public int opcode() {
      return OpCode.DELETE_STREAM;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(name);
    }
  }

  /**
   * Stream info.
   *
   * @param name the stream
   */
  record StreamInfo(String name) implements Request {
    @Override
    public int opcode() {
      return OpCode.STREAM_INFO;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(name);
    }
  }

  /** List streams. */
  record ListStreams() implements Request {
    @Override
    public int opcode() {
      return OpCode.LIST_STREAMS;
    }

    @Override
    public void encodeTo(WireWriter w) {}
  }

  /**
   * Bounded ExQL query.
   *
   * @param sql the SQL text
   */
  record Query(String sql) implements Request {
    @Override
    public int opcode() {
      return OpCode.QUERY;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.lstr(sql);
    }
  }

  /**
   * Create a consumer.
   *
   * @param specJson the snake_case ConsumerSpec JSON
   */
  record CreateConsumer(String specJson) implements Request {
    @Override
    public int opcode() {
      return OpCode.CREATE_CONSUMER;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.bytes(specJson.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Delete a consumer.
   *
   * @param name the consumer
   */
  record DeleteConsumer(String name) implements Request {
    @Override
    public int opcode() {
      return OpCode.DELETE_CONSUMER;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(name);
    }
  }

  /**
   * Consumer info.
   *
   * @param name the consumer
   */
  record ConsumerInfo(String name) implements Request {
    @Override
    public int opcode() {
      return OpCode.CONSUMER_INFO;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(name);
    }
  }

  /**
   * List consumers.
   *
   * @param stream only consumers of this stream, or {@code null} for all
   */
  record ListConsumers(String stream) implements Request {
    @Override
    public int opcode() {
      return OpCode.LIST_CONSUMERS;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.optStr(stream);
    }
  }

  /**
   * Move a consumer's cursor.
   *
   * @param consumer the consumer
   * @param kind {@link #SEEK_EARLIEST}, {@link #SEEK_LATEST}, {@link #SEEK_OFFSET} or {@link #SEEK_TIME}
   * @param value the offset or time (ms), 0 otherwise
   */
  record SeekConsumer(String consumer, int kind, long value) implements Request {
    @Override
    public int opcode() {
      return OpCode.SEEK_CONSUMER;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u8(kind);
      w.u64(value);
    }
  }

  /**
   * Start push delivery.
   *
   * @param consumer the consumer
   * @param credits the initial credit window
   */
  record Subscribe(String consumer, int credits) implements Request {
    @Override
    public int opcode() {
      return OpCode.SUBSCRIBE;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u32(credits);
    }
  }

  /**
   * Grant more push credit.
   *
   * @param subId the subscription id
   * @param credits how many more records may be pushed
   */
  record Credit(int subId, int credits) implements Request {
    @Override
    public int opcode() {
      return OpCode.CREDIT;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
      w.u32(credits);
    }
  }

  /**
   * End a subscription (consumer or core).
   *
   * @param subId the subscription id
   */
  record Unsubscribe(int subId) implements Request {
    @Override
    public int opcode() {
      return OpCode.UNSUBSCRIBE;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
    }
  }

  /**
   * Fetch a batch from a consumer.
   *
   * @param consumer the consumer
   * @param maxMessages at most this many records
   * @param maxBytes byte budget (0 = server default)
   * @param expiresMs wait up to this long for at least one record
   */
  record Pull(String consumer, int maxMessages, int maxBytes, int expiresMs) implements Request {
    @Override
    public int opcode() {
      return OpCode.PULL;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u32(maxMessages);
      w.u32(maxBytes);
      w.u32(expiresMs);
    }
  }

  /**
   * Acknowledge records.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   */
  record Ack(String consumer, long[] offsets) implements Request {
    @Override
    public int opcode() {
      return OpCode.ACK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u32(offsets.length);
      for (long o : offsets) {
        w.u64(o);
      }
    }
  }

  /**
   * Ask for redelivery.
   *
   * @param consumer the consumer
   * @param offset the record
   * @param delayMs redeliver after this long (0 = the consumer's backoff)
   */
  record Nack(String consumer, long offset, int delayMs) implements Request {
    @Override
    public int opcode() {
      return OpCode.NACK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u64(offset);
      w.u32(delayMs);
    }
  }

  /**
   * Dead-letter a record now.
   *
   * @param consumer the consumer
   * @param offset the record
   * @param reason free text, stored in {@code exspeed-dlq-reason}
   */
  record Term(String consumer, long offset, String reason) implements Request {
    @Override
    public int opcode() {
      return OpCode.TERM;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u64(offset);
      w.str(reason);
    }
  }

  /**
   * Reset the ack deadlines of records still being worked on.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   */
  record InProgress(String consumer, long[] offsets) implements Request {
    @Override
    public int opcode() {
      return OpCode.IN_PROGRESS;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(consumer);
      w.u32(offsets.length);
      for (long o : offsets) {
        w.u64(o);
      }
    }
  }

  /**
   * Stateless read.
   *
   * @param stream the stream
   * @param from the first offset
   * @param maxRecords at most this many records
   * @param maxBytes byte budget (0 = server default)
   * @param waitMs long-poll wait when caught up
   * @param filter subject filter ("" = all)
   */
  record Read(String stream, long from, int maxRecords, int maxBytes, int waitMs, String filter)
      implements Request {
    @Override
    public int opcode() {
      return OpCode.READ;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(stream);
      w.u64(from);
      w.u32(maxRecords);
      w.u32(maxBytes);
      w.u32(waitMs);
      w.str(filter);
    }
  }

  /**
   * Publish a core message. With {@code replyTo} it is a request, answered with
   * 404 when nobody received it.
   *
   * @param subject the subject
   * @param replyTo the reply subject, or {@code null}
   * @param headers the headers
   * @param value the payload
   */
  record CorePublish(String subject, String replyTo, List<Header> headers, byte[] value) implements Request {
    @Override
    public int opcode() {
      return OpCode.CORE_PUBLISH;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(subject);
      w.optStr(replyTo);
      w.headers(headers);
      w.bytes(value);
    }
  }

  /**
   * Subscribe to core messages.
   *
   * @param subject the subject filter
   * @param queue the queue group, or {@code null}
   */
  record CoreSubscribe(String subject, String queue) implements Request {
    @Override
    public int opcode() {
      return OpCode.CORE_SUBSCRIBE;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(subject);
      w.optStr(queue);
    }
  }

  /**
   * Create a KV bucket.
   *
   * @param bucket the bucket
   * @param history values kept per key (0 = 1)
   * @param ttlMs key lifetime after the last put (0 = never)
   * @param maxBytes size limit (0 = server default)
   */
  record KvCreateBucket(String bucket, long history, long ttlMs, long maxBytes) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_CREATE_BUCKET;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.u64(history);
      w.u64(ttlMs);
      w.u64(maxBytes);
    }
  }

  /**
   * Set a KV key.
   *
   * @param bucket the bucket
   * @param key the key
   * @param value the value
   * @param expectedRevision only if the key is at this revision (0 = absent), or {@code null}
   * @param ttlMs this value's lifetime, or {@code null}
   */
  record KvPut(String bucket, String key, byte[] value, Long expectedRevision, Long ttlMs) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_PUT;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.str(key);
      w.bytes(value);
      w.optU64(expectedRevision);
      w.optU64(ttlMs);
    }
  }

  /**
   * Read a KV key.
   *
   * @param bucket the bucket
   * @param key the key
   * @param revision a specific revision, or {@code null} for the current value
   */
  record KvGet(String bucket, String key, Long revision) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_GET;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.str(key);
      w.optU64(revision);
    }
  }

  /**
   * Delete or purge a KV key.
   *
   * @param bucket the bucket
   * @param key the key
   * @param purge also hide the key's older values
   * @param expectedRevision only if the key is at this revision, or {@code null}
   */
  record KvDelete(String bucket, String key, boolean purge, Long expectedRevision) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_DELETE;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.str(key);
      w.u8(purge ? 1 : 0);
      w.optU64(expectedRevision);
    }
  }

  /**
   * List KV keys that have a value.
   *
   * @param bucket the bucket
   * @param filter subject filter ("" = all)
   */
  record KvKeys(String bucket, String filter) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_KEYS;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.str(filter);
    }
  }

  /**
   * A KV key's kept revisions.
   *
   * @param bucket the bucket
   * @param key the key
   */
  record KvHistory(String bucket, String key) implements Request {
    @Override
    public int opcode() {
      return OpCode.KV_HISTORY;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(bucket);
      w.str(key);
    }
  }

  private static long[] readOffsets(WireReader r) {
    int n = r.count(8);
    long[] out = new long[n];
    for (int i = 0; i < n; i++) {
      out[i] = r.u64();
    }
    return out;
  }

  /**
   * Decodes a request payload (what a server does).
   *
   * @param opcode the frame's opcode
   * @param payload the payload
   * @return the request
   * @throws ProtocolException when the payload is malformed or the opcode is not a request
   */
  static Request decode(int opcode, byte[] payload) {
    WireReader r = new WireReader(payload);
    Request req;
    switch (opcode) {
      case OpCode.CONNECT -> req = new Connect(r.str(), r.optStr());
      case OpCode.PING -> req = new Ping();
      case OpCode.METADATA -> req = new Metadata();
      case OpCode.PUBLISH -> req = new Publish(r.str(), WirePublishRecord.decode(r));
      case OpCode.PUBLISH_BATCH -> {
        String stream = r.str();
        int n = r.count(10);
        List<WirePublishRecord> records = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          records.add(WirePublishRecord.decode(r));
        }
        req = new PublishBatch(stream, records);
      }
      case OpCode.CREATE_STREAM -> req = new CreateStream(WireStreamSpec.decode(r));
      case OpCode.UPDATE_STREAM -> req = new UpdateStream(WireStreamSpec.decode(r));
      case OpCode.DELETE_STREAM -> req = new DeleteStream(r.str());
      case OpCode.STREAM_INFO -> req = new StreamInfo(r.str());
      case OpCode.LIST_STREAMS -> req = new ListStreams();
      case OpCode.QUERY -> req = new Query(r.lstr());
      case OpCode.CREATE_CONSUMER -> req = new CreateConsumer(new String(r.bytes(), StandardCharsets.UTF_8));
      case OpCode.DELETE_CONSUMER -> req = new DeleteConsumer(r.str());
      case OpCode.CONSUMER_INFO -> req = new ConsumerInfo(r.str());
      case OpCode.LIST_CONSUMERS -> req = new ListConsumers(r.optStr());
      case OpCode.SEEK_CONSUMER -> {
        String consumer = r.str();
        int kind = r.u8();
        long value = r.u64();
        if (kind > 3) {
          throw new ProtocolException("unknown seek kind " + kind);
        }
        req = new SeekConsumer(consumer, kind, value);
      }
      case OpCode.SUBSCRIBE -> req = new Subscribe(r.str(), r.u32());
      case OpCode.CREDIT -> req = new Credit(r.u32(), r.u32());
      case OpCode.UNSUBSCRIBE -> req = new Unsubscribe(r.u32());
      case OpCode.PULL -> req = new Pull(r.str(), r.u32(), r.u32(), r.u32());
      case OpCode.ACK -> req = new Ack(r.str(), readOffsets(r));
      case OpCode.IN_PROGRESS -> req = new InProgress(r.str(), readOffsets(r));
      case OpCode.NACK -> req = new Nack(r.str(), r.u64(), r.u32());
      case OpCode.TERM -> req = new Term(r.str(), r.u64(), r.str());
      case OpCode.READ -> req = new Read(r.str(), r.u64(), r.u32(), r.u32(), r.u32(), r.str());
      case OpCode.CORE_PUBLISH -> req = new CorePublish(r.str(), r.optStr(), r.headers(), r.bytes());
      case OpCode.CORE_SUBSCRIBE -> req = new CoreSubscribe(r.str(), r.optStr());
      case OpCode.KV_CREATE_BUCKET -> req = new KvCreateBucket(r.str(), r.u64(), r.u64(), r.u64());
      case OpCode.KV_PUT -> req = new KvPut(r.str(), r.str(), r.bytes(), r.optU64(), r.optU64());
      case OpCode.KV_GET -> req = new KvGet(r.str(), r.str(), r.optU64());
      case OpCode.KV_DELETE -> req = new KvDelete(r.str(), r.str(), r.u8() != 0, r.optU64());
      case OpCode.KV_KEYS -> req = new KvKeys(r.str(), r.str());
      case OpCode.KV_HISTORY -> req = new KvHistory(r.str(), r.str());
      default -> throw new ProtocolException("opcode " + OpCode.name(opcode) + " is not a client request");
    }
    r.finish();
    return req;
  }
}
