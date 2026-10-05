package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.util.ArrayList;
import java.util.List;

/**
 * Every response and push the server can send, with its binary encoding.
 * This mirrors {@code Response} in {@code crates/exspeed-protocol/src/client.rs}
 * in both directions: {@link #decode(int, byte[], boolean)} is what a client
 * does, {@link #encode()} is what a server sends.
 */
public sealed interface Response {
  /**
   * The response's opcode.
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
   * The response type's name, such as {@code "PublishOk"}.
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
   * @param correlationId the correlation id (0 for pushes)
   * @return the frame bytes
   */
  default byte[] frame(int correlationId) {
    return Frame.encode(opcode(), correlationId, encode());
  }

  /** Success without a payload. */
  record Ok() implements Response {
    @Override
    public int opcode() {
      return OpCode.OK;
    }

    @Override
    public void encodeTo(WireWriter w) {}
  }

  /** Keepalive reply. */
  record Pong() implements Response {
    @Override
    public int opcode() {
      return OpCode.PONG;
    }

    @Override
    public void encodeTo(WireWriter w) {}
  }

  /**
   * A failed request (or, with correlation id 0, a failed fire-and-forget request).
   *
   * @param code HTTP-like error code
   * @param message the message
   * @param detail raw JSON detail, or {@code null}
   */
  record Error(int code, String message, byte[] detail) implements Response {
    @Override
    public int opcode() {
      return OpCode.ERROR;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u16(code);
      w.str(message);
      w.optBytes(detail);
    }
  }

  /**
   * Handshake reply.
   *
   * @param serverVersion the server's version
   * @param nodeId the node's id
   * @param leader the leader's client address when this node is not it, or {@code null}
   */
  record ConnectOk(String serverVersion, String nodeId, String leader) implements Response {
    @Override
    public int opcode() {
      return OpCode.CONNECT_OK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.str(serverVersion);
      w.str(nodeId);
      w.optStr(leader);
    }
  }

  /**
   * A published record's offset.
   *
   * @param offset the offset
   * @param duplicate true when the msg id matched an earlier publish
   */
  record PublishOk(long offset, boolean duplicate) implements Response {
    @Override
    public int opcode() {
      return OpCode.PUBLISH_OK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u64(offset);
      w.u8(duplicate ? 1 : 0);
    }
  }

  /**
   * A published batch's offsets, one per record.
   *
   * @param results the results, in order
   */
  record PublishBatchOk(List<PublishOk> results) implements Response {
    @Override
    public int opcode() {
      return OpCode.PUBLISH_BATCH_OK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(results.size());
      for (PublishOk r : results) {
        r.encodeTo(w);
      }
    }
  }

  /**
   * A new subscription's id.
   *
   * @param subId the id (core subscriptions have the high bit set)
   */
  record SubscribeOk(int subId) implements Response {
    @Override
    public int opcode() {
      return OpCode.SUBSCRIBE_OK;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
    }
  }

  /**
   * Push delivery for a subscription (correlation id 0).
   *
   * @param subId the subscription id
   * @param records the records
   */
  record Deliver(int subId, List<WireRecord> records) implements Response {
    @Override
    public int opcode() {
      return OpCode.DELIVER;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
      writeRecords(w, records);
    }
  }

  /**
   * Push: the subscription ended server-side (correlation id 0).
   *
   * @param subId the subscription id
   * @param code why: 404 consumer deleted, 503 leadership lost
   * @param message the message
   */
  record SubscriptionEnded(int subId, int code, String message) implements Response {
    @Override
    public int opcode() {
      return OpCode.SUBSCRIPTION_ENDED;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
      w.u16(code);
      w.str(message);
    }
  }

  /**
   * Records: the reply to Pull, KvGet and KvHistory.
   *
   * @param records the records
   */
  record Messages(List<WireRecord> records) implements Response {
    @Override
    public int opcode() {
      return OpCode.MESSAGES;
    }

    @Override
    public void encodeTo(WireWriter w) {
      writeRecords(w, records);
    }
  }

  /**
   * The reply to Read.
   *
   * @param nextOffset where to continue
   * @param highWatermark the stream's next offset at the time of the read
   * @param records the records
   */
  record ReadResult(long nextOffset, long highWatermark, List<WireRecord> records) implements Response {
    @Override
    public int opcode() {
      return OpCode.READ_RESULT;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u64(nextOffset);
      w.u64(highWatermark);
      writeRecords(w, records);
    }
  }

  /**
   * Raw UTF-8 JSON (info, lists, query results, metadata).
   *
   * @param json the JSON bytes
   */
  record Json(byte[] json) implements Response {
    @Override
    public int opcode() {
      return OpCode.JSON;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.raw(json);
    }
  }

  /**
   * Push of a core message (correlation id 0).
   *
   * @param subId the core subscription id
   * @param subject the subject
   * @param replyTo the reply subject, or {@code null}
   * @param headers the headers
   * @param value the payload
   */
  record CoreMsg(int subId, String subject, String replyTo, List<Header> headers, byte[] value)
      implements Response {
    @Override
    public int opcode() {
      return OpCode.CORE_MSG;
    }

    @Override
    public void encodeTo(WireWriter w) {
      w.u32(subId);
      w.str(subject);
      w.optStr(replyTo);
      w.headers(headers);
      w.bytes(value);
    }
  }

  private static void writeRecords(WireWriter w, List<WireRecord> records) {
    w.u32(records.size());
    for (WireRecord r : records) {
      w.raw(r.encode());
    }
  }

  private static List<WireRecord> readRecords(WireReader r, boolean verifyCrc) {
    int n = r.count(WireRecord.MIN_RECORD_LEN);
    List<WireRecord> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      out.add(WireRecord.decode(r, verifyCrc));
    }
    return out;
  }

  /**
   * Decodes a response or push payload.
   *
   * @param opcode the frame's opcode
   * @param payload the payload
   * @param verifyCrc whether to verify each record's CRC32C
   * @return the response
   * @throws ProtocolException when the payload is malformed or the opcode is not a response
   */
  static Response decode(int opcode, byte[] payload, boolean verifyCrc) {
    if (opcode == OpCode.JSON) {
      return new Json(payload);
    }
    WireReader r = new WireReader(payload);
    Response resp;
    switch (opcode) {
      case OpCode.OK -> resp = new Ok();
      case OpCode.PONG -> resp = new Pong();
      case OpCode.ERROR -> resp = new Error(r.u16(), r.str(), r.optBytes());
      case OpCode.CONNECT_OK -> resp = new ConnectOk(r.str(), r.str(), r.optStr());
      case OpCode.PUBLISH_OK -> resp = new PublishOk(r.u64(), r.u8() != 0);
      case OpCode.PUBLISH_BATCH_OK -> {
        int n = r.count(9);
        List<PublishOk> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          results.add(new PublishOk(r.u64(), r.u8() != 0));
        }
        resp = new PublishBatchOk(results);
      }
      case OpCode.SUBSCRIBE_OK -> resp = new SubscribeOk(r.u32());
      case OpCode.DELIVER -> resp = new Deliver(r.u32(), readRecords(r, verifyCrc));
      case OpCode.SUBSCRIPTION_ENDED -> resp = new SubscriptionEnded(r.u32(), r.u16(), r.str());
      case OpCode.MESSAGES -> resp = new Messages(readRecords(r, verifyCrc));
      case OpCode.READ_RESULT -> resp = new ReadResult(r.u64(), r.u64(), readRecords(r, verifyCrc));
      case OpCode.CORE_MSG -> resp = new CoreMsg(r.u32(), r.str(), r.optStr(), r.headers(), r.bytes());
      default -> throw new ProtocolException("opcode " + OpCode.name(opcode) + " is not a server response");
    }
    r.finish();
    return resp;
  }
}
