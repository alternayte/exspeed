package io.exspeed.client.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.ExspeedException;
import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Byte-exact codec tests. The fixtures are the ones in
 * {@code sdks/typescript/test/unit/protocol.test.ts}, generated from the Rust
 * encoder in {@code crates/exspeed-protocol/src/client.rs}.
 */
class ProtocolTest {
  /** Hex string (spaces, newlines and {@code |} ignored) to bytes. */
  static byte[] hex(String s) {
    return HexFormat.of().parseHex(s.replaceAll("[\\s|]", ""));
  }

  static String toHex(byte[] b) {
    return HexFormat.of().formatHex(b);
  }

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Same as {@code rec(i)} in the Rust protocol tests. */
  static WireRecord rec(int i) {
    byte[] value = new byte[i % 7];
    Arrays.fill(value, (byte) i);
    return new WireRecord(i, 1_700_000_000_000_000_000L + i, i % 3, "orders." + i,
        i % 2 == 0 ? b("k" + i) : null, value, List.of(new Header("h", "v" + i)));
  }

  static final WirePublishRecord PR =
      new WirePublishRecord("a.b", b("k"), b("{\"x\":1}"), List.of(new Header("h1", "v1")), "m-1");
  static final WirePublishRecord EMPTY_PR = new WirePublishRecord("", null, new byte[0], List.of(), null);
  static final WireStreamSpec SPEC = new WireStreamSpec("s", 1, 2, 3, 4, true, null);
  static final String LIMITS_JSON = "{\"max_msgs\":10,\"discard\":\"new\",\"max_msgs_per_subject\":1,"
      + "\"allow_msg_ttl\":true,\"msg_ttl_ms\":5000,\"allow_delayed\":true,\"retention\":\"work_queue\"}";
  static final WireStreamSpec LIMITED_SPEC = new WireStreamSpec("q", 0, 0, 0, 0, false, LIMITS_JSON);
  static final String CONSUMER_JSON = "{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"orders.>\"],"
      + "\"deliver\":{\"from_time\":123},\"ack\":\"explicit\",\"ack_wait_ms\":30000,\"max_deliver\":5,"
      + "\"backoff_ms\":[100,1000],\"max_ack_pending\":1000,\"dlq_stream\":\"s-dlq\",\"ephemeral\":false,"
      + "\"dead_letter_expired\":false,\"header_match\":\"all\",\"single_active\":false,\"priority_window\":0}";

  static List<Request> allRequests() {
    return List.of(
        new Request.Connect("c", "t"),
        new Request.Connect("c", null),
        new Request.Ping(),
        new Request.Metadata(),
        new Request.Publish("s", PR),
        new Request.PublishBatch("s", List.of(PR, EMPTY_PR)),
        new Request.CreateStream(SPEC),
        new Request.UpdateStream(SPEC),
        new Request.DeleteStream("s"),
        new Request.StreamInfo("s"),
        new Request.ListStreams(),
        new Request.Query("SELECT 1"),
        new Request.CreateConsumer(CONSUMER_JSON),
        new Request.DeleteConsumer("c"),
        new Request.ConsumerInfo("c"),
        new Request.ListConsumers(null),
        new Request.ListConsumers("s"),
        new Request.SeekConsumer("c", Request.SEEK_TIME, 9),
        new Request.SeekConsumer("c", Request.SEEK_LATEST, 0),
        new Request.Subscribe("c", 100),
        new Request.Credit(3, 10),
        new Request.Unsubscribe(3),
        new Request.Pull("c", 10, 1024, 500),
        new Request.Ack("c", new long[] {1, 2, 3}),
        new Request.Nack("c", 4, 100),
        new Request.Term("c", 5, "bad"),
        new Request.InProgress("c", new long[] {6}),
        new Request.Read("s", 7, 100, 1 << 20, 1000, "a.*"),
        new Request.CreateStream(LIMITED_SPEC),
        new Request.CorePublish("a.b", "r", List.of(new Header("h", "v")), b("x")),
        new Request.CorePublish("a", null, List.of(), new byte[0]),
        new Request.CoreSubscribe("a.*", "q"),
        new Request.CoreSubscribe("a.*", null),
        new Request.KvCreateBucket("b", 5, 1000, 0),
        new Request.KvPut("b", "k", b("v"), 0L, null),
        new Request.KvPut("b", "k", b("v"), null, 500L),
        new Request.KvGet("b", "k", 3L),
        new Request.KvGet("b", "k", null),
        new Request.KvDelete("b", "k", true, 7L),
        new Request.KvDelete("b", "k", false, null),
        new Request.KvKeys("b", "a.*"),
        new Request.KvHistory("b", "k"));
  }

  static List<Response> allResponses() {
    List<WireRecord> five = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      five.add(rec(i));
    }
    return List.of(
        new Response.Ok(),
        new Response.Pong(),
        new Response.Error(404, "nope", null),
        new Response.Error(503, "not leader", b("{\"leader\":\"h:1\"}")),
        new Response.ConnectOk("0.6.0", "n1", "h:5933"),
        new Response.PublishOk(9, true),
        new Response.PublishBatchOk(List.of(new Response.PublishOk(1, false), new Response.PublishOk(1, true))),
        new Response.SubscribeOk(2),
        new Response.Deliver(2, five),
        new Response.SubscriptionEnded(2, 404, "consumer deleted"),
        new Response.Messages(List.of(rec(1))),
        new Response.ReadResult(10, 12, List.of(rec(0), rec(1), rec(2))),
        new Response.Json(b("{\"a\":1}")),
        new Response.CoreMsg(0x80000001, "a", "r", List.of(new Header("h", "v")), b("x")),
        new Response.CoreMsg(0x80000002, "a.b", null, List.of(), new byte[0]));
  }

  /** Records equal field by field (byte arrays by content). */
  static void assertRecordEquals(WireRecord a, WireRecord b) {
    assertEquals(a.offset(), b.offset());
    assertEquals(a.timestampNs(), b.timestampNs());
    assertEquals(a.deliveryCount(), b.deliveryCount());
    assertEquals(a.subject(), b.subject());
    assertArrayEquals(a.key(), b.key());
    assertArrayEquals(a.value(), b.value());
    assertEquals(a.headers(), b.headers());
  }

  @Nested
  class Primitives {
    @Test
    void roundTripsEveryPrimitive() {
      WireWriter w = new WireWriter(4); // forces growth
      w.u8(255).u16(65535).u32(0xffffffff).u64(Long.MAX_VALUE).u64(-1L).u64(0);
      w.str("héllo").lstr("SELECT 'ü'").bytes(b("raw"));
      w.optStr(null).optStr("x").optU64(null).optU64(7L).optBytes(null);
      w.headers(List.of(new Header("k", "v"), new Header("k", "v2")));
      WireReader r = new WireReader(w.finish());
      assertEquals(255, r.u8());
      assertEquals(65535, r.u16());
      assertEquals(0xffffffff, r.u32());
      assertEquals(Long.MAX_VALUE, r.u64());
      assertEquals(-1L, r.u64()); // u64::MAX, as unsigned bits
      assertEquals(0, r.u64());
      assertEquals("héllo", r.str());
      assertEquals("SELECT 'ü'", r.lstr());
      assertArrayEquals(b("raw"), r.bytes());
      assertNull(r.optStr());
      assertEquals("x", r.optStr());
      assertNull(r.optU64());
      assertEquals(7L, r.optU64());
      assertNull(r.optBytes());
      assertEquals(List.of(new Header("k", "v"), new Header("k", "v2")), r.headers());
      r.finish();
    }

    @Test
    void encodesStrAsU16LengthPlusUtf8LittleEndian() {
      assertArrayEquals(hex("0200 c3a9"), new WireWriter().str("é").finish());
      assertArrayEquals(hex("0807060504030201"), new WireWriter().u64(0x0102030405060708L).finish());
    }

    @Test
    void rejectsTruncatedTrailingAndMalformedInput() {
      assertThrows(ProtocolException.class, () -> new WireReader(hex("01")).u16());
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("0500 6162")).str())
          .getMessage().contains("truncated"));
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("00")).finish())
          .getMessage().contains("trailing"));
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("02")).optStr())
          .getMessage().contains("option flag"));
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("0200 c328")).str())
          .getMessage().contains("UTF-8"));
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("ffff")).headers())
          .getMessage().contains("header count"));
      assertTrue(assertThrows(ProtocolException.class, () -> new WireReader(hex("ffffffff 00")).bytes())
          .getMessage().contains("truncated"));
    }

    @Test
    void rejectsValuesThatDontFitTheWireTypes() {
      assertThrows(ExspeedException.class, () -> new WireWriter().str("x".repeat(70_000)));
      List<Header> many = new ArrayList<>();
      for (int i = 0; i < 70_000; i++) {
        many.add(new Header("k", "v"));
      }
      assertThrows(ExspeedException.class, () -> new WireWriter().headers(many));
    }
  }

  @Nested
  class Frames {
    @Test
    void encodesTheTenByteHeader() {
      assertArrayEquals(hex("02 01 07000000 02000000 aabb"), Frame.encode(0x01, 7, hex("aabb")));
    }

    @Test
    void readsFramesDeliveredOneByteAtATime() throws IOException {
      ByteArrayOutputStream all = new ByteArrayOutputStream();
      all.writeBytes(new Response.Pong().frame(1));
      all.writeBytes(new Response.PublishOk(3, false).frame(2));
      all.writeBytes(new Response.Deliver(9, List.of(rec(2))).frame(0));
      byte[] bytes = all.toByteArray();
      InputStream trickle = new InputStream() {
        int pos;

        @Override
        public int read() {
          return pos < bytes.length ? bytes[pos++] & 0xff : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
          if (pos >= bytes.length) {
            return -1;
          }
          b[off] = bytes[pos++];
          return 1;
        }
      };
      List<int[]> seen = new ArrayList<>();
      Frame f;
      while ((f = Frame.read(trickle)) != null) {
        seen.add(new int[] {f.opcode(), f.correlationId()});
      }
      assertEquals(3, seen.size());
      assertArrayEquals(new int[] {OpCode.PONG, 1}, seen.get(0));
      assertArrayEquals(new int[] {OpCode.PUBLISH_OK, 2}, seen.get(1));
      assertArrayEquals(new int[] {OpCode.DELIVER, 0}, seen.get(2));
    }

    @Test
    void rejectsABadVersionOrAnOversizeLength() {
      assertTrue(assertThrows(ProtocolException.class,
          () -> Frame.read(new ByteArrayInputStream(hex("01 80 00000000 00000000")))).getMessage()
          .contains("version"));
      byte[] big = new byte[10];
      big[0] = 2;
      big[1] = (byte) 0x80;
      Frame.putU32(big, 6, Frame.MAX_PAYLOAD_SIZE + 1);
      assertTrue(assertThrows(ProtocolException.class, () -> Frame.read(new ByteArrayInputStream(big)))
          .getMessage().contains("too large"));
      assertTrue(assertThrows(ExspeedException.class,
          () -> Frame.encode(0x10, 1, new byte[Frame.MAX_PAYLOAD_SIZE + 1])).getMessage().contains("too large"));
    }

    @Test
    void reportsATruncatedFrame() {
      assertThrows(IOException.class, () -> Frame.read(new ByteArrayInputStream(hex("02 80 01000000 05000000 aa"))));
      assertThrows(IOException.class, () -> Frame.read(new ByteArrayInputStream(hex("02 80 01"))));
    }
  }

  @Nested
  class Requests {
    @TestFactory
    Stream<DynamicTest> everyRequestRoundTrips() {
      return allRequests().stream().map(req -> DynamicTest.dynamicTest(req.typeName(), () -> {
        byte[] frame = req.frame(42);
        Frame f = Frame.read(new ByteArrayInputStream(frame));
        assertEquals(req.opcode(), f.opcode());
        assertEquals(42, f.correlationId());
        Request back = Request.decode(f.opcode(), f.payload());
        assertEquals(req.getClass(), back.getClass());
        assertArrayEquals(req.encode(), back.encode());
      }));
    }

    @Test
    void decodesFieldsBack() {
      Request.Ack ack = (Request.Ack) Request.decode(OpCode.ACK, new Request.Ack("c", new long[] {1, 2, 3}).encode());
      assertEquals("c", ack.consumer());
      assertArrayEquals(new long[] {1, 2, 3}, ack.offsets());
      Request.KvPut put = (Request.KvPut) Request.decode(OpCode.KV_PUT,
          new Request.KvPut("b", "k", b("v"), null, 500L).encode());
      assertNull(put.expectedRevision());
      assertEquals(500L, put.ttlMs());
      Request.CreateStream cs = (Request.CreateStream) Request.decode(OpCode.CREATE_STREAM,
          new Request.CreateStream(LIMITED_SPEC).encode());
      assertEquals(LIMITED_SPEC, cs.spec());
    }

    @Test
    void rejectsTruncatedAndTrailingPayloads() {
      assertTrue(assertThrows(ProtocolException.class, () -> Request.decode(OpCode.PING, hex("09")))
          .getMessage().contains("trailing"));
      byte[] sub = new Request.Subscribe("c", 1).encode();
      assertTrue(assertThrows(ProtocolException.class,
          () -> Request.decode(OpCode.SUBSCRIBE, Arrays.copyOf(sub, sub.length - 1))).getMessage()
          .contains("truncated"));
    }

    @Test
    void rejectsHostileCountsWithoutAllocating() {
      byte[] p = new WireWriter().str("s").u32(0xffffffff).finish();
      assertTrue(assertThrows(ProtocolException.class, () -> Request.decode(OpCode.PUBLISH_BATCH, p))
          .getMessage().contains("exceeds payload"));
    }

    @Test
    void rejectsResponseOpcodesAndUnknownSeekKinds() {
      assertThrows(ProtocolException.class, () -> Request.decode(OpCode.OK, new byte[0]));
      byte[] p = new WireWriter().str("c").u8(4).u64(0).finish();
      assertTrue(assertThrows(ProtocolException.class, () -> Request.decode(OpCode.SEEK_CONSUMER, p))
          .getMessage().contains("seek kind"));
    }

    /** Fixtures derived from {@code Writer} in crates/exspeed-protocol/src/client.rs. */
    @TestFactory
    Stream<DynamicTest> matchTheRustEncodingByteForByte() {
      Object[][] fixtures = {
        {"Connect", new Request.Connect("c", "t"), "0100 63 | 01 0100 74"},
        {"Connect without token", new Request.Connect("c", null), "0100 63 | 00"},
        {"Ping", new Request.Ping(), ""},
        {"Publish", new Request.Publish("s", PR),
          "0100 73 0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31"},
        {"PublishBatch", new Request.PublishBatch("s", List.of(PR, EMPTY_PR)),
          "0100 73 | 02000000"
              + " 0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31"
              + " 0000 00 00000000 0000 00"},
        {"CreateStream", new Request.CreateStream(SPEC),
          "0100 73 0100000000000000 0200000000000000 0300000000000000 0400000000000000 01"},
        {"Query", new Request.Query("SELECT 1"), "08000000 53454c4543542031"},
        {"ListConsumers all", new Request.ListConsumers(null), "00"},
        {"ListConsumers stream", new Request.ListConsumers("s"), "01 0100 73"},
        {"Seek time", new Request.SeekConsumer("c", Request.SEEK_TIME, 9), "0100 63 03 0900000000000000"},
        {"Seek latest", new Request.SeekConsumer("c", Request.SEEK_LATEST, 0), "0100 63 01 0000000000000000"},
        {"Subscribe", new Request.Subscribe("c", 100), "0100 63 64000000"},
        {"Credit", new Request.Credit(3, 10), "03000000 0a000000"},
        {"Unsubscribe", new Request.Unsubscribe(3), "03000000"},
        {"Pull", new Request.Pull("c", 10, 1024, 500), "0100 63 0a000000 00040000 f4010000"},
        {"Ack", new Request.Ack("c", new long[] {1, 2, 3}),
          "0100 63 03000000 0100000000000000 0200000000000000 0300000000000000"},
        {"Nack", new Request.Nack("c", 4, 100), "0100 63 0400000000000000 64000000"},
        {"Term", new Request.Term("c", 5, "bad"), "0100 63 0500000000000000 0300 626164"},
        {"Read", new Request.Read("s", 7, 100, 1 << 20, 1000, "a.*"),
          "0100 73 0700000000000000 64000000 00001000 e8030000 0300 612e2a"},
        {"CreateStream with limits", new Request.CreateStream(LIMITED_SPEC),
          "0100 71 0000000000000000 0000000000000000 0000000000000000 0000000000000000 00 8d000000 "
              + toHex(b(LIMITS_JSON))},
        {"CorePublish", new Request.CorePublish("a.b", "r", List.of(new Header("h", "v")), b("x")),
          "0300 612e62 | 01 0100 72 | 0100 0100 68 0100 76 | 01000000 78"},
        {"CorePublish bare", new Request.CorePublish("a", null, List.of(), new byte[0]),
          "0100 61 | 00 | 0000 | 00000000"},
        {"CoreSubscribe", new Request.CoreSubscribe("a.*", "q"), "0300 612e2a 01 0100 71"},
        {"CoreSubscribe bare", new Request.CoreSubscribe("a.*", null), "0300 612e2a 00"},
        {"KvCreateBucket", new Request.KvCreateBucket("b", 5, 1000, 0),
          "0100 62 0500000000000000 e803000000000000 0000000000000000"},
        {"KvPut expecting revision 0", new Request.KvPut("b", "k", b("v"), 0L, null),
          "0100 62 0100 6b 01000000 76 01 0000000000000000 00"},
        {"KvPut with TTL", new Request.KvPut("b", "k", b("v"), null, 500L),
          "0100 62 0100 6b 01000000 76 00 01 f401000000000000"},
        {"KvGet at revision", new Request.KvGet("b", "k", 3L), "0100 62 0100 6b 01 0300000000000000"},
        {"KvGet", new Request.KvGet("b", "k", null), "0100 62 0100 6b 00"},
        {"KvDelete purge", new Request.KvDelete("b", "k", true, 7L), "0100 62 0100 6b 01 01 0700000000000000"},
        {"KvDelete", new Request.KvDelete("b", "k", false, null), "0100 62 0100 6b 00 00"},
        {"KvKeys", new Request.KvKeys("b", "a.*"), "0100 62 0300 612e2a"},
        {"KvHistory", new Request.KvHistory("b", "k"), "0100 62 0100 6b"},
      };
      return Arrays.stream(fixtures).map(fx -> DynamicTest.dynamicTest((String) fx[0],
          () -> assertEquals(toHex(hex((String) fx[2])), toHex(((Request) fx[1]).encode()))));
    }

    @Test
    void connectFrameMatchesByteForByteHeaderIncluded() {
      assertArrayEquals(hex("02 01 01000000 07000000 | 0100 63 01 0100 74"), new Request.Connect("c", "t").frame(1));
    }

    @Test
    void createConsumerCarriesTheSpecJsonAsBytes() {
      byte[] payload = new Request.CreateConsumer(CONSUMER_JSON).encode();
      WireReader r = new WireReader(payload);
      assertEquals(CONSUMER_JSON, new String(r.bytes(), StandardCharsets.UTF_8));
      r.finish();
    }

    @Test
    void oldStyleStreamSpecWithoutTrailerDecodesWithoutLimits() {
      byte[] payload = hex("0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00");
      Request.CreateStream cs = (Request.CreateStream) Request.decode(OpCode.CREATE_STREAM, payload);
      assertEquals(new WireStreamSpec("s", 1, 0, 0, 0, false, null), cs.spec());
    }
  }

  @Nested
  class Responses {
    @TestFactory
    Stream<DynamicTest> everyResponseRoundTrips() {
      return allResponses().stream().map(resp -> DynamicTest.dynamicTest(resp.typeName(), () -> {
        Frame f = Frame.read(new ByteArrayInputStream(resp.frame(7)));
        assertEquals(resp.opcode(), f.opcode());
        Response back = Response.decode(f.opcode(), f.payload(), true);
        assertEquals(resp.getClass(), back.getClass());
        assertArrayEquals(resp.encode(), back.encode());
      }));
    }

    @Test
    void rejectsHostileRecordCountsAndUnknownOpcodes() {
      byte[] p = new WireWriter().u64(0).u64(0).u32(0xffffffff).finish();
      assertTrue(assertThrows(ProtocolException.class, () -> Response.decode(OpCode.READ_RESULT, p, true))
          .getMessage().contains("exceeds payload"));
      assertTrue(assertThrows(ProtocolException.class, () -> Response.decode(OpCode.PUBLISH, new byte[0], true))
          .getMessage().contains("not a server response"));
    }

    /** Fixtures derived from {@code Writer} in crates/exspeed-protocol/src/client.rs. */
    @TestFactory
    Stream<DynamicTest> decodeFromTheRustEncoding() {
      Object[][] fixtures = {
        {"Error", new Response.Error(404, "nope", null), "9401 0400 6e6f7065 00"},
        {"Error with detail", new Response.Error(503, "not leader", b("{\"leader\":\"h:1\"}")),
          "f701 0a00 6e6f74206c6561646572 01 10000000 7b226c6561646572223a22683a31227d"},
        {"ConnectOk", new Response.ConnectOk("0.6.0", "n1", "h:5933"),
          "0500 302e362e30 0200 6e31 01 0600 683a35393333"},
        {"PublishOk", new Response.PublishOk(9, true), "0900000000000000 01"},
        {"PublishBatchOk",
          new Response.PublishBatchOk(List.of(new Response.PublishOk(1, false), new Response.PublishOk(1, true))),
          "02000000 0100000000000000 00 0100000000000000 01"},
        {"SubscribeOk", new Response.SubscribeOk(2), "02000000"},
        {"SubscriptionEnded", new Response.SubscriptionEnded(2, 404, "consumer deleted"),
          "02000000 9401 1000 636f6e73756d65722064656c65746564"},
        {"Deliver", new Response.Deliver(2, List.of(rec(2))),
          "02000000 | 01000000 36000000 06760cef 0200 0200000000000000 02002a36fe9c9717"
              + " 0800 6f72646572732e32 01 02000000 6b32 02000000 0202 0100 0100 68 0200 7632"},
        {"Messages", new Response.Messages(List.of(rec(0))),
          "01000000 34000000 b6f9c422 0000 0000000000000000 00002a36fe9c9717"
              + " 0800 6f72646572732e30 01 02000000 6b30 00000000 0100 0100 68 0200 7630"},
        {"ReadResult", new Response.ReadResult(10, 12, List.of(rec(1))),
          "0a00000000000000 0c00000000000000 01000000 2f000000 f194cd5a 0100 0100000000000000 01002a36fe9c9717"
              + " 0800 6f72646572732e31 00 01000000 01 0100 0100 68 0200 7631"},
        {"CoreMsg", new Response.CoreMsg(0x80000001, "a", "r", List.of(new Header("h", "v")), b("x")),
          "01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78"},
      };
      return Arrays.stream(fixtures).map(fx -> DynamicTest.dynamicTest((String) fx[0], () -> {
        Response expected = (Response) fx[1];
        byte[] bytes = hex((String) fx[2]);
        Response decoded = Response.decode(expected.opcode(), bytes, true);
        assertEquals(expected.getClass(), decoded.getClass());
        assertEquals(toHex(bytes), toHex(decoded.encode()));
        assertEquals(toHex(bytes), toHex(expected.encode()));
      }));
    }

    @Test
    void decodesRecordFieldsFromTheRustEncoding() {
      byte[] bytes = hex("02000000 | 01000000 36000000 06760cef 0200 0200000000000000 02002a36fe9c9717"
          + " 0800 6f72646572732e32 01 02000000 6b32 02000000 0202 0100 0100 68 0200 7632");
      Response.Deliver d = (Response.Deliver) Response.decode(OpCode.DELIVER, bytes, true);
      assertEquals(2, d.subId());
      assertRecordEquals(rec(2), d.records().get(0));
      Response.Error e = (Response.Error) Response.decode(OpCode.ERROR, hex("9401 0400 6e6f7065 00"), true);
      assertEquals(404, e.code());
      assertEquals("nope", e.message());
      assertNull(e.detail());
      Response.CoreMsg m = (Response.CoreMsg) Response.decode(OpCode.CORE_MSG,
          hex("01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78"), true);
      assertEquals(0x80000001, m.subId());
      assertEquals("r", m.replyTo());
      assertEquals("x", new String(m.value(), StandardCharsets.UTF_8));
    }

    @Test
    void jsonIsPassedThroughRaw() {
      Response.Json j = (Response.Json) Response.decode(OpCode.JSON, b("not even json"), true);
      assertEquals("not even json", new String(j.json(), StandardCharsets.UTF_8));
    }
  }

  @Nested
  class Records {
    @Test
    void carryACrc32cThatIgnoresDeliveryCount() {
      byte[] enc = rec(2).encode();
      assertEquals(0x36 + 4, enc.length);
      assertTrue(WireRecord.verifyCrc(enc));
      // The server patches delivery_count (bytes 8..10) in place.
      enc[8] = 7;
      enc[9] = 0;
      assertTrue(WireRecord.verifyCrc(enc));
      byte[] payload = new WireWriter().u32(1).raw(enc).finish();
      Response.Messages m = (Response.Messages) Response.decode(OpCode.MESSAGES, payload, true);
      assertEquals(7, m.records().get(0).deliveryCount());
      enc[enc.length - 1] ^= 1;
      assertFalse(WireRecord.verifyCrc(enc));
      byte[] corrupt = new WireWriter().u32(1).raw(enc).finish();
      assertTrue(assertThrows(ProtocolException.class, () -> Response.decode(OpCode.MESSAGES, corrupt, true))
          .getMessage().contains("CRC"));
      // Without verification the corrupt record still decodes.
      assertInstanceOf(Response.Messages.class, Response.decode(OpCode.MESSAGES, corrupt, false));
    }

    @Test
    void crc32cMatchesTheStandardCheckValue() {
      CRC32C c = new CRC32C();
      c.update(b("123456789"));
      assertEquals(0xe3069283L, c.getValue());
    }

    @Test
    void rejectsARecordLengthThatDisagreesWithItsContents() {
      byte[] enc = rec(1).encode();
      Frame.putU32(enc, 0, Frame.getU32(enc, 0) + 1);
      byte[] payload = new WireWriter().u32(1).raw(enc).u8(0).finish();
      assertThrows(ProtocolException.class, () -> Response.decode(OpCode.MESSAGES, payload, true));
      byte[] tooSmall = new WireWriter().u32(1).u32(4).raw(new byte[40]).finish();
      assertTrue(assertThrows(ProtocolException.class, () -> Response.decode(OpCode.MESSAGES, tooSmall, true))
          .getMessage().contains("too small"));
    }
  }

  @Test
  void opcodeNamesAreReadable() {
    assertEquals("KvCreateBucket", OpCode.name(OpCode.KV_CREATE_BUCKET));
    assertEquals("0x30", OpCode.name(0x30));
  }
}
