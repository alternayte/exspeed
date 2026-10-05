package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.exspeed.client.protocol.Request;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** How the builders map to the wire: JSON byte-for-byte like serde_json, headers like the Rust builders. */
class SpecMappingTest {
  static final String RUST_CONSUMER_JSON = "{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"orders.>\"],"
      + "\"deliver\":{\"from_time\":123},\"ack\":\"explicit\",\"ack_wait_ms\":30000,\"max_deliver\":5,"
      + "\"backoff_ms\":[100,1000],\"max_ack_pending\":1000,\"dlq_stream\":\"s-dlq\",\"ephemeral\":false,"
      + "\"dead_letter_expired\":false,\"header_match\":\"all\",\"single_active\":false,\"priority_window\":0}";

  static ConsumerSpec.Builder full() {
    return ConsumerSpec.builder("c", "s")
        .filterSubjects("orders.>")
        .deliver(DeliverPolicy.fromTimeMs(123))
        .ack(AckPolicy.EXPLICIT)
        .ackWait(Duration.ofMillis(30000))
        .maxDeliver(5)
        .backoff(Duration.ofMillis(100), Duration.ofMillis(1000))
        .maxAckPending(1000)
        .dlqStream("s-dlq")
        .ephemeral(false);
  }

  @Test
  void consumerSpecJsonMatchesSerdeJsonsOutputForTheSameSpec() {
    // serde_json::to_vec(&spec) in the Rust round-trip test.
    String json = full().deadLetterExpired(false).headerMatch(HeaderMatch.ALL).singleActive(false)
        .priorityWindow(0).build().toJson();
    assertEquals(RUST_CONSUMER_JSON, json);
    byte[] payload = new Request.CreateConsumer(json).encode();
    assertEquals(RUST_CONSUMER_JSON.length(), (payload[0] & 0xff) | (payload[1] & 0xff) << 8);
    assertEquals(4 + RUST_CONSUMER_JSON.length(), payload.length);
  }

  @Test
  void mapsHeaderFiltersSingleActiveAndPrioritySettingsLikeSerdeJson() {
    String json = full().deadLetterExpired(true).filterHeader("tenant", "acme").headerMatch(HeaderMatch.ANY)
        .singleActive(true).priorityWindow(50).build().toJson();
    assertEquals("{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"orders.>\"],\"deliver\":{\"from_time\":123},"
        + "\"ack\":\"explicit\",\"ack_wait_ms\":30000,\"max_deliver\":5,\"backoff_ms\":[100,1000],"
        + "\"max_ack_pending\":1000,\"dlq_stream\":\"s-dlq\",\"ephemeral\":false,\"dead_letter_expired\":true,"
        + "\"filter_headers\":{\"tenant\":\"acme\"},\"header_match\":\"any\",\"single_active\":true,"
        + "\"priority_window\":50}", json);
    // An empty header filter is left out, as serde does.
    assertEquals("{\"name\":\"c\",\"stream\":\"s\"}",
        ConsumerSpec.builder("c", "s").filterHeaders(Map.of()).build().toJson());
  }

  @Test
  void omitsUnsetConsumerSpecFieldsSoTheServerAppliesItsDefaults() {
    assertEquals("{\"name\":\"c\",\"stream\":\"s\"}", ConsumerSpec.of("c", "s").toJson());
    assertEquals("{\"name\":\"c\",\"stream\":\"s\",\"deliver\":{\"from_offset\":5},\"ack\":\"none\"}",
        ConsumerSpec.builder("c", "s").deliver(DeliverPolicy.fromOffset(5)).ack(AckPolicy.NONE).build().toJson());
    assertEquals("{\"name\":\"c\",\"stream\":\"s\",\"deliver\":{\"from_time\":42}}",
        ConsumerSpec.builder("c", "s").deliver(DeliverPolicy.fromTime(Instant.ofEpochMilli(42))).build().toJson());
    assertEquals("{\"name\":\"c\",\"stream\":\"s\",\"deliver\":\"all\"}",
        ConsumerSpec.builder("c", "s").deliver(DeliverPolicy.ALL).build().toJson());
    assertThrows(IllegalArgumentException.class, () -> ConsumerSpec.of("", "s"));
    assertThrows(IllegalArgumentException.class, () -> ConsumerSpec.of("c", ""));
  }

  @Test
  void parsesAConsumerSpecBack() {
    @SuppressWarnings("unchecked")
    Map<String, Object> m = (Map<String, Object>) Json.parse(RUST_CONSUMER_JSON);
    ConsumerSpec s = ConsumerSpec.fromJson(m);
    assertEquals(RUST_CONSUMER_JSON, s.toJson());
    assertEquals(List.of("orders.>"), s.filterSubjects());
    assertEquals(123L, s.deliver().timeMs());
    assertNull(s.deliver().offset());
  }

  @Test
  void sendsNoLimitsTrailerWhenEveryLimitIsAtItsDefault() {
    StreamSpec plain = StreamSpec.builder("s").maxAgeSecs(1).discard(DiscardPolicy.OLD)
        .retention(RetentionPolicy.LIMITS).allowMsgTtl(false).build();
    assertNull(plain.limitsJson());
    byte[] payload = new Request.CreateStream(plain.toWire()).encode();
    assertEquals(("0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00")
        .replace(" ", ""), HexFormat.of().formatHex(payload));
  }

  @Test
  void sendsEveryLimitSerdeStyleOnceAnyOneIsSet() {
    assertEquals("{\"max_msgs\":0,\"discard\":\"old\",\"max_msgs_per_subject\":0,\"allow_msg_ttl\":false,"
        + "\"msg_ttl_ms\":0,\"allow_delayed\":true,\"retention\":\"limits\"}",
        StreamSpec.builder("s").allowDelayed(true).build().limitsJson());
    assertEquals("{\"max_msgs\":10,\"discard\":\"new\",\"max_msgs_per_subject\":1,\"allow_msg_ttl\":true,"
        + "\"msg_ttl_ms\":5000,\"allow_delayed\":true,\"retention\":\"work_queue\"}",
        StreamSpec.builder("q").maxMsgs(10).discard(DiscardPolicy.NEW).maxMsgsPerSubject(1).allowMsgTtl(true)
            .msgTtlMs(5000).allowDelayed(true).retention(RetentionPolicy.WORK_QUEUE).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").maxMsgs(1).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").discard(DiscardPolicy.NEW).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").maxMsgsPerSubject(2).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").allowMsgTtl(true).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").msgTtlMs(3).build().limitsJson());
    assertNotNull(StreamSpec.builder("s").retention(RetentionPolicy.INTEREST).build().limitsJson());
    assertThrows(IllegalArgumentException.class, () -> StreamSpec.of(""));
  }

  @Test
  void addsCaptureSubjectsOnlyWhenThereAreSome() {
    assertNull(StreamSpec.builder("s").captureSubjects(List.of()).build().limitsJson());
    // The JSON pinned by capture_subjects_are_serialized_only_when_set in crates/exspeed-common/src/limits.rs.
    assertEquals("{\"max_msgs\":0,\"discard\":\"old\",\"max_msgs_per_subject\":0,\"allow_msg_ttl\":false,"
        + "\"msg_ttl_ms\":0,\"allow_delayed\":false,\"retention\":\"limits\",\"capture_subjects\":[\"orders.>\"]}",
        StreamSpec.builder("s").captureSubjects("orders.>").build().limitsJson());
    assertEquals(List.of("orders.>"), StreamSpec.builder("s").captureSubjects("orders.>").build().captureSubjects());
  }

  @Test
  void publishOptionsBecomeTheSameHeadersAsTheRustPublishRecordBuilders() {
    // PublishRecord::new("a", "v").ttl(500ms).delay(2s).deliver_at(1700000000000).priority(7)
    PublishRecord r = PublishRecord.builder("a").value("v").header("trace-id", "t")
        .priority(7).deliverAtMs(1_700_000_000_000L).delay("2000ms").ttl(Duration.ofMillis(500)).build();
    assertEquals(List.of(new Header("trace-id", "t"), new Header("exspeed-ttl", "500ms"),
        new Header("exspeed-delay", "2000ms"), new Header("exspeed-deliver-at", "1700000000000"),
        new Header("exspeed-priority", "7")), r.headers());
  }

  @Test
  void acceptsDurationStringsAndInstantsAndRejectsBadValues() {
    PublishRecord r = PublishRecord.builder("a").value("v").ttl("30s").delay(Duration.ZERO)
        .deliverAt(Instant.ofEpochMilli(42)).build();
    assertEquals(List.of(new Header("exspeed-ttl", "30s"), new Header("exspeed-delay", "0ms"),
        new Header("exspeed-deliver-at", "42")), r.headers());
    assertEquals(List.of(new Header("exspeed-ttl", "1ms")),
        PublishRecord.builder("a").ttl(Duration.ofNanos(200_000)).build().headers());
    assertEquals(List.of(new Header("exspeed-delay", "2ms")),
        PublishRecord.builder("a").delay(Duration.ofNanos(1_500_000)).build().headers());
    assertThrows(IllegalArgumentException.class, () -> PublishRecord.builder("a").ttl("soon"));
    assertThrows(IllegalArgumentException.class, () -> PublishRecord.builder("a").delay(Duration.ofMillis(-1)));
    assertThrows(IllegalArgumentException.class, () -> PublishRecord.builder("a").priority(10));
    assertThrows(IllegalArgumentException.class, () -> PublishRecord.builder("a").priority(-1));
    assertThrows(IllegalArgumentException.class, () -> PublishRecord.builder("a").deliverAtMs(-5));
  }

  @Test
  void wiresKeysValuesAndMsgIds() {
    PublishRecord r = PublishRecord.builder("s.x").key("k").jsonValue(List.of(1, "two", true)).msgId("m").build();
    assertEquals("[1,\"two\",true]", new String(r.value(), java.nio.charset.StandardCharsets.UTF_8));
    assertEquals("m", r.toWire().msgId());
    assertEquals("k", new String(r.toWire().key(), java.nio.charset.StandardCharsets.UTF_8));
    assertNull(PublishRecord.of("s", "v").key());
  }
}
