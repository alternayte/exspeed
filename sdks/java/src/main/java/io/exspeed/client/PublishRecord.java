package io.exspeed.client;

import io.exspeed.client.protocol.WirePublishRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A record to publish: subject, value, and optionally a key, headers, an
 * idempotency key and the time and priority options.
 *
 * <pre>{@code
 * PublishRecord r = PublishRecord.builder("orders.placed")
 *     .value("{\"id\":42}")
 *     .key("customer-7")
 *     .header("trace-id", "abc")
 *     .msgId(MsgId.newMsgId())
 *     .build();
 * }</pre>
 *
 * <p>The time and priority options become headers, appended after your own in
 * this order: {@value #TTL_HEADER}, {@value #DELAY_HEADER},
 * {@value #DELIVER_AT_HEADER}, {@value #PRIORITY_HEADER}.
 */
public final class PublishRecord {
  /** Header behind {@link Builder#ttl(Duration)}. */
  public static final String TTL_HEADER = "exspeed-ttl";
  /** Header behind {@link Builder#delay(Duration)}. */
  public static final String DELAY_HEADER = "exspeed-delay";
  /** Header behind {@link Builder#deliverAt(Instant)}. */
  public static final String DELIVER_AT_HEADER = "exspeed-deliver-at";
  /** Header behind {@link Builder#priority(int)}. */
  public static final String PRIORITY_HEADER = "exspeed-priority";

  private static final Pattern DURATION = Pattern.compile("^\\d+\\s*(ms|s|m|h|d)?$");

  private final String subject;
  private final byte[] key;
  private final byte[] value;
  private final List<Header> headers;
  private final String msgId;

  private PublishRecord(Builder b) {
    this.subject = b.subject;
    this.key = b.key;
    this.value = b.value;
    List<Header> h = new ArrayList<>(b.headers);
    if (b.ttl != null) {
      h.add(new Header(TTL_HEADER, b.ttl));
    }
    if (b.delay != null) {
      h.add(new Header(DELAY_HEADER, b.delay));
    }
    if (b.deliverAtMs != null) {
      h.add(new Header(DELIVER_AT_HEADER, Long.toString(b.deliverAtMs)));
    }
    if (b.priority != null) {
      h.add(new Header(PRIORITY_HEADER, Integer.toString(b.priority)));
    }
    this.headers = Collections.unmodifiableList(h);
    this.msgId = b.msgId;
  }

  /**
   * Starts a record.
   *
   * @param subject the subject, such as {@code orders.placed}
   * @return a builder
   */
  public static Builder builder(String subject) {
    return new Builder(subject);
  }

  /**
   * A record with a UTF-8 text value.
   *
   * @param subject the subject
   * @param value the value
   * @return the record
   */
  public static PublishRecord of(String subject, String value) {
    return builder(subject).value(value).build();
  }

  /**
   * A record with a binary value.
   *
   * @param subject the subject
   * @param value the value
   * @return the record
   */
  public static PublishRecord of(String subject, byte[] value) {
    return builder(subject).value(value).build();
  }

  /**
   * The subject.
   *
   * @return the subject
   */
  public String subject() {
    return subject;
  }

  /**
   * The key.
   *
   * @return the key, or {@code null}
   */
  public byte[] key() {
    return key;
  }

  /**
   * The value.
   *
   * @return the value
   */
  public byte[] value() {
    return value;
  }

  /**
   * Every header that will be sent, the time and priority options included.
   *
   * @return the headers (unmodifiable)
   */
  public List<Header> headers() {
    return headers;
  }

  /**
   * The idempotency key.
   *
   * @return the msg id, or {@code null}
   */
  public String msgId() {
    return msgId;
  }

  WirePublishRecord toWire() {
    return new WirePublishRecord(subject, key, value, headers, msgId);
  }

  /** Header value of a duration given in ms: whole ms, rounded up, at least {@code min}. */
  static String durationMs(String name, Duration d, long min) {
    if (d.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative, got " + d);
    }
    long ms = d.toMillis();
    if (d.minusMillis(ms).toNanos() > 0) {
      ms++;
    }
    return Math.max(min, ms) + "ms";
  }

  static String durationText(String name, String v) {
    String t = v.trim();
    if (!DURATION.matcher(t).matches()) {
      throw new IllegalArgumentException(
          "invalid " + name + " '" + v + "': expected a number with an optional unit (ms, s, m, h, d)");
    }
    return t;
  }

  /** Builds a {@link PublishRecord}. */
  public static final class Builder {
    private final String subject;
    private byte[] key;
    private byte[] value = new byte[0];
    private final List<Header> headers = new ArrayList<>();
    private String msgId;
    private String ttl;
    private String delay;
    private Long deliverAtMs;
    private Integer priority;

    private Builder(String subject) {
      this.subject = Objects.requireNonNull(subject, "subject");
    }

    /**
     * Sets a binary value (sent as-is).
     *
     * @param value the value
     * @return this builder
     */
    public Builder value(byte[] value) {
      this.value = Objects.requireNonNull(value, "value");
      return this;
    }

    /**
     * Sets a text value (sent as UTF-8).
     *
     * @param value the value
     * @return this builder
     */
    public Builder value(String value) {
      this.value = value.getBytes(StandardCharsets.UTF_8);
      return this;
    }

    /**
     * Sets a JSON value: a map, list, string, number, boolean or {@code null},
     * serialized with {@link Json#stringify(Object)}.
     *
     * @param value the value
     * @return this builder
     */
    public Builder jsonValue(Object value) {
      this.value = Json.stringify(value).getBytes(StandardCharsets.UTF_8);
      return this;
    }

    /**
     * Sets the key (for compaction and routing).
     *
     * @param key the key bytes
     * @return this builder
     */
    public Builder key(byte[] key) {
      this.key = key;
      return this;
    }

    /**
     * Sets the key as UTF-8 text.
     *
     * @param key the key
     * @return this builder
     */
    public Builder key(String key) {
      this.key = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
      return this;
    }

    /**
     * Adds a header (a key may repeat).
     *
     * @param key the header name
     * @param value the header value
     * @return this builder
     */
    public Builder header(String key, String value) {
      headers.add(new Header(key, value));
      return this;
    }

    /**
     * Adds headers, in the map's iteration order.
     *
     * @param headers the headers
     * @return this builder
     */
    public Builder headers(Map<String, String> headers) {
      headers.forEach(this::header);
      return this;
    }

    /**
     * Sets the idempotency key: a retry with the same msg id and body returns
     * the original offset with {@code duplicate = true} instead of writing
     * again. See {@link MsgId#newMsgId()}.
     *
     * @param msgId the key
     * @return this builder
     */
    public Builder msgId(String msgId) {
      this.msgId = msgId;
      return this;
    }

    /**
     * Expires the record this long after it is appended (whole ms, rounded up,
     * at least 1). The stream needs {@code allowMsgTtl}.
     *
     * @param ttl the lifetime
     * @return this builder
     */
    public Builder ttl(Duration ttl) {
      this.ttl = durationMs("ttl", ttl, 1);
      return this;
    }

    /**
     * Expires the record this long after it is appended, as a duration string
     * such as {@code "30s"} (units ms, s, m, h, d; a bare number is ms).
     *
     * @param ttl the lifetime
     * @return this builder
     */
    public Builder ttl(String ttl) {
      this.ttl = durationText("ttl", ttl);
      return this;
    }

    /**
     * Delivers the record to consumers no earlier than this long after the
     * append. The stream needs {@code allowDelayed}.
     *
     * @param delay the delay
     * @return this builder
     */
    public Builder delay(Duration delay) {
      this.delay = durationMs("delay", delay, 0);
      return this;
    }

    /**
     * Delivers the record to consumers no earlier than this long after the
     * append, as a duration string such as {@code "10s"}.
     *
     * @param delay the delay
     * @return this builder
     */
    public Builder delay(String delay) {
      this.delay = durationText("delay", delay);
      return this;
    }

    /**
     * Delivers the record to consumers no earlier than this time. The stream
     * needs {@code allowDelayed}.
     *
     * @param when the delivery time
     * @return this builder
     */
    public Builder deliverAt(Instant when) {
      return deliverAtMs(when.toEpochMilli());
    }

    /**
     * Delivers the record to consumers no earlier than this time.
     *
     * @param epochMs the delivery time, ms since the Unix epoch
     * @return this builder
     */
    public Builder deliverAtMs(long epochMs) {
      if (epochMs < 0) {
        throw new IllegalArgumentException("invalid deliverAt: " + epochMs);
      }
      this.deliverAtMs = epochMs;
      return this;
    }

    /**
     * Sets the priority, 0 (default) to 9, higher first, for consumers with a
     * {@code priorityWindow}.
     *
     * @param priority the priority
     * @return this builder
     */
    public Builder priority(int priority) {
      if (priority < 0 || priority > 9) {
        throw new IllegalArgumentException("priority must be an integer from 0 to 9, got " + priority);
      }
      this.priority = priority;
      return this;
    }

    /**
     * Builds the record.
     *
     * @return the record
     */
    public PublishRecord build() {
      return new PublishRecord(this);
    }
  }
}
