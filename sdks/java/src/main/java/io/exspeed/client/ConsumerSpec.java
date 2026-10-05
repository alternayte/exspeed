package io.exspeed.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A consumer's settings. Only the name and stream are required; every unset
 * setting (a {@code null} getter) takes the server's default.
 *
 * <pre>{@code
 * ConsumerSpec spec = ConsumerSpec.builder("billing", "orders")
 *     .filterSubjects("orders.placed", "orders.eu.>")
 *     .ackWait(Duration.ofSeconds(30))
 *     .maxDeliver(5)
 *     .backoff(Duration.ofSeconds(1), Duration.ofSeconds(5))
 *     .dlqStream("orders-dlq")
 *     .build();
 * }</pre>
 */
public final class ConsumerSpec {
  private final String name;
  private final String stream;
  private final List<String> filterSubjects;
  private final DeliverPolicy deliver;
  private final AckPolicy ack;
  private final Long ackWaitMs;
  private final Integer maxDeliver;
  private final List<Long> backoffMs;
  private final Integer maxAckPending;
  private final String dlqStream;
  private final Boolean ephemeral;
  private final Boolean deadLetterExpired;
  private final Map<String, String> filterHeaders;
  private final HeaderMatch headerMatch;
  private final Boolean singleActive;
  private final Integer priorityWindow;

  private ConsumerSpec(Builder b) {
    if (b.name == null || b.name.isEmpty()) {
      throw new IllegalArgumentException("consumer name is required");
    }
    if (b.stream == null || b.stream.isEmpty()) {
      throw new IllegalArgumentException("consumer stream is required");
    }
    name = b.name;
    stream = b.stream;
    filterSubjects = b.filterSubjects == null ? null : List.copyOf(b.filterSubjects);
    deliver = b.deliver;
    ack = b.ack;
    ackWaitMs = b.ackWaitMs;
    maxDeliver = b.maxDeliver;
    backoffMs = b.backoffMs == null ? null : List.copyOf(b.backoffMs);
    maxAckPending = b.maxAckPending;
    dlqStream = b.dlqStream;
    ephemeral = b.ephemeral;
    deadLetterExpired = b.deadLetterExpired;
    filterHeaders = Collections.unmodifiableMap(new LinkedHashMap<>(b.filterHeaders));
    headerMatch = b.headerMatch;
    singleActive = b.singleActive;
    priorityWindow = b.priorityWindow;
  }

  /**
   * Starts a spec.
   *
   * @param name the consumer name
   * @param stream the stream it reads
   * @return a builder
   */
  public static Builder builder(String name, String stream) {
    return new Builder(name, stream);
  }

  /**
   * A spec with server defaults for everything but the name and stream.
   *
   * @param name the consumer name
   * @param stream the stream it reads
   * @return the spec
   */
  public static ConsumerSpec of(String name, String stream) {
    return builder(name, stream).build();
  }

  /**
   * The consumer name.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  /**
   * The stream it reads.
   *
   * @return the stream
   */
  public String stream() {
    return stream;
  }

  /**
   * Subject filters (empty = all subjects).
   *
   * @return the filters, or {@code null} when unset
   */
  public List<String> filterSubjects() {
    return filterSubjects;
  }

  /**
   * Where it starts.
   *
   * @return the policy, or {@code null} when unset (server default: all)
   */
  public DeliverPolicy deliver() {
    return deliver;
  }

  /**
   * Whether records must be acked.
   *
   * @return the policy, or {@code null} when unset (server default: explicit)
   */
  public AckPolicy ack() {
    return ack;
  }

  /**
   * Redeliver a record not acked within this time.
   *
   * @return the wait, or {@code null} when unset (server default: 30 s)
   */
  public Duration ackWait() {
    return ackWaitMs == null ? null : Duration.ofMillis(ackWaitMs);
  }

  /**
   * Dead-letter after this many deliveries (0 = never).
   *
   * @return the limit, or {@code null} when unset (server default: 5)
   */
  public Integer maxDeliver() {
    return maxDeliver;
  }

  /**
   * Redelivery delays by delivery count (the last repeats).
   *
   * @return the delays, or {@code null} when unset
   */
  public List<Duration> backoff() {
    if (backoffMs == null) {
      return null;
    }
    List<Duration> out = new ArrayList<>(backoffMs.size());
    for (long ms : backoffMs) {
      out.add(Duration.ofMillis(ms));
    }
    return Collections.unmodifiableList(out);
  }

  /**
   * Pause delivery while this many records await an ack.
   *
   * @return the limit, or {@code null} when unset (server default: 1000)
   */
  public Integer maxAckPending() {
    return maxAckPending;
  }

  /**
   * The stream that receives dead letters.
   *
   * @return the stream, or {@code null} when dead letters are dropped
   */
  public String dlqStream() {
    return dlqStream;
  }

  /**
   * Whether the consumer is deleted when the connection that created it closes.
   *
   * @return the setting, or {@code null} when unset (false)
   */
  public Boolean ephemeral() {
    return ephemeral;
  }

  /**
   * Whether records whose TTL ends before they are acked are dead-lettered.
   *
   * @return the setting, or {@code null} when unset (false)
   */
  public Boolean deadLetterExpired() {
    return deadLetterExpired;
  }

  /**
   * Only records whose headers have these values (header names as given).
   *
   * @return the header filter (empty = none)
   */
  public Map<String, String> filterHeaders() {
    return filterHeaders;
  }

  /**
   * How the header filter combines its entries.
   *
   * @return the mode, or {@code null} when unset (all)
   */
  public HeaderMatch headerMatch() {
    return headerMatch;
  }

  /**
   * Whether one subscription at a time receives records.
   *
   * @return the setting, or {@code null} when unset (false)
   */
  public Boolean singleActive() {
    return singleActive;
  }

  /**
   * How many records ahead to look to deliver higher priorities first (0 = in order).
   *
   * @return the window, or {@code null} when unset (0)
   */
  public Integer priorityWindow() {
    return priorityWindow;
  }

  /** The snake_case JSON the server expects, keys in the Rust struct's order; unset fields left out. */
  Map<String, Object> toJsonMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("stream", stream);
    putIf(m, "filter_subjects", filterSubjects);
    putIf(m, "deliver", deliver == null ? null : deliver.toJson());
    putIf(m, "ack", ack == null ? null : ack.wireName());
    putIf(m, "ack_wait_ms", ackWaitMs);
    putIf(m, "max_deliver", maxDeliver);
    putIf(m, "backoff_ms", backoffMs);
    putIf(m, "max_ack_pending", maxAckPending);
    putIf(m, "dlq_stream", dlqStream);
    putIf(m, "ephemeral", ephemeral);
    putIf(m, "dead_letter_expired", deadLetterExpired);
    if (!filterHeaders.isEmpty()) {
      m.put("filter_headers", new LinkedHashMap<>(filterHeaders));
    }
    putIf(m, "header_match", headerMatch == null ? null : headerMatch.wireName());
    putIf(m, "single_active", singleActive);
    putIf(m, "priority_window", priorityWindow);
    return m;
  }

  String toJson() {
    return Json.stringify(toJsonMap());
  }

  private static void putIf(Map<String, Object> m, String k, Object v) {
    if (v != null) {
      m.put(k, v);
    }
  }

  static ConsumerSpec fromJson(Map<String, Object> m) {
    Builder b = new Builder(JsonMaps.str(m, "name", ""), JsonMaps.str(m, "stream", ""));
    if (m.get("filter_subjects") instanceof List<?> l) {
      List<String> fs = new ArrayList<>();
      for (Object o : l) {
        fs.add(String.valueOf(o));
      }
      b.filterSubjects = fs;
    }
    if (m.containsKey("deliver")) {
      b.deliver = DeliverPolicy.fromJson(m.get("deliver"));
    }
    if (m.get("ack") instanceof String s) {
      b.ack = AckPolicy.fromWire(s);
    }
    b.ackWaitMs = JsonMaps.numOrNull(m, "ack_wait_ms");
    Long md = JsonMaps.numOrNull(m, "max_deliver");
    b.maxDeliver = md == null ? null : md.intValue();
    if (m.get("backoff_ms") instanceof List<?> l) {
      List<Long> bo = new ArrayList<>();
      for (Object o : l) {
        bo.add(o instanceof Number n ? n.longValue() : 0L);
      }
      b.backoffMs = bo;
    }
    Long map = JsonMaps.numOrNull(m, "max_ack_pending");
    b.maxAckPending = map == null ? null : map.intValue();
    b.dlqStream = m.get("dlq_stream") instanceof String s ? s : null;
    b.ephemeral = m.get("ephemeral") instanceof Boolean x ? x : null;
    b.deadLetterExpired = m.get("dead_letter_expired") instanceof Boolean x ? x : null;
    if (m.get("filter_headers") instanceof Map<?, ?> fh) {
      fh.forEach((k, v) -> b.filterHeaders.put(String.valueOf(k), String.valueOf(v)));
    }
    if (m.get("header_match") instanceof String s) {
      b.headerMatch = HeaderMatch.fromWire(s);
    }
    b.singleActive = m.get("single_active") instanceof Boolean x ? x : null;
    Long pw = JsonMaps.numOrNull(m, "priority_window");
    b.priorityWindow = pw == null ? null : pw.intValue();
    if (b.name.isEmpty() || b.stream.isEmpty()) {
      // A malformed reply; keep what we can rather than failing the call.
      b.name = b.name.isEmpty() ? "?" : b.name;
      b.stream = b.stream.isEmpty() ? "?" : b.stream;
    }
    return b.build();
  }

  @Override
  public String toString() {
    return "ConsumerSpec" + toJson();
  }

  /** Builds a {@link ConsumerSpec}. */
  public static final class Builder {
    private String name;
    private String stream;
    private List<String> filterSubjects;
    private DeliverPolicy deliver;
    private AckPolicy ack;
    private Long ackWaitMs;
    private Integer maxDeliver;
    private List<Long> backoffMs;
    private Integer maxAckPending;
    private String dlqStream;
    private Boolean ephemeral;
    private Boolean deadLetterExpired;
    private final Map<String, String> filterHeaders = new LinkedHashMap<>();
    private HeaderMatch headerMatch;
    private Boolean singleActive;
    private Integer priorityWindow;

    private Builder(String name, String stream) {
      this.name = name;
      this.stream = stream;
    }

    /**
     * Subject filters such as {@code orders.placed} or {@code orders.eu.>} (empty = all).
     *
     * @param filters the filters
     * @return this builder
     */
    public Builder filterSubjects(String... filters) {
      return filterSubjects(List.of(filters));
    }

    /**
     * Subject filters (empty = all).
     *
     * @param filters the filters
     * @return this builder
     */
    public Builder filterSubjects(List<String> filters) {
      this.filterSubjects = new ArrayList<>(filters);
      return this;
    }

    /**
     * Where the consumer starts (default {@link DeliverPolicy#ALL}).
     *
     * @param deliver the policy
     * @return this builder
     */
    public Builder deliver(DeliverPolicy deliver) {
      this.deliver = Objects.requireNonNull(deliver);
      return this;
    }

    /**
     * Whether records must be acked (default {@link AckPolicy#EXPLICIT}).
     *
     * @param ack the policy
     * @return this builder
     */
    public Builder ack(AckPolicy ack) {
      this.ack = Objects.requireNonNull(ack);
      return this;
    }

    /**
     * Redeliver a record not acked within this time (server default 30 s).
     *
     * @param wait the ack wait
     * @return this builder
     */
    public Builder ackWait(Duration wait) {
      this.ackWaitMs = wait.toMillis();
      return this;
    }

    /**
     * Dead-letter after this many deliveries; 0 = never (server default 5).
     *
     * @param n the limit
     * @return this builder
     */
    public Builder maxDeliver(int n) {
      this.maxDeliver = n;
      return this;
    }

    /**
     * Redelivery delays after a nack or timeout, by delivery count (the last
     * repeats). Empty = redeliver immediately.
     *
     * @param delays the delays
     * @return this builder
     */
    public Builder backoff(Duration... delays) {
      return backoff(List.of(delays));
    }

    /**
     * Redelivery delays by delivery count (the last repeats).
     *
     * @param delays the delays
     * @return this builder
     */
    public Builder backoff(List<Duration> delays) {
      List<Long> ms = new ArrayList<>(delays.size());
      for (Duration d : delays) {
        ms.add(d.toMillis());
      }
      this.backoffMs = ms;
      return this;
    }

    /**
     * Pause delivery while this many records await an ack (server default 1000).
     *
     * @param n the limit
     * @return this builder
     */
    public Builder maxAckPending(int n) {
      this.maxAckPending = n;
      return this;
    }

    /**
     * The stream that receives dead letters (unset = they are dropped and counted).
     *
     * @param stream the DLQ stream
     * @return this builder
     */
    public Builder dlqStream(String stream) {
      this.dlqStream = stream;
      return this;
    }

    /**
     * Delete the consumer when the connection that created it closes. The
     * client re-creates its ephemeral consumers after a reconnect.
     *
     * @param ephemeral the setting
     * @return this builder
     */
    public Builder ephemeral(boolean ephemeral) {
      this.ephemeral = ephemeral;
      return this;
    }

    /**
     * Dead-letter records whose TTL ends before they are acked (cause
     * {@code expired}) instead of dropping them.
     *
     * @param v the setting
     * @return this builder
     */
    public Builder deadLetterExpired(boolean v) {
      this.deadLetterExpired = v;
      return this;
    }

    /**
     * Only records whose header {@code key} has exactly {@code value}; entries
     * combine by {@link #headerMatch(HeaderMatch)}.
     *
     * @param key the header name
     * @param value the required value
     * @return this builder
     */
    public Builder filterHeader(String key, String value) {
      filterHeaders.put(key, value);
      return this;
    }

    /**
     * Only records whose headers have these exact values.
     *
     * @param headers header names and required values
     * @return this builder
     */
    public Builder filterHeaders(Map<String, String> headers) {
      filterHeaders.putAll(headers);
      return this;
    }

    /**
     * How the header filter combines its entries (default {@link HeaderMatch#ALL}).
     *
     * @param match the mode
     * @return this builder
     */
    public Builder headerMatch(HeaderMatch match) {
      this.headerMatch = Objects.requireNonNull(match);
      return this;
    }

    /**
     * Deliver to one subscription at a time (the oldest connected); the next
     * takes over when it goes away. Pulls are refused.
     *
     * @param v the setting
     * @return this builder
     */
    public Builder singleActive(boolean v) {
      this.singleActive = v;
      return this;
    }

    /**
     * Look this many records ahead and deliver higher priorities first
     * (0 = strictly in order; server maximum 10,000).
     *
     * @param window the window
     * @return this builder
     */
    public Builder priorityWindow(int window) {
      this.priorityWindow = window;
      return this;
    }

    /**
     * Builds the spec.
     *
     * @return the spec
     * @throws IllegalArgumentException when the name or stream is empty
     */
    public ConsumerSpec build() {
      return new ConsumerSpec(this);
    }
  }
}
