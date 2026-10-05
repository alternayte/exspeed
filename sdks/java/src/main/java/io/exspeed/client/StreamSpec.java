package io.exspeed.client;

import io.exspeed.client.protocol.WireStreamSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stream settings, for {@link ExspeedClient#createStream(StreamSpec)} and
 * {@link ExspeedClient#updateStream(StreamSpec)}. Unset (or 0) numeric
 * settings mean "server default".
 *
 * <pre>{@code
 * StreamSpec spec = StreamSpec.builder("jobs")
 *     .maxMsgs(100_000)
 *     .discard(DiscardPolicy.NEW)
 *     .allowMsgTtl(true)
 *     .retention(RetentionPolicy.WORK_QUEUE)
 *     .build();
 * }</pre>
 */
public final class StreamSpec {
  private final String name;
  private final long maxAgeSecs;
  private final long maxBytes;
  private final long dedupWindowSecs;
  private final long dedupMaxEntries;
  private final boolean compaction;
  private final long maxMsgs;
  private final DiscardPolicy discard;
  private final long maxMsgsPerSubject;
  private final boolean allowMsgTtl;
  private final long msgTtlMs;
  private final boolean allowDelayed;
  private final RetentionPolicy retention;
  private final List<String> captureSubjects;

  private StreamSpec(Builder b) {
    if (b.name == null || b.name.isEmpty()) {
      throw new IllegalArgumentException("stream name is required");
    }
    name = b.name;
    maxAgeSecs = b.maxAgeSecs;
    maxBytes = b.maxBytes;
    dedupWindowSecs = b.dedupWindowSecs;
    dedupMaxEntries = b.dedupMaxEntries;
    compaction = b.compaction;
    maxMsgs = b.maxMsgs;
    discard = b.discard;
    maxMsgsPerSubject = b.maxMsgsPerSubject;
    allowMsgTtl = b.allowMsgTtl;
    msgTtlMs = b.msgTtlMs;
    allowDelayed = b.allowDelayed;
    retention = b.retention;
    captureSubjects = List.copyOf(b.captureSubjects);
  }

  /**
   * A stream with server defaults for every setting.
   *
   * @param name the stream name
   * @return the spec
   */
  public static StreamSpec of(String name) {
    return builder(name).build();
  }

  /**
   * Starts a spec.
   *
   * @param name the stream name
   * @return a builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * The stream name.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  /**
   * Retention by age, in seconds (0 = server default).
   *
   * @return the setting
   */
  public long maxAgeSecs() {
    return maxAgeSecs;
  }

  /**
   * Retention by size, in bytes (0 = server default).
   *
   * @return the setting
   */
  public long maxBytes() {
    return maxBytes;
  }

  /**
   * How long msg ids are remembered, in seconds (0 = server default).
   *
   * @return the setting
   */
  public long dedupWindowSecs() {
    return dedupWindowSecs;
  }

  /**
   * How many msg ids are remembered (0 = server default).
   *
   * @return the setting
   */
  public long dedupMaxEntries() {
    return dedupMaxEntries;
  }

  /**
   * Whether only the latest record per key is kept.
   *
   * @return the setting
   */
  public boolean compaction() {
    return compaction;
  }

  /**
   * Most records the stream holds (0 = no limit).
   *
   * @return the setting
   */
  public long maxMsgs() {
    return maxMsgs;
  }

  /**
   * What happens at {@link #maxMsgs()}.
   *
   * @return the setting
   */
  public DiscardPolicy discard() {
    return discard;
  }

  /**
   * Most records kept per subject (0 = no limit).
   *
   * @return the setting
   */
  public long maxMsgsPerSubject() {
    return maxMsgsPerSubject;
  }

  /**
   * Whether records may carry their own TTL.
   *
   * @return the setting
   */
  public boolean allowMsgTtl() {
    return allowMsgTtl;
  }

  /**
   * Default lifetime of every record, in ms (0 = none).
   *
   * @return the setting
   */
  public long msgTtlMs() {
    return msgTtlMs;
  }

  /**
   * Whether records may ask for delayed delivery.
   *
   * @return the setting
   */
  public boolean allowDelayed() {
    return allowDelayed;
  }

  /**
   * Whether acknowledging a record removes it.
   *
   * @return the setting
   */
  public RetentionPolicy retention() {
    return retention;
  }

  /**
   * Subject filters whose core messages the stream also stores.
   *
   * @return the filters (empty = none)
   */
  public List<String> captureSubjects() {
    return captureSubjects;
  }

  /** The limits trailer JSON (serde field order), or null when every limit is at its default. */
  String limitsJson() {
    boolean isDefault = maxMsgs == 0 && discard == DiscardPolicy.OLD && maxMsgsPerSubject == 0 && !allowMsgTtl
        && msgTtlMs == 0 && !allowDelayed && retention == RetentionPolicy.LIMITS && captureSubjects.isEmpty();
    if (isDefault) {
      return null;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("max_msgs", maxMsgs);
    m.put("discard", discard.wireName());
    m.put("max_msgs_per_subject", maxMsgsPerSubject);
    m.put("allow_msg_ttl", allowMsgTtl);
    m.put("msg_ttl_ms", msgTtlMs);
    m.put("allow_delayed", allowDelayed);
    m.put("retention", retention.wireName());
    if (!captureSubjects.isEmpty()) {
      m.put("capture_subjects", captureSubjects);
    }
    return Json.stringify(m);
  }

  WireStreamSpec toWire() {
    return new WireStreamSpec(name, maxAgeSecs, maxBytes, dedupWindowSecs, dedupMaxEntries, compaction, limitsJson());
  }

  /** Builds a {@link StreamSpec}. */
  public static final class Builder {
    private final String name;
    private long maxAgeSecs;
    private long maxBytes;
    private long dedupWindowSecs;
    private long dedupMaxEntries;
    private boolean compaction;
    private long maxMsgs;
    private DiscardPolicy discard = DiscardPolicy.OLD;
    private long maxMsgsPerSubject;
    private boolean allowMsgTtl;
    private long msgTtlMs;
    private boolean allowDelayed;
    private RetentionPolicy retention = RetentionPolicy.LIMITS;
    private final List<String> captureSubjects = new ArrayList<>();

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Retention by age.
     *
     * @param v seconds (0 = server default)
     * @return this builder
     */
    public Builder maxAgeSecs(long v) {
      maxAgeSecs = v;
      return this;
    }

    /**
     * Retention by size.
     *
     * @param v bytes (0 = server default)
     * @return this builder
     */
    public Builder maxBytes(long v) {
      maxBytes = v;
      return this;
    }

    /**
     * How long msg ids are remembered.
     *
     * @param v seconds (0 = server default)
     * @return this builder
     */
    public Builder dedupWindowSecs(long v) {
      dedupWindowSecs = v;
      return this;
    }

    /**
     * How many msg ids are remembered.
     *
     * @param v entries (0 = server default)
     * @return this builder
     */
    public Builder dedupMaxEntries(long v) {
      dedupMaxEntries = v;
      return this;
    }

    /**
     * Keep only the latest record per key.
     *
     * @param v the setting
     * @return this builder
     */
    public Builder compaction(boolean v) {
      compaction = v;
      return this;
    }

    /**
     * Most records the stream holds; what happens at the limit is {@link #discard(DiscardPolicy)}.
     *
     * @param v records (0 = no limit)
     * @return this builder
     */
    public Builder maxMsgs(long v) {
      maxMsgs = v;
      return this;
    }

    /**
     * At {@code maxMsgs}: drop the oldest records ({@code OLD}, default) or reject new ones ({@code NEW}).
     *
     * @param v the policy
     * @return this builder
     */
    public Builder discard(DiscardPolicy v) {
      discard = Objects.requireNonNull(v);
      return this;
    }

    /**
     * Most records kept per subject (older ones disappear).
     *
     * @param v records (0 = no limit)
     * @return this builder
     */
    public Builder maxMsgsPerSubject(long v) {
      maxMsgsPerSubject = v;
      return this;
    }

    /**
     * Accept the per-record TTL option (header {@code exspeed-ttl}).
     *
     * @param v the setting
     * @return this builder
     */
    public Builder allowMsgTtl(boolean v) {
      allowMsgTtl = v;
      return this;
    }

    /**
     * Default lifetime of every record.
     *
     * @param v ms (0 = none)
     * @return this builder
     */
    public Builder msgTtlMs(long v) {
      msgTtlMs = v;
      return this;
    }

    /**
     * Accept the delay / deliver-at options (delayed delivery to consumers).
     *
     * @param v the setting
     * @return this builder
     */
    public Builder allowDelayed(boolean v) {
      allowDelayed = v;
      return this;
    }

    /**
     * Whether acknowledgements remove records.
     *
     * @param v the policy
     * @return this builder
     */
    public Builder retention(RetentionPolicy v) {
      retention = Objects.requireNonNull(v);
      return this;
    }

    /**
     * Also store core messages published to subjects matching these filters
     * (such as {@code orders.>}). No two streams may capture overlapping subjects.
     *
     * @param filters the subject filters
     * @return this builder
     */
    public Builder captureSubjects(String... filters) {
      return captureSubjects(List.of(filters));
    }

    /**
     * Also store core messages published to subjects matching these filters.
     *
     * @param filters the subject filters (empty = none)
     * @return this builder
     */
    public Builder captureSubjects(List<String> filters) {
      captureSubjects.clear();
      captureSubjects.addAll(filters);
      return this;
    }

    /**
     * Builds the spec.
     *
     * @return the spec
     * @throws IllegalArgumentException when the name is empty
     */
    public StreamSpec build() {
      return new StreamSpec(this);
    }
  }
}
