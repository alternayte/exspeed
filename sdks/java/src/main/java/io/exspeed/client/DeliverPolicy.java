package io.exspeed.client;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Where a new consumer starts reading. */
public final class DeliverPolicy {
  private enum Kind { ALL, NEW, FROM_OFFSET, FROM_TIME }

  /** From the first retained record (the default). */
  public static final DeliverPolicy ALL = new DeliverPolicy(Kind.ALL, 0);
  /** Only records appended after the consumer is created. */
  public static final DeliverPolicy NEW = new DeliverPolicy(Kind.NEW, 0);

  private final Kind kind;
  private final long value;

  private DeliverPolicy(Kind kind, long value) {
    this.kind = kind;
    this.value = value;
  }

  /**
   * From a specific offset.
   *
   * @param offset the first offset
   * @return the policy
   */
  public static DeliverPolicy fromOffset(long offset) {
    return new DeliverPolicy(Kind.FROM_OFFSET, offset);
  }

  /**
   * From the first record at or after this time.
   *
   * @param time the start time
   * @return the policy
   */
  public static DeliverPolicy fromTime(Instant time) {
    return fromTimeMs(time.toEpochMilli());
  }

  /**
   * From the first record at or after this time.
   *
   * @param epochMs ms since the Unix epoch
   * @return the policy
   */
  public static DeliverPolicy fromTimeMs(long epochMs) {
    return new DeliverPolicy(Kind.FROM_TIME, epochMs);
  }

  /**
   * The offset of a {@link #fromOffset(long)} policy.
   *
   * @return the offset, or {@code null} for other policies
   */
  public Long offset() {
    return kind == Kind.FROM_OFFSET ? value : null;
  }

  /**
   * The time of a {@link #fromTimeMs(long)} policy.
   *
   * @return ms since the epoch, or {@code null} for other policies
   */
  public Long timeMs() {
    return kind == Kind.FROM_TIME ? value : null;
  }

  Object toJson() {
    switch (kind) {
      case ALL:
        return "all";
      case NEW:
        return "new";
      case FROM_OFFSET: {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("from_offset", value);
        return m;
      }
      default: {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("from_time", value);
        return m;
      }
    }
  }

  static DeliverPolicy fromJson(Object v) {
    if ("new".equals(v)) {
      return NEW;
    }
    if (v instanceof Map<?, ?>) {
      Map<String, Object> m = JsonMaps.asMap(v);
      if (m.containsKey("from_offset")) {
        return fromOffset(JsonMaps.num(m, "from_offset", 0));
      }
      if (m.containsKey("from_time")) {
        return fromTimeMs(JsonMaps.num(m, "from_time", 0));
      }
    }
    return ALL;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DeliverPolicy d && d.kind == kind && d.value == value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, value);
  }

  @Override
  public String toString() {
    return Json.stringify(toJson());
  }
}
