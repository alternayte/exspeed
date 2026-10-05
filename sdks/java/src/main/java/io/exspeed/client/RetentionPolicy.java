package io.exspeed.client;

/** Whether acknowledging a record removes it from the stream. */
public enum RetentionPolicy {
  /** Records stay until a limit removes them (the default). */
  LIMITS("limits"),
  /** At most one consumer (per subject set); a record is removed once acked. */
  WORK_QUEUE("work_queue"),
  /** A record is removed once every consumer acked it. */
  INTEREST("interest");

  private final String wire;

  RetentionPolicy(String wire) {
    this.wire = wire;
  }

  /**
   * The wire name.
   *
   * @return {@code "limits"}, {@code "work_queue"} or {@code "interest"}
   */
  public String wireName() {
    return wire;
  }

  static RetentionPolicy fromWire(String s) {
    if ("work_queue".equals(s)) {
      return WORK_QUEUE;
    }
    return "interest".equals(s) ? INTEREST : LIMITS;
  }
}
