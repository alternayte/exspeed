package io.exspeed.client;

/** Whether delivered records must be acknowledged. */
public enum AckPolicy {
  /** Each record must be acked; unacked records are redelivered after the ack wait (the default). */
  EXPLICIT("explicit"),
  /** Records count as acked when delivered (at-most-once). */
  NONE("none");

  private final String wire;

  AckPolicy(String wire) {
    this.wire = wire;
  }

  /**
   * The wire name.
   *
   * @return {@code "explicit"} or {@code "none"}
   */
  public String wireName() {
    return wire;
  }

  static AckPolicy fromWire(String s) {
    return "none".equals(s) ? NONE : EXPLICIT;
  }
}
