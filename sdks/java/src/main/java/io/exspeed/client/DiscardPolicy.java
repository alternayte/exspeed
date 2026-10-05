package io.exspeed.client;

/** What a stream does at {@code maxMsgs}. */
public enum DiscardPolicy {
  /** Drop the oldest records to make room (the default). */
  OLD("old"),
  /** Reject new records with 429 until there is room. */
  NEW("new");

  private final String wire;

  DiscardPolicy(String wire) {
    this.wire = wire;
  }

  /**
   * The wire name.
   *
   * @return {@code "old"} or {@code "new"}
   */
  public String wireName() {
    return wire;
  }

  static DiscardPolicy fromWire(String s) {
    return "new".equals(s) ? NEW : OLD;
  }
}
