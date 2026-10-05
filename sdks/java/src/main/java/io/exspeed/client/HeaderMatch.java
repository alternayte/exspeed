package io.exspeed.client;

/** How a consumer's header filter combines its entries. */
public enum HeaderMatch {
  /** Every header must match (the default). */
  ALL("all"),
  /** At least one must. */
  ANY("any");

  private final String wire;

  HeaderMatch(String wire) {
    this.wire = wire;
  }

  /**
   * The wire name.
   *
   * @return {@code "all"} or {@code "any"}
   */
  public String wireName() {
    return wire;
  }

  static HeaderMatch fromWire(String s) {
    return "any".equals(s) ? ANY : ALL;
  }
}
