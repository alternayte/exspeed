package io.exspeed.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Options of {@link ExspeedClient#read(String, ReadOptions)}.
 *
 * <pre>{@code
 * ReadOptions opts = ReadOptions.builder().from(100).maxRecords(500).filter("orders.>").waitTime(Duration.ofSeconds(5)).build();
 * }</pre>
 */
public final class ReadOptions {
  /** Read from offset 0, up to 100 records, no wait, no filter. */
  public static final ReadOptions DEFAULT = builder().build();

  private final long from;
  private final int maxRecords;
  private final int maxBytes;
  private final Duration wait;
  private final String filter;

  private ReadOptions(Builder b) {
    from = b.from;
    maxRecords = b.maxRecords;
    maxBytes = b.maxBytes;
    wait = b.wait;
    filter = b.filter;
  }

  /**
   * Starts building options.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Read from an offset with the other defaults.
   *
   * @param offset the first offset
   * @return the options
   */
  public static ReadOptions from(long offset) {
    return builder().from(offset).build();
  }

  /**
   * The first offset to read.
   *
   * @return the offset
   */
  public long from() {
    return from;
  }

  /**
   * At most this many records.
   *
   * @return the limit
   */
  public int maxRecords() {
    return maxRecords;
  }

  /**
   * Byte budget per response (0 = server default, 1 MiB).
   *
   * @return the budget
   */
  public int maxBytes() {
    return maxBytes;
  }

  /**
   * Long-poll wait when caught up.
   *
   * @return the wait
   */
  public Duration waitTime() {
    return wait;
  }

  /**
   * Subject filter ("" = all).
   *
   * @return the filter
   */
  public String filter() {
    return filter;
  }

  /** Builds {@link ReadOptions}. */
  public static final class Builder {
    private long from;
    private int maxRecords = 100;
    private int maxBytes;
    private Duration wait = Duration.ZERO;
    private String filter = "";

    private Builder() {}

    /**
     * The first offset to read (default 0).
     *
     * @param offset the offset
     * @return this builder
     */
    public Builder from(long offset) {
      this.from = offset;
      return this;
    }

    /**
     * At most this many records (default 100; the server caps it at 10,000).
     *
     * @param n the limit
     * @return this builder
     */
    public Builder maxRecords(int n) {
      this.maxRecords = n;
      return this;
    }

    /**
     * Byte budget per response (0 = server default, 1 MiB).
     *
     * @param n the budget
     * @return this builder
     */
    public Builder maxBytes(int n) {
      this.maxBytes = n;
      return this;
    }

    /**
     * Long-poll: when caught up, wait up to this long for new records (default zero).
     *
     * @param wait the wait
     * @return this builder
     */
    public Builder waitTime(Duration wait) {
      if (wait.isNegative()) {
        throw new IllegalArgumentException("wait must not be negative");
      }
      this.wait = wait;
      return this;
    }

    /**
     * NATS-style subject filter ({@code orders.*}, {@code orders.>}); "" = all.
     *
     * @param filter the filter
     * @return this builder
     */
    public Builder filter(String filter) {
      this.filter = Objects.requireNonNull(filter);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public ReadOptions build() {
      return new ReadOptions(this);
    }
  }
}
