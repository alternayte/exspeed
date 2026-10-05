package io.exspeed.client;

import java.time.Duration;

/**
 * Options of {@link ExspeedClient#pull(String, PullOptions)}.
 *
 * @param maxMessages at most this many records (default 100)
 * @param maxBytes byte budget; 0 = server default
 * @param expires wait up to this long for at least one record (default 5 s)
 */
public record PullOptions(int maxMessages, int maxBytes, Duration expires) {
  /** The defaults: 100 messages, the server's byte budget, 5 s. */
  public static final PullOptions DEFAULT = new PullOptions(100, 0, Duration.ofSeconds(5));

  /**
   * Validates the options.
   *
   * @param maxMessages at most this many records
   * @param maxBytes byte budget; 0 = server default
   * @param expires how long to wait
   */
  public PullOptions {
    if (maxMessages < 1) {
      throw new IllegalArgumentException("maxMessages must be at least 1");
    }
    if (expires == null || expires.isNegative()) {
      throw new IllegalArgumentException("expires must be a non-negative duration");
    }
  }

  /**
   * Pull up to {@code maxMessages}, waiting up to {@code expires}.
   *
   * @param maxMessages at most this many records
   * @param expires how long to wait
   * @return the options
   */
  public static PullOptions of(int maxMessages, Duration expires) {
    return new PullOptions(maxMessages, 0, expires);
  }
}
