package io.exspeed.client;

import java.time.Duration;

/**
 * How the client reconnects after the connection drops: exponential backoff
 * from {@code initialDelay}, doubling up to {@code maxDelay}, with jitter.
 *
 * @param maxAttempts give up after this many failed attempts in a row
 * @param initialDelay delay before the first attempt
 * @param maxDelay upper bound for the delay between attempts
 */
public record ReconnectOptions(int maxAttempts, Duration initialDelay, Duration maxDelay) {
  /** Unlimited attempts, 100 ms doubling to 5 s. */
  public static final ReconnectOptions DEFAULT =
      new ReconnectOptions(Integer.MAX_VALUE, Duration.ofMillis(100), Duration.ofSeconds(5));

  /**
   * Validates the options.
   *
   * @param maxAttempts give up after this many failed attempts in a row (at least 1)
   * @param initialDelay delay before the first attempt
   * @param maxDelay upper bound for the delay between attempts
   */
  public ReconnectOptions {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be at least 1");
    }
    if (initialDelay == null || initialDelay.isNegative() || maxDelay == null || maxDelay.isNegative()) {
      throw new IllegalArgumentException("delays must be non-negative durations");
    }
  }

  /**
   * A copy with another attempt limit.
   *
   * @param n give up after this many failed attempts in a row
   * @return the options
   */
  public ReconnectOptions withMaxAttempts(int n) {
    return new ReconnectOptions(n, initialDelay, maxDelay);
  }

  /**
   * A copy with other delays.
   *
   * @param initial delay before the first attempt
   * @param max upper bound for the delay
   * @return the options
   */
  public ReconnectOptions withDelays(Duration initial, Duration max) {
    return new ReconnectOptions(maxAttempts, initial, max);
  }
}
