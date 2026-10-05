package io.exspeed.client;

import java.time.Duration;

/**
 * Settings of a new KV bucket.
 *
 * @param history values kept per key, 1 to 64 (0 = 1)
 * @param ttl keys expire this long after their last put; {@link Duration#ZERO} = never
 * @param maxBytes size limit in bytes (0 = the server's)
 */
public record KvBucketOptions(int history, Duration ttl, long maxBytes) {
  /** History 1, no TTL, the server's size limit. */
  public static final KvBucketOptions DEFAULT = new KvBucketOptions(0, Duration.ZERO, 0);

  /**
   * Validates the options.
   *
   * @param history values kept per key
   * @param ttl key lifetime after the last put
   * @param maxBytes size limit
   */
  public KvBucketOptions {
    if (ttl == null || ttl.isNegative()) {
      throw new IllegalArgumentException("ttl must be a non-negative duration");
    }
  }

  /**
   * Options keeping {@code history} values per key.
   *
   * @param history values kept per key
   * @return the options
   */
  public static KvBucketOptions history(int history) {
    return new KvBucketOptions(history, Duration.ZERO, 0);
  }

  /**
   * A copy with a key TTL.
   *
   * @param ttl key lifetime after the last put
   * @return the options
   */
  public KvBucketOptions withTtl(Duration ttl) {
    return new KvBucketOptions(history, ttl, maxBytes);
  }

  /**
   * A copy with a size limit.
   *
   * @param maxBytes the limit in bytes
   * @return the options
   */
  public KvBucketOptions withMaxBytes(long maxBytes) {
    return new KvBucketOptions(history, ttl, maxBytes);
  }
}
