package io.exspeed.client;

import java.time.Duration;

/**
 * Options of a KV put.
 *
 * @param ttl expire this value this long after the put, or {@code null}
 * @param expectedRevision only if the key is at this revision (0 = absent), else
 *     409; {@code null} = unconditional
 */
public record KvPutOptions(Duration ttl, Long expectedRevision) {
  /** No TTL, unconditional. */
  public static final KvPutOptions NONE = new KvPutOptions(null, null);

  /**
   * A put with a TTL.
   *
   * @param ttl the value's lifetime
   * @return the options
   */
  public static KvPutOptions ttl(Duration ttl) {
    return new KvPutOptions(ttl, null);
  }

  /**
   * A compare-and-set put.
   *
   * @param revision the revision the key must be at (0 = absent)
   * @return the options
   */
  public static KvPutOptions expectedRevision(long revision) {
    return new KvPutOptions(null, revision);
  }

  /**
   * A copy with a TTL.
   *
   * @param ttl the value's lifetime
   * @return the options
   */
  public KvPutOptions withTtl(Duration ttl) {
    return new KvPutOptions(ttl, expectedRevision);
  }

  /**
   * A copy with an expected revision.
   *
   * @param revision the revision the key must be at (0 = absent)
   * @return the options
   */
  public KvPutOptions withExpectedRevision(long revision) {
    return new KvPutOptions(ttl, revision);
  }
}
