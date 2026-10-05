package io.exspeed.client;

import java.util.Objects;

/**
 * One record or message header. Headers are kept in wire order and a key can
 * repeat.
 *
 * @param key the header name
 * @param value the header value
 */
public record Header(String key, String value) {
  /**
   * Validates the header.
   *
   * @param key the header name
   * @param value the header value
   */
  public Header {
    Objects.requireNonNull(key, "header key");
    Objects.requireNonNull(value, "header value");
  }
}
