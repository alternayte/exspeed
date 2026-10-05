package io.exspeed.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Options of {@link ExspeedClient#request(String, byte[], RequestOptions)}.
 *
 * @param timeout how long to wait for the first response; {@code null} = the client's request timeout
 * @param headers the request's headers
 */
public record RequestOptions(Duration timeout, List<Header> headers) {
  /** The client's request timeout, no headers. */
  public static final RequestOptions DEFAULT = new RequestOptions(null, List.of());

  /**
   * Validates the options.
   *
   * @param timeout how long to wait, or {@code null}
   * @param headers the request's headers
   */
  public RequestOptions {
    headers = headers == null ? List.of() : List.copyOf(headers);
    if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  /**
   * Options with a timeout.
   *
   * @param timeout how long to wait for the first response
   * @return the options
   */
  public static RequestOptions timeout(Duration timeout) {
    return new RequestOptions(timeout, List.of());
  }

  /**
   * A copy with these headers.
   *
   * @param headers header names and values
   * @return the options
   */
  public RequestOptions withHeaders(Map<String, String> headers) {
    List<Header> h = new ArrayList<>();
    headers.forEach((k, v) -> h.add(new Header(k, v)));
    return new RequestOptions(timeout, h);
  }
}
