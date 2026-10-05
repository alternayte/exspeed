package io.exspeed.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Options of {@link ExspeedClient#publishCore(String, byte[], CorePublishOptions)}.
 *
 * @param headers the message headers
 * @param replyTo ask receivers to answer on this subject; the publish then fails
 *     with 404 when nobody received it ({@code request} sets this for you)
 */
public record CorePublishOptions(List<Header> headers, String replyTo) {
  /** No headers, no reply subject. */
  public static final CorePublishOptions NONE = new CorePublishOptions(List.of(), null);

  /**
   * Validates the options.
   *
   * @param headers the message headers
   * @param replyTo the reply subject, or {@code null}
   */
  public CorePublishOptions {
    headers = headers == null ? List.of() : List.copyOf(headers);
  }

  /**
   * Options with these headers.
   *
   * @param headers header names and values
   * @return the options
   */
  public static CorePublishOptions headers(Map<String, String> headers) {
    List<Header> h = new ArrayList<>();
    headers.forEach((k, v) -> h.add(new Header(k, v)));
    return new CorePublishOptions(h, null);
  }

  /**
   * A copy with a reply subject.
   *
   * @param replyTo the reply subject
   * @return the options
   */
  public CorePublishOptions withReplyTo(String replyTo) {
    return new CorePublishOptions(headers, replyTo);
  }
}
