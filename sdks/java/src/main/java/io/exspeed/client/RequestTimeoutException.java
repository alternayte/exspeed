package io.exspeed.client;

/**
 * No response arrived within the request timeout (for a pull or a long-poll
 * read: the timeout plus the request's own wait), or a core request got no
 * reply in time.
 */
public class RequestTimeoutException extends ExspeedException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a timeout exception.
   *
   * @param message what timed out
   */
  public RequestTimeoutException(String message) {
    super(message);
  }
}
