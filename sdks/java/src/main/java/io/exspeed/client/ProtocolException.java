package io.exspeed.client;

/**
 * The peer sent bytes this client cannot decode, or a reply of the wrong
 * type; or a value does not fit its wire encoding.
 */
public class ProtocolException extends ExspeedException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a protocol exception.
   *
   * @param message what was malformed
   */
  public ProtocolException(String message) {
    super(message);
  }
}
