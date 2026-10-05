package io.exspeed.client;

/** The connection is closed, was lost, could not be opened, or is being re-established. */
public class ConnectionException extends ExspeedException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a connection exception.
   *
   * @param message what went wrong
   */
  public ConnectionException(String message) {
    super(message);
  }

  /**
   * Creates a connection exception with a cause.
   *
   * @param message what went wrong
   * @param cause the underlying I/O failure
   */
  public ConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
