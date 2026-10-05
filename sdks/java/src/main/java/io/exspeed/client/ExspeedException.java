package io.exspeed.client;

/**
 * Base class of every exception this client throws.
 *
 * <p>All client exceptions are unchecked. Asynchronous methods complete their
 * {@link java.util.concurrent.CompletableFuture} exceptionally with one of the
 * subclasses; the blocking variants throw it directly.
 */
public class ExspeedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception with a message.
   *
   * @param message what went wrong
   */
  public ExspeedException(String message) {
    super(message);
  }

  /**
   * Creates an exception with a message and a cause.
   *
   * @param message what went wrong
   * @param cause the underlying failure
   */
  public ExspeedException(String message, Throwable cause) {
    super(message, cause);
  }
}
