package io.exspeed.client;

/**
 * Connection lifecycle events of an {@link ExspeedClient}. Every method has an
 * empty default; override the ones you need. Events are delivered on the
 * client's callback executor, so a listener may call the client.
 */
public interface ClientListener {
  /**
   * The connection dropped; the client is reconnecting.
   *
   * @param cause why it dropped
   */
  default void onDisconnect(Throwable cause) {}

  /**
   * The client reconnected, and its subscriptions (consumer and core) are restored.
   *
   * @param info the new connection's handshake info
   */
  default void onReconnect(ServerInfo info) {}

  /**
   * The client closed for good: by {@link ExspeedClient#close()}, because the
   * connection dropped with reconnection off, or because reconnecting gave up.
   *
   * @param cause why, or {@code null} after {@code close()}
   */
  default void onClose(Throwable cause) {}

  /**
   * A fire-and-forget request (ack, credit) failed on the server.
   *
   * @param error the failure
   */
  default void onError(ExspeedException error) {}
}
