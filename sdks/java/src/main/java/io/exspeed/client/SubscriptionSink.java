package io.exspeed.client;

import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WireRecord;
import java.util.List;

/**
 * Receives a subscription's pushes (internal). Called on the connection's
 * reader thread; implementations must not block.
 */
interface SubscriptionSink {
  /** Called when SubscribeOk arrives, before any push for it is routed. */
  void onSubscribed(Connection conn, int subId);

  default void onDeliver(List<WireRecord> records) {}

  default void onCoreMsg(Response.CoreMsg msg) {}

  void onEnded(int code, String message);
}
