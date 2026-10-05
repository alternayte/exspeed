package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** What subscriptions, messages and KV buckets need from their client (internal). */
interface Host extends MessageSettler {
  void forget(Subscription sub);

  void forgetCore(CoreSubscription sub);

  long requestTimeoutMs();

  CompletableFuture<Void> publishCoreInternal(String subject, String replyTo, List<Header> headers, byte[] value);

  CompletableFuture<Response> rawRequest(Request req, long timeoutMs);

  CompletableFuture<Response.ReadResult> readWire(String stream, long from, int maxRecords, long waitMs, String filter);
}
