package io.exspeed.client;

import java.util.concurrent.CompletableFuture;

/** What a message needs from the client to settle itself (internal). */
interface MessageSettler {
  void ackNowait(String consumer, long offset);

  CompletableFuture<Void> nackInternal(String consumer, long offset, long delayMs);

  CompletableFuture<Void> termInternal(String consumer, long offset, String reason);

  CompletableFuture<Void> inProgressInternal(String consumer, long[] offsets);

  <T> CompletableFuture<T> publicFuture(CompletableFuture<T> f);
}
