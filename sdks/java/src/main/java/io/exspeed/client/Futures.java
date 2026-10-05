package io.exspeed.client;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Future helpers (internal). */
final class Futures {
  private Futures() {}

  /** Waits for a future and rethrows its failure as the client exception it carries. */
  static <T> T await(CompletableFuture<T> f) {
    try {
      return f.get();
    } catch (ExecutionException e) {
      throw rethrow(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ExspeedException("interrupted while waiting for the server", e);
    } catch (CancellationException e) {
      throw new ExspeedException("cancelled", e);
    }
  }

  static RuntimeException rethrow(Throwable t) {
    Throwable c = unwrap(t);
    if (c instanceof RuntimeException r) {
      return r;
    }
    if (c instanceof java.lang.Error e) {
      throw e;
    }
    return new ExspeedException(String.valueOf(c.getMessage()), c);
  }

  static Throwable unwrap(Throwable t) {
    while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
      t = t.getCause();
    }
    return t;
  }

  static <T> CompletableFuture<T> failed(Throwable t) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(t);
    return f;
  }
}
