package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WirePublishRecord;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pipelined, coalescing publisher, from {@link ExspeedClient#publisher(PublisherOptions)}.
 * Concurrent {@code publishAsync} calls are gathered into {@code PublishBatch}
 * requests (one per run of records for the same stream) and many batches can
 * be in flight at once. Records reach the stream in the order
 * {@code publishAsync} was called, and every call gets its own record's result.
 *
 * <pre>{@code
 * try (Publisher p = client.publisher()) {
 *   List<CompletableFuture<PublishResult>> results = new ArrayList<>();
 *   for (String e : events) {
 *     results.add(p.publishAsync("events", PublishRecord.of("events.raw", e)));
 *   }
 *   p.flush();
 * }
 * }</pre>
 */
public final class Publisher implements AutoCloseable {
  /** Keep each batch frame well below the 16 MiB frame limit. */
  private static final int MAX_BATCH_BYTES = 4 * 1024 * 1024;

  private record Queued(String stream, WirePublishRecord record, int size, CompletableFuture<PublishResult> future) {}

  private final Host host;
  private final ScheduledExecutorService sched;
  private final long batchWindowMs;
  private final int maxBatchRecords;
  private final Object lock = new Object();
  private final ArrayDeque<Queued> queue = new ArrayDeque<>();
  private final Semaphore permits;
  private final AtomicInteger inFlight = new AtomicInteger();
  private final List<CompletableFuture<Void>> idleWaiters = new ArrayList<>();
  private boolean scheduled;
  private volatile boolean closed;

  Publisher(Host host, ScheduledExecutorService sched, PublisherOptions opts) {
    this.host = host;
    this.sched = sched;
    this.batchWindowMs = opts.batchWindow().toMillis();
    this.maxBatchRecords = opts.maxBatchRecords();
    this.permits = new Semaphore(opts.maxInFlight(), true);
  }

  /**
   * Records accepted and not yet acknowledged.
   *
   * @return the count
   */
  public int pending() {
    return inFlight.get();
  }

  /**
   * Publishes one record and waits for its offset. Calls from one thread are
   * not batched together; use {@link #publishAsync(String, PublishRecord)} for that.
   *
   * @param stream the stream
   * @param record the record
   * @return the result
   */
  public PublishResult publish(String stream, PublishRecord record) {
    return Futures.await(publishInternal(stream, record));
  }

  /**
   * Queues one record. Blocks while {@code maxInFlight} records are awaiting
   * acknowledgement.
   *
   * @param stream the stream
   * @param record the record
   * @return completes with the record's offset once the server has it
   */
  public CompletableFuture<PublishResult> publishAsync(String stream, PublishRecord record) {
    return host.publicFuture(publishInternal(stream, record));
  }

  private CompletableFuture<PublishResult> publishInternal(String stream, PublishRecord record) {
    if (closed) {
      return Futures.failed(new ConnectionException("publisher is closed"));
    }
    WirePublishRecord wire = record.toWire();
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Futures.failed(new ExspeedException("interrupted while waiting for publish capacity", e));
    }
    inFlight.incrementAndGet();
    int size = 64 + wire.subject().length() * 3 + wire.value().length
        + (wire.key() == null ? 0 : wire.key().length) + (wire.msgId() == null ? 0 : wire.msgId().length() * 3);
    for (Header h : wire.headers()) {
      size += 4 + h.key().length() * 3 + h.value().length() * 3;
    }
    Queued q = new Queued(stream, wire, size, new CompletableFuture<>());
    synchronized (lock) {
      if (closed) {
        release();
        return Futures.failed(new ConnectionException("publisher is closed"));
      }
      queue.add(q);
      if (queue.size() >= maxBatchRecords) {
        flushQueueLocked();
      } else {
        schedule();
      }
    }
    return q.future();
  }

  /** Waits until every record accepted so far has been acknowledged (or failed). */
  public void flush() {
    Futures.await(flushInternal());
  }

  /**
   * Waits until every record accepted so far has been acknowledged (or failed).
   *
   * @return completes when nothing is pending
   */
  public CompletableFuture<Void> flushAsync() {
    return host.publicFuture(flushInternal());
  }

  private CompletableFuture<Void> flushInternal() {
    synchronized (lock) {
      if (!queue.isEmpty()) {
        flushQueueLocked();
      }
      if (inFlight.get() == 0) {
        return CompletableFuture.completedFuture(null);
      }
      CompletableFuture<Void> f = new CompletableFuture<>();
      idleWaiters.add(f);
      return f;
    }
  }

  /** Flushes, then rejects further publishes. */
  @Override
  public void close() {
    try {
      flush();
    } finally {
      closed = true;
    }
  }

  private void release() {
    permits.release();
    if (inFlight.decrementAndGet() == 0) {
      List<CompletableFuture<Void>> waiters;
      synchronized (lock) {
        waiters = new ArrayList<>(idleWaiters);
        idleWaiters.clear();
      }
      for (CompletableFuture<Void> w : waiters) {
        w.complete(null);
      }
    }
  }

  private void schedule() {
    if (scheduled) {
      return;
    }
    scheduled = true;
    Runnable run = () -> {
      synchronized (lock) {
        scheduled = false;
        flushQueueLocked();
      }
    };
    try {
      if (batchWindowMs == 0) {
        sched.execute(run);
      } else {
        sched.schedule(run, batchWindowMs, TimeUnit.MILLISECONDS);
      }
    } catch (RejectedExecutionException e) {
      scheduled = false;
      flushQueueLocked();
    }
  }

  /** Sends everything queued, as runs of the same stream, in arrival order (lock held). */
  private void flushQueueLocked() {
    while (!queue.isEmpty()) {
      String stream = queue.peek().stream();
      List<Queued> run = new ArrayList<>();
      int bytes = 0;
      while (!queue.isEmpty() && queue.peek().stream().equals(stream) && run.size() < maxBatchRecords
          && (run.isEmpty() || bytes + queue.peek().size() <= MAX_BATCH_BYTES)) {
        Queued q = queue.poll();
        bytes += q.size();
        run.add(q);
      }
      sendRun(stream, run);
    }
  }

  /** The request is queued for writing right away, so wire order matches arrival order. */
  private void sendRun(String stream, List<Queued> run) {
    boolean single = run.size() == 1;
    Request req;
    if (single) {
      req = new Request.Publish(stream, run.get(0).record());
    } else {
      List<WirePublishRecord> records = new ArrayList<>(run.size());
      for (Queued q : run) {
        records.add(q.record());
      }
      req = new Request.PublishBatch(stream, records);
    }
    CompletableFuture<Response> sent;
    try {
      sent = host.rawRequest(req, host.requestTimeoutMs());
    } catch (RuntimeException e) {
      sent = Futures.failed(e);
    }
    sent.whenComplete((resp, err) -> {
      if (err != null) {
        Throwable t = Futures.unwrap(err);
        for (Queued q : run) {
          q.future().completeExceptionally(t);
        }
      } else if (single && resp instanceof Response.PublishOk ok) {
        run.get(0).future().complete(new PublishResult(ok.offset(), ok.duplicate()));
      } else if (resp instanceof Response.PublishBatchOk b && b.results().size() == run.size()) {
        for (int i = 0; i < run.size(); i++) {
          Response.PublishOk r = b.results().get(i);
          run.get(i).future().complete(new PublishResult(r.offset(), r.duplicate()));
        }
      } else {
        ProtocolException e = new ProtocolException("unexpected reply to " + req.typeName() + ": " + resp.typeName());
        for (Queued q : run) {
          q.future().completeExceptionally(e);
        }
      }
      for (int i = 0; i < run.size(); i++) {
        release();
      }
    });
  }
}
