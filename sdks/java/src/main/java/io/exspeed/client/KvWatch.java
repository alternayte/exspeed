package io.exspeed.client;

import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WireRecord;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Changes to a bucket's keys, from {@link KvBucket#watch(String)}: first the
 * current value of every matching key (deleted keys left out), ordered by
 * revision, then every change as it happens, deletes included.
 *
 * <p>It reads the bucket's stream with stateless long-poll reads, so it holds
 * no server-side state. A failed read (for example {@link ConnectionException}
 * when the connection drops) is thrown from {@link #next()}.
 */
public final class KvWatch implements Iterable<KvEntry>, AutoCloseable {
  static final long WAIT_MS = 10_000;
  private static final int BATCH = 1000;

  private final Host host;
  private final String stream;
  private final String filter;
  private final Object lock = new Object();
  private final ArrayDeque<KvEntry> pending = new ArrayDeque<>();
  private long from;
  private boolean snapshotDone;
  private CompletableFuture<Void> fetching;
  private volatile boolean stopped;

  KvWatch(Host host, String stream, String filter) {
    this.host = host;
    this.stream = stream;
    this.filter = filter;
  }

  /**
   * Whether {@link #close()} was called.
   *
   * @return true after close
   */
  public boolean isClosed() {
    return stopped;
  }

  /**
   * The next entry, waiting for changes as long as it takes.
   *
   * @return the entry, or {@code null} after {@link #close()}
   */
  public KvEntry next() {
    return next(null);
  }

  /**
   * The next entry, waiting up to {@code timeout}. A read still in flight when
   * the timeout passes keeps running, and what it brings is kept.
   *
   * @param timeout how long to wait; {@code null} = forever
   * @return the entry, or {@code null} on timeout or after {@link #close()}
   */
  public KvEntry next(Duration timeout) {
    long deadline = timeout == null ? Long.MAX_VALUE : System.nanoTime() + timeout.toNanos();
    while (true) {
      CompletableFuture<Void> f;
      synchronized (lock) {
        if (stopped) {
          return null;
        }
        if (!pending.isEmpty()) {
          return pending.poll();
        }
        f = fill();
      }
      if (timeout == null) {
        Futures.await(f);
        continue;
      }
      long left = deadline - System.nanoTime();
      if (left <= 0) {
        return null;
      }
      try {
        f.get(left, TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        return null;
      } catch (ExecutionException e) {
        throw Futures.rethrow(e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ExspeedException("interrupted while watching", e);
      }
    }
  }

  /** Stops watching: {@link #next()} returns {@code null} from now on. */
  @Override
  public void close() {
    synchronized (lock) {
      stopped = true;
      pending.clear();
    }
  }

  /**
   * A blocking iterator over the entries; it ends after {@link #close()}.
   *
   * @return the iterator
   */
  @Override
  public Iterator<KvEntry> iterator() {
    return new Iterator<>() {
      private KvEntry peeked;
      private boolean done;

      @Override
      public boolean hasNext() {
        if (peeked == null && !done) {
          peeked = KvWatch.this.next();
          done = peeked == null;
        }
        return peeked != null;
      }

      @Override
      public KvEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        KvEntry e = peeked;
        peeked = null;
        return e;
      }
    };
  }

  /** One read at a time (lock held). */
  private CompletableFuture<Void> fill() {
    CompletableFuture<Void> current = fetching;
    if (current != null) {
      return current;
    }
    CompletableFuture<Void> f = snapshotDone ? follow() : snapshotStep(new HashMap<>(), new long[] {-1});
    fetching = f;
    f.whenComplete((v, e) -> {
      synchronized (lock) {
        if (fetching == f) {
          fetching = null;
        }
      }
    });
    return f;
  }

  /**
   * Reads up to the high watermark seen by the first read, keeping the last
   * record per key, then queues the live keys sorted by revision.
   */
  private CompletableFuture<Void> snapshotStep(Map<String, KvEntry> latest, long[] end) {
    long start;
    synchronized (lock) {
      start = from;
    }
    return host.readWire(stream, start, BATCH, 0, filter).thenCompose(r -> {
      if (end[0] < 0) {
        end[0] = r.highWatermark();
      }
      for (WireRecord rec : r.records()) {
        KvEntry e = new KvEntry(rec);
        latest.put(e.key(), e);
      }
      boolean progressed = r.nextOffset() > start;
      boolean finished;
      synchronized (lock) {
        from = Math.max(from, r.nextOffset());
        finished = from >= end[0] || !progressed;
        if (finished && !stopped) {
          List<KvEntry> live = new ArrayList<>();
          for (KvEntry e : latest.values()) {
            if (e.op() == KvOp.PUT) {
              live.add(e);
            }
          }
          live.sort(Comparator.comparingLong(KvEntry::revision));
          pending.addAll(live);
          snapshotDone = true;
        }
      }
      return finished ? CompletableFuture.completedFuture(null) : snapshotStep(latest, end);
    });
  }

  private CompletableFuture<Void> follow() {
    long start;
    synchronized (lock) {
      start = from;
    }
    return host.readWire(stream, start, BATCH, WAIT_MS, filter).thenAccept((Response.ReadResult r) -> {
      synchronized (lock) {
        from = Math.max(from, r.nextOffset());
        if (stopped) {
          return;
        }
        for (WireRecord rec : r.records()) {
          pending.add(new KvEntry(rec));
        }
      }
    });
  }
}
