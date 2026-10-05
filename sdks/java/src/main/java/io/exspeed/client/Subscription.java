package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.WireRecord;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A push subscription to a consumer, from {@link ExspeedClient#subscribe(String, int)}.
 *
 * <p>Take messages with {@link #next()} / {@link #next(Duration)}, iterate it
 * (the iterator blocks and ends with the subscription), or hand it a callback
 * with {@link #listen(Consumer)}. It ends when the server ends it (see
 * {@link #endReason()}), on {@link #unsubscribe()} / {@link #close()}, or when
 * the client closes. Use it in try-with-resources to unsubscribe on exit.
 *
 * <p>Credit flow: the server pushes at most {@link #window()} records ahead of
 * your code. Each message you take returns one credit (sent in batches of half
 * the window), so a slow consumer slows delivery instead of buffering without
 * bound.
 */
public final class Subscription implements Iterable<Message>, AutoCloseable {
  private final Host host;
  private final String consumer;
  private final int window;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private final ArrayDeque<WireRecord> buffer = new ArrayDeque<>();
  private final AtomicBoolean listening = new AtomicBoolean();
  private Connection conn;
  private int subId;
  private int consumed;
  private EndReason endReason;
  final SubscriptionSink sink = new Sink();

  Subscription(Host host, String consumer, int window) {
    this.host = host;
    this.consumer = consumer;
    this.window = window;
  }

  /**
   * The consumer this subscription receives from.
   *
   * @return the consumer's name
   */
  public String consumer() {
    return consumer;
  }

  /**
   * The credit window: how many records the server may push ahead of your code.
   *
   * @return the window
   */
  public int window() {
    return window;
  }

  /**
   * The server-assigned id of the current subscription (changes after a reconnect).
   *
   * @return the id
   */
  public int id() {
    lock.lock();
    try {
      return subId;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Why the subscription ended.
   *
   * @return the reason, or {@code null} while it is active
   */
  public EndReason endReason() {
    lock.lock();
    try {
      return endReason;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Whether the subscription has ended.
   *
   * @return true once ended
   */
  public boolean isClosed() {
    return endReason() != null;
  }

  /**
   * Records received but not yet taken.
   *
   * @return the count
   */
  public int buffered() {
    lock.lock();
    try {
      return buffer.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * The next message, waiting as long as it takes.
   *
   * @return the message, or {@code null} once the subscription has ended (after
   *     buffered messages, when the server ended it)
   */
  public Message next() {
    return next(null);
  }

  /**
   * The next message, waiting up to {@code timeout}.
   *
   * @param timeout how long to wait; {@code null} = forever
   * @return the message, or {@code null} on timeout or once the subscription has ended
   */
  public Message next(Duration timeout) {
    WireRecord r;
    lock.lock();
    try {
      long nanos = timeout == null ? Long.MAX_VALUE : Math.max(0, timeout.toNanos());
      while (buffer.isEmpty() && endReason == null) {
        if (timeout == null) {
          changed.await();
        } else {
          if (nanos <= 0) {
            return null;
          }
          nanos = changed.awaitNanos(nanos);
        }
      }
      if (buffer.isEmpty()) {
        return null;
      }
      r = take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ExspeedException("interrupted while waiting for a message", e);
    } finally {
      lock.unlock();
    }
    return new Message(r, consumer, host);
  }

  /**
   * The next message if one is buffered, without waiting.
   *
   * @return the message, or {@code null}
   */
  public Message poll() {
    WireRecord r;
    lock.lock();
    try {
      if (buffer.isEmpty()) {
        return null;
      }
      r = take();
    } finally {
      lock.unlock();
    }
    return new Message(r, consumer, host);
  }

  /** Takes one record and returns its credit (lock held). */
  private WireRecord take() {
    WireRecord r = buffer.poll();
    Connection c = conn;
    if (c != null && !c.isClosed()) {
      consumed++;
      if (consumed >= Math.max(1, window / 2)) {
        int credits = consumed;
        consumed = 0;
        c.send(new Request.Credit(subId, credits));
      }
    }
    return r;
  }

  /**
   * A blocking iterator over the messages; it ends when the subscription ends.
   * Breaking out of a loop does not unsubscribe: use try-with-resources or
   * call {@link #close()}.
   *
   * @return the iterator
   */
  @Override
  public Iterator<Message> iterator() {
    return new Iterator<>() {
      private Message peeked;
      private boolean done;

      @Override
      public boolean hasNext() {
        if (peeked == null && !done) {
          peeked = Subscription.this.next();
          done = peeked == null;
        }
        return peeked != null;
      }

      @Override
      public Message next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Message m = peeked;
        peeked = null;
        return m;
      }
    };
  }

  /**
   * Hands every message to {@code handler} on a dedicated thread, until the
   * subscription ends. If the handler throws, the subscription is closed (its
   * unacked records are redelivered) and the returned future completes with
   * the exception. Only one listener per subscription.
   *
   * @param handler called for each message, in order
   * @return completes with the end reason when the subscription ends
   * @throws IllegalStateException when a listener is already running
   */
  public CompletableFuture<EndReason> listen(Consumer<Message> handler) {
    if (!listening.compareAndSet(false, true)) {
      throw new IllegalStateException("this subscription already has a listener");
    }
    CompletableFuture<EndReason> done = new CompletableFuture<>();
    Thread t = new Thread(() -> {
      try {
        Message m;
        while ((m = next()) != null) {
          handler.accept(m);
        }
        done.complete(endReason());
      } catch (Throwable e) {
        try {
          unsubscribe();
        } catch (RuntimeException ignored) {
          // already failing
        }
        done.completeExceptionally(e);
      }
    }, "exspeed-sub-" + consumer);
    t.setDaemon(true);
    t.start();
    return done;
  }

  /**
   * Stops delivery and waits for the server to confirm. Records delivered to
   * this subscription and not yet acked (including buffered ones your code
   * never saw) are redelivered by the server.
   */
  public void unsubscribe() {
    Futures.await(unsubscribeInternal());
  }

  /**
   * Stops delivery.
   *
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> unsubscribeAsync() {
    return host.publicFuture(unsubscribeInternal());
  }

  private CompletableFuture<Void> unsubscribeInternal() {
    Connection c;
    int id;
    lock.lock();
    try {
      if (endReason != null) {
        return CompletableFuture.completedFuture(null);
      }
      c = conn;
      id = subId;
    } finally {
      lock.unlock();
    }
    end(new EndReason(0, "unsubscribed"), false);
    if (c != null && !c.isClosed()) {
      c.removeSub(id);
      return c.request(new Request.Unsubscribe(id), host.requestTimeoutMs(), null).handle((r, e) -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  /** Same as {@link #unsubscribe()}. */
  @Override
  public void close() {
    unsubscribe();
  }

  /** The connection was lost; a re-subscribe will follow. Buffered records are dropped (the server redelivers them). */
  void suspend() {
    lock.lock();
    try {
      conn = null;
      buffer.clear();
      consumed = 0;
    } finally {
      lock.unlock();
    }
  }

  void end(EndReason reason, boolean keepBuffered) {
    lock.lock();
    try {
      if (endReason != null) {
        return;
      }
      endReason = reason;
      conn = null;
      if (!keepBuffered) {
        buffer.clear();
      }
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    host.forget(this);
  }

  private final class Sink implements SubscriptionSink {
    @Override
    public void onSubscribed(Connection c, int id) {
      boolean ended;
      lock.lock();
      try {
        ended = endReason != null;
        if (!ended) {
          conn = c;
          subId = id;
          consumed = 0;
        }
      } finally {
        lock.unlock();
      }
      if (ended) {
        // Unsubscribed while a (re-)subscribe was in flight.
        c.removeSub(id);
        c.send(new Request.Unsubscribe(id));
      }
    }

    @Override
    public void onDeliver(List<WireRecord> records) {
      lock.lock();
      try {
        if (endReason != null) {
          return;
        }
        buffer.addAll(records);
        changed.signalAll();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void onEnded(int code, String message) {
      end(new EndReason(code, message), true);
    }
  }
}
