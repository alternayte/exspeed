package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A core-message subscription, from {@link ExspeedClient#subscribeCore(String, String)}.
 * It ends on {@link #unsubscribe()} / {@link #close()}, when the server ends it
 * (503 when leadership moves), or when the client closes.
 *
 * <p>Core messages are not stored: a subscription receives what is published
 * while it is live. After a reconnect the client subscribes again with the
 * same subject and queue group; messages published in the gap are missed.
 */
public final class CoreSubscription implements Iterable<CoreMessage>, AutoCloseable {
  private final Host host;
  private final String subject;
  private final String queue;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private final ArrayDeque<CoreMessage> buffer = new ArrayDeque<>();
  private final AtomicBoolean listening = new AtomicBoolean();
  private Connection conn;
  private int subId;
  private EndReason endReason;
  final SubscriptionSink sink = new Sink();

  CoreSubscription(Host host, String subject, String queue) {
    this.host = host;
    this.subject = subject;
    this.queue = queue;
  }

  /**
   * The subject filter.
   *
   * @return the filter
   */
  public String subject() {
    return subject;
  }

  /**
   * The queue group.
   *
   * @return the group, or {@code null}
   */
  public String queue() {
    return queue;
  }

  /**
   * The server-assigned id (high bit set; changes after a reconnect).
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
   * Messages received but not yet taken.
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
   * @return the message, or {@code null} once the subscription has ended
   */
  public CoreMessage next() {
    return next(null);
  }

  /**
   * The next message, waiting up to {@code timeout}.
   *
   * @param timeout how long to wait; {@code null} = forever
   * @return the message, or {@code null} on timeout or once the subscription has ended
   */
  public CoreMessage next(Duration timeout) {
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
      return buffer.poll();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ExspeedException("interrupted while waiting for a message", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * A blocking iterator over the messages; it ends when the subscription ends.
   *
   * @return the iterator
   */
  @Override
  public Iterator<CoreMessage> iterator() {
    return new Iterator<>() {
      private CoreMessage peeked;
      private boolean done;

      @Override
      public boolean hasNext() {
        if (peeked == null && !done) {
          peeked = CoreSubscription.this.next();
          done = peeked == null;
        }
        return peeked != null;
      }

      @Override
      public CoreMessage next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        CoreMessage m = peeked;
        peeked = null;
        return m;
      }
    };
  }

  /**
   * Hands every message to {@code handler} on a dedicated thread until the
   * subscription ends. If the handler throws, the subscription is closed and
   * the returned future completes with the exception.
   *
   * @param handler called for each message, in order
   * @return completes with the end reason when the subscription ends
   * @throws IllegalStateException when a listener is already running
   */
  public CompletableFuture<EndReason> listen(Consumer<CoreMessage> handler) {
    if (!listening.compareAndSet(false, true)) {
      throw new IllegalStateException("this subscription already has a listener");
    }
    CompletableFuture<EndReason> done = new CompletableFuture<>();
    Thread t = new Thread(() -> {
      try {
        CoreMessage m;
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
    }, "exspeed-core-sub-" + subject);
    t.setDaemon(true);
    t.start();
    return done;
  }

  /** Stops delivery and waits for the server to confirm. Buffered messages are dropped. */
  public void unsubscribe() {
    Futures.await(unsubscribeInternal());
  }

  /**
   * Stops delivery. Buffered messages are dropped.
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

  /** The connection was lost; a re-subscribe will follow. Buffered messages are kept. */
  void suspend() {
    lock.lock();
    try {
      conn = null;
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
    host.forgetCore(this);
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
        }
      } finally {
        lock.unlock();
      }
      if (ended) {
        c.removeSub(id);
        c.send(new Request.Unsubscribe(id));
      }
    }

    @Override
    public void onCoreMsg(Response.CoreMsg m) {
      CoreMessage msg = new CoreMessage(m, host);
      lock.lock();
      try {
        if (endReason != null) {
          return;
        }
        buffer.add(msg);
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
