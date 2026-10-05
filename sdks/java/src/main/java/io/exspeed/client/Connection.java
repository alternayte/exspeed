package io.exspeed.client;

import io.exspeed.client.protocol.Frame;
import io.exspeed.client.protocol.OpCode;
import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WireWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * One TCP (or TLS) connection: handshake, frame reading, correlation-id
 * multiplexing, push routing and keepalive (internal). Reconnection lives one
 * level up, in {@link ExspeedClient}.
 *
 * <p>Threads: a reader thread decodes frames and completes futures; a writer
 * thread drains a queue of outgoing frames, so callers never block on the
 * socket and wire order matches call order. Fire-and-forget acks queued back
 * to back are merged into one {@code Ack} frame per consumer.
 */
final class Connection {
  /** Connection-level callbacks. */
  interface Handlers {
    /** The connection closed for a reason other than {@link #close()}. Called at most once. */
    void onClose(Connection conn, Throwable err);

    /** An {@code Error} with correlation id 0: a fire-and-forget request failed. */
    void onAsyncError(ExspeedException err);
  }

  /** What a connection needs to know. */
  record Settings(
      TlsOptions tls, String clientId, String token, long requestTimeoutMs, long keepaliveMs, boolean verifyCrc) {}

  private static final Object STOP = new Object();
  private static final AtomicInteger IDS = new AtomicInteger();
  private static final Pattern IPV4 = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");
  private static final int MAX_ACKS_PER_FRAME = 8192;

  private record AckItem(String consumer, long offset) {}

  private static final class Drain {
    final CountDownLatch latch = new CountDownLatch(1);
  }

  private static final class Pending {
    final String name;
    final SubscriptionSink sink;
    final CompletableFuture<Response> future = new CompletableFuture<>();
    volatile ScheduledFuture<?> timer;

    Pending(String name, SubscriptionSink sink) {
      this.name = name;
      this.sink = sink;
    }

    void cancelTimer() {
      ScheduledFuture<?> t = timer;
      if (t != null) {
        t.cancel(false);
      }
    }
  }

  private final Socket socket;
  private final InputStream in;
  private final OutputStream out;
  private final Settings settings;
  private final Handlers handlers;
  private final ScheduledExecutorService sched;
  private final String address;
  private final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, SubscriptionSink> subs = new ConcurrentHashMap<>();
  private final AtomicInteger nextCorr = new AtomicInteger(1);
  private final LinkedBlockingQueue<Object> writeQueue = new LinkedBlockingQueue<>();
  private final Thread reader;
  private final Thread writer;
  private volatile boolean closed;
  private volatile boolean closedByUser;
  private volatile boolean opened;
  private volatile ScheduledFuture<?> keepalive;
  private volatile ServerInfo info;

  private Connection(Socket socket, Settings settings, Handlers handlers, ScheduledExecutorService sched,
      String address) throws IOException {
    this.socket = socket;
    this.in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
    this.out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
    this.settings = settings;
    this.handlers = handlers;
    this.sched = sched;
    this.address = address;
    int id = IDS.incrementAndGet();
    this.reader = new Thread(this::readLoop, "exspeed-reader-" + id);
    this.writer = new Thread(this::writeLoop, "exspeed-writer-" + id);
    reader.setDaemon(true);
    writer.setDaemon(true);
  }

  /** Opens a socket and runs the {@code Connect} handshake. Blocks until done. */
  static Connection open(String host, int port, Settings settings, Handlers handlers,
      ScheduledExecutorService sched) {
    String address = host + ":" + port;
    Socket socket = openSocket(host, port, settings.tls(), settings.requestTimeoutMs());
    Connection c;
    try {
      c = new Connection(socket, settings, handlers, sched, address);
    } catch (IOException e) {
      closeQuietly(socket);
      throw new ConnectionException("connect to " + address + " failed: " + e.getMessage(), e);
    }
    c.reader.start();
    c.writer.start();
    Response r;
    try {
      r = Futures.await(c.request(new Request.Connect(settings.clientId(), settings.token()),
          settings.requestTimeoutMs(), null));
    } catch (RequestTimeoutException e) {
      c.abort();
      throw new ConnectionException("handshake with " + address + " timed out");
    } catch (RuntimeException e) {
      c.abort();
      throw e;
    }
    if (!(r instanceof Response.ConnectOk ok)) {
      c.abort();
      throw new ProtocolException("unexpected handshake reply " + r.typeName());
    }
    c.info = new ServerInfo(ok.serverVersion(), ok.nodeId(), ok.leader());
    c.opened = true;
    if (c.closed) {
      throw new ConnectionException("connection to " + address + " closed during the handshake");
    }
    c.startKeepalive();
    return c;
  }

  private static Socket openSocket(String host, int port, TlsOptions tls, long timeoutMs) {
    String address = host + ":" + port;
    Socket raw = new Socket();
    try {
      raw.setTcpNoDelay(true);
      raw.setKeepAlive(true);
      raw.connect(new InetSocketAddress(host, port), (int) Math.min(Integer.MAX_VALUE, timeoutMs));
      if (tls == null) {
        return raw;
      }
      String name = tls.serverName() != null ? tls.serverName() : host;
      SSLSocket ss = (SSLSocket) tls.sslContext().getSocketFactory().createSocket(raw, name, port, true);
      SSLParameters params = ss.getSSLParameters();
      if (tls.verifyHostname()) {
        params.setEndpointIdentificationAlgorithm("HTTPS");
      }
      if (!isIpLiteral(name)) {
        params.setServerNames(List.of(new SNIHostName(name)));
      }
      ss.setSSLParameters(params);
      ss.setSoTimeout((int) Math.min(Integer.MAX_VALUE, timeoutMs));
      ss.startHandshake();
      ss.setSoTimeout(0);
      return ss;
    } catch (IOException | IllegalArgumentException e) {
      closeQuietly(raw);
      throw new ConnectionException("connect to " + address + " failed: " + e.getMessage(), e);
    }
  }

  private static boolean isIpLiteral(String host) {
    return host.indexOf(':') >= 0 || IPV4.matcher(host).matches();
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException ignored) {
      // closing anyway
    }
  }

  ServerInfo info() {
    return info;
  }

  boolean isClosed() {
    return closed;
  }

  String address() {
    return address;
  }

  /** Sends a request and returns its response; error responses complete it with {@link ServerException}. */
  CompletableFuture<Response> request(Request req, long timeoutMs, SubscriptionSink sink) {
    if (closed) {
      return Futures.failed(new ConnectionException("connection closed"));
    }
    int corr = allocCorr();
    byte[] frame;
    try {
      frame = req.frame(corr);
    } catch (RuntimeException e) {
      return Futures.failed(e);
    }
    Pending p = new Pending(req.typeName(), sink);
    pending.put(corr, p);
    if (timeoutMs > 0) {
      try {
        p.timer = sched.schedule(() -> {
          if (pending.remove(corr, p)) {
            p.future.completeExceptionally(
                new RequestTimeoutException(p.name + " timed out after " + timeoutMs + " ms"));
          }
        }, timeoutMs, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        pending.remove(corr, p);
        return Futures.failed(new ConnectionException("client is closed"));
      }
    }
    writeQueue.add(frame);
    if (closed && pending.remove(corr, p)) {
      p.cancelTimer();
      p.future.completeExceptionally(new ConnectionException("connection closed"));
    }
    return p.future;
  }

  /** Sends with correlation id 0 (fire-and-forget). Returns false when not sent. */
  boolean send(Request req) {
    if (closed) {
      return false;
    }
    writeQueue.add(req.frame(0));
    return true;
  }

  /** Queues a fire-and-forget ack; acks queued together share one frame. */
  boolean sendAck(String consumer, long offset) {
    if (closed) {
      return false;
    }
    writeQueue.add(new AckItem(consumer, offset));
    return true;
  }

  /** Stops routing pushes for a subscription. */
  void removeSub(int subId) {
    subs.remove(subId);
  }

  /** Closes gracefully: flushes what was queued (acks), then drops the socket. */
  void close() {
    if (closed) {
      return;
    }
    closedByUser = true;
    Drain d = new Drain();
    writeQueue.add(d);
    try {
      d.latch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    teardown(new ConnectionException("connection closed"));
    joinThreads();
  }

  private void abort() {
    closedByUser = true;
    teardown(new ConnectionException("handshake failed"));
    joinThreads();
  }

  private void joinThreads() {
    Thread self = Thread.currentThread();
    try {
      if (self != reader) {
        reader.join(1000);
      }
      if (self != writer) {
        writer.join(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private int allocCorr() {
    while (true) {
      int c = nextCorr.getAndIncrement();
      if (c != 0) {
        return c;
      }
    }
  }

  private void writeLoop() {
    List<Object> batch = new ArrayList<>();
    Map<String, long[]> acks = new LinkedHashMap<>();
    Map<String, Integer> ackCounts = new LinkedHashMap<>();
    try {
      while (true) {
        batch.clear();
        batch.add(writeQueue.take());
        writeQueue.drainTo(batch, 4096);
        boolean stop = false;
        for (Object item : batch) {
          if (item instanceof AckItem a) {
            int n = ackCounts.getOrDefault(a.consumer(), 0);
            long[] arr = acks.get(a.consumer());
            if (arr == null) {
              arr = new long[16];
            } else if (n == arr.length) {
              arr = Arrays.copyOf(arr, n * 2);
            }
            arr[n] = a.offset();
            acks.put(a.consumer(), arr);
            ackCounts.put(a.consumer(), n + 1);
            continue;
          }
          flushAcks(acks, ackCounts);
          if (item == STOP) {
            stop = true;
            break;
          }
          if (item instanceof Drain d) {
            out.flush();
            d.latch.countDown();
            continue;
          }
          out.write((byte[]) item);
        }
        flushAcks(acks, ackCounts);
        out.flush();
        if (stop) {
          return;
        }
      }
    } catch (InterruptedException e) {
      // exiting
    } catch (IOException e) {
      teardown(new ConnectionException("connection error: " + e.getMessage(), e));
    }
  }

  private void flushAcks(Map<String, long[]> acks, Map<String, Integer> counts) throws IOException {
    if (acks.isEmpty()) {
      return;
    }
    for (Map.Entry<String, long[]> e : acks.entrySet()) {
      int n = counts.get(e.getKey());
      long[] offs = e.getValue();
      for (int start = 0; start < n; start += MAX_ACKS_PER_FRAME) {
        int len = Math.min(MAX_ACKS_PER_FRAME, n - start);
        WireWriter w = new WireWriter(16 + e.getKey().length() * 3 + len * 8);
        w.str(e.getKey());
        w.u32(len);
        for (int i = start; i < start + len; i++) {
          w.u64(offs[i]);
        }
        out.write(Frame.encode(OpCode.ACK, 0, w.finish()));
      }
    }
    acks.clear();
    counts.clear();
  }

  private void readLoop() {
    try {
      while (!closed) {
        Frame f = Frame.read(in);
        if (f == null) {
          teardown(new ConnectionException("connection closed by the server"));
          return;
        }
        Response resp;
        try {
          resp = Response.decode(f.opcode(), f.payload(), settings.verifyCrc());
        } catch (ProtocolException e) {
          Pending p = f.correlationId() != 0 ? takePending(f.correlationId()) : null;
          if (p != null) {
            p.future.completeExceptionally(e);
          } else {
            handlers.onAsyncError(e);
          }
          continue;
        }
        route(f.correlationId(), resp);
      }
    } catch (ProtocolException e) {
      teardown(e);
    } catch (IOException e) {
      teardown(new ConnectionException(closed ? "connection closed" : "connection error: " + e.getMessage(), e));
    } catch (RuntimeException e) {
      teardown(new ConnectionException("internal error: " + e, e));
    }
  }

  private Pending takePending(int corr) {
    Pending p = pending.remove(corr);
    if (p != null) {
      p.cancelTimer();
    }
    return p;
  }

  static ServerException errorFromResponse(Response.Error e) {
    String detail = e.detail() == null ? null : new String(e.detail(), StandardCharsets.UTF_8);
    return new ServerException(e.code(), e.message(), detail);
  }

  private void route(int corr, Response resp) {
    if (corr == 0) {
      if (resp instanceof Response.Deliver d) {
        SubscriptionSink s = subs.get(d.subId());
        if (s != null) {
          s.onDeliver(d.records());
        }
      } else if (resp instanceof Response.CoreMsg m) {
        SubscriptionSink s = subs.get(m.subId());
        if (s != null) {
          s.onCoreMsg(m);
        }
      } else if (resp instanceof Response.SubscriptionEnded e) {
        SubscriptionSink s = subs.remove(e.subId());
        if (s != null) {
          s.onEnded(e.code(), e.message());
        }
      } else if (resp instanceof Response.Error e) {
        handlers.onAsyncError(errorFromResponse(e));
      }
      return;
    }
    Pending p = takePending(corr);
    if (resp instanceof Response.SubscribeOk ok) {
      if (p != null && p.sink != null) {
        // Register before completing so a Deliver right behind it is not lost.
        subs.put(ok.subId(), p.sink);
        p.sink.onSubscribed(this, ok.subId());
      } else {
        // The subscribe call timed out or was abandoned: release the server side.
        send(new Request.Unsubscribe(ok.subId()));
      }
    }
    if (p == null) {
      return;
    }
    if (resp instanceof Response.Error e) {
      p.future.completeExceptionally(errorFromResponse(e));
    } else {
      p.future.complete(resp);
    }
  }

  private void startKeepalive() {
    long ka = settings.keepaliveMs();
    if (ka <= 0) {
      return;
    }
    try {
      keepalive = sched.scheduleAtFixedRate(() -> {
        if (closed) {
          return;
        }
        request(new Request.Ping(), settings.requestTimeoutMs(), null).whenComplete((r, e) -> {
          // A ping that times out means the peer is gone (half-open socket).
          if (e != null && Futures.unwrap(e) instanceof RequestTimeoutException) {
            teardown(new ConnectionException("keepalive ping timed out"));
          }
        });
      }, ka, ka, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // the client is closing
    }
    if (closed && keepalive != null) {
      keepalive.cancel(false);
    }
  }

  private void teardown(Throwable err) {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    ScheduledFuture<?> ka = keepalive;
    if (ka != null) {
      ka.cancel(false);
    }
    closeQuietly(socket);
    writeQueue.clear();
    writeQueue.add(STOP);
    ConnectionException ce = err instanceof ConnectionException c ? c
        : new ConnectionException(String.valueOf(err.getMessage()), err);
    for (Integer k : new ArrayList<>(pending.keySet())) {
      Pending p = pending.remove(k);
      if (p != null) {
        p.cancelTimer();
        p.future.completeExceptionally(ce);
      }
    }
    subs.clear();
    if (opened && !closedByUser) {
      handlers.onClose(this, err);
    }
  }
}
