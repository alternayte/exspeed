package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WirePublishRecord;
import io.exspeed.client.protocol.WireRecord;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A connection to an Exspeed server (client protocol v2).
 *
 * <p>One client is one TCP/TLS connection. Requests are multiplexed by
 * correlation id, so a slow pull or long-poll read never blocks other calls;
 * share one client across your application. The client is thread-safe.
 *
 * <p>Every operation comes in two forms: a blocking one ({@code publish}) that
 * returns the result or throws an {@link ExspeedException}, and an
 * asynchronous one ({@code publishAsync}) that returns a
 * {@link CompletableFuture} completed on the {@linkplain ClientOptions#callbackExecutor()
 * callback executor}, never on the connection's I/O threads.
 *
 * <pre>{@code
 * try (ExspeedClient client = ExspeedClient.connect(ClientOptions.builder().host("127.0.0.1").build())) {
 *   client.createStream(StreamSpec.of("orders"));
 *   client.publish("orders", PublishRecord.of("orders.placed", "{\"id\":42}"));
 *   client.createConsumer(ConsumerSpec.of("billing", "orders"));
 *   try (Subscription sub = client.subscribe("billing")) {
 *     for (Message m : sub) {
 *       handle(m);
 *       m.ack();
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Threads: each connection uses one reader and one writer thread; the
 * client adds one scheduler thread for timeouts, keepalive pings and batching,
 * and a short-lived thread while reconnecting. All of them are daemon threads
 * and stop when the client closes.
 */
public final class ExspeedClient implements AutoCloseable {
  private enum State { CONNECTED, RECONNECTING, CLOSED }

  /** Subjects of request-reply inboxes start with this token. */
  static final String INBOX_PREFIX = "_INBOX";

  private static final SecureRandom RANDOM = new SecureRandom();

  private final ClientOptions opts;
  private final Connection.Settings settings;
  private final ScheduledThreadPoolExecutor sched;
  private final Executor callbacks;
  private final long requestTimeoutMs;
  private final List<ClientListener> listeners = new CopyOnWriteArrayList<>();
  private final Object lock = new Object();
  private final Set<Subscription> subs = ConcurrentHashMap.newKeySet();
  private final Set<CoreSubscription> coreSubs = ConcurrentHashMap.newKeySet();
  private final Map<String, ConsumerSpec> ephemeral = Collections.synchronizedMap(new LinkedHashMap<>());
  private final ConcurrentHashMap<String, PendingReply> replies = new ConcurrentHashMap<>();
  private final AtomicLong nextReply = new AtomicLong(1);
  private final HostImpl host = new HostImpl();
  private volatile State state = State.CONNECTED;
  private volatile Connection conn;
  private Inbox inbox;
  private Thread reconnectThread;

  /** A request waiting for its response. */
  private static final class PendingReply {
    final CompletableFuture<CoreMessage> future = new CompletableFuture<>();
    volatile ScheduledFuture<?> timer;
  }

  /** The connection's request-reply inbox: one core subscription to {@code <prefix>.*}. */
  private static final class Inbox {
    final String prefix;
    volatile Connection conn;
    volatile int subId;
    CompletableFuture<Void> ready;

    Inbox(String prefix) {
      this.prefix = prefix;
    }
  }

  private ExspeedClient(ClientOptions opts) {
    this.opts = opts;
    this.requestTimeoutMs = opts.requestTimeout().toMillis();
    this.settings = new Connection.Settings(opts.tls(), opts.clientId(), opts.token(), requestTimeoutMs,
        opts.keepalive().toMillis(), opts.verifyCrc());
    this.callbacks = opts.callbackExecutor();
    this.listeners.addAll(opts.listeners());
    this.sched = new ScheduledThreadPoolExecutor(1, r -> {
      Thread t = new Thread(r, "exspeed-scheduler");
      t.setDaemon(true);
      return t;
    });
    this.sched.setRemoveOnCancelPolicy(true);
    this.sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  /**
   * Connects to {@code 127.0.0.1:5933} with the default options.
   *
   * @return the connected client
   * @throws ConnectionException when the server can't be reached
   */
  public static ExspeedClient connect() {
    return connect(ClientOptions.defaults());
  }

  /**
   * Connects to {@code host:port} with the default options.
   *
   * @param host the server's host
   * @param port the server's port
   * @return the connected client
   * @throws ConnectionException when the server can't be reached
   */
  public static ExspeedClient connect(String host, int port) {
    return connect(ClientOptions.builder().host(host).port(port).build());
  }

  /**
   * Connects and authenticates. The first connection attempt is not retried.
   *
   * @param options the connection settings
   * @return the connected client
   * @throws ConnectionException when the server can't be reached or the TLS handshake fails
   * @throws ServerException 401 when the token is refused
   */
  public static ExspeedClient connect(ClientOptions options) {
    ExspeedClient c = new ExspeedClient(options);
    try {
      c.conn = c.openLeader(null);
    } catch (RuntimeException e) {
      c.sched.shutdownNow();
      throw e;
    }
    return c;
  }

  /**
   * Connects and authenticates without blocking the caller.
   *
   * @param options the connection settings
   * @return completes with the connected client
   */
  public static CompletableFuture<ExspeedClient> connectAsync(ClientOptions options) {
    CompletableFuture<ExspeedClient> f = new CompletableFuture<>();
    Thread t = new Thread(() -> {
      try {
        f.complete(connect(options));
      } catch (Throwable e) {
        f.completeExceptionally(e);
      }
    }, "exspeed-connect");
    t.setDaemon(true);
    t.start();
    return f;
  }

  // ---- state ------------------------------------------------------------------

  /**
   * Handshake info from the current connection.
   *
   * @return the server info
   */
  public ServerInfo serverInfo() {
    return conn.info();
  }

  /**
   * True while a connection is up (false while reconnecting or after {@link #close()}).
   *
   * @return whether connected
   */
  public boolean isConnected() {
    Connection c = conn;
    return state == State.CONNECTED && c != null && !c.isClosed();
  }

  /**
   * Whether the client is closed for good.
   *
   * @return true after {@link #close()}, or when reconnecting gave up
   */
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /**
   * The options the client was created with.
   *
   * @return the options
   */
  public ClientOptions options() {
    return opts;
  }

  /**
   * Registers a lifecycle listener.
   *
   * @param listener the listener
   */
  public void addListener(ClientListener listener) {
    listeners.add(Objects.requireNonNull(listener));
  }

  /**
   * Removes a lifecycle listener.
   *
   * @param listener the listener
   */
  public void removeListener(ClientListener listener) {
    listeners.remove(listener);
  }

  /**
   * Closes the connection. Queued acks are flushed first; pending requests
   * fail with {@link ConnectionException}, subscriptions end (code 0), and the
   * server deletes this connection's ephemeral consumers. Idempotent.
   */
  @Override
  public void close() {
    Connection c;
    Thread rt;
    synchronized (lock) {
      if (state == State.CLOSED) {
        return;
      }
      state = State.CLOSED;
      c = conn;
      rt = reconnectThread;
    }
    for (Subscription s : new ArrayList<>(subs)) {
      s.end(new EndReason(0, "client closed"), false);
    }
    for (CoreSubscription s : new ArrayList<>(coreSubs)) {
      s.end(new EndReason(0, "client closed"), false);
    }
    dropInbox(new ConnectionException("client closed"));
    if (rt != null) {
      rt.interrupt();
    }
    if (c != null) {
      c.close();
    }
    sched.shutdownNow();
    if (rt != null && rt != Thread.currentThread()) {
      try {
        rt.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    fire(l -> l.onClose(null));
  }

  // ---- basics -------------------------------------------------------------------

  /**
   * Round trip to the server.
   *
   * @return the latency
   */
  public Duration ping() {
    return Futures.await(pingInternal());
  }

  /**
   * Round trip to the server.
   *
   * @return completes with the latency
   */
  public CompletableFuture<Duration> pingAsync() {
    return pub(pingInternal());
  }

  private CompletableFuture<Duration> pingInternal() {
    long start = System.nanoTime();
    return call(new Request.Ping(), Response.Pong.class).thenApply(r -> Duration.ofNanos(System.nanoTime() - start));
  }

  /**
   * Node id, leadership and server version.
   *
   * @return the metadata
   */
  public Metadata metadata() {
    return Futures.await(metadataInternal());
  }

  /**
   * Node id, leadership and server version.
   *
   * @return completes with the metadata
   */
  public CompletableFuture<Metadata> metadataAsync() {
    return pub(metadataInternal());
  }

  private CompletableFuture<Metadata> metadataInternal() {
    return json(new Request.Metadata()).thenApply(ExspeedClient::toMetadata);
  }

  private static Metadata toMetadata(Object v) {
    Map<String, Object> m = JsonMaps.asMap(v);
    return new Metadata(JsonMaps.str(m, "node_id", ""), JsonMaps.bool(m, "is_leader", false),
        JsonMaps.str(m, "leader", null), JsonMaps.str(m, "server_version", ""));
  }

  // ---- streams ------------------------------------------------------------------

  /**
   * Creates a stream with server defaults. Idempotent when it exists with the same settings.
   *
   * @param name the stream name
   * @throws ServerException 409 when it exists with other settings
   */
  public void createStream(String name) {
    createStream(StreamSpec.of(name));
  }

  /**
   * Creates a stream. Idempotent when it exists with the same settings.
   *
   * @param spec the settings
   * @throws ServerException 409 when it exists with other settings
   */
  public void createStream(StreamSpec spec) {
    Futures.await(createStreamInternal(spec));
  }

  /**
   * Creates a stream. Idempotent when it exists with the same settings.
   *
   * @param spec the settings
   * @return completes when created
   */
  public CompletableFuture<Void> createStreamAsync(StreamSpec spec) {
    return pub(createStreamInternal(spec));
  }

  private CompletableFuture<Void> createStreamInternal(StreamSpec spec) {
    return ok(new Request.CreateStream(spec.toWire()));
  }

  /**
   * Replaces a stream's settings (unset fields reset to the server defaults).
   *
   * @param spec the settings
   */
  public void updateStream(StreamSpec spec) {
    Futures.await(ok(new Request.UpdateStream(spec.toWire())));
  }

  /**
   * Replaces a stream's settings.
   *
   * @param spec the settings
   * @return completes when updated
   */
  public CompletableFuture<Void> updateStreamAsync(StreamSpec spec) {
    return pub(ok(new Request.UpdateStream(spec.toWire())));
  }

  /**
   * Deletes a stream.
   *
   * @param name the stream
   * @throws ServerException 409 while consumers exist ({@code detail.consumers})
   */
  public void deleteStream(String name) {
    Futures.await(ok(new Request.DeleteStream(name)));
  }

  /**
   * Deletes a stream.
   *
   * @param name the stream
   * @return completes when deleted
   */
  public CompletableFuture<Void> deleteStreamAsync(String name) {
    return pub(ok(new Request.DeleteStream(name)));
  }

  /**
   * A stream's bounds and settings.
   *
   * @param name the stream
   * @return the info
   * @throws ServerException 404 when the stream doesn't exist
   */
  public StreamInfo streamInfo(String name) {
    return Futures.await(streamInfoInternal(name));
  }

  /**
   * A stream's bounds and settings.
   *
   * @param name the stream
   * @return completes with the info
   */
  public CompletableFuture<StreamInfo> streamInfoAsync(String name) {
    return pub(streamInfoInternal(name));
  }

  private CompletableFuture<StreamInfo> streamInfoInternal(String name) {
    return json(new Request.StreamInfo(name)).thenApply(StreamInfo::fromJson);
  }

  /**
   * The streams this credential can see.
   *
   * @return the streams
   */
  public List<StreamInfo> listStreams() {
    return Futures.await(listStreamsInternal());
  }

  /**
   * The streams this credential can see.
   *
   * @return completes with the streams
   */
  public CompletableFuture<List<StreamInfo>> listStreamsAsync() {
    return pub(listStreamsInternal());
  }

  private CompletableFuture<List<StreamInfo>> listStreamsInternal() {
    return json(new Request.ListStreams()).thenApply(v -> {
      List<StreamInfo> out = new ArrayList<>();
      for (Object o : JsonMaps.asList(v)) {
        out.add(StreamInfo.fromJson(o));
      }
      return out;
    });
  }

  // ---- publishing ---------------------------------------------------------------

  /**
   * Publishes one record and waits for its offset.
   *
   * @param stream the stream
   * @param record the record
   * @return the offset, and whether it was a duplicate
   */
  public PublishResult publish(String stream, PublishRecord record) {
    return Futures.await(publishInternal(stream, record));
  }

  /**
   * Publishes one record with a UTF-8 text value.
   *
   * @param stream the stream
   * @param subject the subject
   * @param value the value
   * @return the offset
   */
  public PublishResult publish(String stream, String subject, String value) {
    return publish(stream, PublishRecord.of(subject, value));
  }

  /**
   * Publishes one record with a binary value.
   *
   * @param stream the stream
   * @param subject the subject
   * @param value the value
   * @return the offset
   */
  public PublishResult publish(String stream, String subject, byte[] value) {
    return publish(stream, PublishRecord.of(subject, value));
  }

  /**
   * Publishes one record.
   *
   * @param stream the stream
   * @param record the record
   * @return completes with the offset
   */
  public CompletableFuture<PublishResult> publishAsync(String stream, PublishRecord record) {
    return pub(publishInternal(stream, record));
  }

  private CompletableFuture<PublishResult> publishInternal(String stream, PublishRecord record) {
    return call(new Request.Publish(stream, record.toWire()), Response.PublishOk.class)
        .thenApply(r -> new PublishResult(r.offset(), r.duplicate()));
  }

  /**
   * Publishes several records in one request.
   *
   * @param stream the stream
   * @param records the records, in order
   * @return one result per record, in order
   */
  public List<PublishResult> publishBatch(String stream, List<PublishRecord> records) {
    return Futures.await(publishBatchInternal(stream, records));
  }

  /**
   * Publishes several records in one request.
   *
   * @param stream the stream
   * @param records the records, in order
   * @return completes with one result per record
   */
  public CompletableFuture<List<PublishResult>> publishBatchAsync(String stream, List<PublishRecord> records) {
    return pub(publishBatchInternal(stream, records));
  }

  private CompletableFuture<List<PublishResult>> publishBatchInternal(String stream, List<PublishRecord> records) {
    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }
    List<WirePublishRecord> wire = new ArrayList<>(records.size());
    for (PublishRecord r : records) {
      wire.add(r.toWire());
    }
    return call(new Request.PublishBatch(stream, wire), Response.PublishBatchOk.class).thenApply(r -> {
      List<PublishResult> out = new ArrayList<>(r.results().size());
      for (Response.PublishOk ok : r.results()) {
        out.add(new PublishResult(ok.offset(), ok.duplicate()));
      }
      return out;
    });
  }

  /**
   * A coalescing, order-preserving publisher with the default settings.
   *
   * @return the publisher
   */
  public Publisher publisher() {
    return publisher(PublisherOptions.DEFAULT);
  }

  /**
   * A coalescing, order-preserving publisher on this client.
   *
   * @param options its settings
   * @return the publisher
   */
  public Publisher publisher(PublisherOptions options) {
    return new Publisher(host, sched, options);
  }

  // ---- stateless reads ------------------------------------------------------------

  /**
   * Reads from offset 0 with the default options.
   *
   * @param stream the stream
   * @return the records
   */
  public ReadResult read(String stream) {
    return read(stream, ReadOptions.DEFAULT);
  }

  /**
   * Reads records without a consumer. With a wait, waits for new records when
   * caught up. Continue from {@link ReadResult#nextOffset()}.
   *
   * @param stream the stream
   * @param options where to start, how much, filter and wait
   * @return the records
   */
  public ReadResult read(String stream, ReadOptions options) {
    return Futures.await(readInternal(stream, options));
  }

  /**
   * Reads records without a consumer.
   *
   * @param stream the stream
   * @param options where to start, how much, filter and wait
   * @return completes with the records
   */
  public CompletableFuture<ReadResult> readAsync(String stream, ReadOptions options) {
    return pub(readInternal(stream, options));
  }

  private CompletableFuture<ReadResult> readInternal(String stream, ReadOptions o) {
    return readWire(stream, o.from(), o.maxRecords(), o.maxBytes(), o.waitTime().toMillis(), o.filter())
        .thenApply(r -> {
          List<StreamRecord> recs = new ArrayList<>(r.records().size());
          for (WireRecord w : r.records()) {
            recs.add(new StreamRecord(w));
          }
          return new ReadResult(recs, r.nextOffset(), r.highWatermark());
        });
  }

  private CompletableFuture<Response.ReadResult> readWire(String stream, long from, int maxRecords, int maxBytes,
      long waitMs, String filter) {
    int wait = (int) Math.min(Integer.MAX_VALUE, Math.max(0, waitMs));
    return call(new Request.Read(stream, from, maxRecords, maxBytes, wait, filter), Response.ReadResult.class,
        requestTimeoutMs + wait, null);
  }

  // ---- SQL ----------------------------------------------------------------------

  /**
   * Runs a bounded ExQL query. Needs a global-admin credential when auth is on.
   *
   * @param sql the query
   * @return the result
   * @throws ServerException 400 for invalid SQL, 408 on timeout, 422 over the memory limit
   */
  public QueryResult query(String sql) {
    return Futures.await(queryInternal(sql));
  }

  /**
   * Runs a bounded ExQL query.
   *
   * @param sql the query
   * @return completes with the result
   */
  public CompletableFuture<QueryResult> queryAsync(String sql) {
    return pub(queryInternal(sql));
  }

  private CompletableFuture<QueryResult> queryInternal(String sql) {
    return json(new Request.Query(sql)).thenApply(v -> {
      Map<String, Object> m = JsonMaps.asMap(v);
      List<String> cols = new ArrayList<>();
      for (Object o : JsonMaps.asList(m.get("columns"))) {
        cols.add(String.valueOf(o));
      }
      List<List<Object>> rows = new ArrayList<>();
      for (Object row : JsonMaps.asList(m.get("rows"))) {
        rows.add(JsonMaps.asList(row));
      }
      return new QueryResult(cols, rows, JsonMaps.num(m, "row_count", rows.size()),
          JsonMaps.dbl(m, "execution_time_ms", 0), JsonMaps.bool(m, "truncated", false));
    });
  }

  // ---- consumers --------------------------------------------------------------------

  /**
   * Creates a consumer. Idempotent for an identical spec.
   *
   * @param spec the consumer's settings
   * @return the consumer's info
   * @throws ServerException 409 when a consumer of that name exists with a different spec
   */
  public ConsumerInfo createConsumer(ConsumerSpec spec) {
    return Futures.await(createConsumerInternal(spec));
  }

  /**
   * Creates a consumer. Idempotent for an identical spec.
   *
   * @param spec the consumer's settings
   * @return completes with the consumer's info
   */
  public CompletableFuture<ConsumerInfo> createConsumerAsync(ConsumerSpec spec) {
    return pub(createConsumerInternal(spec));
  }

  private CompletableFuture<ConsumerInfo> createConsumerInternal(ConsumerSpec spec) {
    return json(new Request.CreateConsumer(spec.toJson())).thenApply(v -> {
      if (Boolean.TRUE.equals(spec.ephemeral())) {
        ephemeral.put(spec.name(), spec);
      }
      return ConsumerInfo.fromJson(v);
    });
  }

  /**
   * Deletes a consumer.
   *
   * @param name the consumer
   */
  public void deleteConsumer(String name) {
    Futures.await(deleteConsumerInternal(name));
  }

  /**
   * Deletes a consumer.
   *
   * @param name the consumer
   * @return completes when deleted
   */
  public CompletableFuture<Void> deleteConsumerAsync(String name) {
    return pub(deleteConsumerInternal(name));
  }

  private CompletableFuture<Void> deleteConsumerInternal(String name) {
    return ok(new Request.DeleteConsumer(name)).thenApply(v -> {
      ephemeral.remove(name);
      return null;
    });
  }

  /**
   * A consumer's spec, position and counters.
   *
   * @param name the consumer
   * @return the info
   * @throws ServerException 404 when the consumer doesn't exist
   */
  public ConsumerInfo consumerInfo(String name) {
    return Futures.await(consumerInfoInternal(name));
  }

  /**
   * A consumer's spec, position and counters.
   *
   * @param name the consumer
   * @return completes with the info
   */
  public CompletableFuture<ConsumerInfo> consumerInfoAsync(String name) {
    return pub(consumerInfoInternal(name));
  }

  private CompletableFuture<ConsumerInfo> consumerInfoInternal(String name) {
    return json(new Request.ConsumerInfo(name)).thenApply(ConsumerInfo::fromJson);
  }

  /**
   * Every consumer this credential can see.
   *
   * @return the consumers
   */
  public List<ConsumerInfo> listConsumers() {
    return listConsumers(null);
  }

  /**
   * The consumers on {@code stream} this credential can see.
   *
   * @param stream the stream, or {@code null} for all
   * @return the consumers
   */
  public List<ConsumerInfo> listConsumers(String stream) {
    return Futures.await(listConsumersInternal(stream));
  }

  /**
   * The consumers on {@code stream} this credential can see.
   *
   * @param stream the stream, or {@code null} for all
   * @return completes with the consumers
   */
  public CompletableFuture<List<ConsumerInfo>> listConsumersAsync(String stream) {
    return pub(listConsumersInternal(stream));
  }

  private CompletableFuture<List<ConsumerInfo>> listConsumersInternal(String stream) {
    return json(new Request.ListConsumers(stream)).thenApply(v -> {
      List<ConsumerInfo> out = new ArrayList<>();
      for (Object o : JsonMaps.asList(v)) {
        out.add(ConsumerInfo.fromJson(o));
      }
      return out;
    });
  }

  /**
   * Moves a consumer's cursor.
   *
   * @param consumer the consumer
   * @param target where to move it
   */
  public void seek(String consumer, SeekTarget target) {
    Futures.await(ok(new Request.SeekConsumer(consumer, target.kind, target.value)));
  }

  /**
   * Moves a consumer's cursor.
   *
   * @param consumer the consumer
   * @param target where to move it
   * @return completes when moved
   */
  public CompletableFuture<Void> seekAsync(String consumer, SeekTarget target) {
    return pub(ok(new Request.SeekConsumer(consumer, target.kind, target.value)));
  }

  /**
   * Starts push delivery from a consumer with a credit window of 256.
   *
   * @param consumer the consumer
   * @return the subscription
   */
  public Subscription subscribe(String consumer) {
    return subscribe(consumer, 256);
  }

  /**
   * Starts push delivery from a consumer. Any number of subscriptions (on any
   * connection, in any process) can share one consumer; each record goes to
   * one of them.
   *
   * @param consumer the consumer
   * @param window how many records the server may push ahead of your code
   * @return the subscription
   * @throws ServerException 404 when the consumer doesn't exist
   */
  public Subscription subscribe(String consumer, int window) {
    return Futures.await(subscribeInternal(consumer, window));
  }

  /**
   * Starts push delivery from a consumer.
   *
   * @param consumer the consumer
   * @param window how many records the server may push ahead of your code
   * @return completes with the subscription
   */
  public CompletableFuture<Subscription> subscribeAsync(String consumer, int window) {
    return pub(subscribeInternal(consumer, window));
  }

  private CompletableFuture<Subscription> subscribeInternal(String consumer, int window) {
    int w = Math.max(1, window);
    Subscription sub = new Subscription(host, consumer, w);
    // The connection binds the sink to its id as soon as SubscribeOk arrives,
    // before any Deliver behind it is routed.
    return call(new Request.Subscribe(consumer, w), Response.SubscribeOk.class, requestTimeoutMs, sub.sink)
        .thenApply(r -> {
          subs.add(sub);
          return sub;
        });
  }

  /**
   * Fetches up to 100 messages, waiting up to 5 s for at least one.
   *
   * @param consumer the consumer
   * @return the messages (empty on timeout)
   */
  public List<Message> pull(String consumer) {
    return pull(consumer, PullOptions.DEFAULT);
  }

  /**
   * Fetches up to {@code maxMessages}, waiting up to {@code expires} for at
   * least one.
   *
   * @param consumer the consumer
   * @param options batch size and wait
   * @return the messages (empty on timeout)
   */
  public List<Message> pull(String consumer, PullOptions options) {
    return Futures.await(pullInternal(consumer, options));
  }

  /**
   * Fetches a batch from a consumer.
   *
   * @param consumer the consumer
   * @param options batch size and wait
   * @return completes with the messages (empty on timeout)
   */
  public CompletableFuture<List<Message>> pullAsync(String consumer, PullOptions options) {
    return pub(pullInternal(consumer, options));
  }

  private CompletableFuture<List<Message>> pullInternal(String consumer, PullOptions o) {
    int expires = (int) Math.min(Integer.MAX_VALUE, o.expires().toMillis());
    return call(new Request.Pull(consumer, o.maxMessages(), o.maxBytes(), expires), Response.Messages.class,
        requestTimeoutMs + expires, null).thenApply(r -> {
          List<Message> out = new ArrayList<>(r.records().size());
          for (WireRecord w : r.records()) {
            out.add(new Message(w, consumer, host));
          }
          return out;
        });
  }

  /**
   * Acknowledges records and waits for the server to confirm.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   */
  public void ack(String consumer, long... offsets) {
    Futures.await(ok(new Request.Ack(consumer, offsets.clone())));
  }

  /**
   * Acknowledges records.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> ackAsync(String consumer, long... offsets) {
    return pub(ok(new Request.Ack(consumer, offsets.clone())));
  }

  /**
   * Asks for redelivery after {@code delay} (zero = the consumer's backoff).
   *
   * @param consumer the consumer
   * @param offset the record
   * @param delay the delay
   */
  public void nack(String consumer, long offset, Duration delay) {
    Futures.await(nackInternal(consumer, offset, delay.toMillis()));
  }

  /**
   * Asks for redelivery after {@code delay} (zero = the consumer's backoff).
   *
   * @param consumer the consumer
   * @param offset the record
   * @param delay the delay
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> nackAsync(String consumer, long offset, Duration delay) {
    return pub(nackInternal(consumer, offset, delay.toMillis()));
  }

  private CompletableFuture<Void> nackInternal(String consumer, long offset, long delayMs) {
    return ok(new Request.Nack(consumer, offset, (int) Math.min(Integer.MAX_VALUE, Math.max(0, delayMs))));
  }

  /**
   * Dead-letters a record now (to the consumer's DLQ stream, if set).
   *
   * @param consumer the consumer
   * @param offset the record
   * @param reason free text, stored in {@code exspeed-dlq-reason}
   */
  public void term(String consumer, long offset, String reason) {
    Futures.await(ok(new Request.Term(consumer, offset, reason)));
  }

  /**
   * Dead-letters a record now.
   *
   * @param consumer the consumer
   * @param offset the record
   * @param reason free text
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> termAsync(String consumer, long offset, String reason) {
    return pub(ok(new Request.Term(consumer, offset, reason)));
  }

  /**
   * Resets the ack deadlines of records still being worked on.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   */
  public void inProgress(String consumer, long... offsets) {
    Futures.await(ok(new Request.InProgress(consumer, offsets.clone())));
  }

  /**
   * Resets the ack deadlines of records still being worked on.
   *
   * @param consumer the consumer
   * @param offsets the offsets
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> inProgressAsync(String consumer, long... offsets) {
    return pub(ok(new Request.InProgress(consumer, offsets.clone())));
  }

  // ---- core messaging -------------------------------------------------------------

  /**
   * Publishes a core message to the core subscriptions live now. Nothing is
   * stored and delivery is at most once. Returns once the server accepted it.
   *
   * @param subject the subject
   * @param value the payload
   */
  public void publishCore(String subject, byte[] value) {
    publishCore(subject, value, CorePublishOptions.NONE);
  }

  /**
   * Publishes a core message with a UTF-8 text payload.
   *
   * @param subject the subject
   * @param value the payload
   */
  public void publishCore(String subject, String value) {
    publishCore(subject, value.getBytes(StandardCharsets.UTF_8), CorePublishOptions.NONE);
  }

  /**
   * Publishes a core message with headers or a reply subject.
   *
   * @param subject the subject
   * @param value the payload
   * @param options headers and reply subject
   * @throws ServerException 404 when a reply subject is set and nobody received it
   */
  public void publishCore(String subject, byte[] value, CorePublishOptions options) {
    Futures.await(publishCoreInternal(subject, options.replyTo(), options.headers(), value));
  }

  /**
   * Publishes a core message.
   *
   * @param subject the subject
   * @param value the payload
   * @param options headers and reply subject
   * @return completes once the server accepted it
   */
  public CompletableFuture<Void> publishCoreAsync(String subject, byte[] value, CorePublishOptions options) {
    return pub(publishCoreInternal(subject, options.replyTo(), options.headers(), value));
  }

  private CompletableFuture<Void> publishCoreInternal(String subject, String replyTo, List<Header> headers,
      byte[] value) {
    return ok(new Request.CorePublish(subject, replyTo, headers, value));
  }

  /**
   * Receives core messages on subjects matching {@code subject} (a filter such as {@code orders.*}).
   *
   * @param subject the subject filter
   * @return the subscription
   */
  public CoreSubscription subscribeCore(String subject) {
    return subscribeCore(subject, null);
  }

  /**
   * Receives core messages on subjects matching {@code subject}. In a queue
   * group, each message goes to one member of the group.
   *
   * @param subject the subject filter
   * @param queue the queue group, or {@code null}
   * @return the subscription
   */
  public CoreSubscription subscribeCore(String subject, String queue) {
    return Futures.await(subscribeCoreInternal(subject, queue));
  }

  /**
   * Receives core messages on subjects matching {@code subject}.
   *
   * @param subject the subject filter
   * @param queue the queue group, or {@code null}
   * @return completes with the subscription
   */
  public CompletableFuture<CoreSubscription> subscribeCoreAsync(String subject, String queue) {
    return pub(subscribeCoreInternal(subject, queue));
  }

  private CompletableFuture<CoreSubscription> subscribeCoreInternal(String subject, String queue) {
    CoreSubscription sub = new CoreSubscription(host, subject, queue);
    return call(new Request.CoreSubscribe(subject, queue), Response.SubscribeOk.class, requestTimeoutMs, sub.sink)
        .thenApply(r -> {
          coreSubs.add(sub);
          return sub;
        });
  }

  /**
   * Sends a request (a core message with a reply subject) and waits for the first response.
   *
   * @param subject the subject
   * @param value the payload
   * @return the response
   * @throws ServerException 404 at once when nobody is subscribed to {@code subject}
   * @throws RequestTimeoutException when no response arrives within the request timeout
   */
  public CoreMessage request(String subject, byte[] value) {
    return request(subject, value, RequestOptions.DEFAULT);
  }

  /**
   * Sends a request with a UTF-8 text payload and waits for the first response.
   *
   * @param subject the subject
   * @param value the payload
   * @return the response
   */
  public CoreMessage request(String subject, String value) {
    return request(subject, value.getBytes(StandardCharsets.UTF_8), RequestOptions.DEFAULT);
  }

  /**
   * Sends a request and waits for the first response. All requests on a
   * connection share one inbox subscription ({@code _INBOX.<random>.*}), set
   * up by the first request.
   *
   * @param subject the subject
   * @param value the payload
   * @param options timeout and headers
   * @return the response
   * @throws ServerException 404 at once when nobody is subscribed to {@code subject}
   * @throws RequestTimeoutException when no response arrives in time
   */
  public CoreMessage request(String subject, byte[] value, RequestOptions options) {
    return Futures.await(requestInternal(subject, value, options));
  }

  /**
   * Sends a request.
   *
   * @param subject the subject
   * @param value the payload
   * @param options timeout and headers
   * @return completes with the first response
   */
  public CompletableFuture<CoreMessage> requestAsync(String subject, byte[] value, RequestOptions options) {
    return pub(requestInternal(subject, value, options));
  }

  private CompletableFuture<CoreMessage> requestInternal(String subject, byte[] value, RequestOptions options) {
    long timeoutMs = options.timeout() == null ? requestTimeoutMs : options.timeout().toMillis();
    return ensureInbox().thenCompose(prefix -> {
      String token = Long.toString(nextReply.getAndIncrement());
      PendingReply p = new PendingReply();
      replies.put(token, p);
      try {
        p.timer = sched.schedule(() -> {
          if (replies.remove(token, p)) {
            p.future.completeExceptionally(
                new RequestTimeoutException("request to " + subject + " timed out after " + timeoutMs + " ms"));
          }
        }, timeoutMs, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        replies.remove(token, p);
        return Futures.<CoreMessage>failed(new ConnectionException("client is closed"));
      }
      call(new Request.CorePublish(subject, prefix + "." + token, options.headers(), value), Response.Ok.class,
          timeoutMs, null).whenComplete((r, e) -> {
            if (e != null && replies.remove(token, p)) {
              cancel(p.timer);
              p.future.completeExceptionally(Futures.unwrap(e));
            }
          });
      return p.future;
    });
  }

  /** Subscribes this connection's inbox if needed; completes with its subject prefix. */
  private CompletableFuture<String> ensureInbox() {
    Inbox ib;
    synchronized (lock) {
      ib = inbox;
      if (ib == null) {
        byte[] rnd = new byte[12];
        RANDOM.nextBytes(rnd);
        StringBuilder hex = new StringBuilder();
        for (byte b : rnd) {
          hex.append(String.format("%02x", b & 0xff));
        }
        Inbox created = new Inbox(INBOX_PREFIX + "." + hex);
        inbox = created;
        ib = created;
        SubscriptionSink sink = new SubscriptionSink() {
          @Override
          public void onSubscribed(Connection c, int id) {
            boolean current;
            synchronized (lock) {
              current = inbox == created;
              if (current) {
                created.conn = c;
                created.subId = id;
              }
            }
            if (!current) {
              // Replaced (connection lost) while subscribing: release it.
              c.removeSub(id);
              c.send(new Request.Unsubscribe(id));
            }
          }

          @Override
          public void onCoreMsg(Response.CoreMsg m) {
            onReply(m);
          }

          @Override
          public void onEnded(int code, String message) {
            synchronized (lock) {
              if (inbox != created) {
                return;
              }
            }
            dropInbox(new ServerException(code, message, null));
          }
        };
        created.ready = call(new Request.CoreSubscribe(created.prefix + ".*", null), Response.SubscribeOk.class,
            requestTimeoutMs, sink).handle((r, e) -> {
              if (e != null) {
                synchronized (lock) {
                  if (inbox == created) {
                    inbox = null; // the next request tries again
                  }
                }
                throw new CompletionException(Futures.unwrap(e));
              }
              return null;
            });
      }
    }
    String prefix = ib.prefix;
    return ib.ready.thenApply(v -> prefix);
  }

  private void onReply(Response.CoreMsg m) {
    String subject = m.subject();
    String token = subject.substring(subject.lastIndexOf('.') + 1);
    PendingReply p = replies.remove(token);
    if (p == null) {
      return; // late (timed out) or duplicate response
    }
    cancel(p.timer);
    p.future.complete(new CoreMessage(m, host));
  }

  /** Forgets the inbox (the next request subscribes a new one) and fails the requests waiting on it. */
  private void dropInbox(ExspeedException err) {
    Inbox ib;
    synchronized (lock) {
      ib = inbox;
      inbox = null;
    }
    if (ib != null && ib.conn != null && !ib.conn.isClosed()) {
      ib.conn.removeSub(ib.subId);
    }
    for (String token : new ArrayList<>(replies.keySet())) {
      PendingReply p = replies.remove(token);
      if (p != null) {
        cancel(p.timer);
        p.future.completeExceptionally(err);
      }
    }
  }

  // ---- key-value buckets ------------------------------------------------------------

  /**
   * A handle to the key-value bucket {@code bucket} (create it with {@link KvBucket#create()}).
   *
   * @param bucket the bucket name
   * @return the handle
   */
  public KvBucket kv(String bucket) {
    return new KvBucket(host, bucket);
  }

  // ---- plumbing -----------------------------------------------------------------------

  private CompletableFuture<Response> rawRequest(Request req, long timeoutMs, SubscriptionSink sink) {
    State s = state;
    if (s == State.CLOSED) {
      return Futures.failed(new ConnectionException("client is closed"));
    }
    if (s == State.RECONNECTING) {
      return Futures.failed(new ConnectionException("not connected (reconnecting)"));
    }
    return conn.request(req, timeoutMs, sink);
  }

  private <T extends Response> CompletableFuture<T> call(Request req, Class<T> type) {
    return call(req, type, requestTimeoutMs, null);
  }

  private <T extends Response> CompletableFuture<T> call(Request req, Class<T> type, long timeoutMs,
      SubscriptionSink sink) {
    return rawRequest(req, timeoutMs, sink).thenApply(r -> {
      if (!type.isInstance(r)) {
        throw new ProtocolException("unexpected reply to " + req.typeName() + ": " + r.typeName());
      }
      return type.cast(r);
    });
  }

  private CompletableFuture<Void> ok(Request req) {
    return call(req, Response.Ok.class).thenApply(r -> null);
  }

  private CompletableFuture<Object> json(Request req) {
    return call(req, Response.Json.class).thenApply(j -> {
      try {
        return Json.parse(new String(j.json(), StandardCharsets.UTF_8));
      } catch (ProtocolException e) {
        throw new ProtocolException("bad JSON in reply to " + req.typeName() + ": " + e.getMessage());
      }
    });
  }

  /** A future completed on the callback executor, never on an I/O thread. */
  private <T> CompletableFuture<T> pub(CompletableFuture<T> f) {
    CompletableFuture<T> out = new CompletableFuture<>();
    f.whenComplete((v, e) -> {
      Runnable complete = () -> {
        if (e != null) {
          out.completeExceptionally(Futures.unwrap(e));
        } else {
          out.complete(v);
        }
      };
      try {
        callbacks.execute(complete);
      } catch (RejectedExecutionException r) {
        complete.run();
      }
    });
    return out;
  }

  private void fire(java.util.function.Consumer<ClientListener> event) {
    if (listeners.isEmpty()) {
      return;
    }
    Runnable run = () -> {
      for (ClientListener l : listeners) {
        try {
          event.accept(l);
        } catch (RuntimeException ignored) {
          // a listener's failure must not break the client
        }
      }
    };
    try {
      callbacks.execute(run);
    } catch (RejectedExecutionException e) {
      run.run();
    }
  }

  private static void cancel(ScheduledFuture<?> t) {
    if (t != null) {
      t.cancel(false);
    }
  }

  // ---- connection loss and reconnection ---------------------------------------------

  private final Connection.Handlers handlers = new Connection.Handlers() {
    @Override
    public void onClose(Connection c, Throwable err) {
      onConnectionLost(c, err);
    }

    @Override
    public void onAsyncError(ExspeedException err) {
      fire(l -> l.onError(err));
    }
  };

  private void onConnectionLost(Connection c, Throwable err) {
    ReconnectOptions r = opts.reconnect();
    synchronized (lock) {
      if (state != State.CONNECTED || c != conn) {
        return;
      }
      state = r == null ? State.CLOSED : State.RECONNECTING;
    }
    // Responses to the inbox can't arrive any more; a request after the
    // reconnect subscribes a new inbox.
    dropInbox(new ConnectionException("connection lost: " + err.getMessage()));
    if (r == null) {
      endAll(ErrorCode.UNAVAILABLE, "connection closed");
      sched.shutdownNow();
      fire(l -> l.onClose(err));
      return;
    }
    for (Subscription s : subs) {
      s.suspend();
    }
    for (CoreSubscription s : coreSubs) {
      s.suspend();
    }
    fire(l -> l.onDisconnect(err));
    Thread t = new Thread(() -> reconnectLoop(r, c), "exspeed-reconnect");
    t.setDaemon(true);
    synchronized (lock) {
      if (state != State.RECONNECTING) {
        return;
      }
      reconnectThread = t;
    }
    t.start();
  }

  private void endAll(int code, String message) {
    for (Subscription s : new ArrayList<>(subs)) {
      s.end(new EndReason(code, message), false);
    }
    for (CoreSubscription s : new ArrayList<>(coreSubs)) {
      s.end(new EndReason(code, message), false);
    }
  }

  private void reconnectLoop(ReconnectOptions r, Connection lost) {
    Throwable lastErr = new ConnectionException("connection lost");
    long initial = r.initialDelay().toMillis();
    long max = r.maxDelay().toMillis();
    for (int attempt = 1; attempt <= r.maxAttempts(); attempt++) {
      long delay = attempt > 30 ? max : Math.min(max, initial * (1L << (attempt - 1)));
      long jittered = (long) (delay * (0.75 + ThreadLocalRandom.current().nextDouble() * 0.5));
      try {
        Thread.sleep(jittered);
      } catch (InterruptedException e) {
        return; // closed meanwhile
      }
      if (state != State.RECONNECTING) {
        return;
      }
      Connection c;
      try {
        ServerInfo old = lost.info();
        c = openLeader(old == null ? null : old.leader());
      } catch (ServerException e) {
        lastErr = e;
        // A rejected credential won't get better by retrying.
        if (e.code() == ErrorCode.UNAUTHORIZED || e.code() == ErrorCode.FORBIDDEN) {
          break;
        }
        continue;
      } catch (RuntimeException e) {
        lastErr = e;
        continue;
      }
      boolean installed;
      synchronized (lock) {
        installed = state == State.RECONNECTING;
        if (installed) {
          conn = c;
          state = State.CONNECTED;
          reconnectThread = null;
        }
      }
      if (!installed) {
        c.close();
        return;
      }
      restore(c);
      if (conn == c && state == State.CONNECTED) {
        ServerInfo info = c.info();
        fire(l -> l.onReconnect(info));
      }
      return;
    }
    synchronized (lock) {
      if (state != State.RECONNECTING) {
        return;
      }
      state = State.CLOSED;
      reconnectThread = null;
    }
    String msg = "connection lost: " + lastErr.getMessage();
    endAll(ErrorCode.UNAVAILABLE, msg);
    sched.shutdownNow();
    Throwable cause = lastErr;
    fire(l -> l.onClose(cause));
  }

  /** Re-creates ephemeral consumers, then re-subscribes every live subscription (consumer and core). */
  private void restore(Connection c) {
    List<ConsumerSpec> specs;
    synchronized (ephemeral) {
      specs = new ArrayList<>(ephemeral.values());
    }
    for (ConsumerSpec spec : specs) {
      try {
        Futures.await(c.request(new Request.CreateConsumer(spec.toJson()), requestTimeoutMs, null));
      } catch (ConnectionException e) {
        return; // lost again; the next attempt retries
      } catch (RuntimeException e) {
        // keep going: the re-subscribe reports the problem
      }
    }
    List<CompletableFuture<Void>> all = new ArrayList<>();
    for (Subscription sub : new ArrayList<>(subs)) {
      all.add(c.request(new Request.Subscribe(sub.consumer(), sub.window()), requestTimeoutMs, sub.sink)
          .handle((resp, e) -> {
            if (e != null) {
              Throwable t = Futures.unwrap(e);
              if (!(t instanceof ConnectionException)) {
                int code = t instanceof ServerException se ? se.code() : ErrorCode.INTERNAL;
                sub.end(new EndReason(code, String.valueOf(t.getMessage())), false);
              }
            }
            return null;
          }));
    }
    for (CoreSubscription sub : new ArrayList<>(coreSubs)) {
      all.add(c.request(new Request.CoreSubscribe(sub.subject(), sub.queue()), requestTimeoutMs, sub.sink)
          .handle((resp, e) -> {
            if (e != null) {
              Throwable t = Futures.unwrap(e);
              if (!(t instanceof ConnectionException)) {
                int code = t instanceof ServerException se ? se.code() : ErrorCode.INTERNAL;
                sub.end(new EndReason(code, String.valueOf(t.getMessage())), true);
              }
            }
            return null;
          }));
    }
    CompletableFuture.allOf(all.toArray(new CompletableFuture<?>[0])).join();
  }

  private static String[] parseAddr(String addr, int fallbackPort) {
    int i = addr.lastIndexOf(':');
    if (i <= 0 || (addr.startsWith("[") && addr.lastIndexOf(']') > i)) {
      return new String[] {addr.replaceAll("^\\[|\\]$", ""), Integer.toString(fallbackPort)};
    }
    String host = addr.substring(0, i).replaceAll("^\\[|\\]$", "");
    String port = addr.substring(i + 1);
    try {
      Integer.parseInt(port);
    } catch (NumberFormatException e) {
      port = Integer.toString(fallbackPort);
    }
    return new String[] {host, port};
  }

  /**
   * Opens a connection to the cluster leader. Without seed servers this is a
   * plain connect to {@code host:port}, except that a node naming another node
   * as leader in its handshake is followed. With seeds, each candidate (the
   * last known leader first) is asked whether it leads; leader hints are
   * followed, and a follower is accepted only when no node claims to lead.
   */
  private Connection openLeader(String hint) {
    List<String> servers = opts.servers();
    ArrayDeque<String> queue = new ArrayDeque<>();
    if (hint != null) {
      queue.add(hint);
    }
    if (!servers.isEmpty()) {
      queue.addAll(servers);
    } else {
      queue.add(opts.host() + ":" + opts.port());
    }
    Set<String> tried = new HashSet<>();
    Connection fallback = null;
    RuntimeException lastErr = null;
    while (!queue.isEmpty()) {
      String addr = queue.poll();
      if (!tried.add(addr)) {
        continue;
      }
      String[] hp = parseAddr(addr, opts.port());
      Connection c;
      try {
        c = Connection.open(hp[0], Integer.parseInt(hp[1]), settings, handlers, sched);
      } catch (ServerException e) {
        if (e.code() == ErrorCode.UNAUTHORIZED || e.code() == ErrorCode.FORBIDDEN) {
          if (fallback != null) {
            fallback.close();
          }
          throw e;
        }
        lastErr = e;
        continue;
      } catch (RuntimeException e) {
        lastErr = e;
        continue;
      }
      boolean isLeader = c.info().leader() == null;
      String leader = c.info().leader();
      if (!servers.isEmpty()) {
        try {
          Response resp = Futures.await(c.request(new Request.Metadata(), requestTimeoutMs, null));
          if (resp instanceof Response.Json j) {
            Metadata m = toMetadata(Json.parse(new String(j.json(), StandardCharsets.UTF_8)));
            isLeader = m.isLeader();
            leader = m.leader();
          }
        } catch (RuntimeException e) {
          // An old server without Metadata: trust the handshake.
        }
      }
      if (isLeader) {
        if (fallback != null) {
          fallback.close();
        }
        return c;
      }
      if (leader != null && !tried.contains(leader)) {
        queue.addFirst(leader);
      }
      if (fallback == null) {
        fallback = c;
      } else {
        c.close();
      }
    }
    if (fallback != null) {
      return fallback;
    }
    throw lastErr != null ? lastErr : new ConnectionException("no server reachable");
  }

  // ---- the host interface for subscriptions, messages, KV and publishers -------------

  private final class HostImpl implements Host {
    @Override
    public void ackNowait(String consumer, long offset) {
      if (state != State.CONNECTED) {
        return; // redelivered after the reconnect
      }
      Connection c = conn;
      if (c != null) {
        c.sendAck(consumer, offset);
      }
    }

    @Override
    public CompletableFuture<Void> nackInternal(String consumer, long offset, long delayMs) {
      return ExspeedClient.this.nackInternal(consumer, offset, delayMs);
    }

    @Override
    public CompletableFuture<Void> termInternal(String consumer, long offset, String reason) {
      return ok(new Request.Term(consumer, offset, reason));
    }

    @Override
    public CompletableFuture<Void> inProgressInternal(String consumer, long[] offsets) {
      return ok(new Request.InProgress(consumer, offsets));
    }

    @Override
    public <T> CompletableFuture<T> publicFuture(CompletableFuture<T> f) {
      return pub(f);
    }

    @Override
    public void forget(Subscription sub) {
      subs.remove(sub);
    }

    @Override
    public void forgetCore(CoreSubscription sub) {
      coreSubs.remove(sub);
    }

    @Override
    public long requestTimeoutMs() {
      return requestTimeoutMs;
    }

    @Override
    public CompletableFuture<Void> publishCoreInternal(String subject, String replyTo, List<Header> headers,
        byte[] value) {
      return ExspeedClient.this.publishCoreInternal(subject, replyTo, headers, value);
    }

    @Override
    public CompletableFuture<Response> rawRequest(Request req, long timeoutMs) {
      return ExspeedClient.this.rawRequest(req, timeoutMs, null);
    }

    @Override
    public CompletableFuture<Response.ReadResult> readWire(String stream, long from, int maxRecords, long waitMs,
        String filter) {
      return ExspeedClient.this.readWire(stream, from, maxRecords, 0, waitMs, filter);
    }
  }
}
