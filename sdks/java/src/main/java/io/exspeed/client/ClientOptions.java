package io.exspeed.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Connection settings for {@link ExspeedClient#connect(ClientOptions)}.
 *
 * <pre>{@code
 * ClientOptions opts = ClientOptions.builder()
 *     .host("exspeed.internal")
 *     .token(System.getenv("EXSPEED_TOKEN"))
 *     .tls(TlsOptions.systemDefault())
 *     .reconnect(ReconnectOptions.DEFAULT.withMaxAttempts(30))
 *     .build();
 * }</pre>
 */
public final class ClientOptions {
  /** The default client port. */
  public static final int DEFAULT_PORT = 5933;

  private final String host;
  private final int port;
  private final List<String> servers;
  private final String token;
  private final TlsOptions tls;
  private final String clientId;
  private final Duration requestTimeout;
  private final Duration keepalive;
  private final ReconnectOptions reconnect;
  private final Executor callbackExecutor;
  private final boolean verifyCrc;
  private final List<ClientListener> listeners;

  private ClientOptions(Builder b) {
    host = b.host;
    port = b.port;
    servers = List.copyOf(b.servers);
    token = b.token;
    tls = b.tls;
    clientId = b.clientId;
    requestTimeout = b.requestTimeout;
    keepalive = b.keepalive;
    reconnect = b.reconnect;
    callbackExecutor = b.callbackExecutor != null ? b.callbackExecutor
        : CompletableFuture.completedFuture(null).defaultExecutor();
    verifyCrc = b.verifyCrc;
    listeners = List.copyOf(b.listeners);
  }

  /**
   * Starts building options.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Defaults for everything: {@code 127.0.0.1:5933}, no TLS, no token.
   *
   * @return the options
   */
  public static ClientOptions defaults() {
    return builder().build();
  }

  /**
   * The host connected to when no seed servers are given.
   *
   * @return the host
   */
  public String host() {
    return host;
  }

  /**
   * The port.
   *
   * @return the port
   */
  public int port() {
    return port;
  }

  /**
   * Cluster seed addresses ({@code host:port}).
   *
   * @return the seeds (empty when unset)
   */
  public List<String> servers() {
    return servers;
  }

  /**
   * The bearer token.
   *
   * @return the token, or {@code null}
   */
  public String token() {
    return token;
  }

  /**
   * The TLS settings.
   *
   * @return the settings, or {@code null} for plain TCP
   */
  public TlsOptions tls() {
    return tls;
  }

  /**
   * Sent in the handshake and shown in server logs.
   *
   * @return the client id
   */
  public String clientId() {
    return clientId;
  }

  /**
   * How long to wait for a response.
   *
   * @return the timeout
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /**
   * Ping interval ({@link Duration#ZERO} = disabled).
   *
   * @return the interval
   */
  public Duration keepalive() {
    return keepalive;
  }

  /**
   * Reconnection settings.
   *
   * @return the settings, or {@code null} when reconnection is off
   */
  public ReconnectOptions reconnect() {
    return reconnect;
  }

  /**
   * Where futures returned by the async methods complete, and where listener events run.
   *
   * @return the executor
   */
  public Executor callbackExecutor() {
    return callbackExecutor;
  }

  /**
   * Whether each received record's CRC32C is verified.
   *
   * @return the setting
   */
  public boolean verifyCrc() {
    return verifyCrc;
  }

  /**
   * Listeners registered at connect time.
   *
   * @return the listeners
   */
  public List<ClientListener> listeners() {
    return listeners;
  }

  /** Builds {@link ClientOptions}. */
  public static final class Builder {
    private String host = "127.0.0.1";
    private int port = DEFAULT_PORT;
    private final List<String> servers = new ArrayList<>();
    private String token;
    private TlsOptions tls;
    private String clientId = "exspeed-java";
    private Duration requestTimeout = Duration.ofSeconds(30);
    private Duration keepalive = Duration.ofSeconds(20);
    private ReconnectOptions reconnect = ReconnectOptions.DEFAULT;
    private Executor callbackExecutor;
    private boolean verifyCrc = true;
    private final List<ClientListener> listeners = new ArrayList<>();

    private Builder() {}

    /**
     * The host (default {@code 127.0.0.1}).
     *
     * @param host the host name or address
     * @return this builder
     */
    public Builder host(String host) {
      this.host = Objects.requireNonNull(host);
      return this;
    }

    /**
     * The port (default 5933).
     *
     * @param port the port
     * @return this builder
     */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /**
     * Cluster seed addresses ({@code host:port}). When set, the client connects
     * to whichever node is the leader, following the leader hints followers
     * return, and finds the new leader again after a failover. Overrides
     * {@link #host(String)} and {@link #port(int)}.
     *
     * @param servers the seeds
     * @return this builder
     */
    public Builder servers(List<String> servers) {
      this.servers.clear();
      this.servers.addAll(servers);
      return this;
    }

    /**
     * Cluster seed addresses ({@code host:port}).
     *
     * @param servers the seeds
     * @return this builder
     */
    public Builder servers(String... servers) {
      return servers(List.of(servers));
    }

    /**
     * Bearer token, when the server runs with auth.
     *
     * @param token the token
     * @return this builder
     */
    public Builder token(String token) {
      this.token = token;
      return this;
    }

    /**
     * Connect with TLS.
     *
     * @param tls the TLS settings, or {@code null} for plain TCP
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      this.tls = tls;
      return this;
    }

    /**
     * Sent in the handshake and shown in server logs (default {@code exspeed-java}).
     *
     * @param clientId the id
     * @return this builder
     */
    public Builder clientId(String clientId) {
      this.clientId = Objects.requireNonNull(clientId);
      return this;
    }

    /**
     * How long to wait for a response, on top of a pull's or read's own wait
     * (default 30 s). Also bounds connecting and the handshake.
     *
     * @param timeout the timeout
     * @return this builder
     */
    public Builder requestTimeout(Duration timeout) {
      if (timeout.isNegative() || timeout.isZero()) {
        throw new IllegalArgumentException("requestTimeout must be positive");
      }
      this.requestTimeout = timeout;
      return this;
    }

    /**
     * Ping interval (default 20 s); the server drops connections idle for
     * 120 s. {@link Duration#ZERO} disables pings.
     *
     * @param interval the interval
     * @return this builder
     */
    public Builder keepalive(Duration interval) {
      if (interval.isNegative()) {
        throw new IllegalArgumentException("keepalive must not be negative");
      }
      this.keepalive = interval;
      return this;
    }

    /**
     * Reconnect automatically when the connection drops (default on).
     *
     * @param enabled whether to reconnect, with {@link ReconnectOptions#DEFAULT}
     * @return this builder
     */
    public Builder reconnect(boolean enabled) {
      this.reconnect = enabled ? ReconnectOptions.DEFAULT : null;
      return this;
    }

    /**
     * Reconnect automatically with these settings ({@code null} = off).
     *
     * @param options the settings
     * @return this builder
     */
    public Builder reconnect(ReconnectOptions options) {
      this.reconnect = options;
      return this;
    }

    /**
     * Where futures returned by the async methods complete, and where
     * listener events and subscription callbacks' completion run (default: the
     * {@link CompletableFuture} default async executor). Futures never
     * complete on the connection's I/O threads, so blocking in a continuation
     * is safe.
     *
     * @param executor the executor
     * @return this builder
     */
    public Builder callbackExecutor(Executor executor) {
      this.callbackExecutor = executor;
      return this;
    }

    /**
     * Verify each received record's CRC32C (default true).
     *
     * @param verify the setting
     * @return this builder
     */
    public Builder verifyCrc(boolean verify) {
      this.verifyCrc = verify;
      return this;
    }

    /**
     * Registers a lifecycle listener from the start.
     *
     * @param listener the listener
     * @return this builder
     */
    public Builder listener(ClientListener listener) {
      this.listeners.add(Objects.requireNonNull(listener));
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public ClientOptions build() {
      return new ClientOptions(this);
    }
  }
}
