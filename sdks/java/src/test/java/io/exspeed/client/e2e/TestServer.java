package io.exspeed.client.e2e;

import io.exspeed.client.ClientOptions;
import io.exspeed.client.ExspeedClient;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Assumptions;

/**
 * Starts a real exspeed server for the end-to-end tests.
 *
 * <p>The binary comes from {@code EXSPEED_BIN}, or else {@code target/debug/exspeed}
 * at the repository root ({@code cargo build -p exspeed --bin exspeed}). When
 * neither exists the e2e tests are skipped with a message.
 */
final class TestServer implements AutoCloseable {
  static final Path REPO_ROOT = System.getProperty("exspeed.repo.root") != null
      ? Path.of(System.getProperty("exspeed.repo.root")).toAbsolutePath().normalize()
      : Path.of(System.getProperty("basedir", System.getProperty("user.dir"))).resolve("../..").normalize();
  static final Path BIN = resolveBinary();
  static final String SKIP_REASON = BIN != null ? null
      : "e2e tests skipped: set EXSPEED_BIN to an exspeed server binary, or build one with "
          + "`cargo build -p exspeed --bin exspeed` (looked for " + REPO_ROOT.resolve("target/debug/exspeed") + ")";

  private static final Set<Process> LIVE = ConcurrentHashMap.newKeySet();
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static boolean warned;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      for (Process p : LIVE) {
        p.destroyForcibly();
      }
    }));
  }

  /** Options for the server process. */
  record Options(String authToken, Path tlsCert, Path tlsKey, Path tlsClientCa) {
    static final Options PLAIN = new Options(null, null, null, null);
  }

  final int port;
  final int apiPort;
  final Path dataDir;
  final Options opts;
  private final Path logFile;
  private final Process process;

  private TestServer(int port, int apiPort, Path dataDir, Options opts, Path logFile, Process process) {
    this.port = port;
    this.apiPort = apiPort;
    this.dataDir = dataDir;
    this.opts = opts;
    this.logFile = logFile;
    this.process = process;
  }

  private static Path resolveBinary() {
    String env = System.getenv("EXSPEED_BIN");
    if (env != null && !env.isEmpty()) {
      Path p = Path.of(env).toAbsolutePath();
      if (!Files.isExecutable(p)) {
        throw new IllegalStateException("EXSPEED_BIN=" + env + " does not exist or is not executable");
      }
      return p;
    }
    Path p = REPO_ROOT.resolve("target/debug/exspeed");
    return Files.isExecutable(p) ? p : null;
  }

  /** Skips the calling test class when no server binary is available. */
  static synchronized void assumeAvailable() {
    if (BIN == null && !warned) {
      warned = true;
      System.err.println("\n[exspeed-client] " + SKIP_REASON + "\n");
    }
    Assumptions.assumeTrue(BIN != null, SKIP_REASON);
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return s.getLocalPort();
    }
  }

  static TestServer start() throws Exception {
    return start(Options.PLAIN);
  }

  static TestServer start(Options opts) throws Exception {
    return start(opts, freePort(), freePort(), Files.createTempDirectory("exspeed-java-e2e-"));
  }

  private static TestServer start(Options opts, int port, int apiPort, Path dataDir) throws Exception {
    List<String> args = new ArrayList<>(List.of(BIN.toString(), "server", "--bind", "127.0.0.1:" + port,
        "--api-bind", "127.0.0.1:" + apiPort, "--data-dir", dataDir.toString()));
    if (opts.authToken() != null) {
      args.addAll(List.of("--auth-token", opts.authToken()));
    }
    if (opts.tlsCert() != null) {
      args.addAll(List.of("--tls-cert", opts.tlsCert().toString(), "--tls-key", opts.tlsKey().toString()));
    }
    if (opts.tlsClientCa() != null) {
      args.addAll(List.of("--tls-client-ca", opts.tlsClientCa().toString()));
    }
    Path log = Files.createTempFile("exspeed-java-e2e-", ".log");
    ProcessBuilder pb = new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(log.toFile());
    // Don't let the developer's environment change the server under test.
    Map<String, String> env = pb.environment();
    env.keySet().removeIf(k -> k.startsWith("EXSPEED_"));
    env.putIfAbsent("RUST_LOG", "warn");
    Process p = pb.start();
    LIVE.add(p);
    TestServer server = new TestServer(port, apiPort, dataDir, opts, log, p);
    try {
      server.waitReady(30_000);
    } catch (Exception e) {
      String output = server.log();
      server.close();
      throw new IllegalStateException(e.getMessage() + "\n--- server log ---\n" + output, e);
    }
    return server;
  }

  String log() {
    try {
      return Files.readString(logFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "(no log: " + e + ")";
    }
  }

  private void waitReady(long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (!process.isAlive()) {
        throw new IllegalStateException("server exited with code " + process.exitValue());
      }
      if (readyz() == 200) {
        return;
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("server did not become ready");
  }

  /** GET /readyz (over HTTPS when the server runs with TLS); 0 when unreachable. */
  private int readyz() {
    boolean tls = opts.tlsCert() != null;
    try {
      URL url = new URL((tls ? "https" : "http") + "://127.0.0.1:" + apiPort + "/readyz");
      HttpURLConnection c = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
      if (c instanceof HttpsURLConnection https) {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[] {new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType) {}

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType) {}

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
        }}, new SecureRandom());
        https.setSSLSocketFactory(ctx.getSocketFactory());
        https.setHostnameVerifier((h, s) -> true);
      }
      c.setConnectTimeout(1000);
      c.setReadTimeout(1000);
      try {
        return c.getResponseCode();
      } finally {
        c.disconnect();
      }
    } catch (Exception e) {
      return 0;
    }
  }

  /** A client for this server, reconnect off unless the customizer turns it on. */
  ExspeedClient connect(Consumer<ClientOptions.Builder> customize) {
    ClientOptions.Builder b = ClientOptions.builder().port(port).reconnect(false);
    customize.accept(b);
    return ExspeedClient.connect(b.build());
  }

  ExspeedClient connect() {
    return connect(b -> {});
  }

  /** Stops the process and starts a new one on the same ports and data directory. */
  TestServer restart() throws Exception {
    kill();
    Files.deleteIfExists(logFile);
    return start(opts, port, apiPort, dataDir);
  }

  private void kill() {
    if (process.isAlive()) {
      process.destroy(); // SIGTERM: ordered shutdown
      try {
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        process.destroyForcibly();
        Thread.currentThread().interrupt();
      }
    }
    LIVE.remove(process);
  }

  /** Stops the server and deletes its data directory and log. */
  @Override
  public void close() {
    kill();
    deleteTree(dataDir);
    try {
      Files.deleteIfExists(logFile);
    } catch (IOException ignored) {
      // best effort
    }
  }

  static void deleteTree(Path dir) {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.deleteIfExists(p);
        } catch (IOException ignored) {
          // best effort
        }
      });
    } catch (IOException ignored) {
      // best effort
    }
  }

  /** A unique, valid stream/consumer name. */
  static String uniq(String prefix) {
    return prefix + "-" + ProcessHandle.current().pid() + "-" + Long.toString(System.currentTimeMillis(), 36) + "-"
        + COUNTER.getAndIncrement();
  }

  /** A supplier that may throw. */
  interface Check<T> {
    T get() throws Exception;
  }

  /** Polls {@code fn} until it returns a non-null, non-false value. */
  static <T> T eventually(Check<T> fn, long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    Exception last = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        T v = fn.get();
        if (v != null && !Boolean.FALSE.equals(v)) {
          return v;
        }
      } catch (Exception e) {
        last = e;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("eventually: timed out" + (last != null ? " (last error: " + last + ")" : ""));
  }

  static <T> T eventually(Check<T> fn) throws Exception {
    return eventually(fn, 10_000);
  }

  /** Runs openssl in {@code dir}; skips the test when openssl is missing. */
  static void openssl(Path dir, String... args) throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add("openssl");
    cmd.addAll(List.of(args));
    Process p = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
      throw new IllegalStateException("openssl " + args[0] + " failed: " + out);
    }
  }

  static boolean hasOpenssl() {
    try {
      Process p = new ProcessBuilder("openssl", "version").redirectErrorStream(true).start();
      p.getInputStream().readAllBytes();
      return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
    } catch (Exception e) {
      return false;
    }
  }
}
