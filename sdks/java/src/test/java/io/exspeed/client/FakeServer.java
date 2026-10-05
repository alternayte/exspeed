package io.exspeed.client;

import io.exspeed.client.protocol.Frame;
import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/**
 * A scriptable protocol-v2 server for unit tests. Connect and Ping are
 * answered automatically unless the handler returns true for them.
 */
final class FakeServer implements AutoCloseable {
  /** Handles a request; returns true when it answered (or deliberately ignored) it. */
  interface Handler {
    boolean handle(FakeConn conn, int corr, Request req);
  }

  record Received(int corr, Request req) {}

  final List<FakeConn> conns = new CopyOnWriteArrayList<>();
  volatile Handler handler = (c, corr, r) -> false;
  private final ServerSocket server;
  private final Thread acceptor;

  private FakeServer(Handler h) throws IOException {
    if (h != null) {
      handler = h;
    }
    server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    acceptor = new Thread(() -> {
      try {
        while (true) {
          Socket s = server.accept();
          s.setTcpNoDelay(true);
          conns.add(new FakeConn(s, this));
        }
      } catch (IOException e) {
        // closed
      }
    }, "fake-server-accept");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  static FakeServer start() throws IOException {
    return new FakeServer(null);
  }

  static FakeServer start(Handler h) throws IOException {
    return new FakeServer(h);
  }

  int port() {
    return server.getLocalPort();
  }

  FakeConn last() {
    return conns.get(conns.size() - 1);
  }

  void handle(FakeConn conn, int corr, Request req) {
    if (handler.handle(conn, corr, req)) {
      return;
    }
    if (req instanceof Request.Connect) {
      conn.reply(corr, new Response.ConnectOk("test", "n1", null));
    } else if (req instanceof Request.Ping) {
      conn.reply(corr, new Response.Pong());
    }
  }

  /** Waits until {@code cond} holds. */
  void until(BooleanSupplier cond) {
    until(cond, 3000);
  }

  void until(BooleanSupplier cond, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("FakeServer.until: timed out");
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }
  }

  @Override
  public void close() {
    try {
      server.close();
    } catch (IOException ignored) {
      // closing
    }
    for (FakeConn c : conns) {
      c.destroy();
    }
    try {
      acceptor.join(1000);
      for (FakeConn c : conns) {
        c.reader.join(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** One accepted client connection. */
  static final class FakeConn {
    final Socket socket;
    final List<Received> received = new CopyOnWriteArrayList<>();
    final Thread reader;
    private final OutputStream out;

    FakeConn(Socket socket, FakeServer server) throws IOException {
      this.socket = socket;
      this.out = socket.getOutputStream();
      InputStream in = socket.getInputStream();
      reader = new Thread(() -> {
        try {
          Frame f;
          while ((f = Frame.read(in)) != null) {
            Request req;
            try {
              req = Request.decode(f.opcode(), f.payload());
            } catch (ProtocolException e) {
              reply(f.correlationId(), new Response.Error(400, e.getMessage(), null));
              continue;
            }
            received.add(new Received(f.correlationId(), req));
            server.handle(this, f.correlationId(), req);
          }
        } catch (IOException | RuntimeException e) {
          // connection gone
        }
      }, "fake-server-conn");
      reader.setDaemon(true);
      reader.start();
    }

    synchronized void reply(int corr, Response resp) {
      try {
        out.write(resp.frame(corr));
        out.flush();
      } catch (IOException e) {
        // client gone
      }
    }

    /** Writes several frames in a single write: {@code corr, response, corr, response, ...}. */
    synchronized void replyMany(Object... pairs) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      for (int i = 0; i < pairs.length; i += 2) {
        buf.writeBytes(((Response) pairs[i + 1]).frame((Integer) pairs[i]));
      }
      try {
        out.write(buf.toByteArray());
        out.flush();
      } catch (IOException e) {
        // client gone
      }
    }

    <T extends Request> List<T> reqs(Class<T> type) {
      List<T> out = new ArrayList<>();
      for (Received r : received) {
        if (type.isInstance(r.req())) {
          out.add(type.cast(r.req()));
        }
      }
      return out;
    }

    List<Received> of(Class<? extends Request> type) {
      List<Received> out = new ArrayList<>();
      for (Received r : received) {
        if (type.isInstance(r.req())) {
          out.add(r);
        }
      }
      return out;
    }

    List<String> types() {
      List<String> out = new ArrayList<>();
      for (Received r : received) {
        out.add(r.req().typeName());
      }
      return out;
    }

    void destroy() {
      try {
        socket.close();
      } catch (IOException ignored) {
        // closing
      }
    }
  }
}
