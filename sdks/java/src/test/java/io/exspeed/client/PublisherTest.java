package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.FakeServer.FakeConn;
import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WirePublishRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PublisherTest {
  FakeServer server;
  ExspeedClient client;
  final AtomicLong nextOffset = new AtomicLong();

  /** Auto-acknowledges publishes with increasing offsets. */
  boolean autoAck(FakeConn conn, int corr, Request req) {
    if (req instanceof Request.Publish) {
      conn.reply(corr, new Response.PublishOk(nextOffset.getAndIncrement(), false));
    }
    if (req instanceof Request.PublishBatch pb) {
      List<Response.PublishOk> rs = new ArrayList<>();
      for (int i = 0; i < pb.records().size(); i++) {
        rs.add(new Response.PublishOk(nextOffset.getAndIncrement(), false));
      }
      conn.reply(corr, new Response.PublishBatchOk(rs));
    }
    return false;
  }

  void setup(FakeServer.Handler handler) throws Exception {
    server = FakeServer.start(handler);
    client = ExspeedClient.connect(
        ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO).reconnect(false).build());
  }

  void setup() throws Exception {
    setup(this::autoAck);
  }

  @AfterEach
  void teardown() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.close();
    }
  }

  List<Request> sent() {
    List<Request> out = new ArrayList<>();
    for (FakeServer.Received r : server.last().received) {
      if (r.req() instanceof Request.Publish || r.req() instanceof Request.PublishBatch) {
        out.add(r.req());
      }
    }
    return out;
  }

  static List<String> subjects(List<Request> reqs) {
    List<String> out = new ArrayList<>();
    for (Request r : reqs) {
      if (r instanceof Request.PublishBatch pb) {
        for (WirePublishRecord x : pb.records()) {
          out.add(x.subject());
        }
      } else if (r instanceof Request.Publish p) {
        out.add(p.record().subject());
      }
    }
    return out;
  }

  static String shape(Request r) {
    if (r instanceof Request.PublishBatch pb) {
      return "PublishBatch " + pb.stream() + " " + pb.records().size();
    }
    return "Publish " + ((Request.Publish) r).stream() + " 1";
  }

  @Test
  void coalescesPublishesWithinTheBatchWindowIntoOneBatchInCallOrder() throws Exception {
    setup();
    Publisher p = client.publisher(PublisherOptions.DEFAULT.withBatchWindow(Duration.ofMillis(300)));
    List<CompletableFuture<PublishResult>> results = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      results.add(p.publishAsync("s", PublishRecord.of("n." + i, Integer.toString(i))));
    }
    for (int i = 0; i < 100; i++) {
      assertEquals(i, results.get(i).get(5, TimeUnit.SECONDS).offset());
    }
    List<Request> sent = sent();
    assertEquals(List.of("PublishBatch s 100"), sent.stream().map(PublisherTest::shape).toList());
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      expected.add("n." + i);
    }
    assertEquals(expected, subjects(sent));
  }

  @Test
  void keepsCallOrderWithTheDefaultWindowFromManyThreads() throws Exception {
    setup();
    Publisher p = client.publisher();
    List<CompletableFuture<PublishResult>> results = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      results.add(p.publishAsync("s", PublishRecord.of("n." + i, "")));
    }
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, results.get(i).get(5, TimeUnit.SECONDS).offset());
    }
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("n." + i);
    }
    assertEquals(expected, subjects(sent()));
    assertTrue(sent().size() < 1000, "records were batched");
  }

  @Test
  void sendsALoneRecordAsAPlainPublish() throws Exception {
    setup();
    PublishResult r = client.publisher().publish("s", PublishRecord.of("one", "x"));
    assertEquals(new PublishResult(0, false), r);
    assertInstanceOf(Request.Publish.class, server.last().received.get(1).req());
  }

  @Test
  void splitsByMaxBatchRecordsAndByStreamKeepingOrder() throws Exception {
    setup();
    Publisher p = client.publisher(new PublisherOptions(Duration.ofSeconds(10), 3, 4096));
    List<CompletableFuture<PublishResult>> calls = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      calls.add(p.publishAsync("a", PublishRecord.of("a." + i, "")));
    }
    for (int i = 0; i < 2; i++) {
      calls.add(p.publishAsync("b", PublishRecord.of("b." + i, "")));
    }
    calls.add(p.publishAsync("a", PublishRecord.of("a.4", "")));
    p.flush();
    for (CompletableFuture<PublishResult> c : calls) {
      c.get(5, TimeUnit.SECONDS);
    }
    List<Request> sent = sent();
    // A full queue (3 records) is flushed at once, split into same-stream runs.
    assertEquals(List.of("PublishBatch a 3", "Publish a 1", "PublishBatch b 2", "Publish a 1"),
        sent.stream().map(PublisherTest::shape).toList());
    assertEquals(List.of("a.0", "a.1", "a.2", "a.3", "b.0", "b.1", "a.4"), subjects(sent));
  }

  @Test
  void waitsForABatchWindowBeforeSending() throws Exception {
    setup();
    Publisher p = client.publisher(PublisherOptions.DEFAULT.withBatchWindow(Duration.ofMillis(300)));
    CompletableFuture<PublishResult> a = p.publishAsync("s", PublishRecord.of("x", "1"));
    Thread.sleep(20);
    CompletableFuture<PublishResult> b = p.publishAsync("s", PublishRecord.of("y", "2"));
    a.get(5, TimeUnit.SECONDS);
    b.get(5, TimeUnit.SECONDS);
    assertEquals(List.of("PublishBatch s 2"), sent().stream().map(PublisherTest::shape).toList());
  }

  @Test
  void boundsRecordsInFlightAndKeepsOrderWhileWaiting() throws Exception {
    List<Object[]> held = new CopyOnWriteArrayList<>();
    setup((conn, corr, req) -> {
      if (req instanceof Request.Publish || req instanceof Request.PublishBatch) {
        held.add(new Object[] {conn, corr, req});
      }
      return false;
    });
    Publisher p = client.publisher(PublisherOptions.DEFAULT.withMaxInFlight(2));
    List<CompletableFuture<PublishResult>> all = new CopyOnWriteArrayList<>();
    Thread producer = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        all.add(p.publishAsync("s", PublishRecord.of("n." + i, "")));
      }
    });
    producer.start();
    server.until(() -> subjects(held.stream().map(h -> (Request) h[2]).toList()).size() == 2);
    Thread.sleep(50);
    assertEquals(2, subjects(held.stream().map(h -> (Request) h[2]).toList()).size()); // the first two only
    assertEquals(2, p.pending());
    long deadline = System.currentTimeMillis() + 5000;
    while ((!held.isEmpty() || producer.isAlive() || p.pending() > 0) && System.currentTimeMillis() < deadline) {
      if (!held.isEmpty()) {
        Object[] h = held.remove(0);
        autoAck((FakeConn) h[0], (Integer) h[1], (Request) h[2]);
      }
      Thread.sleep(5);
    }
    producer.join(1000);
    for (CompletableFuture<PublishResult> f : all) {
      f.get(5, TimeUnit.SECONDS);
    }
    assertEquals(List.of("n.0", "n.1", "n.2", "n.3", "n.4"), subjects(sent()));
  }

  @Test
  void rejectsEveryRecordOfAFailedBatchWithTheServersError() throws Exception {
    setup((conn, corr, req) -> {
      if (req instanceof Request.PublishBatch) {
        conn.reply(corr, new Response.Error(403, "forbidden", null));
      }
      return false;
    });
    Publisher p = client.publisher(PublisherOptions.DEFAULT.withBatchWindow(Duration.ofMillis(200)));
    CompletableFuture<PublishResult> a = p.publishAsync("s", PublishRecord.of("a", ""));
    CompletableFuture<PublishResult> b = p.publishAsync("s", PublishRecord.of("b", ""));
    for (CompletableFuture<PublishResult> f : List.of(a, b)) {
      Throwable t = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS)).getCause();
      assertInstanceOf(ServerException.class, t);
      assertEquals(403, ((ServerException) t).code());
    }
    p.flush();
    assertEquals(0, p.pending());
  }

  @Test
  void flushWaitsForEverythingAcceptedAndCloseRejectsLaterPublishes() throws Exception {
    setup();
    Publisher p = client.publisher();
    for (int i = 0; i < 10; i++) {
      p.publishAsync("s", PublishRecord.of("x", ""));
    }
    p.flush();
    assertEquals(0, p.pending());
    p.close();
    Throwable t = assertThrows(ExecutionException.class,
        () -> p.publishAsync("s", PublishRecord.of("x", "")).get(2, TimeUnit.SECONDS)).getCause();
    assertTrue(t.getMessage().contains("closed"));
    assertThrows(ConnectionException.class, () -> p.publish("s", PublishRecord.of("x", "")));
  }
}
