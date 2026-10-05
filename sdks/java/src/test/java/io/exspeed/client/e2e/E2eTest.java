package io.exspeed.client.e2e;

import static io.exspeed.client.e2e.TestServer.eventually;
import static io.exspeed.client.e2e.TestServer.uniq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.AckPolicy;
import io.exspeed.client.ConsumerInfo;
import io.exspeed.client.ConsumerSpec;
import io.exspeed.client.DeliverPolicy;
import io.exspeed.client.ExspeedClient;
import io.exspeed.client.Message;
import io.exspeed.client.Metadata;
import io.exspeed.client.MsgId;
import io.exspeed.client.PublishRecord;
import io.exspeed.client.PublishResult;
import io.exspeed.client.Publisher;
import io.exspeed.client.PublisherOptions;
import io.exspeed.client.PullOptions;
import io.exspeed.client.QueryResult;
import io.exspeed.client.ReadOptions;
import io.exspeed.client.ReadResult;
import io.exspeed.client.SeekTarget;
import io.exspeed.client.ServerException;
import io.exspeed.client.StreamInfo;
import io.exspeed.client.StreamRecord;
import io.exspeed.client.StreamSpec;
import io.exspeed.client.Subscription;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** End-to-end tests against a real exspeed server; skipped when no binary is available. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2eTest {
  TestServer server;
  ExspeedClient client;

  @BeforeAll
  void start() throws Exception {
    TestServer.assumeAvailable();
    server = TestServer.start();
    client = server.connect(b -> b.clientId("e2e"));
  }

  @AfterAll
  void stop() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.close();
    }
  }

  /** A fresh stream (and its name). */
  String stream(String prefix) {
    String name = uniq(prefix);
    client.createStream(name);
    return name;
  }

  static List<Long> offsets(List<? extends StreamRecord> recs) {
    return recs.stream().map(StreamRecord::offset).toList();
  }

  static List<Long> range(int n) {
    List<Long> out = new ArrayList<>();
    for (long i = 0; i < n; i++) {
      out.add(i);
    }
    return out;
  }

  void publishN(String s, int n) {
    List<PublishRecord> recs = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      recs.add(PublishRecord.builder("work.item").jsonValue(Map.of("i", i)).build());
    }
    client.publishBatch(s, recs);
  }

  @Nested
  class Basics {
    @Test
    void pingsAndReportsMetadata() {
      assertFalse(client.ping().isNegative());
      Metadata md = client.metadata();
      assertTrue(md.isLeader());
      assertEquals(client.serverInfo().serverVersion(), md.serverVersion());
      assertEquals(client.serverInfo().nodeId(), md.nodeId());
    }

    @Test
    void managesStreamsIdempotentCreateConflictInfoListUpdateDelete() {
      String name = uniq("admin");
      client.createStream(StreamSpec.builder(name).maxAgeSecs(3600).build());
      client.createStream(StreamSpec.builder(name).maxAgeSecs(3600).build()); // same settings: ok
      ServerException conflict = assertThrows(ServerException.class,
          () -> client.createStream(StreamSpec.builder(name).maxAgeSecs(60).build()));
      assertEquals(409, conflict.code());

      client.publish(name, PublishRecord.builder("admin.created").jsonValue(Map.of("n", 1)).build());
      StreamInfo info = client.streamInfo(name);
      assertEquals(name, info.name());
      assertEquals(0, info.earliestOffset());
      assertEquals(1, info.nextOffset());
      assertEquals(1, info.records());
      assertFalse(info.internal());
      assertEquals(3600, info.config().maxAgeSecs());
      assertTrue(client.listStreams().stream().anyMatch(s -> s.name().equals(name)));

      client.updateStream(StreamSpec.builder(name).maxAgeSecs(7200).build());
      assertEquals(7200, client.streamInfo(name).config().maxAgeSecs());

      String c = uniq("admin-c");
      client.createConsumer(ConsumerSpec.of(c, name));
      ServerException busy = assertThrows(ServerException.class, () -> client.deleteStream(name));
      assertEquals(409, busy.code());
      assertEquals(Map.of("consumers", List.of(c)), busy.detail());
      client.deleteConsumer(c);
      client.deleteStream(name);
      assertEquals(404, assertThrows(ServerException.class, () -> client.streamInfo(name)).code());
    }

    @Test
    void rejectsInternalStreamNamesAndUnknownStreams() {
      assertEquals(403, assertThrows(ServerException.class, () -> client.createStream("__nope")).code());
      assertEquals(404, assertThrows(ServerException.class,
          () -> client.publish(uniq("missing"), "x.y", "v")).code());
    }

    @Test
    void worksAsynchronously() throws Exception {
      String s = uniq("async");
      client.createStreamAsync(StreamSpec.of(s)).get(5, TimeUnit.SECONDS);
      PublishResult r = client.publishAsync(s, PublishRecord.of("a.b", "x")).get(5, TimeUnit.SECONDS);
      assertEquals(0, r.offset());
      ReadResult page = client.readAsync(s, ReadOptions.DEFAULT).get(5, TimeUnit.SECONDS);
      assertEquals("x", page.records().get(0).text());
    }
  }

  @Nested
  class PublishingAndReading {
    @Test
    void publishesAndReadsBackWithSubjectFilters() {
      String s = stream("orders");
      List<String> subjects =
          List.of("orders.placed", "orders.shipped", "orders.eu.placed", "payments.done", "orders.placed");
      for (int i = 0; i < subjects.size(); i++) {
        PublishResult r = client.publish(s, PublishRecord.builder(subjects.get(i)).jsonValue(Map.of("i", i))
            .key("k" + i).header("x-index", Integer.toString(i)).build());
        assertEquals(new PublishResult(i, false), r);
      }

      ReadResult all = client.read(s);
      assertEquals(subjects, all.records().stream().map(StreamRecord::subject).toList());
      assertEquals(5, all.nextOffset());
      assertEquals(5, all.highWatermark());
      StreamRecord first = all.records().get(0);
      assertEquals(Map.of("i", 0L), first.json());
      assertEquals("k0", first.keyText());
      assertEquals("0", first.header("x-index"));
      assertTrue(first.timestamp().isAfter(Instant.now().minusSeconds(60)));

      assertEquals(List.of(0L, 1L, 4L), offsets(client.read(s, ReadOptions.builder().filter("orders.*").build())
          .records()));
      assertEquals(List.of(0L, 1L, 2L, 4L),
          offsets(client.read(s, ReadOptions.builder().filter("orders.>").build()).records()));
      ReadResult page = client.read(s, ReadOptions.builder().from(1).maxRecords(2).build());
      assertEquals(List.of(1L, 2L), offsets(page.records()));
      assertEquals(3, page.nextOffset());

      ServerException bad = assertThrows(ServerException.class,
          () -> client.read(s, ReadOptions.builder().filter("orders.>.x").build()));
      assertEquals(400, bad.code());
    }

    @Test
    void longPollsAReadUntilNewDataArrives() throws Exception {
      String s = stream("s");
      long start = System.nanoTime();
      CompletableFuture<ReadResult> pending =
          client.readAsync(s, ReadOptions.builder().waitTime(Duration.ofSeconds(5)).build());
      Thread.sleep(200);
      client.publish(s, "late.arrival", "hello");
      ReadResult r = pending.get(10, TimeUnit.SECONDS);
      assertEquals(List.of("hello"), r.records().stream().map(StreamRecord::text).toList());
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(4));
    }

    @Test
    void publishesBatchesAndDeduplicatesByMsgId() {
      String s = stream("s");
      String m1 = MsgId.newMsgId();
      String m2 = MsgId.newMsgId();
      String m3 = MsgId.newMsgId();
      List<PublishResult> first = client.publishBatch(s, List.of(
          PublishRecord.builder("orders.placed").value("{\"id\":1}").msgId(m1).build(),
          PublishRecord.builder("orders.placed").value("{\"id\":2}").msgId(m2).build()));
      assertEquals(List.of(new PublishResult(0, false), new PublishResult(1, false)), first);
      List<PublishResult> retry = client.publishBatch(s, List.of(
          PublishRecord.builder("orders.placed").value("{\"id\":1}").msgId(m1).build(),
          PublishRecord.builder("orders.placed").value("{\"id\":3}").msgId(m3).build()));
      assertEquals(List.of(new PublishResult(0, true), new PublishResult(2, false)), retry);
      assertEquals(new PublishResult(1, true),
          client.publish(s, PublishRecord.builder("orders.placed").value("{\"id\":2}").msgId(m2).build()));

      ServerException reused = assertThrows(ServerException.class,
          () -> client.publish(s, PublishRecord.builder("orders.placed").value("{\"id\":99}").msgId(m1).build()));
      assertEquals(409, reused.code());
      assertEquals(Map.of("stored_offset", 0L), reused.detail());
      assertEquals(3, client.streamInfo(s).nextOffset());
    }

    @Test
    void keepsTheCoalescingPublishersRecordsInCallOrder() throws Exception {
      String s = stream("s");
      int n = 1000;
      List<CompletableFuture<PublishResult>> results = new ArrayList<>();
      try (Publisher p = client.publisher(PublisherOptions.DEFAULT.withMaxBatchRecords(64))) {
        for (int i = 0; i < n; i++) {
          results.add(p.publishAsync(s, PublishRecord.builder("seq.value").jsonValue(Map.of("i", i)).build()));
        }
        for (int i = 0; i < n; i++) {
          assertEquals(i, results.get(i).get(10, TimeUnit.SECONDS).offset());
        }
      }
      List<Long> seen = new ArrayList<>();
      long from = 0;
      while (seen.size() < n) {
        ReadResult r = client.read(s, ReadOptions.builder().from(from).maxRecords(500).build());
        for (StreamRecord rec : r.records()) {
          seen.add((Long) ((Map<?, ?>) rec.json()).get("i"));
        }
        from = r.nextOffset();
      }
      assertEquals(range(n), seen);
    }
  }

  @Nested
  class Consumers {
    @Test
    void createsAConsumerSubscribesReceivesAndAcks() throws Exception {
      String s = stream("s");
      String c = uniq("billing");
      ConsumerInfo info = client.createConsumer(ConsumerSpec.builder(c, s).filterSubjects("work.>").build());
      assertEquals(c, info.spec().name());
      assertEquals(s, info.spec().stream());
      assertEquals(List.of("work.>"), info.spec().filterSubjects());
      assertEquals(DeliverPolicy.ALL, info.spec().deliver());
      assertEquals(AckPolicy.EXPLICIT, info.spec().ack());
      // Idempotent for the same spec; 409 for a different one.
      client.createConsumer(ConsumerSpec.builder(c, s).filterSubjects("work.>").build());
      assertEquals(409, assertThrows(ServerException.class, () -> client.createConsumer(ConsumerSpec.of(c, s))).code());
      assertEquals(List.of(c), client.listConsumers(s).stream().map(i -> i.spec().name()).toList());

      publishN(s, 3);
      client.publish(s, "other.thing", "filtered out");
      List<Message> got = new ArrayList<>();
      try (Subscription sub = client.subscribe(c, 10)) {
        for (Message m : sub) {
          got.add(m);
          m.ack();
          if (got.size() == 3) {
            break;
          }
        }
        sub.close();
        assertEquals(0, sub.endReason().code());
      }
      for (int i = 0; i < 3; i++) {
        assertEquals(i, got.get(i).offset());
        assertEquals(1, got.get(i).deliveryCount());
        assertEquals(Map.of("i", (long) i), got.get(i).json());
      }
      ConsumerInfo after = eventually(() -> {
        ConsumerInfo i = client.consumerInfo(c);
        return i.numUnacked() == 0 && i.stats().acked() == 3 ? i : null;
      });
      assertTrue(after.ackFloor() >= 3);
    }

    @Test
    void neverPushesMoreThanTheCreditWindowAndContinuesAsMessagesAreConsumed() throws Exception {
      String s = stream("s");
      String c = uniq("credit");
      client.createConsumer(ConsumerSpec.of(c, s));
      publishN(s, 50);
      try (Subscription sub = client.subscribe(c, 4)) {
        eventually(() -> sub.buffered() == 4);
        Thread.sleep(300);
        assertEquals(4, sub.buffered()); // nothing beyond the window
        List<Long> offs = new ArrayList<>();
        while (offs.size() < 50) {
          Message m = sub.next(Duration.ofSeconds(5));
          assertNotNull(m);
          assertTrue(sub.buffered() <= 4);
          offs.add(m.offset());
          m.ack();
        }
        assertEquals(range(50), offs);
      }
    }

    @Test
    void redeliversANackedMessageWithAHigherDeliveryCount() {
      String s = stream("s");
      String c = uniq("nack");
      client.createConsumer(ConsumerSpec.of(c, s));
      client.publish(s, "work.item", "retry me");
      try (Subscription sub = client.subscribe(c, 10)) {
        Message first = sub.next(Duration.ofSeconds(5));
        assertEquals(1, first.deliveryCount());
        first.nack();
        Message second = sub.next(Duration.ofSeconds(5));
        assertEquals(first.offset(), second.offset());
        assertEquals(2, second.deliveryCount());
        second.nack(Duration.ofMillis(200));
        long t = System.nanoTime();
        Message third = sub.next(Duration.ofSeconds(5));
        assertEquals(3, third.deliveryCount());
        assertTrue(System.nanoTime() - t >= TimeUnit.MILLISECONDS.toNanos(150));
        client.ack(c, third.offset()); // confirmed ack
        assertEquals(0, client.consumerInfo(c).numUnacked());
      }
    }

    @Test
    void sharesWorkBetweenTwoClientsOnOneConsumerEachRecordExactlyOnce() throws Exception {
      String s = stream("s");
      String c = uniq("shared");
      client.createConsumer(ConsumerSpec.of(c, s));
      try (ExspeedClient other = server.connect(b -> b.clientId("e2e-2"))) {
        Subscription subA = client.subscribe(c, 8);
        Subscription subB = other.subscribe(c, 8);
        List<Long> a = new CopyOnWriteArrayList<>();
        List<Long> b = new CopyOnWriteArrayList<>();
        List<Integer> counts = new CopyOnWriteArrayList<>();
        CompletableFuture<?> doneA = subA.listen(m -> {
          counts.add(m.deliveryCount());
          a.add(m.offset());
          m.ack();
          sleepQuietly(1); // let the other subscriber get a share
        });
        CompletableFuture<?> doneB = subB.listen(m -> {
          counts.add(m.deliveryCount());
          b.add(m.offset());
          m.ack();
          sleepQuietly(1);
        });
        int total = 200;
        publishN(s, total);
        eventually(() -> a.size() + b.size() >= total, 15_000);
        Thread.sleep(200); // anything extra would show up now
        subA.close();
        subB.close();
        doneA.get(5, TimeUnit.SECONDS);
        doneB.get(5, TimeUnit.SECONDS);
        assertFalse(a.isEmpty());
        assertFalse(b.isEmpty());
        List<Long> all = new ArrayList<>(a);
        all.addAll(b);
        Collections.sort(all);
        assertEquals(range(total), all);
        assertTrue(counts.stream().allMatch(x -> x == 1));
      }
    }

    @Test
    void deadLettersAfterMaxDeliverAndImmediatelyOnTerm() throws Exception {
      String s = stream("s");
      String dlq = stream("dlq");
      String c = uniq("dlq-c");
      client.createConsumer(ConsumerSpec.builder(c, s).maxDeliver(2).dlqStream(dlq).build());
      client.publish(s, "work.poison", "bad");
      client.publish(s, "work.terminal", "worse");

      Message m1 = client.pull(c, PullOptions.of(1, Duration.ofSeconds(2))).get(0);
      assertEquals(1, m1.deliveryCount());
      m1.nack();
      Message m2 = client.pull(c, PullOptions.of(1, Duration.ofSeconds(2))).get(0);
      assertEquals(0, m2.offset());
      assertEquals(2, m2.deliveryCount());
      m2.nack();

      Message t = client.pull(c, PullOptions.of(1, Duration.ofSeconds(2))).get(0);
      assertEquals(1, t.offset());
      t.term("cannot parse");

      List<StreamRecord> dead = eventually(() -> {
        ReadResult r = client.read(dlq);
        return r.records().size() == 2 ? r.records() : null;
      });
      assertEquals(List.of("bad", "worse"), dead.stream().map(StreamRecord::text).toList());
      assertEquals(c, dead.get(0).header("exspeed-dlq-origin"));
      assertEquals(s, dead.get(0).header("exspeed-dlq-stream"));
      assertEquals("0", dead.get(0).header("exspeed-dlq-original-offset"));
      assertEquals("2", dead.get(0).header("exspeed-dlq-deliveries"));
      assertEquals("max_deliver", dead.get(0).header("exspeed-dlq-cause"));
      assertEquals("1", dead.get(1).header("exspeed-dlq-original-offset"));
      assertTrue(dead.get(1).header("exspeed-dlq-reason").contains("cannot parse"));
      assertEquals("rejected", dead.get(1).header("exspeed-dlq-cause"));
      assertEquals(2, client.consumerInfo(c).stats().deadLettered());
      assertEquals(List.of(), client.pull(c, PullOptions.of(100, Duration.ofMillis(300))));
    }

    @Test
    void longPollsAPullUntilAMessageArrivesAndTimesOutEmpty() throws Exception {
      String s = stream("s");
      String c = uniq("pull");
      client.createConsumer(ConsumerSpec.of(c, s));

      long t0 = System.nanoTime();
      assertEquals(List.of(), client.pull(c, PullOptions.of(100, Duration.ofMillis(300))));
      assertTrue(System.nanoTime() - t0 >= TimeUnit.MILLISECONDS.toNanos(250));

      long t1 = System.nanoTime();
      CompletableFuture<List<Message>> pending = client.pullAsync(c, PullOptions.of(10, Duration.ofSeconds(10)));
      Thread.sleep(200);
      client.publish(s, "work.item", "now");
      List<Message> msgs = pending.get(10, TimeUnit.SECONDS);
      assertEquals(List.of("now"), msgs.stream().map(Message::text).toList());
      assertTrue(System.nanoTime() - t1 < TimeUnit.SECONDS.toNanos(5));
      // A long pull doesn't block other requests on the same connection.
      CompletableFuture<List<Message>> slow = client.pullAsync(c, PullOptions.of(100, Duration.ofSeconds(1)));
      assertTrue(client.ping().toMillis() < 500);
      slow.get(5, TimeUnit.SECONDS);
      msgs.get(0).ack();
    }

    @Test
    void seeksAConsumerToAnOffsetTheStartTheEndAndATime() {
      String s = stream("s");
      String c = uniq("seek");
      client.createConsumer(ConsumerSpec.builder(c, s).ack(AckPolicy.NONE).build());
      publishN(s, 5);
      java.util.function.Supplier<List<Long>> offs =
          () -> offsets(client.pull(c, PullOptions.of(100, Duration.ofMillis(300))));

      assertEquals(range(5), offs.get());
      client.seek(c, SeekTarget.offset(2));
      assertEquals(List.of(2L, 3L, 4L), offs.get());
      client.seek(c, SeekTarget.EARLIEST);
      assertEquals(range(5), offs.get());
      client.seek(c, SeekTarget.LATEST);
      assertEquals(List.of(), offs.get());
      client.seek(c, SeekTarget.timeMs(0));
      assertEquals(range(5), offs.get());
      client.seek(c, SeekTarget.time(Instant.now().plusSeconds(60)));
      assertEquals(List.of(), offs.get());
      assertEquals(404, assertThrows(ServerException.class, () -> client.seek(uniq("nobody"), SeekTarget.EARLIEST))
          .code());
    }

    @Test
    void removesAnEphemeralConsumerWhenItsConnectionCloses() throws Exception {
      String s = stream("s");
      String c = uniq("eph");
      ExspeedClient owner = server.connect();
      owner.createConsumer(ConsumerSpec.builder(c, s).ephemeral(true).deliver(DeliverPolicy.NEW).build());
      assertEquals(true, client.consumerInfo(c).spec().ephemeral());
      owner.close();
      ServerException err = eventually(() -> {
        try {
          client.consumerInfo(c);
          return null;
        } catch (ServerException e) {
          return e;
        }
      });
      assertEquals(404, err.code());
    }

    @Test
    void endsSubscriptionsWith404WhenTheConsumerIsDeleted() throws Exception {
      String s = stream("s");
      String c = uniq("gone");
      client.createConsumer(ConsumerSpec.of(c, s));
      Subscription sub = client.subscribe(c);
      CompletableFuture<Message> next = CompletableFuture.supplyAsync(sub::next);
      client.deleteConsumer(c);
      assertNull(next.get(5, TimeUnit.SECONDS));
      assertEquals(404, sub.endReason().code());
    }
  }

  @Test
  void runsSqlQueries() {
    String s = uniq("q").replace('-', '_');
    client.createStream(s);
    List<PublishRecord> recs = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      recs.add(PublishRecord.builder("metrics.cpu").jsonValue(Map.of("region", i == 2 ? "us" : "eu", "i", i)).build());
    }
    client.publishBatch(s, recs);
    QueryResult r = client.query("SELECT COUNT(*) AS cnt FROM \"" + s + "\"");
    assertEquals(List.of("cnt"), r.columns());
    assertEquals(List.of(List.of(3L)), r.rows());
    assertEquals(1, r.rowCount());
    assertTrue(r.executionTimeMs() >= 0);
    assertFalse(r.truncated());
    assertEquals(400, assertThrows(ServerException.class, () -> client.query("SELEKT nonsense")).code());
  }

  static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
