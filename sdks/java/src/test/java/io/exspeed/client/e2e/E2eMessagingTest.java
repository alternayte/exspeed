package io.exspeed.client.e2e;

import static io.exspeed.client.e2e.TestServer.eventually;
import static io.exspeed.client.e2e.TestServer.uniq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.ConsumerInfo;
import io.exspeed.client.ConsumerSpec;
import io.exspeed.client.CoreMessage;
import io.exspeed.client.CorePublishOptions;
import io.exspeed.client.CoreSubscription;
import io.exspeed.client.DiscardPolicy;
import io.exspeed.client.ExspeedClient;
import io.exspeed.client.HeaderMatch;
import io.exspeed.client.KvBucket;
import io.exspeed.client.KvBucketOptions;
import io.exspeed.client.KvEntry;
import io.exspeed.client.KvOp;
import io.exspeed.client.KvPutOptions;
import io.exspeed.client.KvWatch;
import io.exspeed.client.Message;
import io.exspeed.client.PublishRecord;
import io.exspeed.client.PullOptions;
import io.exspeed.client.RequestOptions;
import io.exspeed.client.RequestTimeoutException;
import io.exspeed.client.RetentionPolicy;
import io.exspeed.client.ServerException;
import io.exspeed.client.StreamRecord;
import io.exspeed.client.StreamSpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end tests of stream limits, time headers, routing settings, core
 * pub/sub, request-reply and KV buckets against a real exspeed server.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2eMessagingTest {
  TestServer server;
  ExspeedClient client;

  @BeforeAll
  void start() throws Exception {
    TestServer.assumeAvailable();
    server = TestServer.start();
    client = server.connect(b -> b.clientId("e2e-messaging"));
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

  static List<String> texts(List<? extends StreamRecord> recs) {
    return recs.stream().map(StreamRecord::text).toList();
  }

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Nested
  class StreamLimitsAndTimeHeaders {
    @Test
    void hidesRecordsWhoseTtlHasPassedFromReadsAndConsumers() throws Exception {
      String s = uniq("ttl");
      client.createStream(StreamSpec.builder(s).allowMsgTtl(true).build());
      assertTrue(client.streamInfo(s).config().allowMsgTtl());

      client.publish(s, PublishRecord.builder("jobs.a").value("short").ttl(Duration.ofMillis(150)).build());
      client.publish(s, PublishRecord.of("jobs.a", "keep"));
      client.publish(s, PublishRecord.builder("jobs.a").value("long").ttl("1h").build());
      String c = uniq("ttl-c");
      client.createConsumer(ConsumerSpec.of(c, s));
      Thread.sleep(400);

      assertEquals(List.of("keep", "long"), texts(client.read(s).records()));
      List<Message> got = client.pull(c, PullOptions.of(10, Duration.ofMillis(500)));
      assertEquals(List.of("keep", "long"), texts(got));
      assertEquals("1h", got.get(1).header("exspeed-ttl"));
    }

    @Test
    void rejectsTimeHeadersOnAStreamThatDoesntAllowThem() {
      String s = uniq("plain");
      client.createStream(s);
      for (PublishRecord r : List.of(PublishRecord.builder("a.b").value("x").ttl(Duration.ofSeconds(1)).build(),
          PublishRecord.builder("a.b").value("x").delay(Duration.ofSeconds(1)).build())) {
        assertEquals(400, assertThrows(ServerException.class, () -> client.publish(s, r)).code());
      }
    }

    @Test
    void deliversDelayedRecordsToConsumersWhenDue() throws Exception {
      String s = uniq("delay");
      client.createStream(StreamSpec.builder(s).allowDelayed(true).build());
      String c = uniq("delay-c");
      client.createConsumer(ConsumerSpec.of(c, s));
      client.publish(s, PublishRecord.builder("later.a").value("delayed").delay(Duration.ofMillis(700)).build());
      client.publish(s, PublishRecord.builder("later.a").value("at")
          .deliverAt(Instant.now().plusMillis(900)).build());
      client.publish(s, PublishRecord.of("later.a", "now"));

      List<Message> first = client.pull(c, PullOptions.of(10, Duration.ofMillis(300)));
      assertEquals(List.of("now"), texts(first));
      client.ack(c, first.stream().mapToLong(Message::offset).toArray());
      assertEquals(2, client.consumerInfo(c).numDelayed());

      List<String> due = new ArrayList<>();
      eventually(() -> {
        for (Message m : client.pull(c, PullOptions.of(10, Duration.ofMillis(200)))) {
          due.add(m.text());
          m.ack();
        }
        return due.size() == 2;
      });
      assertEquals(List.of("delayed", "at"), due);
      // A stateless read sees every record right away: delays apply to consumers.
      assertEquals(List.of("delayed", "at", "now"), texts(client.read(s).records()));
    }

    @Test
    void keepsAtMostMaxMsgsDroppingTheOldestOrRejectingNewOnes() {
      String old = uniq("max-old");
      client.createStream(StreamSpec.builder(old).maxMsgs(2).build());
      for (String v : List.of("1", "2", "3")) {
        client.publish(old, "m.a", v);
      }
      assertEquals(List.of("2", "3"), texts(client.read(old).records()));

      String strict = uniq("max-new");
      client.createStream(StreamSpec.builder(strict).maxMsgs(2).discard(DiscardPolicy.NEW).build());
      client.publish(strict, "m.a", "1");
      client.publish(strict, "m.a", "2");
      ServerException e = assertThrows(ServerException.class, () -> client.publish(strict, "m.a", "3"));
      assertEquals(429, e.code());
      assertEquals(List.of("1", "2"), texts(client.read(strict).records()));
      assertEquals(2, client.streamInfo(strict).config().maxMsgs());
      assertEquals(DiscardPolicy.NEW, client.streamInfo(strict).config().discard());
    }

    @Test
    void capturesCoreMessagesPublishedToMatchingSubjects() throws Exception {
      String s = uniq("capture");
      String prefix = "cap"; // the server is fresh per test class; no other stream captures cap.>
      client.createStream(StreamSpec.builder(s).captureSubjects(prefix + ".>").build());
      assertEquals(List.of(prefix + ".>"), client.streamInfo(s).config().captureSubjects());
      client.publishCore(prefix + ".x", b("{\"captured\":true}"), CorePublishOptions.headers(Map.of("h", "v")));
      client.publishCore("other." + prefix, "not captured");
      List<StreamRecord> recs = eventually(() -> {
        List<StreamRecord> r = client.read(s).records();
        return r.isEmpty() ? null : r;
      });
      assertEquals(1, recs.size());
      assertEquals(prefix + ".x", recs.get(0).subject());
      assertEquals(Map.of("captured", true), recs.get(0).json());
      assertEquals("v", recs.get(0).header("h"));
    }

    @Test
    void keepsTheNewestRecordsPerSubject() {
      String s = uniq("per-subject");
      client.createStream(StreamSpec.builder(s).maxMsgsPerSubject(1).build());
      client.publish(s, "price.a", "1");
      client.publish(s, "price.b", "1");
      client.publish(s, "price.a", "2");
      assertEquals(List.of("price.b", "price.a"),
          client.read(s).records().stream().map(StreamRecord::subject).toList());
      assertEquals(List.of("1", "2"), texts(client.read(s).records()));
    }
  }

  @Nested
  class ConsumerRouting {
    @Test
    void filtersByHeadersAllAndAny() {
      String s = uniq("hdr");
      client.createStream(s);
      String[][] rows = {{"eu", "gold", "a"}, {"us", "gold", "b"}, {"eu", "free", "c"}, {"asia", "free", "d"}};
      for (String[] r : rows) {
        client.publish(s, PublishRecord.builder("e.x").value(r[2]).header("region", r[0]).header("tier", r[1]).build());
      }
      String all = uniq("hdr-all");
      ConsumerInfo info = client.createConsumer(
          ConsumerSpec.builder(all, s).filterHeader("region", "eu").filterHeader("tier", "gold").build());
      assertEquals(Map.of("region", "eu", "tier", "gold"), info.spec().filterHeaders());
      assertEquals(List.of("a"), texts(client.pull(all, PullOptions.of(100, Duration.ofMillis(300)))));

      String any = uniq("hdr-any");
      client.createConsumer(ConsumerSpec.builder(any, s).filterHeader("region", "eu").filterHeader("tier", "gold")
          .headerMatch(HeaderMatch.ANY).build());
      assertEquals(List.of("a", "b", "c"), texts(client.pull(any, PullOptions.of(100, Duration.ofMillis(300)))));
    }

    @Test
    void refusesPullsOnASingleActiveConsumer() {
      String s = uniq("single");
      client.createStream(s);
      String c = uniq("single-c");
      ConsumerInfo info = client.createConsumer(ConsumerSpec.builder(c, s).singleActive(true).build());
      assertEquals(true, info.spec().singleActive());
      assertEquals(400, assertThrows(ServerException.class,
          () -> client.pull(c, PullOptions.of(1, Duration.ofMillis(100)))).code());
    }

    @Test
    void removesAckedRecordsFromAWorkQueue() throws Exception {
      String s = uniq("wq");
      client.createStream(StreamSpec.builder(s).retention(RetentionPolicy.WORK_QUEUE).build());
      String c = uniq("wq-c");
      client.createConsumer(ConsumerSpec.of(c, s));
      for (int i = 0; i < 3; i++) {
        client.publish(s, "job.x", Integer.toString(i));
      }
      List<Message> got = client.pull(c, PullOptions.of(2, Duration.ofSeconds(2)));
      assertEquals(List.of("0", "1"), texts(got));
      client.ack(c, got.stream().mapToLong(Message::offset).toArray());
      eventually(() -> client.streamInfo(s).earliestOffset() == 2);
      assertEquals(List.of("2"), texts(client.read(s).records()));
    }

    @Test
    void deliversHigherPrioritiesFirstWithinThePriorityWindow() {
      String s = uniq("prio");
      client.createStream(s);
      Object[][] rows = {{"low1", 0}, {"high1", 9}, {"mid", 5}, {"low2", 0}, {"high2", 9}};
      for (Object[] r : rows) {
        client.publish(s, PublishRecord.builder("t.x").value((String) r[0]).priority((Integer) r[1]).build());
      }
      String c = uniq("prio-c");
      client.createConsumer(ConsumerSpec.builder(c, s).priorityWindow(100).build());
      List<String> order = new ArrayList<>();
      while (order.size() < 5) {
        List<Message> got = client.pull(c, PullOptions.of(10, Duration.ofMillis(500)));
        client.ack(c, got.stream().mapToLong(Message::offset).toArray());
        order.addAll(texts(got));
      }
      assertEquals(List.of("high1", "high2", "mid", "low1", "low2"), order);
    }
  }

  @Nested
  class CorePubSub {
    @Test
    void fansOutToEveryMatchingSubscriptionAndStoresNothing() throws Exception {
      try (ExspeedClient a = server.connect(); ExspeedClient bb = server.connect()) {
        CoreSubscription all = a.subscribeCore("orders.>");
        CoreSubscription eu = bb.subscribeCore("orders.eu.*");
        client.publishCore("orders.eu.created", b("{\"id\":1}"), CorePublishOptions.headers(Map.of("trace-id", "t1")));
        client.publishCore("orders.us.created", "2");
        client.publishCore("billing.x", "ignored");

        CoreMessage m1 = all.next(Duration.ofSeconds(5));
        assertEquals("orders.eu.created", m1.subject());
        assertEquals(Map.of("id", 1L), m1.json());
        assertEquals("t1", m1.header("trace-id"));
        assertNull(m1.replyTo());
        assertEquals("orders.us.created", all.next(Duration.ofSeconds(5)).subject());
        assertEquals("orders.eu.created", eu.next(Duration.ofSeconds(5)).subject());
        assertNull(eu.next(Duration.ofMillis(200)));
        assertNull(all.next(Duration.ofMillis(200)));

        // A late subscriber sees only what comes next.
        CoreSubscription late = a.subscribeCore("orders.>");
        assertNull(late.next(Duration.ofMillis(200)));
        late.unsubscribe();
        all.unsubscribe();
        client.publishCore("orders.eu.created", "3");
        assertEquals("3", eu.next(Duration.ofSeconds(5)).text());
        assertTrue(all.isClosed());
      }
    }

    @Test
    void splitsMessagesAcrossAQueueGroup() {
      try (ExspeedClient w1 = server.connect(); ExspeedClient w2 = server.connect()) {
        String subject = "jobs." + uniq("q");
        CoreSubscription s1 = w1.subscribeCore(subject, "workers");
        CoreSubscription s2 = w2.subscribeCore(subject, "workers");
        for (int i = 0; i < 20; i++) {
          client.publishCore(subject, Integer.toString(i));
        }
        int n1 = 0;
        while (s1.next(Duration.ofMillis(300)) != null) {
          n1++;
        }
        int n2 = 0;
        while (s2.next(Duration.ofMillis(300)) != null) {
          n2++;
        }
        assertEquals(20, n1 + n2);
        assertTrue(n1 > 0);
        assertTrue(n2 > 0);
      }
    }
  }

  @Nested
  class RequestReply {
    @Test
    void answersRequestsThroughOneInboxAndFailsFastWithNoResponders() throws Exception {
      try (ExspeedClient svc = server.connect()) {
        CoreSubscription reqs = svc.subscribeCore("svc.upper", "svc");
        CompletableFuture<?> responder = reqs.listen(m -> m.respond(m.text().toUpperCase()));

        CoreMessage r = client.request("svc.upper", b("hello"), RequestOptions.timeout(Duration.ofSeconds(5)));
        assertEquals("HELLO", r.text());
        List<CompletableFuture<CoreMessage>> many = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
          many.add(client.requestAsync("svc.upper", b("m" + i), RequestOptions.timeout(Duration.ofSeconds(5))));
        }
        for (int i = 0; i < 20; i++) {
          assertEquals("M" + i, many.get(i).get(10, TimeUnit.SECONDS).text());
        }

        long t = System.nanoTime();
        ServerException e = assertThrows(ServerException.class,
            () -> client.request("svc.nobody", b("x"), RequestOptions.timeout(Duration.ofSeconds(5))));
        assertEquals(404, e.code());
        assertTrue(System.nanoTime() - t < TimeUnit.SECONDS.toNanos(2));

        reqs.unsubscribe();
        responder.get(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void timesOutWhenAResponderNeverAnswers() {
      try (ExspeedClient svc = server.connect()) {
        String subject = "svc." + uniq("silent");
        CoreSubscription silent = svc.subscribeCore(subject);
        assertThrows(RequestTimeoutException.class,
            () -> client.request(subject, b("x"), RequestOptions.timeout(Duration.ofMillis(300))));
        CoreMessage seen = silent.next(Duration.ofSeconds(1));
        assertNotNull(seen);
        assertTrue(seen.replyTo().startsWith("_INBOX."), seen.replyTo());
      }
    }
  }

  @Nested
  class KvBuckets {
    @Test
    void putsGetsComparesAndSetsDeletesAndListsKeys() {
      KvBucket kv = client.kv(uniq("cfg"));
      kv.create(KvBucketOptions.history(3));
      kv.create(KvBucketOptions.history(3)); // idempotent
      assertNull(kv.get("app.mode"));

      long r1 = kv.put("app.mode", "dev");
      long r2 = kv.put("app.mode", b("{\"mode\":\"prod\"}"));
      assertEquals(1, r1);
      assertEquals(2, r2);
      KvEntry e = kv.get("app.mode");
      assertEquals("app.mode", e.key());
      assertEquals(r2, e.revision());
      assertEquals(KvOp.PUT, e.op());
      assertEquals(Map.of("mode", "prod"), e.json());
      assertEquals("dev", kv.getRevision("app.mode", r1).text());

      // Compare-and-set.
      long created = kv.createKey("app.port", "8080");
      ServerException dup = assertThrows(ServerException.class, () -> kv.createKey("app.port", "9090"));
      assertEquals(409, dup.code());
      long updated = kv.update("app.port", "9090", created);
      ServerException stale = assertThrows(ServerException.class, () -> kv.update("app.port", "1", created));
      assertEquals(409, stale.code());
      assertEquals(updated, stale.detail("current_revision"));

      kv.put("db.url", "postgres://");
      assertEquals(List.of("app.mode", "app.port", "db.url"), kv.keys());
      assertEquals(List.of("app.mode", "app.port"), kv.keys("app.*"));

      long del = kv.delete("app.port");
      assertTrue(del > updated);
      assertNull(kv.get("app.port"));
      assertEquals(List.of("app.mode"), kv.keys("app.*"));
      List<KvEntry> h = kv.history("app.port");
      assertEquals(List.of("8080", "9090", ""), h.stream().map(KvEntry::text).toList());
      assertEquals(List.of(KvOp.PUT, KvOp.PUT, KvOp.DELETE), h.stream().map(KvEntry::op).toList());
      // A deleted key can be created again.
      assertTrue(kv.createKey("app.port", "7070") > del);

      kv.purge("app.mode");
      assertNull(kv.get("app.mode"));

      ServerException missing = assertThrows(ServerException.class, () -> client.kv(uniq("missing")).get("x"));
      assertEquals(404, missing.code());

      kv.destroy();
      assertEquals(404, assertThrows(ServerException.class, () -> client.streamInfo(kv.stream())).code());
    }

    @Test
    void deletesWithAnExpectedRevision() {
      KvBucket kv = client.kv(uniq("cas-del"));
      kv.create();
      long r = kv.put("k", "v");
      assertEquals(409, assertThrows(ServerException.class, () -> kv.delete("k", r + 5)).code());
      assertTrue(kv.delete("k", r) > r);
      assertNull(kv.get("k"));
    }

    @Test
    void expiresKeysAfterTheirTtl() throws Exception {
      KvBucket kv = client.kv(uniq("ttl"));
      kv.create();
      kv.put("session.a", b("x"), KvPutOptions.ttl(Duration.ofMillis(200)));
      kv.put("session.b", "y");
      Thread.sleep(500);
      assertNull(kv.get("session.a"));
      assertEquals("y", kv.get("session.b").text());
    }

    @Test
    void watchesCurrentValuesFirstThenEveryChange() {
      KvBucket kv = client.kv(uniq("watch"));
      kv.create();
      kv.put("user.1", "alice");
      kv.put("user.2", "bob");
      kv.put("user.1", "alice2");
      kv.put("other.x", "filtered");
      kv.put("user.3", "gone");
      kv.delete("user.3");

      try (KvWatch w = kv.watch("user.*")) {
        List<KvEntry> snapshot = take(w, 2);
        assertEquals(List.of("user.2/bob/2", "user.1/alice2/3"),
            snapshot.stream().map(e -> e.key() + "/" + e.text() + "/" + e.revision()).toList());

        kv.put("user.4", "dave");
        kv.put("other.y", "filtered");
        kv.delete("user.2");
        List<KvEntry> changes = take(w, 2);
        assertEquals(List.of("user.4/PUT", "user.2/DELETE"),
            changes.stream().map(e -> e.key() + "/" + e.op()).toList());
        assertNull(w.next(Duration.ofMillis(200)));
        w.close();
        assertNull(w.next());
      }
    }

    private List<KvEntry> take(KvWatch w, int n) {
      List<KvEntry> out = new ArrayList<>();
      while (out.size() < n) {
        KvEntry e = w.next(Duration.ofSeconds(5));
        if (e == null) {
          throw new AssertionError("watch stalled after " + out.size() + " entries");
        }
        out.add(e);
      }
      return out;
    }
  }
}
