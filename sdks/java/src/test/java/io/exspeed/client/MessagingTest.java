package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.FakeServer.FakeConn;
import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WireRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Core pub/sub, request-reply, KV buckets and consumer info, against the fake server. */
class MessagingTest {
  FakeServer server;
  ExspeedClient client;

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  ExspeedClient setup() throws Exception {
    server = FakeServer.start();
    client = ExspeedClient.connect(
        ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO).reconnect(false).build());
    return client;
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

  static void ok(FakeConn conn, int corr) {
    conn.reply(corr, new Response.Ok());
  }

  /** A KV record as the server stores it: subject = key, raw stream offset. */
  static WireRecord kvRec(long offset, String key, String value, String op) {
    return new WireRecord(offset, 1_700_000_000_000_000_000L + offset, 0, key, null, b(value),
        op == null ? List.of() : List.of(new Header(KvBucket.KV_OP_HEADER, op)));
  }

  static Throwable cause(CompletableFuture<?> f) {
    return assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS)).getCause();
  }

  static Response.CoreMsg coreMsg(int subId, String subject, String replyTo, String value) {
    return new Response.CoreMsg(subId, subject, replyTo, List.of(), b(value));
  }

  @Nested
  class CorePubSub {
    @Test
    void publishesACoreMessageAndWaitsForOk() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CorePublish) {
          ok(conn, corr);
        }
        return false;
      };
      c.publishCore("orders.created", b("{\"id\":1}"), CorePublishOptions.headers(Map.of("trace-id", "t")));
      FakeServer.Received p = server.last().of(Request.CorePublish.class).get(0);
      assertNotEquals(0, p.corr());
      Request.CorePublish r = (Request.CorePublish) p.req();
      assertEquals("orders.created", r.subject());
      assertNull(r.replyTo());
      assertEquals(List.of(new Header("trace-id", "t")), r.headers());
      assertEquals("{\"id\":1}", new String(r.value(), StandardCharsets.UTF_8));
      c.publishCore("a", "text");
      c.publishCoreAsync("a", b("x"), CorePublishOptions.NONE).get(2, TimeUnit.SECONDS);
      assertEquals(3, server.last().of(Request.CorePublish.class).size());
    }

    @Test
    void subscribesWithAQueueGroupRespondsAndUnsubscribes() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(0x80000001),
              0, new Response.CoreMsg(0x80000001, "svc.echo", "_INBOX.x.1", List.of(new Header("h", "1")),
                  b("{\"q\":1}")),
              0, coreMsg(0x80000099, "other", null, "ignored"));
        } else if (req instanceof Request.CorePublish || req instanceof Request.Unsubscribe) {
          ok(conn, corr);
        }
        return false;
      };
      CoreSubscription sub = c.subscribeCore("svc.*", "workers");
      assertEquals(new Request.CoreSubscribe("svc.*", "workers"),
          server.last().reqs(Request.CoreSubscribe.class).get(0));
      assertEquals(0x80000001, sub.id());
      assertEquals("workers", sub.queue());
      CoreMessage m = sub.next(Duration.ofSeconds(1));
      assertEquals("svc.echo", m.subject());
      assertEquals("_INBOX.x.1", m.replyTo());
      assertEquals("1", m.header("h"));
      assertEquals(Map.of("q", 1L), m.json());
      m.respond("pong");
      Request.CorePublish resp = server.last().reqs(Request.CorePublish.class).get(0);
      assertEquals("_INBOX.x.1", resp.subject());
      assertNull(resp.replyTo());
      assertEquals("pong", new String(resp.value(), StandardCharsets.UTF_8));
      assertNull(sub.next(Duration.ofMillis(100))); // the other subscription's message isn't routed here

      sub.unsubscribe();
      assertEquals(new Request.Unsubscribe(0x80000001), server.last().reqs(Request.Unsubscribe.class).get(0));
      assertEquals(new EndReason(0, "unsubscribed"), sub.endReason());
    }

    @Test
    void refusesToRespondToAMessageWithoutReplyTo() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(0x80000001), 0, coreMsg(0x80000001, "a", null, "x"));
        }
        return false;
      };
      CoreSubscription sub = c.subscribeCore("a");
      CoreMessage m = sub.next();
      assertThrows(ExspeedException.class, () -> m.respond("no"));
      assertInstanceOf(ExspeedException.class, cause(m.respondAsync(b("no"), List.of())));
    }

    @Test
    void endsWhenTheServerEndsItAfterYieldingBufferedMessages() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(0x80000002), 0, coreMsg(0x80000002, "a", null, "1"),
              0, new Response.SubscriptionEnded(0x80000002, 503, "leadership moved"));
        }
        return false;
      };
      CoreSubscription sub = c.subscribeCore("a");
      List<String> seen = new ArrayList<>();
      for (CoreMessage m : sub) {
        seen.add(m.text());
      }
      assertEquals(List.of("1"), seen);
      assertEquals(new EndReason(503, "leadership moved"), sub.endReason());
    }

    @Test
    void deliversToACallbackAndUnsubscribesOnClose() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(0x80000003), 0, coreMsg(0x80000003, "a", null, "1"),
              0, coreMsg(0x80000003, "a", null, "2"));
        } else if (req instanceof Request.Unsubscribe) {
          ok(conn, corr);
        }
        return false;
      };
      CoreSubscription sub = c.subscribeCore("a");
      List<String> seen = new CopyOnWriteArrayList<>();
      CompletableFuture<EndReason> done = sub.listen(m -> seen.add(m.text()));
      server.until(() -> seen.size() == 2);
      sub.close();
      assertEquals(0, done.get(2, TimeUnit.SECONDS).code());
      assertEquals(new Request.Unsubscribe(0x80000003), server.last().reqs(Request.Unsubscribe.class).get(0));
      assertTrue(sub.isClosed());
    }
  }

  @Nested
  class RequestReply {
    /** Answers CoreSubscribe for the inbox and records requests; replies are sent by the test. */
    List<String> inboxServer() {
      List<String> subs = new CopyOnWriteArrayList<>();
      AtomicInteger id = new AtomicInteger(0x80000010);
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe cs) {
          subs.add(cs.subject());
          conn.reply(corr, new Response.SubscribeOk(id.getAndIncrement()));
        } else if (req instanceof Request.CorePublish) {
          ok(conn, corr);
        }
        return false;
      };
      return subs;
    }

    @Test
    void sharesOneInboxSubscriptionAndRoutesResponsesByTheLastSubjectToken() throws Exception {
      ExspeedClient c = setup();
      List<String> subs = inboxServer();
      CompletableFuture<CoreMessage> a = c.requestAsync("svc.a", b("1"), RequestOptions.DEFAULT);
      CompletableFuture<CoreMessage> bb = c.requestAsync("svc.b", b("{\"n\":2}"),
          RequestOptions.DEFAULT.withHeaders(Map.of("h", "v")));
      server.until(() -> server.last().of(Request.CorePublish.class).size() == 2);
      assertEquals(1, subs.size());
      assertTrue(subs.get(0).matches("^_INBOX\\.[0-9a-f]+\\.\\*$"), subs.get(0));
      String prefix = subs.get(0).substring(0, subs.get(0).length() - 2);
      List<Request.CorePublish> pubs = server.last().reqs(Request.CorePublish.class);
      Request.CorePublish pa = pubs.stream().filter(p -> p.subject().equals("svc.a")).findFirst().orElseThrow();
      Request.CorePublish pb = pubs.stream().filter(p -> p.subject().equals("svc.b")).findFirst().orElseThrow();
      assertTrue(pa.replyTo().startsWith(prefix + "."));
      assertTrue(pb.replyTo().startsWith(prefix + "."));
      assertNotEquals(pa.replyTo(), pb.replyTo());
      assertEquals(List.of(new Header("h", "v")), pb.headers());
      // Out of order, on the inbox subscription.
      server.last().reply(0, coreMsg(0x80000010, pb.replyTo(), null, "B"));
      server.last().reply(0, coreMsg(0x80000010, pa.replyTo(), null, "A"));
      assertEquals("A", a.get(2, TimeUnit.SECONDS).text());
      assertEquals("B", bb.get(2, TimeUnit.SECONDS).text());

      CompletableFuture<CoreMessage> third = c.requestAsync("svc.a", b("3"), RequestOptions.DEFAULT);
      server.until(() -> server.last().of(Request.CorePublish.class).size() == 3);
      assertEquals(1, subs.size()); // still the same inbox
      Request.CorePublish p3 = server.last().reqs(Request.CorePublish.class).get(2);
      server.last().reply(0, coreMsg(0x80000010, p3.replyTo(), null, "C"));
      assertEquals("C", third.get(2, TimeUnit.SECONDS).text());
    }

    @Test
    void failsAtOnceWith404WhenThereAreNoResponders() throws Exception {
      ExspeedClient c = setup();
      inboxServer();
      FakeServer.Handler prev = server.handler;
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CorePublish) {
          conn.reply(corr, new Response.Error(404, "no responders for 'svc.none'", null));
          return true;
        }
        return prev.handle(conn, corr, req);
      };
      long t = System.nanoTime();
      ServerException e = assertThrows(ServerException.class,
          () -> c.request("svc.none", b("x"), RequestOptions.timeout(Duration.ofSeconds(5))));
      assertEquals(404, e.code());
      assertTrue(System.nanoTime() - t < TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void timesOutAndIgnoresALateResponse() throws Exception {
      ExspeedClient c = setup();
      inboxServer();
      assertThrows(RequestTimeoutException.class,
          () -> c.request("svc.slow", b("x"), RequestOptions.timeout(Duration.ofMillis(100))));
      Request.CorePublish p = server.last().reqs(Request.CorePublish.class).get(0);
      server.last().reply(0, coreMsg(0x80000010, p.replyTo(), null, "late"));
      c.ping(); // the late response was dropped without trouble
    }

    @Test
    void failsWaitingRequestsWhenTheInboxEndsAndSubscribesANewOneNextTime() throws Exception {
      ExspeedClient c = setup();
      List<String> subs = inboxServer();
      CompletableFuture<CoreMessage> pending =
          c.requestAsync("svc.a", b("x"), RequestOptions.timeout(Duration.ofSeconds(5)));
      server.until(() -> server.last().of(Request.CorePublish.class).size() == 1);
      server.last().reply(0, new Response.SubscriptionEnded(0x80000010, 503, "leadership moved"));
      Throwable err = cause(pending);
      assertInstanceOf(ServerException.class, err);
      assertEquals(503, ((ServerException) err).code());

      CompletableFuture<CoreMessage> again = c.requestAsync("svc.a", b("y"), RequestOptions.DEFAULT);
      server.until(() -> server.last().of(Request.CorePublish.class).size() == 2);
      assertEquals(2, subs.size());
      assertNotEquals(subs.get(0), subs.get(1));
      Request.CorePublish p = server.last().reqs(Request.CorePublish.class).get(1);
      assertTrue(p.replyTo().startsWith(subs.get(1).substring(0, subs.get(1).length() - 2)));
      server.last().reply(0, coreMsg(0x80000011, p.replyTo(), null, "ok"));
      assertEquals("ok", again.get(2, TimeUnit.SECONDS).text());
    }

    @Test
    void afterAReconnectResubscribesCoreSubscriptionsAndSetsUpANewInbox() throws Exception {
      server = FakeServer.start();
      AtomicInteger id = new AtomicInteger(0x80000020);
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.CoreSubscribe) {
          conn.reply(corr, new Response.SubscribeOk(id.getAndIncrement()));
        } else if (req instanceof Request.CorePublish) {
          ok(conn, corr);
        }
        return false;
      };
      CountDownLatch reconnected = new CountDownLatch(1);
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofMillis(10), Duration.ofMillis(20)))
          .listener(new ClientListener() {
            @Override
            public void onReconnect(ServerInfo info) {
              reconnected.countDown();
            }
          }).build());
      ExspeedClient c = client;
      CoreSubscription sub = c.subscribeCore("events.>", "g");
      CompletableFuture<CoreMessage> pending =
          c.requestAsync("svc.a", b("x"), RequestOptions.timeout(Duration.ofSeconds(5)));
      server.until(() -> server.last().of(Request.CorePublish.class).size() == 1);
      String firstInbox = server.last().reqs(Request.CoreSubscribe.class).get(1).subject();

      server.conns.get(0).destroy();
      assertInstanceOf(ConnectionException.class, cause(pending));
      assertTrue(reconnected.await(5, TimeUnit.SECONDS));
      FakeConn second = server.conns.get(1);
      assertEquals(List.of(new Request.CoreSubscribe("events.>", "g")), second.reqs(Request.CoreSubscribe.class));
      second.reply(0, coreMsg(sub.id(), "events.x", null, "after"));
      assertEquals("after", sub.next(Duration.ofSeconds(1)).text());

      CompletableFuture<CoreMessage> again = c.requestAsync("svc.a", b("y"), RequestOptions.DEFAULT);
      server.until(() -> second.of(Request.CorePublish.class).size() == 1);
      String inbox = second.reqs(Request.CoreSubscribe.class).get(1).subject();
      assertNotEquals(firstInbox, inbox);
      Request.CorePublish p = second.reqs(Request.CorePublish.class).get(0);
      second.reply(0, coreMsg(id.get() - 1, p.replyTo(), null, "ok"));
      assertEquals("ok", again.get(2, TimeUnit.SECONDS).text());
    }
  }

  @Nested
  class KvBuckets {
    @Test
    void encodesCreatePutCreateKeyUpdateDeleteAndPurge() throws Exception {
      ExspeedClient c = setup();
      AtomicInteger rev = new AtomicInteger();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.KvCreateBucket) {
          ok(conn, corr);
        }
        if (req instanceof Request.KvPut || req instanceof Request.KvDelete) {
          conn.reply(corr, new Response.PublishOk(rev.incrementAndGet(), false));
        }
        return false;
      };
      KvBucket kv = c.kv("cfg");
      assertEquals("KV_cfg", kv.stream());
      kv.create(KvBucketOptions.history(5).withTtl(Duration.ofSeconds(60)));
      c.kv("plain").create();
      assertEquals(1, kv.put("a", b("{\"on\":true}"), KvPutOptions.ttl(Duration.ofMillis(500))));
      assertEquals(2, kv.createKey("b", "x"));
      assertEquals(3, kv.update("b", "y", 2));
      assertEquals(4, kv.delete("a"));
      assertEquals(5, kv.purge("b", 3));
      assertEquals(6, kv.putAsync("c", b("z"), KvPutOptions.ttl(Duration.ofNanos(1))).get(2, TimeUnit.SECONDS));

      List<Request> reqs = new ArrayList<>();
      for (FakeServer.Received r : server.last().received) {
        if (r.req().typeName().startsWith("Kv")) {
          reqs.add(r.req());
        }
      }
      assertEquals(new Request.KvCreateBucket("cfg", 5, 60_000, 0), reqs.get(0));
      assertEquals(new Request.KvCreateBucket("plain", 0, 0, 0), reqs.get(1));
      Request.KvPut p1 = (Request.KvPut) reqs.get(2);
      assertEquals("a", p1.key());
      assertEquals("{\"on\":true}", new String(p1.value(), StandardCharsets.UTF_8));
      assertNull(p1.expectedRevision());
      assertEquals(500L, p1.ttlMs());
      Request.KvPut p2 = (Request.KvPut) reqs.get(3);
      assertEquals(0L, p2.expectedRevision());
      assertNull(p2.ttlMs());
      assertEquals(2L, ((Request.KvPut) reqs.get(4)).expectedRevision());
      assertEquals(new Request.KvDelete("cfg", "a", false, null), reqs.get(5));
      assertEquals(new Request.KvDelete("cfg", "b", true, 3L), reqs.get(6));
      assertEquals(1L, ((Request.KvPut) reqs.get(7)).ttlMs()); // sub-ms TTLs round up to 1 ms
    }

    @Test
    void passesACasConflictThroughAsServerException409() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.KvPut) {
          conn.reply(corr, new Response.Error(409, "wrong revision", b("{\"current_revision\":7}")));
        }
        return false;
      };
      ServerException e = assertThrows(ServerException.class, () -> c.kv("b").update("k", "v", 3));
      assertEquals(409, e.code());
      assertEquals(Map.of("current_revision", 7L), e.detail());
      assertEquals(7L, e.detail("current_revision"));
    }

    @Test
    void turnsRecordsIntoEntriesWithRevisionOffsetPlusOneAndNullFor404Keys() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (!(req instanceof Request.KvGet g)) {
          return false;
        }
        switch (g.key()) {
          case "live" -> conn.reply(corr, new Response.Messages(List.of(kvRec(4, "live", "{\"n\":1}", null))));
          case "gone" -> conn.reply(corr, new Response.Error(404, "key 'gone' not found", null));
          case "old" -> conn.reply(corr, new Response.Messages(List.of(kvRec(g.revision() - 1, "old", "v1", null))));
          default -> conn.reply(corr, new Response.Messages(List.of()));
        }
        return true;
      };
      KvBucket kv = c.kv("b");
      KvEntry e = kv.get("live");
      assertEquals("live", e.key());
      assertEquals(5, e.revision());
      assertEquals(KvOp.PUT, e.op());
      assertEquals(Map.of("n", 1L), e.json());
      assertEquals(1_700_000_000_000_000_004L, e.timestampNs());
      assertEquals(1_700_000_000_000L, e.timestamp().toEpochMilli());
      assertNull(kv.get("gone"));
      assertNull(kv.get("empty"));
      assertNull(kv.getAsync("gone").get(2, TimeUnit.SECONDS));
      KvEntry old = kv.getRevision("old", 2);
      assertEquals(2, old.revision());
      assertEquals("v1", old.text());
      List<Long> revs = new ArrayList<>();
      for (Request.KvGet g : server.last().reqs(Request.KvGet.class)) {
        revs.add(g.revision());
      }
      List<Long> expected = new ArrayList<>();
      expected.add(null);
      expected.add(null);
      expected.add(null);
      expected.add(null);
      expected.add(2L);
      assertEquals(expected, revs);
    }

    @Test
    void throwsWhenTheBucketDoesntExist() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.KvGet) {
          conn.reply(corr, new Response.Error(404, "bucket 'nope' not found", null));
        }
        return false;
      };
      ServerException e = assertThrows(ServerException.class, () -> c.kv("nope").get("k"));
      assertEquals(404, e.code());
    }

    @Test
    void listsKeysAndHistoryWithTombstones() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.KvKeys) {
          conn.reply(corr, new Response.Json(b("[\"a.1\",\"a.2\"]")));
        }
        if (req instanceof Request.KvHistory) {
          conn.reply(corr, new Response.Messages(List.of(kvRec(0, "k", "v1", null), kvRec(3, "k", "", "DEL"),
              kvRec(5, "k", "v2", null), kvRec(6, "k", "", "PURGE"))));
        }
        return false;
      };
      KvBucket kv = c.kv("b");
      assertEquals(List.of("a.1", "a.2"), kv.keys("a.*"));
      assertEquals(List.of("a.1", "a.2"), kv.keys());
      assertEquals(List.of("a.*", ""), server.last().reqs(Request.KvKeys.class).stream().map(Request.KvKeys::filter)
          .toList());
      List<KvEntry> h = kv.history("k");
      assertEquals(List.of(1L, 4L, 6L, 7L), h.stream().map(KvEntry::revision).toList());
      assertEquals(List.of(KvOp.PUT, KvOp.DELETE, KvOp.PUT, KvOp.PURGE), h.stream().map(KvEntry::op).toList());
    }

    @Test
    void watchesTheLiveKeysFirstSortedByRevisionThenEveryChange() throws Exception {
      ExspeedClient c = setup();
      List<Request.Read> reads = new CopyOnWriteArrayList<>();
      server.handler = (conn, corr, req) -> {
        if (!(req instanceof Request.Read r)) {
          return false;
        }
        reads.add(r);
        if (r.waitMs() == 0 && r.from() == 0) {
          // Snapshot, page 1 of 2 (high watermark 5).
          conn.reply(corr, new Response.ReadResult(3, 5, List.of(kvRec(0, "a", "a1", null), kvRec(1, "b", "b1", null),
              kvRec(2, "a", "a2", null))));
        } else if (r.waitMs() == 0 && r.from() == 3) {
          conn.reply(corr, new Response.ReadResult(5, 6, List.of(kvRec(3, "c", "c1", null), kvRec(4, "b", "", "DEL"))));
        } else {
          conn.reply(corr, new Response.ReadResult(7, 7, List.of(kvRec(5, "a", "a3", null), kvRec(6, "c", "", "DEL"))));
        }
        return true;
      };
      List<String> seen = new ArrayList<>();
      try (KvWatch w = c.kv("b").watch("x.>")) {
        for (KvEntry e : w) {
          seen.add(e.key() + "/" + e.revision() + "/" + e.op());
          if (seen.size() == 4) {
            break;
          }
        }
        // b was deleted before the snapshot ended: left out. a (rev 3) before c (rev 4).
        assertEquals(List.of("a/3/PUT", "c/4/PUT", "a/6/PUT", "c/7/DELETE"), seen);
        assertEquals(List.of("KV_b/0/0/x.>", "KV_b/3/0/x.>", "KV_b/5/10000/x.>"),
            reads.subList(0, 3).stream().map(r -> r.stream() + "/" + r.from() + "/" + r.waitMs() + "/" + r.filter())
                .toList());
        w.close();
        assertTrue(w.isClosed());
        assertNull(w.next());
      }
    }

    @Test
    void returnsNullOnTimeoutWhileALongPollIsPendingKeepingWhatItBrings() throws Exception {
      ExspeedClient c = setup();
      AtomicReference<Integer> held = new AtomicReference<>();
      server.handler = (conn, corr, req) -> {
        if (!(req instanceof Request.Read r)) {
          return false;
        }
        if (r.waitMs() == 0) {
          conn.reply(corr, new Response.ReadResult(0, 0, List.of()));
        } else {
          held.set(corr);
        }
        return true;
      };
      KvWatch w = c.kv("b").watch();
      assertNull(w.next(Duration.ofMillis(100)));
      server.until(() -> held.get() != null);
      server.last().reply(held.get(), new Response.ReadResult(1, 1, List.of(kvRec(0, "k", "v", null))));
      KvEntry e = w.next(Duration.ofSeconds(1));
      assertEquals("k", e.key());
      assertEquals(1, e.revision());
      assertEquals(2, server.last().of(Request.Read.class).size()); // the timed-out next() didn't start another read
      w.close();
    }

    @Test
    void surfacesAFailedWatchRead() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Read) {
          conn.reply(corr, new Response.Error(404, "stream 'KV_b' not found", null));
          return true;
        }
        return false;
      };
      KvWatch w = c.kv("b").watch();
      assertEquals(404, assertThrows(ServerException.class, () -> w.next(Duration.ofSeconds(1))).code());
      // The failed read is not cached: the next call reads again.
      assertThrows(ServerException.class, w::next);
      assertEquals(2, server.last().of(Request.Read.class).size());
    }
  }

  @Nested
  class ConsumerInfos {
    @Test
    void keepsFilterHeaderKeysAsSentAndReadsTheNewerFields() throws Exception {
      ExspeedClient c = setup();
      String info = "{\"spec\":{\"name\":\"c\",\"stream\":\"s\",\"filter_headers\":{\"x_tenant_id\":\"acme\"},"
          + "\"header_match\":\"any\",\"single_active\":true,\"priority_window\":5,\"dead_letter_expired\":true,"
          + "\"deliver\":{\"from_offset\":3},\"ack_wait_ms\":1500,\"backoff_ms\":[10,20]},"
          + "\"num_delayed\":2,\"num_unacked\":1,\"ack_floor\":4,\"stats\":{\"dead_lettered\":3}}";
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.ConsumerInfo || req instanceof Request.CreateConsumer) {
          conn.reply(corr, new Response.Json(b(info)));
        }
        if (req instanceof Request.ListConsumers) {
          conn.reply(corr, new Response.Json(b("[" + info + ",{\"spec\":{\"name\":\"d\",\"stream\":\"s\"}}]")));
        }
        return false;
      };
      ConsumerInfo i = c.consumerInfo("c");
      assertEquals(Map.of("x_tenant_id", "acme"), i.spec().filterHeaders());
      assertEquals(HeaderMatch.ANY, i.spec().headerMatch());
      assertEquals(true, i.spec().singleActive());
      assertEquals(5, i.spec().priorityWindow());
      assertEquals(true, i.spec().deadLetterExpired());
      assertEquals(DeliverPolicy.fromOffset(3), i.spec().deliver());
      assertEquals(Duration.ofMillis(1500), i.spec().ackWait());
      assertEquals(List.of(Duration.ofMillis(10), Duration.ofMillis(20)), i.spec().backoff());
      assertEquals(2, i.numDelayed());
      assertEquals(1, i.numUnacked());
      assertEquals(4, i.ackFloor());
      assertEquals(3, i.stats().deadLettered());
      ConsumerInfo created = c.createConsumer(ConsumerSpec.builder("c", "s").filterHeader("x_tenant_id", "acme")
          .headerMatch(HeaderMatch.ANY).build());
      assertEquals(Map.of("x_tenant_id", "acme"), created.spec().filterHeaders());
      assertTrue(server.last().reqs(Request.CreateConsumer.class).get(0).specJson()
          .contains("\"filter_headers\":{\"x_tenant_id\":\"acme\"},\"header_match\":\"any\""));
      List<ConsumerInfo> list = c.listConsumers();
      assertEquals(List.of(Map.of("x_tenant_id", "acme"), Map.of()),
          list.stream().map(x -> x.spec().filterHeaders()).toList());
      assertNull(server.last().reqs(Request.ListConsumers.class).get(0).stream());
      c.listConsumers("s");
      assertEquals("s", server.last().reqs(Request.ListConsumers.class).get(1).stream());
    }
  }
}
