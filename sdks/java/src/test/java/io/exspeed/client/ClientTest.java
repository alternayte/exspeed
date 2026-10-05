package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Connection logic against the scriptable fake server. */
class ClientTest {
  FakeServer server;
  ExspeedClient client;

  static WireRecord rec(long offset) {
    return rec(offset, "v" + offset);
  }

  static WireRecord rec(long offset, String value) {
    return new WireRecord(offset, 1_700_000_000_000_000_000L + offset, 1, "orders.placed", null,
        value.getBytes(StandardCharsets.UTF_8), List.of(new Header("h", "1")));
  }

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  ExspeedClient setup() throws Exception {
    return setup(o -> {});
  }

  ExspeedClient setup(Consumer<ClientOptions.Builder> extra) throws Exception {
    server = FakeServer.start();
    ClientOptions.Builder b = ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO).reconnect(false);
    extra.accept(b);
    client = ExspeedClient.connect(b.build());
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

  @Nested
  class Handshake {
    @Test
    void sendsClientIdAndTokenAndExposesServerInfo() throws Exception {
      ExspeedClient c = setup(b -> b.token("secret").clientId("unit"));
      FakeServer.Received first = server.last().received.get(0);
      assertEquals(new Request.Connect("unit", "secret"), first.req());
      assertNotEquals(0, first.corr());
      assertEquals(new ServerInfo("test", "n1", null), c.serverInfo());
      assertTrue(c.isConnected());
    }

    @Test
    void rejectsWithServerException401WhenTheTokenIsRefused() throws Exception {
      server = FakeServer.start((conn, corr, req) -> {
        if (!(req instanceof Request.Connect)) {
          return false;
        }
        conn.reply(corr, new Response.Error(401, "unauthorized", null));
        conn.destroy();
        return true;
      });
      ServerException e = assertThrows(ServerException.class,
          () -> ExspeedClient.connect(ClientOptions.builder().port(server.port()).token("bad").build()));
      assertEquals(401, e.code());
    }

    @Test
    void failsWithConnectionExceptionWhenNothingListens() throws Exception {
      FakeServer s = FakeServer.start();
      int port = s.port();
      s.close();
      assertThrows(ConnectionException.class,
          () -> ExspeedClient.connect(ClientOptions.builder().port(port).build()));
    }

    @Test
    void failsWithConnectionExceptionWhenTheHandshakeTimesOut() throws Exception {
      server = FakeServer.start((conn, corr, req) -> true);
      assertThrows(ConnectionException.class, () -> ExspeedClient.connect(
          ClientOptions.builder().port(server.port()).requestTimeout(Duration.ofMillis(200)).build()));
    }

    @Test
    void connectsAsynchronously() throws Exception {
      server = FakeServer.start();
      client = ExspeedClient.connectAsync(ClientOptions.builder().port(server.port()).reconnect(false).build())
          .get(5, TimeUnit.SECONDS);
      assertTrue(client.isConnected());
    }
  }

  @Nested
  class Requests {
    @Test
    void matchesOutOfOrderResponsesByCorrelationId() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> true; // answer manually
      CompletableFuture<PublishResult> a = c.publishAsync("s", PublishRecord.of("a", "1"));
      CompletableFuture<PublishResult> b = c.publishAsync("s", PublishRecord.of("b", "2"));
      server.until(() -> server.last().of(Request.Publish.class).size() == 2);
      List<FakeServer.Received> pubs = server.last().of(Request.Publish.class);
      server.last().reply(pubs.get(1).corr(), new Response.PublishOk(2, false));
      server.last().reply(pubs.get(0).corr(), new Response.PublishOk(1, true));
      assertEquals(new PublishResult(1, true), a.get(2, TimeUnit.SECONDS));
      assertEquals(new PublishResult(2, false), b.get(2, TimeUnit.SECONDS));
    }

    @Test
    void encodesValuesKeysHeadersAndMsgIds() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.PublishBatch pb) {
          List<Response.PublishOk> rs = new ArrayList<>();
          for (int i = 0; i < pb.records().size(); i++) {
            rs.add(new Response.PublishOk(i, false));
          }
          conn.reply(corr, new Response.PublishBatchOk(rs));
        }
        return false;
      };
      List<PublishResult> results = c.publishBatch("s", List.of(
          PublishRecord.of("a", new byte[] {1, 2}),
          PublishRecord.of("a", "hé"),
          PublishRecord.builder("a").jsonValue(Map.of("id", 1)).key("k").header("x", "y").msgId("m1").build()));
      assertEquals(3, results.size());
      var recs = server.last().reqs(Request.PublishBatch.class).get(0).records();
      assertArrayEquals(new byte[] {1, 2}, recs.get(0).value());
      assertEquals("hé", new String(recs.get(1).value(), StandardCharsets.UTF_8));
      assertEquals("{\"id\":1}", new String(recs.get(2).value(), StandardCharsets.UTF_8));
      assertEquals("k", new String(recs.get(2).key(), StandardCharsets.UTF_8));
      assertEquals(List.of(new Header("x", "y")), recs.get(2).headers());
      assertEquals("m1", recs.get(2).msgId());
      assertEquals(List.of(), c.publishBatch("s", List.of()));
    }

    @Test
    void surfacesCodeMessageDetailAndTheLeaderHintOfServerErrors() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.StreamInfo) {
          conn.reply(corr, new Response.Error(503, "not the leader", b("{\"leader\":\"h2:5933\"}")));
        }
        return false;
      };
      ServerException e = assertThrows(ServerException.class, () -> c.streamInfo("s"));
      assertEquals(503, e.code());
      assertEquals("not the leader", e.getMessage());
      assertEquals(Map.of("leader", "h2:5933"), e.detail());
      assertEquals("h2:5933", e.leaderHint());
      assertEquals("{\"leader\":\"h2:5933\"}", e.detailJson());
      // The async form fails with the same exception, unwrapped.
      Throwable t = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> c.streamInfoAsync("s").get(2, TimeUnit.SECONDS)).getCause();
      assertInstanceOf(ServerException.class, t);
    }

    @Test
    void timesOutRequestsTheServerNeverAnswers() throws Exception {
      ExspeedClient c = setup(b -> b.requestTimeout(Duration.ofMillis(100)));
      server.handler = (conn, corr, req) -> true;
      assertThrows(RequestTimeoutException.class, c::metadata);
    }

    @Test
    void givesPullAndReadExtraTimeForTheirServerSideWait() throws Exception {
      ExspeedClient c = setup(b -> b.requestTimeout(Duration.ofMillis(50)));
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Pull) {
          new Thread(() -> {
            sleep(150);
            conn.reply(corr, new Response.Messages(List.of(rec(3))));
          }).start();
          return true;
        }
        if (req instanceof Request.Read) {
          new Thread(() -> {
            sleep(150);
            conn.reply(corr, new Response.ReadResult(5, 5, List.of(rec(4))));
          }).start();
          return true;
        }
        return false;
      };
      List<Message> msgs = c.pull("c", PullOptions.of(100, Duration.ofMillis(200)));
      assertEquals(List.of(3L), msgs.stream().map(Message::offset).toList());
      Request.Pull p = server.last().reqs(Request.Pull.class).get(0);
      assertEquals(new Request.Pull("c", 100, 0, 200), p);
      ReadResult r = c.read("s", ReadOptions.builder().waitTime(Duration.ofMillis(200)).build());
      assertEquals(4, r.records().get(0).offset());
      assertEquals(5, r.nextOffset());
      assertEquals(new Request.Read("s", 0, 100, 0, 200, ""), server.last().reqs(Request.Read.class).get(0));
    }

    @Test
    void parsesJsonReplies() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Metadata) {
          conn.reply(corr,
              new Response.Json(b("{\"node_id\":\"n1\",\"is_leader\":true,\"leader\":null,\"server_version\":\"x\"}")));
        }
        if (req instanceof Request.StreamInfo) {
          conn.reply(corr, new Response.Json(b("{\"name\":\"s\",\"earliest_offset\":2,\"next_offset\":7,\"records\":5,"
              + "\"config\":{\"max_age_secs\":60,\"max_bytes\":0,\"dedup_window_secs\":300,\"dedup_max_entries\":10,"
              + "\"compaction\":false,\"max_msgs\":2,\"discard\":\"new\",\"retention\":\"work_queue\","
              + "\"allow_msg_ttl\":true},\"internal\":false}")));
        }
        if (req instanceof Request.Query) {
          conn.reply(corr, new Response.Json(b("{\"columns\":[\"n\"],\"rows\":[[3]],\"row_count\":1,"
              + "\"execution_time_ms\":2,\"truncated\":false}")));
        }
        return false;
      };
      assertEquals(new Metadata("n1", true, null, "x"), c.metadata());
      StreamInfo info = c.streamInfo("s");
      assertEquals("s", info.name());
      assertEquals(2, info.earliestOffset());
      assertEquals(7, info.nextOffset());
      assertEquals(5, info.records());
      assertEquals(60, info.config().maxAgeSecs());
      assertEquals(2, info.config().maxMsgs());
      assertEquals(DiscardPolicy.NEW, info.config().discard());
      assertEquals(RetentionPolicy.WORK_QUEUE, info.config().retention());
      assertTrue(info.config().allowMsgTtl());
      QueryResult q = c.query("SELECT 1");
      assertEquals(List.of("n"), q.columns());
      assertEquals(List.of(List.of(3L)), q.rows());
      assertEquals(1, q.rowCount());
    }

    @Test
    void rejectsAReplyOfTheWrongType() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.DeleteStream) {
          conn.reply(corr, new Response.Pong());
        }
        return false;
      };
      assertTrue(assertThrows(ProtocolException.class, () -> c.deleteStream("s")).getMessage()
          .contains("unexpected reply to DeleteStream"));
    }

    @Test
    void failsPendingRequestsWithConnectionExceptionWhenTheConnectionDrops() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> true;
      CompletableFuture<Metadata> p = c.metadataAsync();
      server.until(() -> server.last().of(Request.Metadata.class).size() == 1);
      server.last().destroy();
      Throwable t = assertThrows(java.util.concurrent.ExecutionException.class, () -> p.get(2, TimeUnit.SECONDS))
          .getCause();
      assertInstanceOf(ConnectionException.class, t);
      assertThrows(ConnectionException.class, c::ping);
    }

    @Test
    void completesFuturesOffTheIoThreads() throws Exception {
      ExspeedClient c = setup();
      String thread = c.pingAsync().thenApply(d -> Thread.currentThread().getName()).get(2, TimeUnit.SECONDS);
      assertFalse(thread.startsWith("exspeed-reader"), thread);
      // Blocking inside a continuation is safe.
      Duration nested = c.pingAsync().thenApply(d -> c.ping()).get(2, TimeUnit.SECONDS);
      assertFalse(nested.isNegative());
    }

    @Test
    void sendsSeekTargetsAndConsumerSpecs() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.SeekConsumer || req instanceof Request.DeleteConsumer) {
          conn.reply(corr, new Response.Ok());
        }
        if (req instanceof Request.CreateConsumer) {
          conn.reply(corr, new Response.Json(b("{\"spec\":{\"name\":\"c\",\"stream\":\"s\"},\"stats\":{}}")));
        }
        return false;
      };
      c.seek("c", SeekTarget.EARLIEST);
      c.seek("c", SeekTarget.LATEST);
      c.seek("c", SeekTarget.offset(42));
      c.seek("c", SeekTarget.timeMs(1234));
      assertEquals(List.of(new Request.SeekConsumer("c", 0, 0), new Request.SeekConsumer("c", 1, 0),
          new Request.SeekConsumer("c", 2, 42), new Request.SeekConsumer("c", 3, 1234)),
          server.last().reqs(Request.SeekConsumer.class));
      c.createConsumer(ConsumerSpec.builder("c", "s").ack(AckPolicy.NONE).deliver(DeliverPolicy.NEW).build());
      assertEquals("{\"name\":\"c\",\"stream\":\"s\",\"deliver\":\"new\",\"ack\":\"none\"}",
          server.last().reqs(Request.CreateConsumer.class).get(0).specJson());
      c.deleteConsumer("c");
    }
  }

  @Nested
  class Subscriptions {
    @Test
    void keepsDeliverFramesThatArriveRightBehindSubscribeOk() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (!(req instanceof Request.Subscribe)) {
          return false;
        }
        conn.replyMany(corr, new Response.SubscribeOk(5), 0, new Response.Deliver(5, List.of(rec(0), rec(1))));
        return true;
      };
      Subscription sub = c.subscribe("billing", 10);
      assertEquals(5, sub.id());
      Message m0 = sub.next(Duration.ofMillis(500));
      Message m1 = sub.next(Duration.ofMillis(500));
      assertEquals(0, m0.offset());
      assertEquals(1, m1.offset());
      assertEquals("v0", m0.text());
      assertEquals("1", m0.header("h"));
      assertEquals(1, m0.deliveryCount());
      assertEquals("billing", m0.consumer());
      assertEquals(1_700_000_000_000L, m0.timestampMs());
    }

    @Test
    void returnsCreditInBatchesOfHalfTheWindowAsMessagesAreTaken() throws Exception {
      ExspeedClient c = setup();
      int[] subCorr = {0};
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          subCorr[0] = corr;
          conn.reply(corr, new Response.SubscribeOk(9));
        }
        return false;
      };
      Subscription sub = c.subscribe("c", 4);
      assertEquals(4, server.last().reqs(Request.Subscribe.class).get(0).credits());
      assertNotEquals(0, subCorr[0]);
      server.last().reply(0, new Response.Deliver(9, List.of(rec(0), rec(1), rec(2), rec(3))));
      sub.next();
      Thread.sleep(50);
      assertEquals(0, server.last().of(Request.Credit.class).size());
      sub.next();
      server.until(() -> server.last().of(Request.Credit.class).size() == 1);
      FakeServer.Received credit = server.last().of(Request.Credit.class).get(0);
      assertEquals(0, credit.corr());
      assertEquals(new Request.Credit(9, 2), credit.req());
      sub.next();
      sub.next();
      server.until(() -> server.last().of(Request.Credit.class).size() == 2);
      assertEquals(0, sub.buffered());
    }

    @Test
    void acksFireAndForgetAndSettlesWithNackTermInProgress() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(1), 0, new Response.Deliver(1, List.of(rec(7))));
        } else if (req instanceof Request.Nack || req instanceof Request.Term || req instanceof Request.InProgress) {
          conn.reply(corr, new Response.Ok());
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      Message m = sub.next();
      m.ack();
      m.nack(Duration.ofMillis(250));
      m.term("poison");
      m.inProgress();
      m.nackAsync(Duration.ZERO).get(2, TimeUnit.SECONDS);
      List<FakeServer.Received> got = server.last().received.subList(2, server.last().received.size());
      assertEquals(5, got.size());
      assertEquals(0, got.get(0).corr());
      Request.Ack ack = (Request.Ack) got.get(0).req();
      assertEquals("c", ack.consumer());
      assertArrayEquals(new long[] {7}, ack.offsets());
      assertNotEquals(0, got.get(1).corr());
      assertEquals(new Request.Nack("c", 7, 250), got.get(1).req());
      assertEquals(new Request.Term("c", 7, "poison"), got.get(2).req());
      Request.InProgress ip = (Request.InProgress) got.get(3).req();
      assertArrayEquals(new long[] {7}, ip.offsets());
      assertEquals(new Request.Nack("c", 7, 0), got.get(4).req());
    }

    @Test
    void coalescesBackToBackAcksAndKeepsThemBeforeLaterRequests() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          List<WireRecord> recs = new ArrayList<>();
          for (int i = 0; i < 200; i++) {
            recs.add(rec(i));
          }
          conn.replyMany(corr, new Response.SubscribeOk(1), 0, new Response.Deliver(1, recs));
        }
        return false;
      };
      Subscription sub = c.subscribe("c", 1000);
      server.until(() -> sub.buffered() == 200);
      List<Message> msgs = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        msgs.add(sub.next());
      }
      for (Message m : msgs) {
        m.ack();
      }
      c.ping();
      List<String> types = server.last().types();
      int ping = types.lastIndexOf("Ping");
      List<Long> acked = new ArrayList<>();
      for (FakeServer.Received r : server.last().of(Request.Ack.class)) {
        assertEquals(0, r.corr());
        assertTrue(server.last().received.indexOf(r) < ping, "acks go out before the later ping");
        for (long o : ((Request.Ack) r.req()).offsets()) {
          acked.add(o);
        }
      }
      List<Long> expected = new ArrayList<>();
      for (long i = 0; i < 200; i++) {
        expected.add(i);
      }
      assertEquals(expected, acked);
      assertTrue(server.last().of(Request.Ack.class).size() < 200, "acks are merged into fewer frames");
    }

    @Test
    void flushesQueuedAcksOnClose() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(1), 0, new Response.Deliver(1, List.of(rec(4))));
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      sub.next().ack();
      FakeConn conn = server.last();
      c.close();
      client = null;
      server.until(() -> conn.of(Request.Ack.class).size() == 1);
    }

    @Test
    void reportsFailedFireAndForgetRequestsToListeners() throws Exception {
      List<ExspeedException> errors = new CopyOnWriteArrayList<>();
      ExspeedClient c = setup(b -> b.listener(new ClientListener() {
        @Override
        public void onError(ExspeedException error) {
          errors.add(error);
        }
      }));
      assertTrue(c.isConnected());
      server.last().reply(0, new Response.Error(404, "consumer 'x' not found", null));
      server.until(() -> errors.size() == 1);
      assertEquals(404, ((ServerException) errors.get(0)).code());
    }

    @Test
    void endsOnSubscriptionEndedAfterYieldingBufferedRecords() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(3), 0, new Response.Deliver(3, List.of(rec(0))), 0,
              new Response.SubscriptionEnded(3, 404, "consumer deleted"));
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      List<Long> seen = new ArrayList<>();
      for (Message m : sub) {
        seen.add(m.offset());
      }
      assertEquals(List.of(0L), seen);
      assertEquals(new EndReason(404, "consumer deleted"), sub.endReason());
      assertTrue(sub.isClosed());
    }

    @Test
    void unsubscribesOnClose() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(4), 0, new Response.Deliver(4, List.of(rec(0), rec(1))));
        } else if (req instanceof Request.Unsubscribe) {
          conn.reply(corr, new Response.Ok());
        }
        return false;
      };
      try (Subscription sub = c.subscribe("c")) {
        for (Message m : sub) {
          assertEquals(0, m.offset());
          break;
        }
        sub.close();
        assertEquals(new Request.Unsubscribe(4), server.last().reqs(Request.Unsubscribe.class).get(0));
        assertEquals(new EndReason(0, "unsubscribed"), sub.endReason());
        assertNull(sub.next());
      }
      assertEquals(1, server.last().of(Request.Unsubscribe.class).size()); // idempotent
    }

    @Test
    void deliversToACallbackListener() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(6), 0, new Response.Deliver(6, List.of(rec(0), rec(1), rec(2))),
              0, new Response.SubscriptionEnded(6, 404, "gone"));
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      List<Long> seen = new CopyOnWriteArrayList<>();
      EndReason end = sub.listen(m -> seen.add(m.offset())).get(2, TimeUnit.SECONDS);
      assertEquals(List.of(0L, 1L, 2L), seen);
      assertEquals(404, end.code());
      assertThrows(IllegalStateException.class, () -> sub.listen(m -> {}));
    }

    @Test
    void aFailingCallbackClosesTheSubscription() throws Exception {
      ExspeedClient c = setup();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.replyMany(corr, new Response.SubscribeOk(6), 0, new Response.Deliver(6, List.of(rec(0))));
        } else if (req instanceof Request.Unsubscribe) {
          conn.reply(corr, new Response.Ok());
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      CompletableFuture<EndReason> done = sub.listen(m -> {
        throw new IllegalStateException("boom");
      });
      Throwable t = assertThrows(java.util.concurrent.ExecutionException.class, () -> done.get(2, TimeUnit.SECONDS))
          .getCause();
      assertEquals("boom", t.getMessage());
      assertTrue(sub.isClosed());
      server.until(() -> server.last().of(Request.Unsubscribe.class).size() == 1);
    }

    @Test
    void endsSubscriptionsWith503WhenTheConnectionIsLostAndReconnectIsOff() throws Exception {
      CountDownLatch closed = new CountDownLatch(1);
      ExspeedClient c = setup(b -> b.listener(new ClientListener() {
        @Override
        public void onClose(Throwable cause) {
          closed.countDown();
        }
      }));
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.reply(corr, new Response.SubscribeOk(1));
        }
        return false;
      };
      Subscription sub = c.subscribe("c");
      CompletableFuture<Message> next = CompletableFuture.supplyAsync(sub::next);
      Thread.sleep(50);
      server.last().destroy();
      assertNull(next.get(2, TimeUnit.SECONDS));
      assertTrue(closed.await(2, TimeUnit.SECONDS));
      assertEquals(503, sub.endReason().code());
      assertFalse(c.isConnected());
      assertTrue(c.isClosed());
    }

    @Test
    void releasesASubscriptionWhoseSubscribeTimedOut() throws Exception {
      ExspeedClient c = setup(b -> b.requestTimeout(Duration.ofMillis(100)));
      List<Integer> held = new CopyOnWriteArrayList<>();
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          held.add(corr);
          return true;
        }
        return false;
      };
      assertThrows(RequestTimeoutException.class, () -> c.subscribe("c"));
      server.last().reply(held.get(0), new Response.SubscribeOk(77));
      server.until(() -> server.last().of(Request.Unsubscribe.class).size() == 1);
      FakeServer.Received u = server.last().of(Request.Unsubscribe.class).get(0);
      assertEquals(0, u.corr());
      assertEquals(new Request.Unsubscribe(77), u.req());
    }
  }

  @Nested
  class Reconnection {
    @Test
    void reconnectsRecreatesEphemeralConsumersAndResubscribes() throws Exception {
      server = FakeServer.start();
      int[] nextSub = {1};
      server.handler = (conn, corr, req) -> {
        if (req instanceof Request.Subscribe) {
          conn.reply(corr, new Response.SubscribeOk(nextSub[0]++));
        }
        if (req instanceof Request.CreateConsumer) {
          conn.reply(corr, new Response.Json(b("{}")));
        }
        return false;
      };
      List<String> events = new CopyOnWriteArrayList<>();
      CountDownLatch reconnected = new CountDownLatch(1);
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofMillis(10), Duration.ofMillis(20)))
          .listener(new ClientListener() {
            @Override
            public void onDisconnect(Throwable cause) {
              events.add("disconnect");
            }

            @Override
            public void onReconnect(ServerInfo info) {
              events.add("reconnect");
              reconnected.countDown();
            }
          }).build());
      ExspeedClient c = client;
      c.createConsumer(ConsumerSpec.builder("tmp", "s").ephemeral(true).build());
      Subscription sub = c.subscribe("tmp", 8);
      assertEquals(1, sub.id());
      server.last().reply(0, new Response.Deliver(1, List.of(rec(0))));
      assertEquals(0, sub.next(Duration.ofSeconds(2)).offset());

      server.conns.get(0).destroy();
      assertTrue(reconnected.await(5, TimeUnit.SECONDS));
      assertEquals(List.of("disconnect", "reconnect"), events);
      assertEquals(2, server.conns.size());
      FakeConn second = server.conns.get(1);
      assertEquals(List.of("Connect", "CreateConsumer", "Subscribe"), second.types());
      assertEquals(new Request.Subscribe("tmp", 8), second.reqs(Request.Subscribe.class).get(0));
      assertEquals(2, sub.id());

      second.reply(0, new Response.Deliver(2, List.of(rec(1))));
      assertEquals(1, sub.next(Duration.ofSeconds(2)).offset());
      assertTrue(c.isConnected());
    }

    @Test
    void endsASubscriptionWhoseResubscribeFails() throws Exception {
      server = FakeServer.start();
      boolean[] first = {true};
      server.handler = (conn, corr, req) -> {
        if (!(req instanceof Request.Subscribe)) {
          return false;
        }
        if (first[0]) {
          conn.reply(corr, new Response.SubscribeOk(1));
        } else {
          conn.reply(corr, new Response.Error(404, "consumer 'c' not found", null));
        }
        first[0] = false;
        return true;
      };
      CountDownLatch reconnected = new CountDownLatch(1);
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofMillis(10), Duration.ofMillis(50)))
          .listener(new ClientListener() {
            @Override
            public void onReconnect(ServerInfo info) {
              reconnected.countDown();
            }
          }).build());
      Subscription sub = client.subscribe("c");
      server.conns.get(0).destroy();
      assertTrue(reconnected.await(5, TimeUnit.SECONDS));
      assertNull(sub.next(Duration.ofSeconds(1)));
      assertEquals(new EndReason(404, "consumer 'c' not found"), sub.endReason());
    }

    @Test
    void failsRequestsWhileReconnecting() throws Exception {
      server = FakeServer.start();
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofSeconds(5), Duration.ofSeconds(5))).build());
      server.last().destroy();
      ExspeedClient c = client;
      server.until(() -> !c.isConnected());
      ConnectionException e = assertThrows(ConnectionException.class, c::ping);
      assertTrue(e.getMessage().contains("reconnecting"), e.getMessage());
    }

    @Test
    void givesUpAfterMaxAttemptsAndCloses() throws Exception {
      server = FakeServer.start();
      CompletableFuture<Throwable> closed = new CompletableFuture<>();
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(new ReconnectOptions(2, Duration.ofMillis(10), Duration.ofMillis(20)))
          .listener(new ClientListener() {
            @Override
            public void onClose(Throwable cause) {
              closed.complete(cause);
            }
          }).build());
      server.close();
      Throwable err = closed.get(5, TimeUnit.SECONDS);
      assertInstanceOf(ConnectionException.class, err);
      assertFalse(client.isConnected());
      assertTrue(client.isClosed());
      assertTrue(assertThrows(ConnectionException.class, client::ping).getMessage().contains("closed"));
    }

    @Test
    void closeStopsAReconnectInProgress() throws Exception {
      server = FakeServer.start();
      client = ExspeedClient.connect(ClientOptions.builder().port(server.port()).keepalive(Duration.ZERO)
          .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofSeconds(10), Duration.ofSeconds(10))).build());
      server.close();
      ExspeedClient c = client;
      server.until(() -> !c.isConnected());
      long t = System.nanoTime();
      c.close();
      assertTrue(System.nanoTime() - t < TimeUnit.SECONDS.toNanos(3));
      assertTrue(c.isClosed());
    }
  }

  @Nested
  class Keepalive {
    @Test
    void pingsOnTheConfiguredInterval() throws Exception {
      setup(b -> b.keepalive(Duration.ofMillis(30)));
      server.until(() -> server.last().of(Request.Ping.class).size() >= 2, 2000);
    }

    @Test
    void dropsTheConnectionWhenAPingTimesOut() throws Exception {
      CountDownLatch closed = new CountDownLatch(1);
      setup(b -> b.keepalive(Duration.ofMillis(50)).requestTimeout(Duration.ofMillis(100))
          .listener(new ClientListener() {
            @Override
            public void onClose(Throwable cause) {
              closed.countDown();
            }
          }));
      server.handler = (conn, corr, req) -> req instanceof Request.Ping; // never answer pings
      assertTrue(closed.await(3, TimeUnit.SECONDS));
      assertFalse(client.isConnected());
    }
  }

  @Nested
  class ClusterLeaderDiscovery {
    @Test
    void connectsToTheLeaderByFollowingHintsFromASeed() throws Exception {
      int[] leaderPort = {0};
      FakeServer follower = FakeServer.start((conn, corr, req) -> {
        if (req instanceof Request.Connect) {
          conn.reply(corr, new Response.ConnectOk("t", "f", "127.0.0.1:" + leaderPort[0]));
          return true;
        }
        if (req instanceof Request.Metadata) {
          conn.reply(corr, new Response.Json(b("{\"node_id\":\"f\",\"is_leader\":false,\"leader\":\"127.0.0.1:"
              + leaderPort[0] + "\",\"server_version\":\"t\"}")));
          return true;
        }
        return false;
      });
      FakeServer leader = FakeServer.start((conn, corr, req) -> {
        if (req instanceof Request.Connect) {
          conn.reply(corr, new Response.ConnectOk("t", "l", null));
          return true;
        }
        if (req instanceof Request.Metadata) {
          conn.reply(corr, new Response.Json(b("{\"node_id\":\"l\",\"is_leader\":true,\"leader\":null,"
              + "\"server_version\":\"t\"}")));
          return true;
        }
        return false;
      });
      leaderPort[0] = leader.port();
      server = follower;
      try {
        client = ExspeedClient.connect(ClientOptions.builder().servers("127.0.0.1:" + follower.port())
            .keepalive(Duration.ZERO).reconnect(false).build());
        assertEquals("l", client.serverInfo().nodeId());
        client.close();
        client = null;
        // Without seeds, a handshake naming another leader is followed too.
        client = ExspeedClient.connect(ClientOptions.builder().port(follower.port()).keepalive(Duration.ZERO)
            .reconnect(false).build());
        assertEquals("l", client.serverInfo().nodeId());
      } finally {
        if (client != null) {
          client.close();
          client = null;
        }
        leader.close();
      }
    }
  }

  @Test
  void closeLeavesNoThreadsBehind() throws Exception {
    java.util.Set<Thread> before = exspeedThreads();
    ExspeedClient c = setup(b -> b.keepalive(Duration.ofMillis(50)));
    server.handler = (conn, corr, req) -> {
      if (req instanceof Request.Subscribe) {
        conn.reply(corr, new Response.SubscribeOk(1));
      }
      return false;
    };
    Subscription sub = c.subscribe("c");
    sub.listen(m -> {});
    c.publisher().publishAsync("s", PublishRecord.of("a", "b"));
    assertTrue(exspeedThreads().size() > before.size());
    c.close();
    client = null;
    server.until(() -> {
      java.util.Set<Thread> now = exspeedThreads();
      now.removeAll(before);
      return now.isEmpty();
    });
  }

  static java.util.Set<Thread> exspeedThreads() {
    java.util.Set<Thread> out = new java.util.HashSet<>();
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.isAlive() && t.getName().startsWith("exspeed-")) {
        out.add(t);
      }
    }
    return out;
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
