package io.exspeed.client.e2e;

import static io.exspeed.client.e2e.TestServer.uniq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.exspeed.client.ClientListener;
import io.exspeed.client.ConnectionException;
import io.exspeed.client.ConsumerSpec;
import io.exspeed.client.CoreSubscription;
import io.exspeed.client.ExspeedClient;
import io.exspeed.client.Message;
import io.exspeed.client.ReconnectOptions;
import io.exspeed.client.ServerInfo;
import io.exspeed.client.Subscription;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Reconnection across a real server restart. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2eReconnectTest {
  TestServer server;

  @BeforeAll
  void start() throws Exception {
    TestServer.assumeAvailable();
    server = TestServer.start();
  }

  @AfterAll
  void stop() {
    if (server != null) {
      server.close();
    }
  }

  @Test
  void resubscribesAfterTheServerRestartsAndRedeliversUnackedRecords() throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch reconnected = new CountDownLatch(1);
    try (ExspeedClient client = server.connect(b -> b
        .reconnect(ReconnectOptions.DEFAULT.withDelays(Duration.ofMillis(50), Duration.ofMillis(200)))
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
        }))) {
      String s = uniq("durable");
      String c = uniq("durable-c");
      client.createStream(s);
      client.createConsumer(ConsumerSpec.of(c, s));
      client.publish(s, "work.item", "before");
      Subscription sub = client.subscribe(c, 10);
      CoreSubscription core = client.subscribeCore("notify.>");
      Message m1 = sub.next(Duration.ofSeconds(5));
      assertEquals("before", m1.text()); // not acked

      server = server.restart();
      assertTrue(reconnected.await(15, TimeUnit.SECONDS));
      assertEquals(List.of("disconnect", "reconnect"), events);
      assertTrue(client.isConnected());

      Message again = sub.next(Duration.ofSeconds(10));
      assertEquals("before", again.text());
      // (The delivery count may restart at 1: the server persists consumer
      // state in periodic snapshots.)
      again.ack();
      client.publish(s, "work.item", "after");
      Message m2 = sub.next(Duration.ofSeconds(5));
      assertEquals("after", m2.text());
      m2.ack();
      assertFalse(sub.isClosed());

      // The core subscription was restored too.
      client.publishCore("notify.x", "hi");
      assertEquals("hi", core.next(Duration.ofSeconds(5)).text());
    }
  }

  @Test
  void failsRequestsWithConnectionExceptionWhenDisconnectedAndReconnectIsOff() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    ExspeedClient client = server.connect(b -> b.reconnect(false).listener(new ClientListener() {
      @Override
      public void onClose(Throwable cause) {
        closed.countDown();
      }
    }));
    server = server.restart();
    assertTrue(closed.await(10, TimeUnit.SECONDS));
    assertThrows(ConnectionException.class, client::ping);
    client.close();
  }
}
