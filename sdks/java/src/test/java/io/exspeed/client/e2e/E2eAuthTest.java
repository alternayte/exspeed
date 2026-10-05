package io.exspeed.client.e2e;

import static io.exspeed.client.e2e.TestServer.uniq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.exspeed.client.ExspeedClient;
import io.exspeed.client.ServerException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Token authentication against a real server. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2eAuthTest {
  TestServer server;

  @BeforeAll
  void start() throws Exception {
    TestServer.assumeAvailable();
    server = TestServer.start(new TestServer.Options("s3cret-token", null, null, null));
  }

  @AfterAll
  void stop() {
    if (server != null) {
      server.close();
    }
  }

  @Test
  void rejectsAWrongOrMissingTokenWith401() {
    for (String token : new String[] {"wrong", null}) {
      ServerException e = assertThrows(ServerException.class, () -> server.connect(b -> b.token(token)));
      assertEquals(401, e.code());
    }
  }

  @Test
  void acceptsTheRightToken() {
    try (ExspeedClient c = server.connect(b -> b.token("s3cret-token"))) {
      String s = uniq("authed");
      c.createStream(s);
      assertEquals(0, c.publish(s, "auth.ok", "yes").offset());
      assertEquals(List.of(List.of(1L)), c.query("SELECT COUNT(*) AS n FROM \"" + s + "\"").rows());
    }
  }
}
