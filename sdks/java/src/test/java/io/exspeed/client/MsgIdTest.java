package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MsgIdTest {
  @Test
  void returnsUniqueUuidV7Strings() {
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String id = MsgId.newMsgId();
      assertTrue(id.matches("^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"), id);
      assertEquals(36, id.length());
      ids.add(id);
    }
    assertEquals(1000, ids.size());
  }

  @Test
  void laterIdsSortAfterEarlierOnes() throws Exception {
    String a = MsgId.newMsgId();
    Thread.sleep(2);
    assertTrue(a.compareTo(MsgId.newMsgId()) < 0);
  }

  @Test
  void embedsTheCurrentTime() {
    long before = System.currentTimeMillis();
    String id = MsgId.newMsgId();
    long ts = Long.parseLong(id.substring(0, 8) + id.substring(9, 13), 16);
    assertTrue(ts >= before && ts <= System.currentTimeMillis());
  }
}
