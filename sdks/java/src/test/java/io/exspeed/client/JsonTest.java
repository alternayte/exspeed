package io.exspeed.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {
  @Test
  void parsesEveryValueType() {
    Object v = Json.parse(" {\"a\": [1, -2.5, \"x\\n\\u00e9\\\"\", true, false, null], \"b\": {}, \"c\": []} ");
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("a", Arrays.asList(1L, -2.5, "x\né\"", true, false, null));
    expected.put("b", Map.of());
    expected.put("c", List.of());
    assertEquals(expected, v);
    assertEquals(new BigInteger("18446744073709551615"), Json.parse("18446744073709551615"));
    assertEquals(1e3, Json.parse("1e3"));
    assertNull(Json.parse("null"));
  }

  @Test
  void rejectsInvalidJson() {
    for (String bad : new String[] {"", "{", "[1,", "{\"a\" 1}", "tru", "\"unterminated", "1 2", "{1:2}", "-",
        "\"\\x\""}) {
      assertThrows(ProtocolException.class, () -> Json.parse(bad), bad);
    }
    assertThrows(ProtocolException.class, () -> Json.parse("[".repeat(1000)));
  }

  @Test
  void stringifiesCompactlyLikeSerdeJson() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("s", "é\"\\\n\t\u0001/");
    m.put("n", 1L);
    m.put("d", 1.5);
    m.put("whole", 2.0);
    m.put("l", List.of(true, false));
    m.put("arr", new Object[] {1, "x"});
    m.put("null", null);
    assertEquals("{\"s\":\"é\\\"\\\\\\n\\t\\u0001/\",\"n\":1,\"d\":1.5,\"whole\":2.0,\"l\":[true,false],"
        + "\"arr\":[1,\"x\"],\"null\":null}", Json.stringify(m));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(new Object()));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Double.NaN));
  }

  @Test
  void roundTrips() {
    String s = "{\"name\":\"c\",\"nested\":{\"list\":[1,2,{\"x\":\"y\"}]},\"f\":0.25}";
    assertEquals(s, Json.stringify(Json.parse(s)));
  }
}
