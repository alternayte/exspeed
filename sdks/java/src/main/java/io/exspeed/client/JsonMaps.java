package io.exspeed.client;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Typed access to parsed JSON objects (internal). */
final class JsonMaps {
  private JsonMaps() {}

  @SuppressWarnings("unchecked")
  static Map<String, Object> asMap(Object v) {
    return v instanceof Map<?, ?> m ? (Map<String, Object>) m : Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  static List<Object> asList(Object v) {
    return v instanceof List<?> l ? (List<Object>) l : Collections.emptyList();
  }

  static long num(Map<String, Object> m, String key, long dflt) {
    Object v = m.get(key);
    if (v instanceof BigInteger b) {
      return b.longValue();
    }
    return v instanceof Number n ? n.longValue() : dflt;
  }

  static Long numOrNull(Map<String, Object> m, String key) {
    Object v = m.get(key);
    return v instanceof Number n ? n.longValue() : null;
  }

  static double dbl(Map<String, Object> m, String key, double dflt) {
    Object v = m.get(key);
    return v instanceof Number n ? n.doubleValue() : dflt;
  }

  static boolean bool(Map<String, Object> m, String key, boolean dflt) {
    Object v = m.get(key);
    return v instanceof Boolean b ? b : dflt;
  }

  static String str(Map<String, Object> m, String key, String dflt) {
    Object v = m.get(key);
    return v instanceof String s ? s : dflt;
  }
}
