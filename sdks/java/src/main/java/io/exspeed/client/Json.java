package io.exspeed.client;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON codec, used for the protocol's JSON payloads and available to
 * applications for record values.
 *
 * <p>{@link #parse(String)} produces {@code Map<String, Object>} (insertion
 * ordered), {@code List<Object>}, {@code String}, {@code Long} (or
 * {@code BigInteger} when it doesn't fit), {@code Double}, {@code Boolean} and
 * {@code null}. {@link #stringify(Object)} accepts those types plus any
 * {@code Number}, {@code Collection}, array of objects and {@code Character},
 * and writes compact JSON the way {@code serde_json} does (no spaces;
 * non-ASCII characters unescaped).
 */
public final class Json {
  private Json() {}

  /**
   * Parses a JSON document.
   *
   * @param text the JSON text
   * @return the parsed value
   * @throws ProtocolException when the text is not valid JSON
   */
  public static Object parse(String text) {
    Parser p = new Parser(text);
    p.ws();
    Object v = p.value();
    p.ws();
    if (p.pos != text.length()) {
      throw p.error("trailing characters");
    }
    return v;
  }

  /**
   * Serializes a value as compact JSON.
   *
   * @param value a map, collection, array, string, number, boolean or {@code null}
   * @return the JSON text
   * @throws IllegalArgumentException for a value of another type, or a non-finite number
   */
  public static String stringify(Object value) {
    StringBuilder sb = new StringBuilder();
    write(sb, value);
    return sb.toString();
  }

  private static void write(StringBuilder sb, Object v) {
    if (v == null) {
      sb.append("null");
    } else if (v instanceof String s) {
      quote(sb, s);
    } else if (v instanceof Character c) {
      quote(sb, c.toString());
    } else if (v instanceof Boolean b) {
      sb.append(b.booleanValue());
    } else if (v instanceof Double || v instanceof Float) {
      double d = ((Number) v).doubleValue();
      if (Double.isNaN(d) || Double.isInfinite(d)) {
        throw new IllegalArgumentException("JSON cannot represent " + d);
      }
      if (d == Math.rint(d) && Math.abs(d) < 1e15) {
        sb.append((long) d).append(".0");
      } else {
        sb.append(d);
      }
    } else if (v instanceof BigDecimal bd) {
      sb.append(bd.toString());
    } else if (v instanceof Number n) {
      sb.append(n.toString());
    } else if (v instanceof Map<?, ?> m) {
      sb.append('{');
      boolean first = true;
      for (Map.Entry<?, ?> e : m.entrySet()) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        quote(sb, String.valueOf(e.getKey()));
        sb.append(':');
        write(sb, e.getValue());
      }
      sb.append('}');
    } else if (v instanceof Collection<?> c) {
      sb.append('[');
      boolean first = true;
      for (Object x : c) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        write(sb, x);
      }
      sb.append(']');
    } else if (v instanceof Object[] arr) {
      write(sb, List.of(arr));
    } else {
      throw new IllegalArgumentException("not JSON-serializable: " + v.getClass().getName());
    }
  }

  private static void quote(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    sb.append('"');
  }

  private static final class Parser {
    final String s;
    int pos;
    int depth;

    Parser(String s) {
      this.s = s;
    }

    ProtocolException error(String what) {
      return new ProtocolException("invalid JSON at position " + pos + ": " + what);
    }

    void ws() {
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
          pos++;
        } else {
          break;
        }
      }
    }

    Object value() {
      if (pos >= s.length()) {
        throw error("unexpected end");
      }
      char c = s.charAt(pos);
      switch (c) {
        case '{':
          return object();
        case '[':
          return array();
        case '"':
          return string();
        case 't':
          literal("true");
          return Boolean.TRUE;
        case 'f':
          literal("false");
          return Boolean.FALSE;
        case 'n':
          literal("null");
          return null;
        default:
          if (c == '-' || (c >= '0' && c <= '9')) {
            return number();
          }
          throw error("unexpected character '" + c + "'");
      }
    }

    void literal(String word) {
      if (!s.startsWith(word, pos)) {
        throw error("expected " + word);
      }
      pos += word.length();
    }

    void enter() {
      if (++depth > 512) {
        throw error("nested too deeply");
      }
    }

    Map<String, Object> object() {
      enter();
      pos++;
      Map<String, Object> out = new LinkedHashMap<>();
      ws();
      if (pos < s.length() && s.charAt(pos) == '}') {
        pos++;
        depth--;
        return out;
      }
      while (true) {
        ws();
        if (pos >= s.length() || s.charAt(pos) != '"') {
          throw error("expected a string key");
        }
        String k = string();
        ws();
        if (pos >= s.length() || s.charAt(pos) != ':') {
          throw error("expected ':'");
        }
        pos++;
        ws();
        out.put(k, value());
        ws();
        if (pos >= s.length()) {
          throw error("unexpected end");
        }
        char c = s.charAt(pos++);
        if (c == '}') {
          depth--;
          return out;
        }
        if (c != ',') {
          throw error("expected ',' or '}'");
        }
      }
    }

    List<Object> array() {
      enter();
      pos++;
      List<Object> out = new ArrayList<>();
      ws();
      if (pos < s.length() && s.charAt(pos) == ']') {
        pos++;
        depth--;
        return out;
      }
      while (true) {
        ws();
        out.add(value());
        ws();
        if (pos >= s.length()) {
          throw error("unexpected end");
        }
        char c = s.charAt(pos++);
        if (c == ']') {
          depth--;
          return out;
        }
        if (c != ',') {
          throw error("expected ',' or ']'");
        }
      }
    }

    String string() {
      pos++;
      StringBuilder sb = new StringBuilder();
      while (true) {
        if (pos >= s.length()) {
          throw error("unterminated string");
        }
        char c = s.charAt(pos++);
        if (c == '"') {
          return sb.toString();
        }
        if (c == '\\') {
          if (pos >= s.length()) {
            throw error("unterminated escape");
          }
          char e = s.charAt(pos++);
          switch (e) {
            case '"' -> sb.append('"');
            case '\\' -> sb.append('\\');
            case '/' -> sb.append('/');
            case 'b' -> sb.append('\b');
            case 'f' -> sb.append('\f');
            case 'n' -> sb.append('\n');
            case 'r' -> sb.append('\r');
            case 't' -> sb.append('\t');
            case 'u' -> {
              if (pos + 4 > s.length()) {
                throw error("bad unicode escape");
              }
              try {
                sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
              } catch (NumberFormatException ex) {
                throw error("bad unicode escape");
              }
              pos += 4;
            }
            default -> throw error("bad escape '\\" + e + "'");
          }
        } else if (c < 0x20) {
          throw error("control character in string");
        } else {
          sb.append(c);
        }
      }
    }

    Object number() {
      int start = pos;
      if (s.charAt(pos) == '-') {
        pos++;
      }
      boolean fraction = false;
      boolean digits = false;
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c >= '0' && c <= '9') {
          digits = true;
          pos++;
        } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || (c == '-' && pos > start)) {
          fraction = true;
          pos++;
        } else {
          break;
        }
      }
      if (!digits) {
        throw error("bad number");
      }
      String n = s.substring(start, pos);
      try {
        if (fraction) {
          return Double.parseDouble(n);
        }
        BigInteger big = new BigInteger(n);
        return big.bitLength() < 64 ? (Object) big.longValue() : big;
      } catch (NumberFormatException e) {
        throw error("bad number '" + n + "'");
      }
    }
  }
}
