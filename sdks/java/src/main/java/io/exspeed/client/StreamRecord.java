package io.exspeed.client;

import io.exspeed.client.protocol.WireRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * A record read from a stream. The byte arrays returned by {@link #key()} and
 * {@link #value()} are the record's own; don't modify them.
 */
public class StreamRecord {
  private final long offset;
  private final long timestampNs;
  private final String subject;
  private final byte[] key;
  private final byte[] value;
  private final List<Header> headers;

  StreamRecord(WireRecord r) {
    this.offset = r.offset();
    this.timestampNs = r.timestampNs();
    this.subject = r.subject();
    this.key = r.key();
    this.value = r.value();
    this.headers = Collections.unmodifiableList(r.headers());
  }

  /**
   * The record's offset in its stream.
   *
   * @return the offset
   */
  public long offset() {
    return offset;
  }

  /**
   * Append time, nanoseconds since the Unix epoch (full precision).
   *
   * @return the timestamp in ns
   */
  public long timestampNs() {
    return timestampNs;
  }

  /**
   * Append time, milliseconds since the Unix epoch.
   *
   * @return the timestamp in ms
   */
  public long timestampMs() {
    return Long.divideUnsigned(timestampNs, 1_000_000L);
  }

  /**
   * Append time.
   *
   * @return the timestamp
   */
  public Instant timestamp() {
    return Instant.ofEpochSecond(Long.divideUnsigned(timestampNs, 1_000_000_000L),
        Long.remainderUnsigned(timestampNs, 1_000_000_000L));
  }

  /**
   * The subject.
   *
   * @return the subject
   */
  public String subject() {
    return subject;
  }

  /**
   * The key.
   *
   * @return the key bytes, or {@code null} when the record has none
   */
  public byte[] key() {
    return key;
  }

  /**
   * The key as UTF-8 text.
   *
   * @return the key, or {@code null} when the record has none
   */
  public String keyText() {
    return key == null ? null : new String(key, StandardCharsets.UTF_8);
  }

  /**
   * The value.
   *
   * @return the value bytes
   */
  public byte[] value() {
    return value;
  }

  /**
   * The value as UTF-8 text.
   *
   * @return the text
   */
  public String text() {
    return new String(value, StandardCharsets.UTF_8);
  }

  /**
   * The value parsed as JSON (see {@link Json#parse(String)}).
   *
   * @return the parsed value
   */
  public Object json() {
    return Json.parse(text());
  }

  /**
   * The headers, in wire order; a key can repeat.
   *
   * @return the headers (unmodifiable)
   */
  public List<Header> headers() {
    return headers;
  }

  /**
   * The first header named {@code name}.
   *
   * @param name the header name
   * @return its value, or {@code null} when absent
   */
  public String header(String name) {
    for (Header h : headers) {
      if (h.key().equals(name)) {
        return h.value();
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{offset=" + offset + ", subject=" + subject + ", value=" + value.length
        + " bytes}";
  }
}
