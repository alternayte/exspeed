package io.exspeed.client;

import io.exspeed.client.protocol.WireRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** One revision of a KV key. */
public final class KvEntry {
  private final String key;
  private final byte[] value;
  private final long revision;
  private final long timestampNs;
  private final KvOp op;

  KvEntry(WireRecord r) {
    String opHeader = null;
    for (Header h : r.headers()) {
      if (h.key().equals(KvBucket.KV_OP_HEADER)) {
        opHeader = h.value();
        break;
      }
    }
    this.key = r.subject();
    this.value = r.value();
    this.revision = r.offset() + 1;
    this.timestampNs = r.timestampNs();
    this.op = "DEL".equals(opHeader) ? KvOp.DELETE : "PURGE".equals(opHeader) ? KvOp.PURGE : KvOp.PUT;
  }

  /**
   * The key.
   *
   * @return the key
   */
  public String key() {
    return key;
  }

  /**
   * The value (empty for tombstones). Don't modify the array.
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
   * The revision: the record's offset in the bucket's stream plus one (0 = absent).
   *
   * @return the revision
   */
  public long revision() {
    return revision;
  }

  /**
   * Write time, ns since the Unix epoch.
   *
   * @return the timestamp in ns
   */
  public long timestampNs() {
    return timestampNs;
  }

  /**
   * Write time.
   *
   * @return the timestamp
   */
  public Instant timestamp() {
    return Instant.ofEpochSecond(timestampNs / 1_000_000_000L, timestampNs % 1_000_000_000L);
  }

  /**
   * What this revision holds.
   *
   * @return {@code PUT}, or {@code DELETE} / {@code PURGE} for tombstones
   */
  public KvOp op() {
    return op;
  }

  @Override
  public String toString() {
    return "KvEntry{key=" + key + ", revision=" + revision + ", op=" + op + "}";
  }
}
