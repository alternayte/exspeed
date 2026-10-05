package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Little-endian writer for the protocol's building blocks, mirroring
 * {@code Writer} in {@code crates/exspeed-protocol/src/client.rs}.
 *
 * <ul>
 *   <li>{@code str} = u16 length + UTF-8 bytes
 *   <li>{@code lstr} = u32 length + UTF-8 bytes (SQL text)
 *   <li>{@code bytes} = u32 length + raw bytes
 *   <li>{@code opt<T>} = u8 flag (0 = absent, 1 = present) + T
 *   <li>{@code headers} = u16 count + (str key, str value) pairs
 *   <li>{@code vec<T>} = u32 count + items
 * </ul>
 *
 * <p>A value that doesn't fit its length prefix is never truncated: the writer
 * throws {@link ProtocolException}.
 */
public final class WireWriter {
  private byte[] buf;
  private int pos;

  /** Creates a writer with a small initial buffer. */
  public WireWriter() {
    this(128);
  }

  /**
   * Creates a writer.
   *
   * @param initialSize the initial buffer size in bytes
   */
  public WireWriter(int initialSize) {
    buf = new byte[Math.max(1, initialSize)];
  }

  private void ensure(int n) {
    int need = pos + n;
    if (need > buf.length) {
      int size = buf.length * 2;
      while (size < need) {
        size *= 2;
      }
      buf = Arrays.copyOf(buf, size);
    }
  }

  /**
   * The bytes written so far.
   *
   * @return a copy of the written bytes
   */
  public byte[] finish() {
    return Arrays.copyOf(buf, pos);
  }

  /**
   * Number of bytes written so far.
   *
   * @return the size
   */
  public int size() {
    return pos;
  }

  /**
   * Writes an unsigned byte.
   *
   * @param v the value (low 8 bits are written)
   * @return this writer
   */
  public WireWriter u8(int v) {
    ensure(1);
    buf[pos++] = (byte) v;
    return this;
  }

  /**
   * Writes a u16, little-endian.
   *
   * @param v the value (low 16 bits are written)
   * @return this writer
   */
  public WireWriter u16(int v) {
    ensure(2);
    buf[pos++] = (byte) v;
    buf[pos++] = (byte) (v >>> 8);
    return this;
  }

  /**
   * Writes a u32, little-endian.
   *
   * @param v the value (its 32 bits, read as unsigned)
   * @return this writer
   */
  public WireWriter u32(int v) {
    ensure(4);
    Frame.putU32(buf, pos, v);
    pos += 4;
    return this;
  }

  /**
   * Writes a u64, little-endian.
   *
   * @param v the value (its 64 bits, read as unsigned)
   * @return this writer
   */
  public WireWriter u64(long v) {
    ensure(8);
    for (int i = 0; i < 8; i++) {
      buf[pos++] = (byte) (v >>> (8 * i));
    }
    return this;
  }

  /**
   * Writes bytes with no length prefix.
   *
   * @param b the bytes
   * @return this writer
   */
  public WireWriter raw(byte[] b) {
    return raw(b, 0, b.length);
  }

  /**
   * Writes a slice of bytes with no length prefix.
   *
   * @param b the bytes
   * @param off where the slice starts
   * @param len the slice length
   * @return this writer
   */
  public WireWriter raw(byte[] b, int off, int len) {
    ensure(len);
    System.arraycopy(b, off, buf, pos, len);
    pos += len;
    return this;
  }

  /**
   * Writes a {@code str}: u16 length + UTF-8.
   *
   * @param s the string
   * @return this writer
   * @throws ProtocolException when the UTF-8 encoding exceeds 65,535 bytes
   */
  public WireWriter str(String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    if (b.length > 0xffff) {
      throw new ProtocolException("string of " + b.length + " bytes exceeds the 65535 byte limit");
    }
    u16(b.length);
    return raw(b);
  }

  /**
   * Writes an {@code lstr}: u32 length + UTF-8.
   *
   * @param s the string
   * @return this writer
   */
  public WireWriter lstr(String s) {
    return bytes(s.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Writes {@code bytes}: u32 length + raw bytes.
   *
   * @param b the bytes
   * @return this writer
   */
  public WireWriter bytes(byte[] b) {
    u32(b.length);
    return raw(b);
  }

  /**
   * Writes an optional {@code str}.
   *
   * @param s the string, or {@code null} for absent
   * @return this writer
   */
  public WireWriter optStr(String s) {
    if (s == null) {
      return u8(0);
    }
    u8(1);
    return str(s);
  }

  /**
   * Writes optional {@code bytes}.
   *
   * @param b the bytes, or {@code null} for absent
   * @return this writer
   */
  public WireWriter optBytes(byte[] b) {
    if (b == null) {
      return u8(0);
    }
    u8(1);
    return bytes(b);
  }

  /**
   * Writes an optional u64.
   *
   * @param v the value, or {@code null} for absent
   * @return this writer
   */
  public WireWriter optU64(Long v) {
    if (v == null) {
      return u8(0);
    }
    u8(1);
    return u64(v);
  }

  /**
   * Writes {@code headers}: u16 count + (str, str) pairs.
   *
   * @param headers the headers, in order
   * @return this writer
   * @throws ProtocolException with more than 65,535 headers or an oversize key or value
   */
  public WireWriter headers(List<Header> headers) {
    if (headers.size() > 0xffff) {
      throw new ProtocolException(headers.size() + " headers exceed the limit of 65535");
    }
    u16(headers.size());
    for (Header h : headers) {
      str(h.key());
      str(h.value());
    }
    return this;
  }
}
