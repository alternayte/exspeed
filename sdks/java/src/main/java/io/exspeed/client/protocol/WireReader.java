package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bounds-checked little-endian reader, mirroring {@code Reader} in
 * {@code crates/exspeed-protocol/src/client.rs}. Truncated input, trailing
 * bytes, bad option flags, invalid UTF-8 and counts the remaining bytes can't
 * hold all throw {@link ProtocolException}; a hostile length never causes a
 * large allocation.
 */
public final class WireReader {
  private final byte[] buf;
  private int pos;
  private final int end;

  /**
   * Reads the whole array.
   *
   * @param buf the bytes
   */
  public WireReader(byte[] buf) {
    this(buf, 0, buf.length);
  }

  /**
   * Reads a slice.
   *
   * @param buf the bytes
   * @param off where the slice starts
   * @param len the slice length
   */
  public WireReader(byte[] buf, int off, int len) {
    this.buf = buf;
    this.pos = off;
    this.end = off + len;
  }

  /**
   * Bytes left.
   *
   * @return the number of unread bytes
   */
  public int remaining() {
    return end - pos;
  }

  /**
   * Current read position in the underlying array.
   *
   * @return the position
   */
  public int position() {
    return pos;
  }

  private void need(int n) {
    if (remaining() < n) {
      throw new ProtocolException("truncated payload: need " + n + " bytes, have " + remaining());
    }
  }

  /**
   * Fails if bytes are left over (catches encoder/decoder drift).
   *
   * @throws ProtocolException when bytes remain
   */
  public void finish() {
    if (remaining() > 0) {
      throw new ProtocolException(remaining() + " trailing bytes");
    }
  }

  /**
   * Reads an unsigned byte.
   *
   * @return the value, 0 to 255
   */
  public int u8() {
    need(1);
    return buf[pos++] & 0xff;
  }

  /**
   * Reads a u16.
   *
   * @return the value, 0 to 65,535
   */
  public int u16() {
    need(2);
    int v = (buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8;
    pos += 2;
    return v;
  }

  /**
   * Reads a u32.
   *
   * @return the 32 bits (use {@link Integer#toUnsignedLong(int)} for the unsigned value)
   */
  public int u32() {
    need(4);
    int v = Frame.getU32(buf, pos);
    pos += 4;
    return v;
  }

  /**
   * Reads a u64.
   *
   * @return the 64 bits
   */
  public long u64() {
    need(8);
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v |= (buf[pos + i] & 0xffL) << (8 * i);
    }
    pos += 8;
    return v;
  }

  /**
   * Reads {@code n} raw bytes.
   *
   * @param n how many
   * @return a copy of the bytes
   */
  public byte[] raw(int n) {
    need(n);
    byte[] out = Arrays.copyOfRange(buf, pos, pos + n);
    pos += n;
    return out;
  }

  /**
   * Skips {@code n} bytes.
   *
   * @param n how many
   */
  public void skip(int n) {
    need(n);
    pos += n;
  }

  private String utf8(int n) {
    need(n);
    try {
      String s = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(buf, pos, n))
          .toString();
      pos += n;
      return s;
    } catch (CharacterCodingException e) {
      throw new ProtocolException("invalid UTF-8");
    }
  }

  /**
   * Reads a {@code str}.
   *
   * @return the string
   */
  public String str() {
    return utf8(u16());
  }

  /**
   * Reads an {@code lstr}.
   *
   * @return the string
   */
  public String lstr() {
    long n = Integer.toUnsignedLong(u32());
    if (n > remaining()) {
      throw new ProtocolException("truncated payload: need " + n + " bytes, have " + remaining());
    }
    return utf8((int) n);
  }

  /**
   * Reads {@code bytes}.
   *
   * @return the bytes
   */
  public byte[] bytes() {
    long n = Integer.toUnsignedLong(u32());
    if (n > remaining()) {
      throw new ProtocolException("truncated payload: need " + n + " bytes, have " + remaining());
    }
    return raw((int) n);
  }

  private boolean flag() {
    int v = u8();
    if (v > 1) {
      throw new ProtocolException("invalid option flag " + v);
    }
    return v == 1;
  }

  /**
   * Reads an optional {@code str}.
   *
   * @return the string, or {@code null} when absent
   */
  public String optStr() {
    return flag() ? str() : null;
  }

  /**
   * Reads optional {@code bytes}.
   *
   * @return the bytes, or {@code null} when absent
   */
  public byte[] optBytes() {
    return flag() ? bytes() : null;
  }

  /**
   * Reads an optional u64.
   *
   * @return the value, or {@code null} when absent
   */
  public Long optU64() {
    return flag() ? u64() : null;
  }

  /**
   * Reads a u32 element count, rejecting counts the remaining bytes can't hold.
   *
   * @param minSize the smallest possible size of one element
   * @return the count
   */
  public int count(int minSize) {
    long n = Integer.toUnsignedLong(u32());
    if (n * Math.max(1, minSize) > remaining()) {
      throw new ProtocolException("count " + n + " exceeds payload size");
    }
    return (int) n;
  }

  /**
   * Reads {@code headers}.
   *
   * @return the headers, in wire order
   */
  public List<Header> headers() {
    int n = u16();
    if (n * 4L > remaining()) {
      throw new ProtocolException("header count exceeds payload");
    }
    List<Header> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      out.add(new Header(str(), str()));
    }
    return out;
  }
}
