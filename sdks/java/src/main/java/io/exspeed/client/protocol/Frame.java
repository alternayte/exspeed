package io.exspeed.client.protocol;

import io.exspeed.client.ExspeedException;
import io.exspeed.client.ProtocolException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * One protocol frame: {@code [version u8][opcode u8][correlation id u32 LE][payload length u32 LE][payload]}.
 *
 * @param opcode the operation code
 * @param correlationId the correlation id (unsigned 32-bit, stored in an {@code int})
 * @param payload the payload bytes
 */
public record Frame(int opcode, int correlationId, byte[] payload) {
  /** Wire protocol version spoken by this client (byte 0 of every frame). */
  public static final int VERSION = 0x02;
  /** Size of the frame header in bytes. */
  public static final int HEADER_SIZE = 10;
  /** Largest payload either side accepts (16 MiB). */
  public static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;

  /**
   * Serializes a frame.
   *
   * @param opcode the operation code
   * @param correlationId the correlation id
   * @param payload the payload
   * @return the frame bytes
   * @throws ExspeedException when the payload exceeds {@link #MAX_PAYLOAD_SIZE}
   */
  public static byte[] encode(int opcode, int correlationId, byte[] payload) {
    if (payload.length > MAX_PAYLOAD_SIZE) {
      throw new ExspeedException("payload too large: " + payload.length + " bytes (max "
          + MAX_PAYLOAD_SIZE + "); split the batch");
    }
    byte[] out = new byte[HEADER_SIZE + payload.length];
    out[0] = (byte) VERSION;
    out[1] = (byte) opcode;
    putU32(out, 2, correlationId);
    putU32(out, 6, payload.length);
    System.arraycopy(payload, 0, out, HEADER_SIZE, payload.length);
    return out;
  }

  /**
   * Reads one frame from a stream, blocking until it is complete.
   *
   * @param in the stream
   * @return the frame, or {@code null} at a clean end of stream (between frames)
   * @throws IOException on an I/O error, or end of stream inside a frame
   * @throws ProtocolException on a bad version or an oversize length; the stream
   *     can't be resynchronized after that
   */
  public static Frame read(InputStream in) throws IOException {
    byte[] header = new byte[HEADER_SIZE];
    int n = 0;
    while (n < HEADER_SIZE) {
      int r = in.read(header, n, HEADER_SIZE - n);
      if (r < 0) {
        if (n == 0) {
          return null;
        }
        throw new EOFException("connection closed inside a frame header");
      }
      n += r;
    }
    int version = header[0] & 0xff;
    if (version != VERSION) {
      throw new ProtocolException(String.format("unsupported protocol version 0x%x", version));
    }
    int opcode = header[1] & 0xff;
    int corr = getU32(header, 2);
    long len = getU32(header, 6) & 0xffffffffL;
    if (len > MAX_PAYLOAD_SIZE) {
      throw new ProtocolException("payload too large: " + len + " bytes (max " + MAX_PAYLOAD_SIZE + ")");
    }
    byte[] payload = new byte[(int) len];
    int off = 0;
    while (off < payload.length) {
      int r = in.read(payload, off, payload.length - off);
      if (r < 0) {
        throw new EOFException("connection closed inside a frame");
      }
      off += r;
    }
    return new Frame(opcode, corr, payload);
  }

  static void putU32(byte[] b, int at, int v) {
    b[at] = (byte) v;
    b[at + 1] = (byte) (v >>> 8);
    b[at + 2] = (byte) (v >>> 16);
    b[at + 3] = (byte) (v >>> 24);
  }

  static int getU32(byte[] b, int at) {
    return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8 | (b[at + 2] & 0xff) << 16 | (b[at + 3] & 0xff) << 24;
  }
}
