package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import io.exspeed.client.ProtocolException;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * A record as the server delivers it ({@code WireRecord} in {@code docs/protocol.md}):
 * {@code u32 len}, {@code u32 crc} (CRC32C of the bytes after
 * {@code delivery_count}), {@code u16 delivery_count}, {@code u64 offset},
 * {@code u64 timestamp_ns}, {@code str subject}, {@code opt<bytes> key},
 * {@code bytes value}, {@code headers}. This is also how the server stores
 * records on disk.
 *
 * @param offset the record's stream offset
 * @param timestampNs append time, nanoseconds since the Unix epoch
 * @param deliveryCount 1 on first delivery, +1 per redelivery, 0 for stateless reads
 * @param subject the subject
 * @param key the key, or {@code null}
 * @param value the value
 * @param headers the headers, in wire order
 */
public record WireRecord(
    long offset,
    long timestampNs,
    int deliveryCount,
    String subject,
    byte[] key,
    byte[] value,
    List<Header> headers) {

  /** Size of the smallest valid record. */
  public static final int MIN_RECORD_LEN = 35;
  private static final int CRC_START = 10;

  /**
   * Encodes this record (length, CRC, delivery count, fields).
   *
   * @return the encoded record
   */
  public byte[] encode() {
    WireWriter body = new WireWriter(64 + value.length + (key == null ? 0 : key.length));
    body.u64(offset);
    body.u64(timestampNs);
    body.str(subject);
    body.optBytes(key);
    body.bytes(value);
    body.headers(headers);
    byte[] b = body.finish();
    CRC32C crc = new CRC32C();
    crc.update(b);
    return new WireWriter(CRC_START + b.length)
        .u32(CRC_START - 4 + b.length)
        .u32((int) crc.getValue())
        .u16(deliveryCount)
        .raw(b)
        .finish();
  }

  /**
   * Whether a complete encoded record's CRC matches its contents.
   *
   * @param record the encoded record, length field included
   * @return true when the CRC is valid
   */
  public static boolean verifyCrc(byte[] record) {
    if (record.length < MIN_RECORD_LEN) {
      return false;
    }
    CRC32C crc = new CRC32C();
    crc.update(record, CRC_START, record.length - CRC_START);
    return (int) crc.getValue() == Frame.getU32(record, 4);
  }

  /**
   * Decodes one record, checking its length field, structure and (optionally) CRC.
   *
   * @param r the reader, positioned at the record's length field
   * @param verifyCrc whether to verify the CRC32C
   * @return the record
   * @throws ProtocolException when the record is malformed or its CRC doesn't match
   */
  public static WireRecord decode(WireReader r, boolean verifyCrc) {
    long len = Integer.toUnsignedLong(r.u32());
    if (len + 4 < MIN_RECORD_LEN) {
      throw new ProtocolException("record length " + (len + 4) + " too small");
    }
    if (len > r.remaining()) {
      throw new ProtocolException("truncated payload: need " + len + " bytes, have " + r.remaining());
    }
    byte[] rec = r.raw((int) len);
    WireReader rr = new WireReader(rec);
    int storedCrc = rr.u32();
    int deliveryCount = rr.u16();
    if (verifyCrc) {
      CRC32C crc = new CRC32C();
      crc.update(rec, 6, rec.length - 6);
      if ((int) crc.getValue() != storedCrc) {
        throw new ProtocolException("record: CRC mismatch");
      }
    }
    long offset = rr.u64();
    long ts = rr.u64();
    String subject = rr.str();
    byte[] key = rr.optBytes();
    byte[] value = rr.bytes();
    List<Header> headers = rr.headers();
    rr.finish();
    return new WireRecord(offset, ts, deliveryCount, subject, key, value, headers);
  }
}
