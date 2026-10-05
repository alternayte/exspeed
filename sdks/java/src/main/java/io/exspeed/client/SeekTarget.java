package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import java.time.Instant;

/** Where {@link ExspeedClient#seek(String, SeekTarget)} moves a consumer's cursor. */
public final class SeekTarget {
  /** The first retained record. */
  public static final SeekTarget EARLIEST = new SeekTarget(Request.SEEK_EARLIEST, 0);
  /** The end of the stream: only records appended from now on. */
  public static final SeekTarget LATEST = new SeekTarget(Request.SEEK_LATEST, 0);

  final int kind;
  final long value;

  private SeekTarget(int kind, long value) {
    this.kind = kind;
    this.value = value;
  }

  /**
   * A specific offset.
   *
   * @param offset the offset
   * @return the target
   */
  public static SeekTarget offset(long offset) {
    return new SeekTarget(Request.SEEK_OFFSET, offset);
  }

  /**
   * The first record at or after a time.
   *
   * @param time the time
   * @return the target
   */
  public static SeekTarget time(Instant time) {
    return timeMs(time.toEpochMilli());
  }

  /**
   * The first record at or after a time.
   *
   * @param epochMs ms since the Unix epoch
   * @return the target
   */
  public static SeekTarget timeMs(long epochMs) {
    return new SeekTarget(Request.SEEK_TIME, epochMs);
  }
}
