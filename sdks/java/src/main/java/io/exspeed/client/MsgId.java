package io.exspeed.client;

import java.security.SecureRandom;

/** Generates idempotency keys for {@link PublishRecord.Builder#msgId(String)}. */
public final class MsgId {
  private static final SecureRandom RANDOM = new SecureRandom();

  private MsgId() {}

  /**
   * A time-ordered UUIDv7 ({@code xxxxxxxx-xxxx-7xxx-[89ab]xxx-xxxxxxxxxxxx}):
   * 48 bits of Unix time in ms, then random bits. Ids generated later sort
   * after earlier ones (at millisecond granularity).
   *
   * @return a new id
   */
  public static String newMsgId() {
    long ms = System.currentTimeMillis();
    byte[] rand = new byte[10];
    RANDOM.nextBytes(rand);
    int randA = (((rand[0] & 0xff) << 8) | (rand[1] & 0xff)) & 0x0fff | 0x7000;
    int randBHi = (rand[2] & 0x3f) | 0x80;
    StringBuilder sb = new StringBuilder(36);
    String ts = String.format("%012x", ms & 0xffffffffffffL);
    sb.append(ts, 0, 8).append('-').append(ts, 8, 12).append('-');
    sb.append(String.format("%04x", randA)).append('-');
    sb.append(String.format("%02x%02x", randBHi, rand[3] & 0xff)).append('-');
    for (int i = 4; i < 10; i++) {
      sb.append(String.format("%02x", rand[i] & 0xff));
    }
    return sb.toString();
  }
}
