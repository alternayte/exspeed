package io.exspeed.client;

/** Error codes the server returns in {@link ServerException#code()} (HTTP-like). */
public final class ErrorCode {
  private ErrorCode() {}

  /** Malformed request, invalid name or filter, invalid config. */
  public static final int BAD_REQUEST = 400;
  /** Not authenticated (bad or missing token). */
  public static final int UNAUTHORIZED = 401;
  /** The credential lacks the needed action on the stream. */
  public static final int FORBIDDEN = 403;
  /** Stream, consumer, bucket or key not found; or a core request had no responders. */
  public static final int NOT_FOUND = 404;
  /** A bounded query timed out. */
  public static final int QUERY_TIMEOUT = 408;
  /**
   * Exists with different settings, stream still has consumers, a {@code msgId}
   * reused with a different body, or a KV key not at the expected revision.
   */
  public static final int CONFLICT = 409;
  /** A bounded query exceeded the server's query memory limit. */
  public static final int QUERY_TOO_LARGE = 422;
  /** Retry later: dedup map full, stream full ({@code discard = new}), too many waiting requests. */
  public static final int TOO_MANY_REQUESTS = 429;
  /** Internal server error. */
  public static final int INTERNAL = 500;
  /** Not the leader (see {@link ServerException#leaderHint()}), still starting, or too few in-sync replicas. */
  public static final int UNAVAILABLE = 503;
  /** The server's disk is full; nothing was written. */
  public static final int INSUFFICIENT_STORAGE = 507;
}
