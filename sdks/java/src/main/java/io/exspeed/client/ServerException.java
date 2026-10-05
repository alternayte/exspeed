package io.exspeed.client;

import java.util.Map;

/**
 * The server answered a request with an {@code Error} frame.
 *
 * <p>{@link #code()} is HTTP-like (see {@link ErrorCode}). {@link #detail()} is
 * the optional machine-readable JSON the server attached, parsed with
 * {@link Json#parse(String)} and passed through as sent (snake_case keys), for
 * example {@code {"leader": "host:5933"}} (503), {@code {"stored_offset": 7}}
 * (409) or {@code {"retry_after_secs": 30}} (429).
 */
public class ServerException extends ExspeedException {
  private static final long serialVersionUID = 1L;

  private final int code;
  private final String detailJson;
  private final transient Object detail;

  /**
   * Creates a server exception.
   *
   * @param code the HTTP-like error code
   * @param message the server's message
   * @param detailJson the raw JSON detail, or {@code null}
   */
  public ServerException(int code, String message, String detailJson) {
    super(message);
    this.code = code;
    this.detailJson = detailJson == null || detailJson.isEmpty() ? null : detailJson;
    Object parsed = null;
    if (this.detailJson != null) {
      try {
        parsed = Json.parse(this.detailJson);
      } catch (RuntimeException e) {
        parsed = this.detailJson;
      }
    }
    this.detail = parsed;
  }

  /**
   * The HTTP-like error code.
   *
   * @return the code, such as 404 or 409
   */
  public int code() {
    return code;
  }

  /**
   * The parsed JSON detail: usually a {@code Map<String, Object>}; the raw text
   * when it isn't valid JSON; {@code null} when the server sent none.
   *
   * @return the detail, or {@code null}
   */
  public Object detail() {
    return detail;
  }

  /**
   * The detail exactly as the server sent it.
   *
   * @return the raw JSON text, or {@code null}
   */
  public String detailJson() {
    return detailJson;
  }

  /**
   * A field of the detail object.
   *
   * @param key the field name (snake_case, as sent)
   * @return the value, or {@code null} when absent or the detail is not an object
   */
  public Object detail(String key) {
    return detail instanceof Map<?, ?> m ? m.get(key) : null;
  }

  /**
   * {@code detail.leader} of a 503 "not the leader" error: the leader's client
   * address, when known.
   *
   * @return the leader's {@code host:port}, or {@code null}
   */
  public String leaderHint() {
    return detail("leader") instanceof String s ? s : null;
  }

  @Override
  public String toString() {
    return "ServerException " + code + ": " + getMessage();
  }
}
