package io.exspeed.client.protocol;

import io.exspeed.client.Header;
import java.util.List;

/**
 * {@code PublishRecord}: {@code str subject}, {@code opt<bytes> key},
 * {@code bytes value}, {@code headers}, {@code opt<str> msg_id}.
 *
 * @param subject the subject
 * @param key the key, or {@code null}
 * @param value the value
 * @param headers the headers, in order
 * @param msgId the idempotency key, or {@code null}
 */
public record WirePublishRecord(String subject, byte[] key, byte[] value, List<Header> headers, String msgId) {
  void encode(WireWriter w) {
    w.str(subject);
    w.optBytes(key);
    w.bytes(value);
    w.headers(headers);
    w.optStr(msgId);
  }

  static WirePublishRecord decode(WireReader r) {
    return new WirePublishRecord(r.str(), r.optBytes(), r.bytes(), r.headers(), r.optStr());
  }
}
