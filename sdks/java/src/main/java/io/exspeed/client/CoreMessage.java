package io.exspeed.client;

import io.exspeed.client.protocol.Response;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A core (non-persistent) message: from a core subscription, or the response
 * to a {@link ExspeedClient#request(String, byte[])}.
 */
public final class CoreMessage {
  private final String subject;
  private final String replyTo;
  private final List<Header> headers;
  private final byte[] value;
  private final Host host;

  CoreMessage(Response.CoreMsg m, Host host) {
    this.subject = m.subject();
    this.replyTo = m.replyTo();
    this.headers = Collections.unmodifiableList(m.headers());
    this.value = m.value();
    this.host = host;
  }

  /**
   * The subject it was published to.
   *
   * @return the subject
   */
  public String subject() {
    return subject;
  }

  /**
   * Where to send the answer to a request.
   *
   * @return the reply subject, or {@code null} when it is not a request
   */
  public String replyTo() {
    return replyTo;
  }

  /**
   * The headers, in wire order; a key can repeat.
   *
   * @return the headers
   */
  public List<Header> headers() {
    return headers;
  }

  /**
   * The payload. Don't modify the array.
   *
   * @return the payload bytes
   */
  public byte[] value() {
    return value;
  }

  /**
   * The payload as UTF-8 text.
   *
   * @return the text
   */
  public String text() {
    return new String(value, StandardCharsets.UTF_8);
  }

  /**
   * The payload parsed as JSON (see {@link Json#parse(String)}).
   *
   * @return the parsed value
   */
  public Object json() {
    return Json.parse(text());
  }

  /**
   * The first header named {@code name}.
   *
   * @param name the header name
   * @return its value, or {@code null}
   */
  public String header(String name) {
    for (Header h : headers) {
      if (h.key().equals(name)) {
        return h.value();
      }
    }
    return null;
  }

  /**
   * Answers a request: publishes {@code value} to its {@link #replyTo()} subject.
   *
   * @param value the answer
   * @throws ExspeedException when the message has no reply subject
   */
  public void respond(byte[] value) {
    Futures.await(respondInternal(value, List.of()));
  }

  /**
   * Answers a request with UTF-8 text.
   *
   * @param value the answer
   * @throws ExspeedException when the message has no reply subject
   */
  public void respond(String value) {
    respond(value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Answers a request.
   *
   * @param value the answer
   * @param headers headers for the answer
   * @return completes when the server accepted it; fails when the message has no reply subject
   */
  public CompletableFuture<Void> respondAsync(byte[] value, List<Header> headers) {
    return host.publicFuture(respondInternal(value, headers));
  }

  private CompletableFuture<Void> respondInternal(byte[] value, List<Header> headers) {
    if (replyTo == null) {
      return Futures.failed(new ExspeedException("message has no replyTo"));
    }
    return host.publishCoreInternal(replyTo, null, headers, value);
  }

  @Override
  public String toString() {
    return "CoreMessage{subject=" + subject + ", replyTo=" + replyTo + ", value=" + value.length + " bytes}";
  }
}
