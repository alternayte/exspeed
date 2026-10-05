package io.exspeed.client;

import io.exspeed.client.protocol.WireRecord;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** A record delivered by a consumer (push or pull), with the means to settle it. */
public final class Message extends StreamRecord {
  private final int deliveryCount;
  private final String consumer;
  private final MessageSettler settler;

  Message(WireRecord r, String consumer, MessageSettler settler) {
    super(r);
    this.deliveryCount = r.deliveryCount();
    this.consumer = consumer;
    this.settler = settler;
  }

  /**
   * 1 on the first delivery, incremented on each redelivery.
   *
   * @return the delivery count
   */
  public int deliveryCount() {
    return deliveryCount;
  }

  /**
   * The consumer that delivered it.
   *
   * @return the consumer's name
   */
  public String consumer() {
    return consumer;
  }

  /**
   * Acknowledges the message, fire-and-forget: no round trip, and acks made in
   * quick succession share one frame. If the connection is down the ack is
   * dropped and the record will be redelivered. A failure the server reports
   * reaches {@link ClientListener#onError(ExspeedException)}. Use
   * {@link ExspeedClient#ack(String, long...)} to wait for confirmation.
   */
  public void ack() {
    settler.ackNowait(consumer, offset());
  }

  /** Asks for redelivery after the consumer's backoff, and waits for the server to confirm. */
  public void nack() {
    Futures.await(settler.nackInternal(consumer, offset(), 0));
  }

  /**
   * Asks for redelivery after {@code delay}, and waits for the server to confirm.
   *
   * @param delay how long to wait before redelivering (zero = the consumer's backoff)
   */
  public void nack(Duration delay) {
    Futures.await(settler.nackInternal(consumer, offset(), delay.toMillis()));
  }

  /**
   * Asks for redelivery after {@code delay}.
   *
   * @param delay how long to wait before redelivering (zero = the consumer's backoff)
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> nackAsync(Duration delay) {
    return settler.publicFuture(settler.nackInternal(consumer, offset(), delay.toMillis()));
  }

  /**
   * Never redeliver: dead-letters the message now (to the consumer's DLQ
   * stream, if it has one), and waits for the server to confirm.
   *
   * @param reason free text, stored in the {@code exspeed-dlq-reason} header
   */
  public void term(String reason) {
    Futures.await(settler.termInternal(consumer, offset(), reason));
  }

  /**
   * Never redeliver: dead-letters the message now.
   *
   * @param reason free text, stored in the {@code exspeed-dlq-reason} header
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> termAsync(String reason) {
    return settler.publicFuture(settler.termInternal(consumer, offset(), reason));
  }

  /** Still working on it: resets the ack deadline, and waits for the server to confirm. */
  public void inProgress() {
    Futures.await(settler.inProgressInternal(consumer, new long[] {offset()}));
  }

  /**
   * Still working on it: resets the ack deadline.
   *
   * @return completes when the server confirmed
   */
  public CompletableFuture<Void> inProgressAsync() {
    return settler.publicFuture(settler.inProgressInternal(consumer, new long[] {offset()}));
  }
}
