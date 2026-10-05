package io.exspeed.client;

import java.time.Duration;

/**
 * Settings of a {@link Publisher}.
 *
 * @param batchWindow how long to gather records before sending a batch;
 *     {@link Duration#ZERO} sends whatever has queued up as soon as the
 *     publisher's flush task runs, without adding latency
 * @param maxBatchRecords most records per batch request
 * @param maxInFlight records accepted but not yet acknowledged; {@code publish} blocks when full
 */
public record PublisherOptions(Duration batchWindow, int maxBatchRecords, int maxInFlight) {
  /** No batch window, 512 records per batch, 4096 records in flight. */
  public static final PublisherOptions DEFAULT = new PublisherOptions(Duration.ZERO, 512, 4096);

  /**
   * Validates the options.
   *
   * @param batchWindow how long to gather records
   * @param maxBatchRecords most records per batch
   * @param maxInFlight most records awaiting acknowledgement
   */
  public PublisherOptions {
    if (batchWindow == null || batchWindow.isNegative()) {
      throw new IllegalArgumentException("batchWindow must be a non-negative duration");
    }
    if (maxBatchRecords < 1 || maxInFlight < 1) {
      throw new IllegalArgumentException("maxBatchRecords and maxInFlight must be at least 1");
    }
  }

  /**
   * A copy with another batch window.
   *
   * @param window the window
   * @return the options
   */
  public PublisherOptions withBatchWindow(Duration window) {
    return new PublisherOptions(window, maxBatchRecords, maxInFlight);
  }

  /**
   * A copy with another batch size.
   *
   * @param n most records per batch
   * @return the options
   */
  public PublisherOptions withMaxBatchRecords(int n) {
    return new PublisherOptions(batchWindow, n, maxInFlight);
  }

  /**
   * A copy with another in-flight limit.
   *
   * @param n most records awaiting acknowledgement
   * @return the options
   */
  public PublisherOptions withMaxInFlight(int n) {
    return new PublisherOptions(batchWindow, maxBatchRecords, n);
  }
}
