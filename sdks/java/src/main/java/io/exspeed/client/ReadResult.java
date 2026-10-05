package io.exspeed.client;

import java.util.List;

/**
 * Result of a stateless {@link ExspeedClient#read(String, ReadOptions)}.
 *
 * @param records the records
 * @param nextOffset pass as {@code from} to continue
 * @param highWatermark the stream's next offset at the time of the read
 */
public record ReadResult(List<StreamRecord> records, long nextOffset, long highWatermark) {}
