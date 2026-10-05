package io.exspeed.client;

import java.util.Map;

/**
 * A consumer's counters.
 *
 * @param delivered records delivered for the first time
 * @param redelivered redeliveries
 * @param acked acknowledged records
 * @param deadLettered dead-lettered records
 * @param gone unacked records that disappeared before redelivery (retention or compaction)
 * @param skipped records skipped because the consumer fell behind retention
 */
public record ConsumerStats(long delivered, long redelivered, long acked, long deadLettered, long gone, long skipped) {
  static ConsumerStats fromJson(Map<String, Object> m) {
    return new ConsumerStats(
        JsonMaps.num(m, "delivered", 0),
        JsonMaps.num(m, "redelivered", 0),
        JsonMaps.num(m, "acked", 0),
        JsonMaps.num(m, "dead_lettered", 0),
        JsonMaps.num(m, "gone", 0),
        JsonMaps.num(m, "skipped", 0));
  }
}
