package io.exspeed.client;

import java.util.Map;

/**
 * A consumer's spec, position and counters.
 *
 * @param spec the consumer's spec, every field filled in by the server
 * @param nextOffset next stream offset to be delivered for the first time
 * @param ackFloor everything below this offset is acked (or filtered out)
 * @param numUnacked delivered records not yet acked
 * @param numInFlight records delivered and waiting for an ack
 * @param numDelayed records held back until their delivery time
 * @param numWaiting records not yet delivered (approximate)
 * @param lag {@code high watermark - ack floor}
 * @param subscribers live push subscriptions
 * @param pullWaiters pull requests waiting
 * @param stats the counters
 * @param raw every field as the server sent it (snake_case keys)
 */
public record ConsumerInfo(
    ConsumerSpec spec,
    long nextOffset,
    long ackFloor,
    long numUnacked,
    long numInFlight,
    long numDelayed,
    long numWaiting,
    long lag,
    long subscribers,
    long pullWaiters,
    ConsumerStats stats,
    Map<String, Object> raw) {

  static ConsumerInfo fromJson(Object json) {
    Map<String, Object> m = JsonMaps.asMap(json);
    return new ConsumerInfo(
        ConsumerSpec.fromJson(JsonMaps.asMap(m.get("spec"))),
        JsonMaps.num(m, "next_offset", 0),
        JsonMaps.num(m, "ack_floor", 0),
        JsonMaps.num(m, "num_unacked", 0),
        JsonMaps.num(m, "num_in_flight", 0),
        JsonMaps.num(m, "num_delayed", 0),
        JsonMaps.num(m, "num_waiting", 0),
        JsonMaps.num(m, "lag", 0),
        JsonMaps.num(m, "subscribers", 0),
        JsonMaps.num(m, "pull_waiters", 0),
        ConsumerStats.fromJson(JsonMaps.asMap(m.get("stats"))),
        m);
  }
}
