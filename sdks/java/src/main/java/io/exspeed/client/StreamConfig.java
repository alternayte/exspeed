package io.exspeed.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A stream's effective settings, from {@link StreamInfo#config()}.
 *
 * @param maxAgeSecs retention by age, in seconds
 * @param maxBytes retention by size, in bytes
 * @param dedupWindowSecs how long msg ids are remembered
 * @param dedupMaxEntries how many msg ids are remembered
 * @param compaction whether only the latest record per key is kept
 * @param maxMsgs most records the stream holds (0 = no limit)
 * @param discard what happens at {@code maxMsgs}
 * @param maxMsgsPerSubject most records kept per subject (0 = no limit)
 * @param allowMsgTtl whether records may carry their own TTL
 * @param msgTtlMs default lifetime of every record, in ms (0 = none)
 * @param allowDelayed whether records may ask for delayed delivery
 * @param retention whether acknowledgements remove records
 * @param captureSubjects subject filters whose core messages the stream also stores
 * @param raw every field as the server sent it (snake_case keys)
 */
public record StreamConfig(
    long maxAgeSecs,
    long maxBytes,
    long dedupWindowSecs,
    long dedupMaxEntries,
    boolean compaction,
    long maxMsgs,
    DiscardPolicy discard,
    long maxMsgsPerSubject,
    boolean allowMsgTtl,
    long msgTtlMs,
    boolean allowDelayed,
    RetentionPolicy retention,
    List<String> captureSubjects,
    Map<String, Object> raw) {

  static StreamConfig fromJson(Map<String, Object> m) {
    List<String> capture = new ArrayList<>();
    for (Object o : JsonMaps.asList(m.get("capture_subjects"))) {
      capture.add(String.valueOf(o));
    }
    return new StreamConfig(
        JsonMaps.num(m, "max_age_secs", 0),
        JsonMaps.num(m, "max_bytes", 0),
        JsonMaps.num(m, "dedup_window_secs", 0),
        JsonMaps.num(m, "dedup_max_entries", 0),
        JsonMaps.bool(m, "compaction", false),
        JsonMaps.num(m, "max_msgs", 0),
        DiscardPolicy.fromWire(JsonMaps.str(m, "discard", "old")),
        JsonMaps.num(m, "max_msgs_per_subject", 0),
        JsonMaps.bool(m, "allow_msg_ttl", false),
        JsonMaps.num(m, "msg_ttl_ms", 0),
        JsonMaps.bool(m, "allow_delayed", false),
        RetentionPolicy.fromWire(JsonMaps.str(m, "retention", "limits")),
        List.copyOf(capture),
        m);
  }
}
