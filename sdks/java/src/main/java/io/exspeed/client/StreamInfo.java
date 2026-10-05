package io.exspeed.client;

import java.util.Map;

/**
 * A stream's bounds and settings.
 *
 * @param name the stream name
 * @param earliestOffset the first retained offset
 * @param nextOffset the offset the next record will get
 * @param records {@code nextOffset - earliestOffset}
 * @param config the effective settings
 * @param internal whether the stream is internal (name starts with {@code __})
 * @param raw every field as the server sent it (snake_case keys)
 */
public record StreamInfo(
    String name,
    long earliestOffset,
    long nextOffset,
    long records,
    StreamConfig config,
    boolean internal,
    Map<String, Object> raw) {

  static StreamInfo fromJson(Object json) {
    Map<String, Object> m = JsonMaps.asMap(json);
    return new StreamInfo(
        JsonMaps.str(m, "name", ""),
        JsonMaps.num(m, "earliest_offset", 0),
        JsonMaps.num(m, "next_offset", 0),
        JsonMaps.num(m, "records", 0),
        StreamConfig.fromJson(JsonMaps.asMap(m.get("config"))),
        JsonMaps.bool(m, "internal", false),
        m);
  }
}
