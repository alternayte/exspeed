package io.exspeed.client.protocol;

import java.nio.charset.StandardCharsets;

/**
 * {@code StreamSpec}: {@code str name}, four u64 settings (0 = server default),
 * a u8 compaction flag, then optionally {@code bytes} holding the limits JSON,
 * sent only when a limit is not at its default.
 *
 * @param name the stream name
 * @param maxAgeSecs retention by age (0 = default)
 * @param maxBytes retention by size (0 = default)
 * @param dedupWindowSecs how long msg ids are remembered (0 = default)
 * @param dedupMaxEntries how many msg ids are remembered (0 = default)
 * @param compaction keep only the latest record per key
 * @param limitsJson the limits trailer JSON, or {@code null} to send none
 */
public record WireStreamSpec(
    String name,
    long maxAgeSecs,
    long maxBytes,
    long dedupWindowSecs,
    long dedupMaxEntries,
    boolean compaction,
    String limitsJson) {

  void encode(WireWriter w) {
    w.str(name);
    w.u64(maxAgeSecs);
    w.u64(maxBytes);
    w.u64(dedupWindowSecs);
    w.u64(dedupMaxEntries);
    w.u8(compaction ? 1 : 0);
    if (limitsJson != null) {
      w.bytes(limitsJson.getBytes(StandardCharsets.UTF_8));
    }
  }

  static WireStreamSpec decode(WireReader r) {
    String name = r.str();
    long a = r.u64();
    long b = r.u64();
    long c = r.u64();
    long d = r.u64();
    boolean comp = r.u8() != 0;
    String limits = r.remaining() > 0 ? new String(r.bytes(), StandardCharsets.UTF_8) : null;
    return new WireStreamSpec(name, a, b, c, d, comp, limits);
  }
}
