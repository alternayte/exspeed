package io.exspeed.client;

import io.exspeed.client.protocol.Request;
import io.exspeed.client.protocol.Response;
import io.exspeed.client.protocol.WireRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A key-value bucket, from {@link ExspeedClient#kv(String)}. The bucket
 * {@code B} is the stream {@code KV_B}: each key is a subject, each put a
 * record, and a key's revision is its record's offset plus one.
 *
 * <pre>{@code
 * KvBucket kv = client.kv("config");
 * kv.create(KvBucketOptions.history(5));
 * long rev = kv.put("app.mode", "prod");
 * KvEntry e = kv.get("app.mode");          // null when absent or deleted
 * kv.update("app.mode", "dev", rev);       // compare-and-set: 409 unless still at rev
 * }</pre>
 */
public final class KvBucket {
  /** Header marking a tombstone: {@code DEL} or {@code PURGE}. */
  public static final String KV_OP_HEADER = "exspeed-kv-op";

  private final Host host;
  private final String bucket;

  KvBucket(Host host, String bucket) {
    this.host = host;
    this.bucket = bucket;
  }

  /**
   * The bucket's name.
   *
   * @return the name
   */
  public String bucket() {
    return bucket;
  }

  /**
   * The bucket's stream, {@code KV_<bucket>}.
   *
   * @return the stream name
   */
  public String stream() {
    return "KV_" + bucket;
  }

  private <T extends Response> CompletableFuture<T> call(Request req, Class<T> type) {
    return host.rawRequest(req, host.requestTimeoutMs()).thenApply(r -> {
      if (!type.isInstance(r)) {
        throw new ProtocolException("unexpected reply to " + req.typeName() + ": " + r.typeName());
      }
      return type.cast(r);
    });
  }

  /** Creates the bucket with the defaults. Idempotent for the same settings. */
  public void create() {
    create(KvBucketOptions.DEFAULT);
  }

  /**
   * Creates the bucket. Idempotent for the same settings.
   *
   * @param opts the settings
   */
  public void create(KvBucketOptions opts) {
    Futures.await(createInternal(opts));
  }

  /**
   * Creates the bucket. Idempotent for the same settings.
   *
   * @param opts the settings
   * @return completes when created
   */
  public CompletableFuture<Void> createAsync(KvBucketOptions opts) {
    return host.publicFuture(createInternal(opts));
  }

  private CompletableFuture<Void> createInternal(KvBucketOptions opts) {
    return call(new Request.KvCreateBucket(bucket, opts.history(), opts.ttl().toMillis(), opts.maxBytes()),
        Response.Ok.class).thenApply(r -> null);
  }

  /** Deletes the bucket and every key in it. */
  public void destroy() {
    Futures.await(destroyInternal());
  }

  /**
   * Deletes the bucket and every key in it.
   *
   * @return completes when deleted
   */
  public CompletableFuture<Void> destroyAsync() {
    return host.publicFuture(destroyInternal());
  }

  private CompletableFuture<Void> destroyInternal() {
    return call(new Request.DeleteStream(stream()), Response.Ok.class).thenApply(r -> null);
  }

  /**
   * The current value of {@code key}.
   *
   * @param key the key
   * @return the entry, or {@code null} when the key is absent, deleted or expired
   * @throws ServerException 404 when the bucket doesn't exist
   */
  public KvEntry get(String key) {
    return Futures.await(getInternal(key, null));
  }

  /**
   * The current value of {@code key}.
   *
   * @param key the key
   * @return the entry, or {@code null} when the key is absent
   */
  public CompletableFuture<KvEntry> getAsync(String key) {
    return host.publicFuture(getInternal(key, null));
  }

  /**
   * {@code key} at {@code revision}, while the bucket still keeps it.
   *
   * @param key the key
   * @param revision the revision
   * @return the entry, or {@code null}
   */
  public KvEntry getRevision(String key, long revision) {
    return Futures.await(getInternal(key, revision));
  }

  /**
   * {@code key} at {@code revision}, while the bucket still keeps it.
   *
   * @param key the key
   * @param revision the revision
   * @return the entry, or {@code null}
   */
  public CompletableFuture<KvEntry> getRevisionAsync(String key, long revision) {
    return host.publicFuture(getInternal(key, revision));
  }

  private CompletableFuture<KvEntry> getInternal(String key, Long revision) {
    Request req = new Request.KvGet(bucket, key, revision);
    return host.rawRequest(req, host.requestTimeoutMs()).handle((r, e) -> {
      if (e != null) {
        Throwable t = Futures.unwrap(e);
        // 404 is "key '...' not found" or "bucket '...' not found".
        if (t instanceof ServerException se && se.code() == ErrorCode.NOT_FOUND
            && se.getMessage().startsWith("key ")) {
          return null;
        }
        throw Futures.rethrow(t);
      }
      if (!(r instanceof Response.Messages m)) {
        throw new ProtocolException("unexpected reply to KvGet: " + r.typeName());
      }
      return m.records().isEmpty() ? null : new KvEntry(m.records().get(0));
    });
  }

  /**
   * Sets {@code key}.
   *
   * @param key the key
   * @param value the value
   * @return the new revision
   */
  public long put(String key, byte[] value) {
    return put(key, value, KvPutOptions.NONE);
  }

  /**
   * Sets {@code key} to UTF-8 text.
   *
   * @param key the key
   * @param value the value
   * @return the new revision
   */
  public long put(String key, String value) {
    return put(key, utf8(value), KvPutOptions.NONE);
  }

  /**
   * Sets {@code key}, optionally with a TTL or an expected revision.
   *
   * @param key the key
   * @param value the value
   * @param opts TTL and compare-and-set options
   * @return the new revision
   * @throws ServerException 409 when the key is not at the expected revision
   */
  public long put(String key, byte[] value, KvPutOptions opts) {
    return Futures.await(putInternal(key, value, opts));
  }

  /**
   * Sets {@code key}, optionally with a TTL or an expected revision.
   *
   * @param key the key
   * @param value the value
   * @param opts TTL and compare-and-set options
   * @return the new revision
   */
  public CompletableFuture<Long> putAsync(String key, byte[] value, KvPutOptions opts) {
    return host.publicFuture(putInternal(key, value, opts));
  }

  private CompletableFuture<Long> putInternal(String key, byte[] value, KvPutOptions opts) {
    Long ttlMs = null;
    if (opts.ttl() != null) {
      Duration d = opts.ttl();
      long ms = d.toMillis();
      if (d.minusMillis(ms).toNanos() > 0) {
        ms++;
      }
      ttlMs = Math.max(1, ms);
    }
    return write(new Request.KvPut(bucket, key, value, opts.expectedRevision(), ttlMs));
  }

  /**
   * Sets {@code key} only if it doesn't exist (or was deleted).
   *
   * @param key the key
   * @param value the value
   * @return the new revision
   * @throws ServerException 409 when the key exists
   */
  public long createKey(String key, byte[] value) {
    return put(key, value, KvPutOptions.expectedRevision(0));
  }

  /**
   * Sets {@code key} to UTF-8 text only if it doesn't exist (or was deleted).
   *
   * @param key the key
   * @param value the value
   * @return the new revision
   * @throws ServerException 409 when the key exists
   */
  public long createKey(String key, String value) {
    return createKey(key, utf8(value));
  }

  /**
   * Sets {@code key} only if it doesn't exist (or was deleted).
   *
   * @param key the key
   * @param value the value
   * @return the new revision
   */
  public CompletableFuture<Long> createKeyAsync(String key, byte[] value) {
    return putAsync(key, value, KvPutOptions.expectedRevision(0));
  }

  /**
   * Sets {@code key} only if it is at {@code revision} (compare-and-set).
   *
   * @param key the key
   * @param value the value
   * @param revision the revision the key must be at
   * @return the new revision
   * @throws ServerException 409 otherwise, with {@code detail.current_revision}
   */
  public long update(String key, byte[] value, long revision) {
    return put(key, value, KvPutOptions.expectedRevision(revision));
  }

  /**
   * Sets {@code key} to UTF-8 text only if it is at {@code revision}.
   *
   * @param key the key
   * @param value the value
   * @param revision the revision the key must be at
   * @return the new revision
   * @throws ServerException 409 otherwise, with {@code detail.current_revision}
   */
  public long update(String key, String value, long revision) {
    return update(key, utf8(value), revision);
  }

  /**
   * Sets {@code key} only if it is at {@code revision} (compare-and-set).
   *
   * @param key the key
   * @param value the value
   * @param revision the revision the key must be at
   * @return the new revision
   */
  public CompletableFuture<Long> updateAsync(String key, byte[] value, long revision) {
    return putAsync(key, value, KvPutOptions.expectedRevision(revision));
  }

  /**
   * Deletes {@code key} (its history stays until it ages out).
   *
   * @param key the key
   * @return the tombstone's revision
   */
  public long delete(String key) {
    return Futures.await(deleteInternal(key, false, null));
  }

  /**
   * Deletes {@code key} if it is at {@code expectedRevision}.
   *
   * @param key the key
   * @param expectedRevision the revision the key must be at
   * @return the tombstone's revision
   * @throws ServerException 409 when the key is at another revision
   */
  public long delete(String key, long expectedRevision) {
    return Futures.await(deleteInternal(key, false, expectedRevision));
  }

  /**
   * Deletes {@code key}.
   *
   * @param key the key
   * @param expectedRevision the revision the key must be at, or {@code null}
   * @return the tombstone's revision
   */
  public CompletableFuture<Long> deleteAsync(String key, Long expectedRevision) {
    return host.publicFuture(deleteInternal(key, false, expectedRevision));
  }

  /**
   * Deletes {@code key} and hides its older values.
   *
   * @param key the key
   * @return the tombstone's revision
   */
  public long purge(String key) {
    return Futures.await(deleteInternal(key, true, null));
  }

  /**
   * Purges {@code key} if it is at {@code expectedRevision}.
   *
   * @param key the key
   * @param expectedRevision the revision the key must be at
   * @return the tombstone's revision
   */
  public long purge(String key, long expectedRevision) {
    return Futures.await(deleteInternal(key, true, expectedRevision));
  }

  /**
   * Deletes {@code key} and hides its older values.
   *
   * @param key the key
   * @param expectedRevision the revision the key must be at, or {@code null}
   * @return the tombstone's revision
   */
  public CompletableFuture<Long> purgeAsync(String key, Long expectedRevision) {
    return host.publicFuture(deleteInternal(key, true, expectedRevision));
  }

  private CompletableFuture<Long> deleteInternal(String key, boolean purge, Long expectedRevision) {
    return write(new Request.KvDelete(bucket, key, purge, expectedRevision));
  }

  private CompletableFuture<Long> write(Request req) {
    return call(req, Response.PublishOk.class).thenApply(Response.PublishOk::offset);
  }

  /**
   * Every key that has a value, sorted.
   *
   * @return the keys
   */
  public List<String> keys() {
    return keys("");
  }

  /**
   * Keys that have a value and match {@code filter} (NATS-style; "" = all), sorted.
   *
   * @param filter the subject filter
   * @return the keys
   */
  public List<String> keys(String filter) {
    return Futures.await(keysInternal(filter));
  }

  /**
   * Keys that have a value and match {@code filter}, sorted.
   *
   * @param filter the subject filter ("" = all)
   * @return the keys
   */
  public CompletableFuture<List<String>> keysAsync(String filter) {
    return host.publicFuture(keysInternal(filter));
  }

  private CompletableFuture<List<String>> keysInternal(String filter) {
    return call(new Request.KvKeys(bucket, filter), Response.Json.class).thenApply(j -> {
      Object v;
      try {
        v = Json.parse(new String(j.json(), StandardCharsets.UTF_8));
      } catch (ProtocolException e) {
        throw new ProtocolException("bad JSON in reply to KvKeys: " + e.getMessage());
      }
      List<String> out = new ArrayList<>();
      for (Object o : JsonMaps.asList(v)) {
        out.add(String.valueOf(o));
      }
      return out;
    });
  }

  /**
   * Kept revisions of {@code key}, oldest first (deletes included).
   *
   * @param key the key
   * @return the revisions
   */
  public List<KvEntry> history(String key) {
    return Futures.await(historyInternal(key));
  }

  /**
   * Kept revisions of {@code key}, oldest first (deletes included).
   *
   * @param key the key
   * @return the revisions
   */
  public CompletableFuture<List<KvEntry>> historyAsync(String key) {
    return host.publicFuture(historyInternal(key));
  }

  private CompletableFuture<List<KvEntry>> historyInternal(String key) {
    return call(new Request.KvHistory(bucket, key), Response.Messages.class).thenApply(m -> {
      List<KvEntry> out = new ArrayList<>(m.records().size());
      for (WireRecord r : m.records()) {
        out.add(new KvEntry(r));
      }
      return out;
    });
  }

  /**
   * Watches every key: first the current value of each (deleted keys left out),
   * then every change as it happens, deletes included.
   *
   * @return the watch
   */
  public KvWatch watch() {
    return watch("");
  }

  /**
   * Watches keys matching {@code filter} ("" = all).
   *
   * @param filter the subject filter
   * @return the watch
   */
  public KvWatch watch(String filter) {
    return new KvWatch(host, stream(), filter);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
