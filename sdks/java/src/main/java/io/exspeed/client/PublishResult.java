package io.exspeed.client;

/**
 * The outcome of publishing one record.
 *
 * @param offset the record's offset in the stream
 * @param duplicate true when the record's msg id matched an earlier publish; nothing was written
 */
public record PublishResult(long offset, boolean duplicate) {}
