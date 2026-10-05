package io.exspeed.client;

import java.util.List;

/**
 * The result of a bounded ExQL query.
 *
 * @param columns the column names
 * @param rows the rows; each value as {@link Json#parse(String)} produces it
 * @param rowCount the number of rows
 * @param executionTimeMs server-side execution time
 * @param truncated true when the server's row cap cut the result short
 */
public record QueryResult(
    List<String> columns, List<List<Object>> rows, long rowCount, double executionTimeMs, boolean truncated) {}
