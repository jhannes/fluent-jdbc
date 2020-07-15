package org.fluentjdbc;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Interface to execute terminal operations for <code>SELECT</code>-statements. Usage:
 *
 * <pre>
 * table.where...().list(connection, row -&gt; row.getUUID("column"));
 * table.where...().single(connection, row -&gt; row.getLocalDate("column"));
 * </pre>
 */
public interface DatabaseListableQueryBuilder {

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    <T> Stream<T> stream(Connection connection, DatabaseResult.RowMapper<T> mapper);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    <T> List<T> list(Connection connection, DatabaseResult.RowMapper<T> mapper);

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link org.fluentjdbc.DatabaseResult.RowConsumer} for each returned row
     */
    void forEach(Connection connection, DatabaseResult.RowConsumer consumer);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    int getCount(Connection connection);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    default List<Long> listLongs(Connection connection, final String fieldName) {
        return list(connection, row -> row.getLong(fieldName));
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    default List<String> listStrings(Connection connection, final String fieldName) {
        return list(connection, row -> row.getString(fieldName));
    }

}
