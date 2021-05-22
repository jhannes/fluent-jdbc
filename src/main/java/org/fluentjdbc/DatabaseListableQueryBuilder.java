package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Interface to execute terminal operations for <code>SELECT</code>-statements. Usage:
 *
 * <pre>
 * table.where...().list(connection, row -&gt; row.getUUID("column"));
 * table.where...().single(connection, row -&gt; row.getLocalDate("column"));
 * </pre>
 */
public interface DatabaseListableQueryBuilder<T extends DatabaseListableQueryBuilder<T>> {

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @CheckReturnValue
    T orderBy(String orderByClause);

    /**
     * Adds <code>FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code> statement.
     * FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @CheckReturnValue
    default T limit(int rowCount) {
        return skipAndLimit(0, rowCount);
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @CheckReturnValue
    T skipAndLimit(int offset, int rowCount);

    /**
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param connection Database connection
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    <OBJECT> Optional<OBJECT> singleObject(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    <OBJECT> Stream<OBJECT> stream(@Nonnull Connection connection, DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    default <OBJECT> List<OBJECT> list(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return stream(connection, mapper).collect(Collectors.toList());
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link org.fluentjdbc.DatabaseResult.RowConsumer} for each returned row
     */
    void forEach(Connection connection, DatabaseResult.RowConsumer consumer);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @CheckReturnValue
    int getCount(Connection connection);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @CheckReturnValue
    default List<Long> listLongs(Connection connection, final String fieldName) {
        return list(connection, row -> row.getLong(fieldName));
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @CheckReturnValue
    default List<String> listStrings(Connection connection, final String fieldName) {
        return list(connection, row -> row.getString(fieldName));
    }

}
