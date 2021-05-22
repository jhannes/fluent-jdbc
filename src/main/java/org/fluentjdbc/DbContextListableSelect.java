package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.InputStream;
import java.io.Reader;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;


/**
 * Interface to execute terminal operations for <code>SELECT</code>-statements. Usage:
 *
 * <pre>
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     table.where...().list(row -&gt; row.getUUID("column"));
 *     table.where...().single(row -&gt; row.getLocalDate("column"));
 * }
 * </pre>
 */
public interface DbContextListableSelect<T extends DbContextListableSelect<T>> extends DatabaseQueryable<T> {

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @CheckReturnValue
    T orderBy(String orderByClause);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @CheckReturnValue
    int getCount();

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query and returns the result as a list
     */
    @CheckReturnValue
    default List<String> listStrings(String fieldName) {
        return list(row -> row.getString(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query and returns the result as a list
     */
    @CheckReturnValue
    default List<Long> listLongs(String fieldName) {
        return list(row -> row.getLong(fieldName));
    }

    /**
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    <OBJECT> Optional<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Returns a string from the specified column name
     *
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    default Optional<String> singleString(String fieldName) {
        return singleObject(row -> row.getString(fieldName));
    }

    /**
     * Returns a long from the specified column name
     *
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    default Optional<Number> singleLong(String fieldName) {
        return singleObject(row -> row.getLong(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query.
     * If there is no rows, returns {@link Optional#empty()}, if there is one result, returns it, if there
     * are more than one result, throws {@link IllegalStateException}
     */
    @Nonnull
    @CheckReturnValue
    default Optional<Instant> singleInstant(String fieldName) {
        return singleObject(row -> row.getInstant(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query.
     * If there is no rows, returns {@link Optional#empty()}, if there is one result, returns the
     * binary stream of the object. Used with BLOB (Binary Large Objects) and bytea (PostgreSQL) data types
     */
    @Nonnull
    @CheckReturnValue
    default Optional<InputStream> singleInputStream(String fieldName) {
        return singleObject(row -> row.getInputStream(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query.
     * If there is no rows, returns {@link Optional#empty()}, if there is one result, returns the
     * character reader of the object. Used with CLOB (Character Large Objects) and text (PostgreSQL) data types
     */
    @Nonnull
    @CheckReturnValue
    default Optional<Reader> singleReader(String fieldName) {
        return singleObject(row -> row.getReader(fieldName));
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    void forEach(DatabaseResult.RowConsumer consumer);

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
}
