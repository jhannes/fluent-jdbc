package org.fluentjdbc;

import javax.annotation.Nonnull;
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
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> object);

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    int getCount();

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query and returns the result as a list
     */
    default List<String> listStrings(String fieldName) {
        return list(row -> row.getString(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query and returns the result as a list
     */
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
    <OBJECT> Optional<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper);

    /**
     * Returns a string from the specified column name
     *
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
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
    default Optional<Number> singleLong(String fieldName) {
        return singleObject(row -> row.getLong(fieldName));
    }

    /**
     * Executes <code>SELECT fieldName FROM ...</code> on the query.
     * If there is no rows, returns {@link Optional#empty()}, if there is one result, returns it, if there
     * are more than one result, throws {@link IllegalStateException}
     */
    @Nonnull
    default Optional<Instant> singleInstant(String fieldName) {
        return singleObject(row -> row.getInstant(fieldName));
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    void forEach(DatabaseResult.RowConsumer row);

}
