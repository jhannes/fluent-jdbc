package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.time.Instant;
import java.util.Optional;

/**
 * Interface for consistent query operations in a fluent way
 */
public interface DatabaseQueryBuilder<T extends DatabaseQueryBuilder<T>> extends DatabaseQueryable<T> {

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @CheckReturnValue
    T unordered();

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    @CheckReturnValue
    T orderBy(String orderByClause);

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
     * Returns a string from the specified column name
     *
     * @param connection Database connection
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    default Optional<String> singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }

    /**
     * Returns a long from the specified column name
     *
     * @param connection Database connection
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    default Optional<Number> singleLong(Connection connection, final String fieldName) {
        return singleObject(connection, row -> row.getLong(fieldName));
    }

    /**
     * Returns an instant from the specified column name
     *
     * @param connection Database connection
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @CheckReturnValue
    default Optional<Instant> singleInstant(Connection connection, final String fieldName) {
        return singleObject(connection, row -> row.getInstant(fieldName));
    }

}
