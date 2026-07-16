package org.fluentjdbc;

import javax.annotation.CheckReturnValue;


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
public interface DbContextListableSelect<T extends DbContextListableSelect<T>> extends DatabaseQueryable<T>, DbContextSelectResult {

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
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    void forEach(DatabaseResult.RowConsumer consumer);

}
