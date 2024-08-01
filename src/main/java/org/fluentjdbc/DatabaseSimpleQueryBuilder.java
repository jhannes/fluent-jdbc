package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.sql.Connection;

/**
 * Interface for consistent <code>UPDATE</code> and <code>DELETE</code> operations in a fluent way
 */
public interface DatabaseSimpleQueryBuilder<T extends DatabaseSimpleQueryBuilder<T>> extends DatabaseQueryBuilder<T> {

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @CheckReturnValue
    DatabaseUpdateBuilder update();

    /**
     * Creates a {@link DatabaseInsertOrUpdateBuilder} object to fluently generate a statement that will result
     * in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending on whether the row exists already
     */
    @CheckReturnValue
    DatabaseInsertOrUpdateBuilder insertOrUpdate();

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code> based on the query parameters
     */
    int delete(Connection connection);

}
