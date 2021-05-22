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
     * Executes <code>DELETE FROM tableName WHERE ....</code> based on the query parameters
     */
    int delete(Connection connection);

}
