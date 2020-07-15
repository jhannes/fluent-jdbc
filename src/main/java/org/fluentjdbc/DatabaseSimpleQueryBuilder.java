package org.fluentjdbc;

import java.sql.Connection;

/**
 * Interface for consistent <code>UPDATE</code> and <code>DELETE</code> operations in a fluent way
 */
public interface DatabaseSimpleQueryBuilder extends DatabaseQueryBuilder<DatabaseSimpleQueryBuilder> {

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    DatabaseUpdateBuilder update();

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code> based on the query parameters
     */
    int delete(Connection connection);

}
