package org.fluentjdbc;

import java.sql.Connection;

public interface DatabaseSimpleQueryBuilder extends DatabaseQueryBuilder<DatabaseSimpleQueryBuilder> {

    DatabaseUpdateBuilder update();

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code> based on the query parameters
     */
    int delete(Connection connection);

}
