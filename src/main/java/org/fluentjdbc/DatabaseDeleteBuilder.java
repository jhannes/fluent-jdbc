package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.sql.Connection;

/**
 * Generate <code>DELETE</code> statements by collecting field names and parameters. Example:
 *
 * <pre>
 * int count = table
 *      .where("id", id)
 *      .delete()
 *      .execute(connection);
 * </pre>
 */
class DatabaseDeleteBuilder {

    private final String tableName;
    private final DatabaseTableOperationReporter reporter;

    private DatabaseWhereBuilder whereClause = new DatabaseWhereBuilder();

    public DatabaseDeleteBuilder(String tableName, DatabaseTableOperationReporter reporter) {
        this.tableName = tableName;
        this.reporter = reporter;
    }

    public int execute(Connection connection) {
        return new DatabaseStatement("delete from " + tableName + whereClause.whereClause(), whereClause.getParameters(), reporter)
                .executeUpdate(connection);
    }

    @CheckReturnValue
    public DatabaseDeleteBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
