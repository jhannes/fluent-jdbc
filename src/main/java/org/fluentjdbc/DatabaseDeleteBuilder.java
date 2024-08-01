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
public class DatabaseDeleteBuilder {

    private final DatabaseTable table;

    private DatabaseWhereBuilder whereClause = new DatabaseWhereBuilder();

    public DatabaseDeleteBuilder(DatabaseTable table) {
        this.table = table;
    }

    public int execute(Connection connection) {
        return table.newStatement("DELETE", "delete from " + table.getTableName() + whereClause.whereClause(), whereClause.getParameters())
                .executeUpdate(connection);
    }

    @CheckReturnValue
    public DatabaseDeleteBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
