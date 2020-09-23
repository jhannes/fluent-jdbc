package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.fluentjdbc.DatabaseStatement.createDeleteStatement;
import static org.fluentjdbc.DatabaseStatement.executeUpdate;

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

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Object> whereParameters = new ArrayList<>();
    private final DatabaseTableOperationReporter reporter;

    public DatabaseDeleteBuilder(String tableName, DatabaseTableOperationReporter reporter) {
        this.tableName = tableName;
        this.reporter = reporter;
    }

    /**
     * Add the expressions to the <code>WHERE .... AND ...</code> clause and the where parameters
     * to the parameterlist for the WHERE-clause
     */
    DatabaseDeleteBuilder setWhereFields(List<String> whereConditions, List<Object> whereParameters) {
        this.whereConditions.addAll(whereConditions);
        this.whereParameters.addAll(whereParameters);
        return this;
    }

    /**
     * Will {@link DatabaseStatement#createUpdateStatement(String, List, List)}, set parameters and execute to database
     */
    public int execute(Connection connection) {
        return executeUpdate(
                createDeleteStatement(tableName, whereConditions),
                whereParameters,
                connection,
                reporter);
    }

}
