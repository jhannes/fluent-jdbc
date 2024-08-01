package org.fluentjdbc;

import java.sql.PreparedStatement;
import java.util.Collection;

/**
 * Controls the creation of {@link DatabaseStatement} objects, which allows for fine custom interception
 * that affects all database operations.
 *
 * <p><strong>Currently not used for the bulk operations {@link DatabaseBulkDeleteBuilder},
 * {@link DatabaseBulkUpdatable}, {@link DatabaseBulkInsertBuilder} and {@link DbContext} variations of these</strong></p>
 */
public class DatabaseStatementFactory {

    private final DatabaseReporter reporter;

    public DatabaseStatementFactory(DatabaseReporter reporter) {
        this.reporter = reporter;
    }

    /**
     * Creates a new {@link DatabaseStatement}, giving the context of tableName and operation for logging
     * and reporting purposes. The sql parameter is passed to {@link java.sql.Connection#prepareStatement(String)}
     * and the parameters are bound with {@link DatabaseStatement#bindParameter(PreparedStatement, int, Object)}
     */
    public DatabaseStatement newStatement(String tableName, String operation, String sql, Collection<?> parameters) {
        return new DatabaseStatement(sql, parameters, reporter.table(tableName).operation(operation));
    }
}
