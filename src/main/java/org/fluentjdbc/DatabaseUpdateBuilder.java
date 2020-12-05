package org.fluentjdbc;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generate <code>UPDATE</code> statements by collecting field names and parameters. Example:
 *
 * <pre>
 * int count = table
 *      .where("id", id)
 *      .update()
 *      .setField("name", "Something")
 *      .setField("code", 102)
 *      .execute(connection);
 * </pre>
 */
@ParametersAreNonnullByDefault
public class DatabaseUpdateBuilder implements DatabaseUpdatable<DatabaseUpdateBuilder> {

    private final String tableName;
    private final List<String> updateFields = new ArrayList<>();
    private final List<Object> updateValues = new ArrayList<>();
    private final DatabaseTableOperationReporter reporter;
    private DatabaseWhereBuilder whereClause;

    public DatabaseUpdateBuilder(String tableName, DatabaseTableOperationReporter reporter) {
        this.tableName = tableName;
        this.reporter = reporter;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseUpdateBuilder setFields(Collection<String> fields, Collection<?> values) {
        this.updateFields.addAll(fields);
        this.updateValues.addAll(values);
        return this;
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = ?</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseUpdateBuilder setField(String field, @Nullable Object value) {
        this.updateFields.add(field);
        this.updateValues.add(value);
        return this;
    }

    /**
     * Will generate <code>UPDATE</code> statements, set parameters and execute to database
     */
    public int execute(Connection connection) {
        if (updateFields.isEmpty()) {
            return 0;
        }
        List<Object> parameters = new ArrayList<>();
        parameters.addAll(updateValues);
        parameters.addAll(whereClause.getParameters());
        return new DatabaseStatement("update " + tableName
                + " set " + updateFields.stream().map(column -> column + " = ?").collect(Collectors.joining(","))
                + whereClause.whereClause(), parameters, reporter)
            .executeUpdate(connection);
    }

    public DatabaseUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
