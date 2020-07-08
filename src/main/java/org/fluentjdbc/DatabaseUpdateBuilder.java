package org.fluentjdbc;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Generate <code>UPDATE</code> insert statements by collecting field names and parameters. Support
 * autogeneration of primary keys. Example:
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
public class DatabaseUpdateBuilder extends DatabaseStatement implements DatabaseUpdatable<DatabaseUpdateBuilder> {

    private final String tableName;
    private final List<String> whereConditions = new ArrayList<>();
    private final List<Object> whereParameters = new ArrayList<>();
    private final List<String> updateFields = new ArrayList<>();
    private final List<Object> updateValues = new ArrayList<>();

    public DatabaseUpdateBuilder(String tableName) {
        this.tableName = tableName;
    }

    DatabaseUpdateBuilder setWhereFields(List<String> whereConditions, List<Object> whereParameters) {
        this.whereConditions.addAll(whereConditions);
        this.whereParameters.addAll(whereParameters);
        return this;
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
     * Will {@link #createUpdateStatement(String, List, List)}, set parameters and execute to database
     */
    public int execute(Connection connection) {
        if (updateFields.isEmpty()) {
            return 0;
        }
        List<Object> parameters = new ArrayList<>();
        parameters.addAll(updateValues);
        parameters.addAll(whereParameters);
        return executeUpdate(createUpdateStatement(tableName, updateFields, whereConditions), parameters, connection);
    }

}
