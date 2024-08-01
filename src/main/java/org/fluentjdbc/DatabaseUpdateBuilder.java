package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    @Nonnull
    private final DatabaseTable table;
    private final Map<String, Object> updateFields = new LinkedHashMap<>();
    private DatabaseWhereBuilder whereClause;

    public DatabaseUpdateBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseUpdateBuilder setFields(List<String> fields, List<?> values) {
        for (int i = 0; i < fields.size(); i++) {
            updateFields.put(fields.get(i), values.get(i));
        }
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseUpdateBuilder setFields(Map<String, ?> fields) {
        updateFields.putAll(fields);
        return this;
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = ?</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseUpdateBuilder setField(String field, @Nullable Object value) {
        this.updateFields.put(field, value);
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
        parameters.addAll(updateFields.values());
        parameters.addAll(whereClause.getParameters());
        return table.newStatement("UPDATE", createUpdateStatement(), parameters).executeUpdate(connection);
    }

    private String createUpdateStatement() {
        return "update " + table.getTableName()
                + " set " + updateFields.keySet().stream().map(column -> column + " = ?").collect(Collectors.joining(","))
                + whereClause.whereClause();
    }

    @CheckReturnValue
    public DatabaseUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
