package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private final Map<String, String> updateExpressions = new LinkedHashMap<>();
    private final Map<String, Collection<?>> updateFields = new LinkedHashMap<>();
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
            //noinspection ResultOfMethodCallIgnored
            setField(fields.get(i), values.get(i));
        }
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseUpdateBuilder setFields(Map<String, ?> fields) {
        //noinspection ResultOfMethodCallIgnored
        fields.forEach(this::setField);
        return this;
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = ?</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseUpdateBuilder setField(String field, @Nullable Object value) {
        return setField(field, "?", Collections.singleton(value));
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = expression</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseUpdateBuilder setField(String field, String expression, Collection<?> values) {
        this.updateFields.put(field, values);
        this.updateExpressions.put(field, field + " = " + expression);
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
        updateFields.values().forEach(parameters::addAll);
        parameters.addAll(whereClause.getParameters());
        return table.newStatement("UPDATE", createUpdateStatement(), parameters).executeUpdate(connection);
    }

    private String createUpdateStatement() {
        return "update " + table.getTableName() + " set " + String.join(", ", updateExpressions.values()) + whereClause.whereClause();
    }

    @CheckReturnValue
    public DatabaseUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
