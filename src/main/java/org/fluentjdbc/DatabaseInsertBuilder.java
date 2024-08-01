package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
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
 * Generate <code>INSERT</code> statements by collecting field names and parameters. Support
 * autogeneration of primary keys. Example:
 *
 * <pre>
 * Long id = table.insert()
 *    .setPrimaryKey("id", (Long)null)
 *    .setField("name", "Something")
 *    .setField("code", 102)
 *    .execute(connection);
 * </pre>
 */
@ParametersAreNonnullByDefault
public class DatabaseInsertBuilder implements DatabaseUpdatable<DatabaseInsertBuilder> {

    private final Map<String, String> expressions = new LinkedHashMap<>();
    private final Map<String, Collection<?>> parameters = new LinkedHashMap<>();
    private final DatabaseTable table;

    public DatabaseInsertBuilder(DatabaseTable table) {
        this.table = table;
    }

    @CheckReturnValue
    Collection<Object> getParameters() {
        ArrayList<Object> parameters = new ArrayList<>();
        this.parameters.values().forEach(parameters::addAll);
        return parameters;
    }

    /**
     * Adds fieldName to the <code>INSERT (fieldName) VALUES (?)</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseInsertBuilder setField(String field, @Nullable Object value) {
        return setField(field, "?", Collections.singletonList(value));
    }

    /**
     * Adds fieldName to the <code>INSERT (fieldName) VALUES (expression)</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseInsertBuilder setField(String field, String expression, Collection<?> values) {
        this.parameters.put(field, values);
        this.expressions.put(field, expression);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseInsertBuilder setFields(Map<String, ?> fields) {
        //noinspection ResultOfMethodCallIgnored
        fields.forEach(this::setField);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseInsertBuilder setFields(List<String> fieldNames, List<?> values) {
        for (int i = 0; i < fieldNames.size(); i++) {
            //noinspection ResultOfMethodCallIgnored
            setField(fieldNames.get(i), values.get(i));
        }
        return this;
    }

    /**
     * Executes the insert statement and returns the number of rows inserted. Calls
     * {@link #createInsertStatement()} to generate SQL and
     * {@link DatabaseStatement#executeUpdate(Connection)}
     * to bind parameters and execute statement
     */
    public int execute(Connection connection) {
        return table
                .newStatement("INSERT", createInsertStatement(), getParameters())
                .executeUpdate(connection);
    }

    /**
     * Creates String for
     * <code>INSERT INTO tableName (fieldName, fieldName, ...) VALUES (?, ?, ...)</code>
     */
    @CheckReturnValue
    String createInsertStatement() {
        return "insert into " + table.getTableName() + " (" + String.join(",", expressions.keySet()) + ") values (" + String.join(", ", expressions.values()) + ")";
    }

    /**
     * Adds primary key to the <code>INSERT</code> statement if idValue is not null. If idValue is null
     * this will {@link java.sql.PreparedStatement#execute(String, String[])} to generate the primary
     * key using the underlying table autogeneration mechanism
     *
     * <p><strong>Bug: This doesn't work for Android when idValue is null</strong></p>
     */
    @CheckReturnValue
    public <T> DatabaseInsertWithPkBuilder<T> setPrimaryKey(String idField, @Nullable T idValue) {
        if (idValue != null) {
            //noinspection ResultOfMethodCallIgnored
            setField(idField, idValue);
        }
        return new DatabaseInsertWithPkBuilder<>(this, idField, idValue);
    }

    public DatabaseTable getTable() {
        return table;
    }
}
