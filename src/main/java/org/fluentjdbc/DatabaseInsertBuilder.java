package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private final LinkedHashMap<String, DatabaseQueryParameter> queryParameters = new LinkedHashMap<>();
    private final DatabaseTable table;

    public DatabaseInsertBuilder(DatabaseTable table) {
        this.table = table;
    }

    @CheckReturnValue
    Collection<Object> getParameters() {
        ArrayList<Object> parameters = new ArrayList<>();
        this.queryParameters.values().forEach(q -> parameters.addAll(q.getParameters()));
        return parameters;
    }

    /**
     * Adds a parameter to the <code>INSERT (fieldName) VALUES (expression)</code> and to the list of parameters
     */
    @Override
    public DatabaseInsertBuilder addParameter(DatabaseQueryParameter parameter) {
        this.queryParameters.put(parameter.getColumnName(), parameter);
        return this;
    }

    /**
     * Adds parameters to the <code>INSERT (fieldName) VALUES (expression)</code> and to the list of parameters
     */
    @Override
    public DatabaseInsertBuilder addParameters(List<DatabaseQueryParameter> queryParameters) {
        //noinspection ResultOfMethodCallIgnored
        queryParameters.forEach(this::addParameter);
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
        return "insert into " + table.getTableName() + " (" +
                queryParameters.values().stream().map(DatabaseQueryParameter::getColumnName).collect(Collectors.joining(", ")) +
                ") values (" +
                queryParameters.values().stream().map(DatabaseQueryParameter::getUpdateExpression).collect(Collectors.joining(", ")) +
                ")";
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
