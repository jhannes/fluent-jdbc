package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Generate <code>INSERT</code> insert statements by collecting field names and parameters. Support
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
public class DatabaseInsertBuilder extends DatabaseStatement implements DatabaseUpdatable<DatabaseInsertBuilder> {

    private final List<String> fieldNames = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();
    private final String table;

    public DatabaseInsertBuilder(String table) {
        this.table = table;
    }

    List<Object> getParameters() {
        return parameters;
    }

    /**
     * Adds fieldName to the <code>INSERT (fieldName) VALUES (?)</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseInsertBuilder setField(String fieldName, @Nullable Object parameter) {
        this.fieldNames.add(fieldName);
        this.parameters.add(parameter);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseInsertBuilder setFields(Collection<String> fieldNames, Collection<?> parameters) {
        this.fieldNames.addAll(fieldNames);
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Executes the insert statement and returns the number of rows inserted. Calls
     * {@link #createInsertStatement()} to generate SQL and {@link #executeUpdate(String, List, Connection)}
     * to bind parameters and execute statement
     */
    public int execute(Connection connection) {
        return executeUpdate(createInsertStatement(), parameters, connection);
    }

    /**
     * Creates String for
     * <code>INSERT INTO tableName (fieldName, fieldName, ...) VALUES (?, ?, ...)</code>
     */
    String createInsertStatement() {
        return createInsertSql(table, fieldNames);
    }

    /**
     * Adds primary key to the <code>INSERT</code> statement if idValue is not null. If idValue is null
     * this will {@link java.sql.PreparedStatement#execute(String, String[])} to generate the primary
     * key using the underlying table autogeneration mechanism
     */
    // TODO: This doesn't work for Android when idValue is null
    public <T> DatabaseInsertWithPkBuilder<T> setPrimaryKey(String idField, @Nullable T idValue) {
        if (idValue != null) {
            setField(idField, idValue);
        }
        return new DatabaseInsertWithPkBuilder<>(this, idField, idValue);
    }

}
