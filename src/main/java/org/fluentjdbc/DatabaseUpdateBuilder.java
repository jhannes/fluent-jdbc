package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
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
    private final Map<String, DatabaseQueryParameter> updateParameters = new LinkedHashMap<>();
    private DatabaseWhereBuilder whereClause;

    public DatabaseUpdateBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Adds parameters to the <code>UPDATE SET fieldName = (expression)</code> and to the list of parameters
     */
    public DatabaseUpdateBuilder addParameters(List<DatabaseQueryParameter> queryParameters) {
        //noinspection ResultOfMethodCallIgnored
        queryParameters.forEach(this::addParameter);
        return this;
    }

    /**
     * Adds a parameter to the <code>UPDATE SET fieldName = (expression)</code> and to the list of parameters
     */
    @CheckReturnValue
    public DatabaseUpdateBuilder addParameter(DatabaseQueryParameter parameter) {
        updateParameters.put(parameter.getColumnName(), parameter);
        return this;
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
     * Will generate <code>UPDATE</code> statements, set parameters and execute to database
     */
    public int execute(Connection connection) {
        if (updateParameters.isEmpty()) {
            return 0;
        }
        List<Object> parameters = new ArrayList<>();
        updateParameters.values().forEach(p -> parameters.addAll(p.getParameters()));
        parameters.addAll(whereClause.getParameters());
        return table.newStatement("UPDATE", createUpdateStatement(), parameters).executeUpdate(connection);
    }

    private String createUpdateStatement() {
        return "update " + table.getTableName() + " set " + updateParameters.values().stream().map(p -> p.getColumnName() + " = " + p.getUpdateExpression()).collect(Collectors.joining(", ")) + whereClause.whereClause();
    }

    @CheckReturnValue
    public DatabaseUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }
}
