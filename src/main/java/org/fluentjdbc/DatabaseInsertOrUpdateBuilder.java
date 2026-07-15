package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Generate a statement that will result in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending
 * on whether the row exists already by collecting field names and parameters. Example:
 *
 * <pre>
 * int count = table
 *      .where("id", id)
 *      .insertOrUpdate()
 *      .setField("name", "Something")
 *      .setField("code", 102)
 *      .execute(connection);
 * </pre>
 */
public class DatabaseInsertOrUpdateBuilder implements DatabaseUpdatable<DatabaseInsertOrUpdateBuilder> {

    protected DatabaseUpdateBuilder updateBuilder;
    protected DatabaseInsertBuilder insertBuilder;

    public DatabaseInsertOrUpdateBuilder(DatabaseTable table) {
        this(new DatabaseUpdateBuilder(table), new DatabaseInsertBuilder(table));
    }

    public DatabaseInsertOrUpdateBuilder(DatabaseUpdateBuilder updateBuilder, DatabaseInsertBuilder insertBuilder) {
        this.updateBuilder = updateBuilder;
        this.insertBuilder = insertBuilder;
    }

    /**
     * Adds parameters to the <code>INSERT</code> and <code>UPDATE</code> statements and list of parameters
     */
    public DatabaseInsertOrUpdateBuilder addParameters(List<DatabaseQueryParameter> queryParameters) {
        updateBuilder = updateBuilder.addParameters(queryParameters);
        insertBuilder = insertBuilder.addParameters(queryParameters);
        return this;
    }

    /**
     * Adds a parameter to the <code>INSERT</code> and <code>UPDATE</code> statements and list of parameters
     */
    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder addParameter(DatabaseQueryParameter parameter) {
        updateBuilder = updateBuilder.addParameter(parameter);
        insertBuilder = insertBuilder.addParameter(parameter);
        return this;
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list for insert only (not update)
     */
    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder setInsertField(String field, Object value) {
        insertBuilder = insertBuilder.addParameter(new DatabaseQueryParameter(field + " = ?", Collections.singleton(value), field, "?"));
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(List<String> fields, List<?> values) {
        updateBuilder = updateBuilder.setFields(fields, values);
        insertBuilder = insertBuilder.setFields(fields, values);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(Map<String, ?> fields) {
        updateBuilder = updateBuilder.setFields(fields);
        insertBuilder = insertBuilder.setFields(fields);
        return this;
    }

    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        updateBuilder = updateBuilder.where(whereClause);
        //noinspection ResultOfMethodCallIgnored
        insertBuilder.addParameters(whereClause.getQueryParameters());
        return this;
    }

    /**
     * Will generate <code>UPDATE</code> statements, set parameters and execute to database
     * @return returns the number of rows updated or -1 if a row was inserted
     */
    public int execute(Connection connection) {
        int rowCount = updateBuilder.execute(connection);
        if (rowCount != 0) {
            return rowCount;
        }
        insertBuilder.execute(connection);
        return -1;
    }

}
